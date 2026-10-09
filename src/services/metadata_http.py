import base64
import json
import logging
import math
import threading
import time
from contextlib import contextmanager
from urllib.parse import quote, unquote, urlsplit

import httpx

from services.metadata import MetadataBatch
from storage.metadata import normalize_entity_id, prepare_metadata_record
from storage.source_redaction import redact_bytes, redact_headers


METADATA_HTTP_DECODER_VERSION = 1
_DIAGNOSTICS = threading.local()
_DIAGNOSTIC_LOCK = threading.Lock()


class _DiagnosticFilter(logging.Filter):
    def filter(self, record):
        return not getattr(_DIAGNOSTICS, "suppressed", False)


_DIAGNOSTIC_FILTER = _DiagnosticFilter()


@contextmanager
def _quiet_http(*extra_names):
    with _DIAGNOSTIC_LOCK:
        names = {
            "httpx", "httpcore", "httpcore.connection", "httpcore.http11",
            "httpcore.http2", "httpcore.proxy", "httpcore.socks",
            *extra_names,
            *[name for name in list(logging.Logger.manager.loggerDict) if name.startswith(
                ("httpx.", "httpcore.", *(value + "." for value in extra_names))
            )],
        }
        for name in names:
            logger = logging.getLogger(name)
            if _DIAGNOSTIC_FILTER not in logger.filters:
                logger.addFilter(_DIAGNOSTIC_FILTER)
    previous = getattr(_DIAGNOSTICS, "suppressed", False)
    _DIAGNOSTICS.suppressed = True
    try:
        yield
    finally:
        _DIAGNOSTICS.suppressed = previous


def _base_url(value: str) -> str:
    if not isinstance(value, str) or not value or any(ord(character) <= 32 for character in value):
        raise ValueError("A valid metadata API base URL is required")
    try:
        parts = urlsplit(value)
        port = parts.port
        if (
            parts.scheme.lower() not in ("http", "https") or not parts.hostname
            or parts.username is not None or parts.password is not None
            or "?" in value or "#" in value or "\\" in value or parts.netloc.endswith(":")
            or (port is not None and not 1 <= port <= 65535)
        ):
            raise ValueError
        path = parts.path
        for _ in range(8):
            if any(segment in (".", "..") for segment in path.replace("\\", "/").split("/")):
                raise ValueError
            decoded = unquote(path)
            if decoded == path:
                break
            path = decoded
        else:
            raise ValueError
        url = httpx.URL(value)
        if not url.host:
            raise ValueError
    except (ValueError, httpx.InvalidURL):
        raise ValueError("Invalid metadata API base URL") from None
    return str(url).rstrip("/")


def _reject_constant(_):
    raise ValueError("Invalid JSON number")


def _finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Invalid JSON number")
    return number


def _unique_object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Duplicate JSON object key")
        result[name] = value
    return result


def _json_body(value: bytes):
    return json.loads(
        value.decode("utf-8-sig"), parse_constant=_reject_constant,
        parse_float=_finite_float, object_pairs_hook=_unique_object,
    )


def _records(entity_type: str, requested: set[str], body) -> dict:
    identity_field = "user_id" if entity_type == "user" else "id"
    keyed = isinstance(body, dict) and identity_field not in body
    if keyed:
        rows = list(body.items())
    elif isinstance(body, dict):
        rows = [(None, body)]
    elif isinstance(body, list):
        rows = [(None, item) for item in body]
    else:
        raise ValueError("Invalid metadata response shape")
    records = {}
    for key, record in rows:
        if not isinstance(record, dict) or identity_field not in record:
            raise ValueError("Metadata responses require explicit entity identities")
        entity_id = normalize_entity_id(entity_type, record[identity_field])
        if keyed and normalize_entity_id(entity_type, key) != entity_id:
            raise ValueError("Metadata record identity disagrees with its key")
        if entity_id not in requested or entity_id in records:
            raise ValueError("Metadata response identities do not match the requested batch")
        records[entity_id] = prepare_metadata_record(record)
    return records


class MetadataHttpFetcher:
    def __init__(
        self, base_url: str, *, timeout: float = 10.0,
        max_response_bytes: int = 16 * 1024 * 1024, transport=None,
    ):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("Metadata HTTP timeout must be positive and finite")
        if type(max_response_bytes) is not int or max_response_bytes <= 0:
            raise ValueError("Metadata response size limit must be positive")
        self.base_url = _base_url(base_url)
        self.timeout = timeout
        self.max_response_bytes = max_response_bytes
        self.lock = threading.Lock()
        self.client = httpx.Client(
            timeout=timeout, transport=transport, trust_env=False,
            follow_redirects=False, headers={"Accept": "application/json", "Accept-Encoding": "identity"},
        )

    def __call__(self, entity_type: str, ids) -> MetadataBatch:
        if not isinstance(ids, (list, tuple)) or not 1 <= len(ids) <= 200:
            raise ValueError("Metadata HTTP batches must contain between 1 and 200 IDs")
        normalized = [normalize_entity_id(entity_type, value) for value in ids]
        if any("," in value or value in (".", "..") for value in normalized) or len(set(normalized)) != len(normalized):
            raise ValueError("Metadata HTTP IDs must be unique and cannot contain batch delimiters or dot segments")
        endpoint = "user" if entity_type == "user" else "movie"
        url = self.base_url + "/" + endpoint + "/" + ",".join(quote(value, safe="") for value in normalized)
        with self.lock, _quiet_http():
            self.client.cookies.clear()
            try:
                return self._fetch(url, entity_type, set(normalized))
            finally:
                self.client.cookies.clear()

    def _fetch(self, url: str, entity_type: str, requested: set[str]) -> MetadataBatch:
        status = None
        headers = []
        headers_redacted = False
        chunks = bytearray()
        complete = False
        truncated = False
        error_type = None
        close_error_type = None
        response = None
        started = time.monotonic()
        try:
            request = self.client.build_request("GET", url)
            response = self.client.send(request, stream=True)
            status = response.status_code
            raw_headers = [(name.decode("latin-1"), value) for name, value in response.headers.raw]
            safe_headers, headers_redacted = redact_headers(raw_headers)
            headers = [[name, value.decode("latin-1") if value is not None else None] for name, value in safe_headers]
            for chunk in response.iter_bytes():
                remaining = self.max_response_bytes - len(chunks)
                chunks.extend(chunk[:remaining])
                if len(chunk) > remaining:
                    truncated = True
                    error_type = "ResponseTooLarge"
                    break
                if time.monotonic() - started >= self.timeout:
                    truncated = True
                    error_type = "RequestTimeout"
                    break
            else:
                complete = True
            if time.monotonic() - started >= self.timeout and error_type is None:
                error_type = "RequestTimeout"
        except httpx.TimeoutException as error:
            error_type = type(error).__name__
            truncated = True
        except httpx.RequestError:
            error_type = "TransportError"
            truncated = True
        except Exception:
            if response is not None and response.is_closed:
                complete = True
                error_type = "ResponseCloseError"
                close_error_type = "ResponseCloseError"
            else:
                error_type = "TransportError"
                truncated = True
        finally:
            if response is not None:
                try:
                    response.close()
                except Exception:
                    close_error_type = "ResponseCloseError"
                    if error_type is None:
                        error_type = close_error_type
        raw = bytes(chunks)
        safe_raw, body_redacted = redact_bytes(raw)
        body = None
        records = {}
        parsed_body = None
        json_error = False
        if complete:
            try:
                parsed_body = _json_body(raw)
                body = prepare_metadata_record({"body": parsed_body})["body"]
                try:
                    safe_parsed = _json_body(safe_raw)
                except (ValueError, RecursionError):
                    safe_parsed = None
                if safe_parsed != body:
                    safe_raw = json.dumps(body, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")
                    body_redacted = True
            except (ValueError, TypeError, RecursionError):
                json_error = True
        if error_type is None:
            if status is None or not 200 <= status < 300:
                error_type = "HttpError"
            elif json_error:
                error_type = "InvalidJson"
            else:
                try:
                    records = _records(entity_type, requested, parsed_body)
                except (ValueError, TypeError, RecursionError):
                    error_type = "InvalidMetadata"
        safe_url, request_redacted = redact_bytes(url.encode("utf-8"))
        try:
            retained = safe_raw.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            retained = base64.b64encode(safe_raw).decode("ascii")
            encoding = "base64"
        envelope = {
            "decoder_version": METADATA_HTTP_DECODER_VERSION,
            "request": {"method": "GET", "url": safe_url.decode("utf-8")},
            "status": status, "headers": headers, "body": body,
            "raw_body": retained, "raw_body_encoding": encoding,
            "body_redacted": body_redacted, "headers_redacted": headers_redacted,
            "request_redacted": request_redacted, "body_truncated": truncated,
            "body_complete": complete, "close_error_type": close_error_type,
        }
        return MetadataBatch(status, records, envelope, error_type)

    def close(self):
        with self.lock, _quiet_http():
            self.client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
