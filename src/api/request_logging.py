import logging
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from storage.requests import REDACTED, redact_sensitive_fields


LOGGER = logging.getLogger(__name__)
MAX_RESPONSE_BYTES = 65536


def grouped_parameters(pairs) -> dict[str, list[str]]:
    grouped = {}
    for name, value in pairs:
        grouped.setdefault(name, []).append(value)
    return grouped


def safe_url(value: str) -> str:
    try:
        parts = urlsplit(value)
        query = redact_sensitive_fields(grouped_parameters(parse_qsl(parts.query, keep_blank_values=True)))
        fragment = parts.fragment
        if "=" in fragment:
            fragment = urlencode(
                redact_sensitive_fields(grouped_parameters(parse_qsl(fragment, keep_blank_values=True))),
                doseq=True,
            )
        return urlunsplit((
            parts.scheme, parts.netloc.rsplit("@", 1)[-1], parts.path,
            urlencode(query, doseq=True), fragment,
        ))
    except ValueError:
        return REDACTED


def request_metadata(scope: dict) -> dict:
    headers = grouped_parameters(
        (name.decode("latin-1").lower(), value.decode("latin-1"))
        for name, value in scope.get("headers", [])
    )
    for name, values in headers.items():
        headers[name] = [
            safe_url(value) if value.startswith(("http://", "https://")) else value
            for value in values
        ]
    for name in ("referer", "origin", "location", "content-location"):
        if name in headers:
            headers[name] = [safe_url(value) for value in headers[name]]
    return redact_sensitive_fields({
        "headers": headers,
        "client": scope.get("client"),
        "server": scope.get("server"),
        "scheme": scope.get("scheme"),
        "http_version": scope.get("http_version"),
        "root_path": scope.get("root_path", ""),
    })


def requested_user_id(scope: dict) -> int | None:
    value = str(scope.get("path_params", {}).get("userid", ""))
    if not value.isascii() or not value.isdecimal():
        return None
    value = value.lstrip("0") or "0"
    if len(value) > 19:
        return None
    user_id = int(value)
    return user_id if 0 < user_id <= 2**63 - 1 else None


def storage_status(state) -> dict:
    writer = state.request_log
    status = writer.status() if writer is not None else {
        "healthy": False, "accepted": 0, "written": 0, "duplicates": 0,
        "failed": 0, "rejected": 0, "queued": 0, "last_write_at": None,
        "last_error": state.request_log_error or "not_initialized",
    }
    status["capture_failed"] = state.request_log_capture_failed
    status["unavailable"] = state.request_log_unavailable
    status["healthy"] = status["healthy"] and not status["capture_failed"] and not status["unavailable"]
    if state.request_log_error is not None:
        status["last_error"] = state.request_log_error
    return status


class RequestLoggingMiddleware:
    def __init__(self, app, state):
        self.app = app
        self.state = state

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if scope["type"] != "http" or not (path == "/recommend" or path.startswith("/recommend/")):
            await self.app(scope, receive, send)
            return
        state = scope.setdefault("state", {})
        request_id = uuid.uuid4().hex
        state["request_id"] = request_id
        started_at = datetime.now(timezone.utc).isoformat()
        started = time.perf_counter()
        status = None
        body = bytearray()
        body_truncated = False
        response_complete = False
        error_type = None
        finished_at = None
        latency_ms = None

        async def capture_send(message):
            nonlocal status, body_truncated, response_complete, finished_at, latency_ms
            if message["type"] == "http.response.start":
                headers = [
                    (name, value) for name, value in message.get("headers", [])
                    if name.lower() != b"x-request-id"
                ]
                message = {**message, "headers": [*headers, (b"x-request-id", request_id.encode("ascii"))]}
            await send(message)
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body":
                if status == 200:
                    chunk = message.get("body", b"")
                    remaining = MAX_RESPONSE_BYTES - len(body)
                    body.extend(chunk[:remaining])
                    body_truncated = body_truncated or len(chunk) > remaining
                if not message.get("more_body", False):
                    response_complete = True
                    finished_at = datetime.now(timezone.utc).isoformat()
                    latency_ms = (time.perf_counter() - started) * 1000

        try:
            await self.app(scope, receive, capture_send)
        except BaseException as error:
            error_type = type(error).__name__
            raise
        finally:
            try:
                result = state.get("recommendation_result")
                successful = status == 200 and response_complete and result is not None
                recommendations = [
                    {**item.model_dump(mode="json"), "rank": rank}
                    for rank, item in enumerate(result.recommendations, 1)
                ] if successful else []
                versions = {"code": self.state.code_version, **state.get("serving_versions", {})}
                versions.setdefault("model", None)
                versions.setdefault("dataset", None)
                versions["profile"] = result.profile_version if successful else None
                query = grouped_parameters(parse_qsl(
                    scope.get("query_string", b"").decode("utf-8", errors="replace"),
                    keep_blank_values=True,
                ))
                for name, values in query.items():
                    query[name] = [
                        safe_url(value) if value.startswith(("http://", "https://")) else value
                        for value in values
                    ]
                record = {
                    "request_id": request_id,
                    "started_at": started_at,
                    "finished_at": finished_at or datetime.now(timezone.utc).isoformat(),
                    "user_id": state.get("user_id", requested_user_id(scope)),
                    "path": path,
                    "method": scope["method"],
                    "query": query,
                    "request_metadata": request_metadata(scope),
                    "recommendations": recommendations,
                    "response_body": body.decode("utf-8", errors="replace") if successful else None,
                    "response_body_truncated": body_truncated,
                    "response_complete": response_complete,
                    "serving_method": result.method if successful else None,
                    "fallback_reason": result.fallback_reason if successful else None,
                    "cached": result.cached if successful else None,
                    "llm_model": result.llm_model if successful else None,
                    "status": status,
                    "error_type": error_type or state.get("error_type") or (
                        "RequestValidationError" if status == 422 else
                        "HTTPError" if status is not None and status >= 400 else
                        "ResponseIncomplete" if not response_complete else None
                    ),
                    "latency_ms": latency_ms if latency_ms is not None else (time.perf_counter() - started) * 1000,
                    "versions": versions,
                }
                writer = self.state.request_log
                if writer is None:
                    self.state.request_log_unavailable += 1
                    LOGGER.error("Request logging unavailable for request %s", request_id)
                else:
                    writer.submit(record)
            except Exception as error:
                self.state.request_log_capture_failed += 1
                self.state.request_log_error = type(error).__name__
                LOGGER.error("Request capture failed for request %s (%s)", request_id, type(error).__name__)
