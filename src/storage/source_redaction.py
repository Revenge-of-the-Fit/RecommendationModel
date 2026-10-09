import json
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from storage.requests import REDACTED, redact_sensitive_fields


SENSITIVE_VALUE = re.compile(
    rb'''(?ix)
    (\b(?:[\w.-]*(?:authorization|password|passwd|secret|api[_.-]?key|credential(?:s)?|cookie(?:s)?|token|session[_.-]?(?:id|key)|private[_.-]?key)[\w.-]*|auth|session)
    ["']?\s*[:=]\s*)
    ("(?:\\.|[^"\\\r\n])*(?:"|\\?$)|'(?:\\.|[^'\\\r\n])*(?:'|\\?$)|[^,;&\r\n}\]]*)
    '''
)
URL_CREDENTIALS = re.compile(rb"(?i)(https?://)[^\s/]+@")
URL_IN_TEXT = re.compile(rb'''(?:https?://|(?<=GET ))[^\s"'<>,]+''')
AUTH_HEADER_VALUE = re.compile(
    rb'''(?i)(\b(?:authorization|proxy-authorization|cookie|set-cookie)\s*[:=]\s*)[^\r\n]*'''
)


def _reject_constant(_):
    raise ValueError("Invalid JSON number")


def _redact_assignment(match):
    name = match[1].strip().rstrip(b":=").rstrip().rstrip(b"\"'").decode("ascii")
    if redact_sensitive_fields({name: True})[name] == REDACTED:
        return match[1] + b'"[redacted]"'
    return match[0]


def _redact_url(match):
    return _redact_url_bytes(match[0])


def _redact_url_bytes(original, depth=0):
    if depth >= 8:
        return REDACTED.encode("ascii")
    try:
        text = original.decode("utf-8")
        parts = urlsplit(text)
        query = {}
        for name, value in parse_qsl(parts.query, keep_blank_values=True):
            query.setdefault(name, []).append(value)
        safe_query = redact_sensitive_fields(query)
        safe_query = _redact_nested_urls(safe_query, depth)
        netloc = parts.netloc.rsplit("@", 1)[-1]
        fragment = parts.fragment
        if "=" in fragment:
            fragment_values = {}
            for name, value in parse_qsl(fragment, keep_blank_values=True):
                fragment_values.setdefault(name, []).append(value)
            safe_fragment = redact_sensitive_fields(fragment_values)
            safe_fragment = _redact_nested_urls(safe_fragment, depth)
            if safe_fragment != fragment_values:
                fragment = urlencode(safe_fragment, doseq=True)
        if netloc == parts.netloc and safe_query == query and fragment == parts.fragment:
            return original
        return urlunsplit((
            parts.scheme, netloc, parts.path, urlencode(safe_query, doseq=True), fragment,
        )).encode("utf-8")
    except (UnicodeDecodeError, ValueError):
        return REDACTED.encode("ascii")


def _redact_nested_urls(values, depth):
    return {
        name: [
            URL_IN_TEXT.sub(
                lambda match: _redact_url_bytes(match[0], depth + 1),
                item.encode("utf-8"),
            ).decode("utf-8")
            for item in content
        ] if isinstance(content, list) else content
        for name, content in values.items()
    }


def redact_bytes(value: bytes | None) -> tuple[bytes | None, bool]:
    if value is None:
        return None, False
    if not isinstance(value, bytes):
        raise TypeError("Kafka keys and values must be bytes or null")
    original = value
    structured = False
    try:
        parsed = json.loads(value, parse_constant=_reject_constant)
        structured = isinstance(parsed, (dict, list))
        redacted = redact_sensitive_fields(parsed)
        if redacted != parsed:
            value = json.dumps(redacted, allow_nan=False).encode("utf-8")
    except (ValueError, UnicodeDecodeError, RecursionError):
        structured = False
    value = URL_IN_TEXT.sub(_redact_url, value)
    if not structured:
        value = AUTH_HEADER_VALUE.sub(lambda match: match[1] + b"[redacted]", value)
    safe = SENSITIVE_VALUE.sub(_redact_assignment, value)
    safe = URL_CREDENTIALS.sub(rb"\1", safe)
    return safe, safe != original


def redact_headers(headers: list[tuple[str, bytes | None]]) -> tuple[list[tuple[str, bytes | None]], bool]:
    saved = []
    changed = False
    for name, value in headers:
        if redact_sensitive_fields({name: True})[name] == REDACTED:
            safe = REDACTED.encode("ascii") if value is not None else None
        else:
            safe, _ = redact_bytes(value)
        saved.append((name, safe))
        changed = changed or safe != value
    return saved, changed


def redact_source_fields(value):
    if isinstance(value, dict):
        return {key: redact_source_fields(item) for key, item in redact_sensitive_fields(value).items()}
    if isinstance(value, (list, tuple)):
        return [redact_source_fields(item) for item in value]
    if isinstance(value, str):
        safe, _ = redact_bytes(value.encode("utf-8"))
        return safe.decode("utf-8")
    return value
