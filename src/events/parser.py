import ast
import math
import re
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


PARSER_VERSION = 1
WATCH = re.compile(r"GET /data/m/(?P<movie_id>[^/\s?#]+)/(?P<minute>[+-]?[0-9]+)\.mpg")
RATING = re.compile(r"GET /rate/(?P<movie_id>[^/\s?#=]+)=(?P<rating>[+-]?[0-9]+)")
RECOMMENDATION = re.compile(
    r"recommendation request (?P<server>[^,]+),\s*status (?P<status>[0-9]{3}),\s*result:\s*(?P<tail>.*)"
)
RESPONSE_TIME = re.compile(
    r"(?P<value>[0-9]+(?:\.[0-9]+)?)\s*(?P<unit>ms|milliseconds?|s|seconds?|us|µs|microseconds?)",
    re.IGNORECASE,
)
MOVIE_ID = re.compile(r"[^,\s\[\]]+")


def resolve_timezone(value: str):
    if value.upper() in ("UTC", "Z"):
        return timezone.utc
    offset = re.fullmatch(r"([+-])([0-9]{2}):?([0-9]{2})", value)
    if offset:
        hours = int(offset[2])
        minutes = int(offset[3])
        if hours > 23 or minutes > 59:
            raise ValueError("Invalid timezone offset")
        delta = timedelta(hours=hours, minutes=minutes)
        return timezone(delta if offset[1] == "+" else -delta)
    return ZoneInfo(value)


def _timestamp(value: str, event_timezone: str | None):
    if not value:
        return None, "missing", "MissingTimestamp"
    try:
        timestamp = datetime.fromisoformat(value)
    except ValueError:
        return None, "invalid", "InvalidTimestamp"
    if timestamp.utcoffset() is None:
        if not event_timezone:
            return None, "timezone_missing", None
        try:
            zone = resolve_timezone(event_timezone)
        except (ValueError, ZoneInfoNotFoundError):
            return None, "invalid", "InvalidTimezone"
        timestamp = timestamp.replace(tzinfo=zone)
        if isinstance(zone, ZoneInfo):
            try:
                restored = timestamp.astimezone(timezone.utc).astimezone(zone)
            except (OverflowError, ValueError):
                return None, "invalid", "InvalidTimestamp"
            if restored.replace(tzinfo=None) != timestamp.replace(tzinfo=None):
                return None, "invalid", "InvalidTimestamp"
            if timestamp.replace(fold=0).utcoffset() != timestamp.replace(fold=1).utcoffset():
                return None, "invalid", "AmbiguousTimestamp"
    try:
        return timestamp.astimezone(timezone.utc).isoformat(), "utc", None
    except (OverflowError, ValueError):
        return None, "invalid", "InvalidTimestamp"


def _body(body: str):
    match = WATCH.fullmatch(body)
    if match:
        fields = {"minute": None}
        try:
            minute = int(match["minute"])
        except ValueError:
            return "watch", match["movie_id"], fields, "InvalidWatchMinute"
        fields["minute"] = minute
        return "watch", match["movie_id"], fields, None if minute >= 0 else "InvalidWatchMinute"
    if body.startswith("GET /data/m/"):
        return "watch", None, {"body": body}, "InvalidWatchEvent"
    match = RATING.fullmatch(body)
    if match:
        fields = {"rating": None}
        try:
            rating = int(match["rating"])
        except ValueError:
            return "rating", match["movie_id"], fields, "InvalidRating"
        fields["rating"] = rating
        return "rating", match["movie_id"], fields, None if 1 <= rating <= 10 else "InvalidRating"
    if body.startswith("GET /rate/"):
        return "rating", None, {"body": body}, "InvalidRatingEvent"
    if body == "GET /create_account":
        return "account_created", None, {}, None
    match = RECOMMENDATION.fullmatch(body)
    if match:
        result_text, separator, response_time_raw = match["tail"].rpartition(",")
        status = int(match["status"])
        fields = {
            "server": match["server"].strip(),
            "status": status,
            "result_text": result_text.strip() if separator else match["tail"].strip(),
            "response_time_raw": response_time_raw.strip() if separator else None,
        }
        if not separator or not fields["server"]:
            return "recommendation", None, fields, "InvalidRecommendationEvent"
        if not 100 <= status <= 599:
            return "recommendation", None, fields, "InvalidRecommendationStatus"
        duration = RESPONSE_TIME.fullmatch(fields["response_time_raw"])
        if duration:
            unit = duration["unit"].lower()
            multiplier = 1000 if unit in ("s", "second", "seconds") else (
                0.001 if unit in ("us", "µs", "microsecond", "microseconds") else 1
            )
            duration_ms = float(duration["value"]) * multiplier
            if not math.isfinite(duration_ms):
                return "recommendation", None, fields, "InvalidResponseTime"
            fields["response_time_ms"] = duration_ms
        if status == 200:
            received = fields["result_text"]
            movie_ids = None
            if received.startswith("[") and received.endswith("]"):
                try:
                    literals = ast.literal_eval(received)
                except (SyntaxError, ValueError, RecursionError):
                    pass
                else:
                    if not isinstance(literals, list) or not all(isinstance(item, str) for item in literals):
                        return "recommendation", None, fields, "InvalidRecommendationResult"
                    movie_ids = literals
                received = received[1:-1].strip()
            if movie_ids is None:
                movie_ids = [item.strip() for item in received.split(",")] if received else []
            if any(MOVIE_ID.fullmatch(movie_id) is None for movie_id in movie_ids):
                return "recommendation", None, fields, "InvalidRecommendationResult"
            fields["recommendations"] = [
                {"movie_id": movie_id, "rank": rank}
                for rank, movie_id in enumerate(movie_ids, 1)
            ]
        return "recommendation", None, fields, None
    if body.startswith("recommendation request"):
        return "recommendation", None, {"body": body}, "InvalidRecommendationEvent"
    return None, None, {"body": body}, None


def parse_event(value: bytes | None, event_timezone: str | None = None) -> dict:
    result = {
        "parser_version": PARSER_VERSION,
        "parse_status": "failed",
        "error_type": None,
        "event_type": None,
        "event_timestamp": None,
        "event_timestamp_raw": None,
        "timestamp_status": "missing",
        "user_id": None,
        "movie_id": None,
        "fields": {},
    }
    if value is None:
        result["error_type"] = "Tombstone"
        return result
    try:
        text = value.decode("utf-8")
    except UnicodeDecodeError:
        result["error_type"] = "UnicodeDecodeError"
        return result
    parts = text.split(",", 2)
    if len(parts) != 3:
        result["error_type"] = "InvalidEnvelope"
        result["fields"] = {"body": text}
        return result
    timestamp_raw, user_raw, body = (part.strip() for part in parts)
    result["event_timestamp_raw"] = timestamp_raw or None
    result["event_timestamp"], result["timestamp_status"], timestamp_error = _timestamp(
        timestamp_raw, event_timezone,
    )
    user_error = None
    if user_raw.isascii() and user_raw.isdecimal():
        digits = user_raw.lstrip("0") or "0"
        if len(digits) <= 19:
            user_id = int(digits)
            if 0 < user_id <= 2**63 - 1:
                result["user_id"] = user_id
    if result["user_id"] is None:
        user_error = "InvalidUserID"
    result["event_type"], result["movie_id"], result["fields"], body_error = _body(body)
    if body.startswith("GET "):
        result["fields"]["body"] = body
        try:
            query_string = urlsplit(body[4:]).query
            if query_string:
                query = {}
                for name, item in parse_qsl(query_string, keep_blank_values=True):
                    query.setdefault(name, []).append(item)
                result["fields"]["query"] = query
        except ValueError:
            pass
    if user_error:
        result["fields"]["user_id_raw"] = user_raw
    result["error_type"] = body_error or user_error or timestamp_error
    if result["error_type"] is None:
        result["parse_status"] = "parsed" if result["event_type"] else "unrecognized"
    return result
