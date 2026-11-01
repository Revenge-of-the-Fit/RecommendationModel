"""Shared metadata validation and small distribution checks for external data."""

import math
from collections import Counter, defaultdict


SOURCES = ("kafka", "user_api", "movie_api", "recommendation_api")
MIN_SAMPLES = 50
DRIFT_THRESHOLD = 0.30
MAX_INVALID_FRACTION = 0.10


def number(value, field, *, maximum=None):
    """Accept numeric API strings and supported missing values, but never booleans."""
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (ValueError, TypeError, OverflowError):
        raise ValueError(f"invalid_{field}") from None
    if isinstance(value, bool) or not math.isfinite(result) or result < 0 or (maximum is not None and result > maximum):
        raise ValueError(f"invalid_{field}")
    return result


def genres(value):
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split("|") if item.strip()]
    if not isinstance(value, list):
        raise ValueError("invalid_genres")
    names = [item.get("name") if isinstance(item, dict) else item for item in value]
    if any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("invalid_genres")
    return [name.strip() for name in names]


def validate_metadata(entity_type, record):
    """Validate fields we use while allowing incomplete metadata and extra fields.

    HTTP decoding already verifies identities against the requested batch.
    """
    if not isinstance(record, dict):
        raise ValueError("invalid_metadata_object")
    if entity_type == "user":
        text = ("self_description_likes", "self_description_dislikes", "gender", "occupation")
        age = number(record.get("age"), "age", maximum=120)
        if age is not None and not age.is_integer():
            raise ValueError("invalid_age")
    elif entity_type == "movie":
        text = ("title", "overview")
        number(record.get("runtime"), "runtime")
        for field in ("cost", "license_cost"):
            number(record.get(field), field)
        genres(record.get("genres"))
    else:
        raise ValueError("invalid_entity_type")
    for field in text:
        if record.get(field) is not None and not isinstance(record[field], str):
            raise ValueError(f"invalid_{field}")


def bucket(value, boundaries):
    if value is None:
        return "missing"
    return str(sum(value >= boundary for boundary in boundaries))


def profile(source, rows):
    """Keep aggregate counts rather than descriptions or high-cardinality IDs."""
    features, errors = defaultdict(Counter), Counter()
    count = 0
    for row in rows:
        count += 1
        try:
            if row.get("_error"):
                raise ValueError(row["_error"])
            if source == "kafka":
                if row.get("parse_status") != "parsed":
                    raise ValueError(row.get("error_type") or "unsupported_event")
                values = {"event_type": row["event_type"]}
                if row["event_type"] == "rating":
                    values["rating"] = str(row["fields"]["rating"])
            elif source in ("user_api", "movie_api"):
                kind = "user" if source == "user_api" else "movie"
                validate_metadata(kind, row)
                if kind == "user":
                    values = {"age": bucket(number(row.get("age"), "age"), [18, 30, 45, 60]),
                              "description_present": str(bool(row.get("self_description_likes") or row.get("self_description_dislikes")))}
                else:
                    values = {"runtime": bucket(number(row.get("runtime"), "runtime"), [1, 60, 120, 180]),
                              "cost": bucket(number(row.get("cost", row.get("license_cost")), "cost"), [0.01, 0.1, 1, 10]),
                              "genres": "|".join(sorted(set(genres(row.get("genres"))))) or "missing"}
            elif source == "recommendation_api":
                if type(row.get("user_id")) is not int or not 0 < row["user_id"] <= 2**63 - 1:
                    raise ValueError("invalid_request_user")
                if row.get("status") == 200:
                    body = row.get("response_body")
                    ids = body.split(",") if isinstance(body, str) else []
                    if (not 1 <= len(ids) <= 20 or len(set(ids)) != len(ids)
                            or any(not value or any(c.isspace() for c in value) for value in ids)):
                        raise ValueError("invalid_recommendation_response")
                values = {"status": str(row.get("status")), "serving_method": row.get("serving_method") or "none",
                          "latency": bucket(number(row.get("latency_ms"), "latency"), [100, 400, 600])}
            else:
                raise ValueError("unknown_source")
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            errors[str(error) if isinstance(error, ValueError) else "invalid_record"] += 1
            features["schema"]["invalid"] += 1
            continue
        features["schema"]["valid"] += 1
        for name, value in values.items():
            features[name][value] += 1
    return {"sample_size": count, "invalid": sum(errors.values()), "schema_errors": dict(errors),
            "features": {name: dict(histogram) for name, histogram in features.items()}}


def compare(reference, current):
    """Flag large categorical shifts using total variation distance."""
    findings, distances = [], {}
    for source in SOURCES:
        before, after = reference["sources"][source], current["sources"][source]
        if min(before["sample_size"], after["sample_size"]) < MIN_SAMPLES:
            findings.append({"source": source, "reason": "insufficient_samples", "minimum": MIN_SAMPLES,
                             "reference": before["sample_size"], "current": after["sample_size"]})
            continue
        invalid_fraction = after["invalid"] / after["sample_size"]
        if invalid_fraction > MAX_INVALID_FRACTION:
            findings.append({"source": source, "reason": "schema_failure_rate", "fraction": invalid_fraction,
                             "threshold": MAX_INVALID_FRACTION, "errors": after["schema_errors"]})
        distances[source] = {}
        for feature in before["features"].keys() | after["features"].keys():
            left, right = before["features"].get(feature, {}), after["features"].get(feature, {})
            if source == "kafka" and feature == "event_type":
                # Watch rows are buffered and replaced per user/movie. Their retained
                # share is not a stream frequency, especially during consumer replay.
                left = {key: value for key, value in left.items() if key != "watch"}
                right = {key: value for key, value in right.items() if key != "watch"}
                feature = "event_type_non_watch"
            left_total, right_total = sum(left.values()), sum(right.values())
            if min(left_total, right_total) < MIN_SAMPLES:
                distances[source][feature] = None
                continue
            distance = 0.5 * sum(abs(left.get(key, 0) / left_total - right.get(key, 0) / right_total)
                                 for key in left.keys() | right.keys())
            distances[source][feature] = round(distance, 6)
            if distance > DRIFT_THRESHOLD:
                findings.append({"source": source, "feature": feature, "reason": "strong_drift",
                                 "distance": round(distance, 6), "threshold": DRIFT_THRESHOLD})
    return {"status": "failed" if findings else "passed", "findings": findings, "distances": distances,
            "samples": {source: current["sources"][source]["sample_size"] for source in SOURCES},
            "schema_errors": {source: current["sources"][source]["schema_errors"] for source in SOURCES},
            "window": current["window"], "coverage": current.get("coverage", {}),
            "metadata_observed_at": current.get("metadata_observed_at")}
