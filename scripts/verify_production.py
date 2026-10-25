"""Count course-confirmed requests from the existing SQLite event store.

This only reads stored data. It does not produce Kafka messages or change offsets.
"""

import argparse
import csv
import gzip
import hashlib
import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from storage.payloads import load_json


def utc(value):
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("Timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def hostname(value):
    return (urlsplit(value if "://" in value else "//" + value).hostname or "").lower()


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    position = (len(values) - 1) * fraction
    low = int(position)
    high = min(low + 1, len(values) - 1)
    return round(values[low] + (values[high] - values[low]) * (position - low), 3)


def ranking_key(ids):
    return hashlib.sha256(json.dumps(ids, separators=(",", ":")).encode()).digest()


def summarize(connection, start, end, servers, catalog, match_seconds=5):
    if not 0 < (end - start).total_seconds() <= 86400:
        raise ValueError("Choose a positive window no longer than 24 hours")
    connection.row_factory = sqlite3.Row
    connection.execute("BEGIN")
    api = defaultdict(list)
    padding = timedelta(seconds=match_seconds)
    for row in connection.execute("""
        SELECT request_id, user_id, started_at, record_json FROM recommendation_requests
        WHERE julianday(started_at) >= julianday(?) AND julianday(started_at) < julianday(?)
    """, ((start - padding).isoformat(), (end + padding).isoformat())):
        record = load_json(row["record_json"])
        if record.get("status") != 200 or not record.get("response_complete"):
            continue
        ids = tuple(item["movie_id"] for item in record.get("recommendations", []))
        compact = {name: record.get(name) for name in ("serving_method", "versions", "latency_ms")}
        compact["versions"] = {name: record.get("versions", {}).get(name)
                               for name in ("code", "model", "dataset")}
        api[(row["user_id"], ranking_key(ids))].append(
            (row["request_id"], utc(row["started_at"]), compact))

    totals = Counter({name: 0 for name in (
        "all_course_requests", "successful_course_requests", "invalid_status_200_results",
        "uniquely_matched_successes", "successful_requests_without_unique_api_match",
        "confirmed_personalized_successes",
    )})
    courses = []
    course_latencies = []
    offsets = {}
    candidates_by_request = Counter()
    first_stamp = last_stamp = None
    for row in connection.execute("""
        SELECT source_id, topic, partition, offset, user_id, event_timestamp,
               parsed_json, raw_value FROM kafka_events
        WHERE source_id='cmu-movielog' AND topic='movielog2' AND event_type='recommendation'
          AND julianday(event_timestamp) >= julianday(?) AND julianday(event_timestamp) < julianday(?)
    """, (start.isoformat(), end.isoformat())):
        parsed = json.loads(row["parsed_json"])
        fields = parsed.get("fields", {})
        server = fields.get("server")
        status = fields.get("status")
        if server is None:
            raw = (row["raw_value"] or b"").decode("utf-8", errors="replace")
            match = re.search(r"recommendation request ([^,]+),", raw)
            server = match[1] if match else ""
            status_match = re.search(r",\s*status (\d+),", raw)
            if status_match:
                status = int(status_match[1])
        if hostname(server.strip()) not in servers:
            continue
        totals["all_course_requests"] += 1
        partition = str(row["partition"])
        bounds = offsets.setdefault(partition, {"first": row["offset"], "last": row["offset"]})
        bounds["first"] = min(bounds["first"], row["offset"])
        bounds["last"] = max(bounds["last"], row["offset"])
        totals["status_" + str(status if status is not None else "unparsed")] += 1
        stamp = utc(row["event_timestamp"])
        first_stamp = min(first_stamp, stamp) if first_stamp else stamp
        last_stamp = max(last_stamp, stamp) if last_stamp else stamp
        ids = tuple(item["movie_id"] for item in fields.get("recommendations", []))
        valid = (parsed.get("parse_status") == "parsed" and 1 <= len(ids) <= 20
                 and len(ids) == len(set(ids)) and set(ids).issubset(catalog))
        if status != 200 or not valid:
            if status == 200:
                totals["invalid_status_200_results"] += 1
            continue
        totals["successful_course_requests"] += 1
        duration = fields.get("response_time_ms")
        if isinstance(duration, (int, float)):
            course_latencies.append(duration)
        key = ranking_key(ids)
        matches = [item for item in api.get((row["user_id"], key), ())
                   if abs((item[1] - stamp).total_seconds()) <= match_seconds]
        if len(matches) == 1:
            candidates_by_request[matches[0][0]] += 1
        courses.append((row["user_id"], key, matches))

    methods, versions = Counter(), Counter()
    personalized_rankings = set()
    personalized_users = set()
    latencies = []
    for user_id, ids, matches in courses:
        if len(matches) != 1 or candidates_by_request[matches[0][0]] != 1:
            totals["successful_requests_without_unique_api_match"] += 1
            continue
        record = matches[0][2]
        totals["uniquely_matched_successes"] += 1
        method = record.get("serving_method", "unknown")
        methods[method] += 1
        versions[json.dumps(record.get("versions", {}), sort_keys=True)] += 1
        duration = record.get("latency_ms")
        if isinstance(duration, (int, float)):
            latencies.append(duration)
        if method in ("ease", "llm_cold_start"):
            totals["confirmed_personalized_successes"] += 1
            personalized_rankings.add(ids)
            personalized_users.add(user_id)

    denominator = totals["all_course_requests"]
    personalized = totals["confirmed_personalized_successes"]
    return {
        "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        "window_start_inclusive_utc": start.isoformat(),
        "window_end_exclusive_utc": end.isoformat(),
        "server_hosts": sorted(servers),
        "counts": dict(totals),
        "course_event_offsets_by_partition": offsets,
        "course_success_latency_ms": {
            "samples": len(course_latencies), "p50": percentile(course_latencies, .5),
            "p95": percentile(course_latencies, .95),
            "max": max(course_latencies) if course_latencies else None,
            "over_600_ms": sum(value > 600 for value in course_latencies),
        },
        "personalized_percent_of_all_requests": round(100 * personalized / denominator, 3) if denominator else None,
        "personalized_users": len(personalized_users),
        "distinct_personalized_rankings": len(personalized_rankings),
        "matched_serving_methods": dict(methods),
        "matched_api_latency_ms": {"p50": percentile(latencies, .5), "p95": percentile(latencies, .95), "max": max(latencies) if latencies else None},
        "matched_versions": [{"versions": json.loads(value), "requests": count} for value, count in versions.items()],
        "first_course_event_in_window_utc": first_stamp.isoformat() if first_stamp else None,
        "last_course_event_in_window_utc": last_stamp.isoformat() if last_stamp else None,
        "observed_thresholds": {
            "at_least_2000_successes": totals["successful_course_requests"] >= 2000,
            "at_least_10_percent_personalized": bool(denominator and personalized / denominator >= .1
                and len(personalized_users) >= 2 and len(personalized_rankings) >= 2),
        },
        "method": "Kafka identities (source/topic/partition/offset) are unique in storage. Match course successes to API records by user, SHA-256 of ordered IDs, and timestamps within 5 seconds; require a one-to-one match. Count only EASE or cached cold-start matches as personalized. Failures stay in the denominator. Local probes cannot add course successes.",
        "limits": "Counts cover captured records only. Check Kafka capture/lag and rerun with the actual submission time before claiming a grading window. API timings exclude the network round trip.",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-path", type=Path, required=True)
    parser.add_argument("--catalog", type=Path, required=True)
    parser.add_argument("--server", action="append", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    with gzip.open(args.catalog, "rt", newline="") as source:
        catalog = {row["movie_id"] for row in csv.DictReader(source)}
    connection = sqlite3.connect(args.storage_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    try:
        result = summarize(connection, utc(args.start), utc(args.end), {hostname(value) for value in args.server}, catalog)
    finally:
        connection.close()
    rendered = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        with args.output.open("x") as target:
            target.write(rendered)
    else:
        print(rendered, end="")


if __name__ == "__main__":
    main()
