"""Check bounded production samples against an explicit production reference."""

import argparse
import hashlib
import json
import math
import os
import sqlite3
import tempfile
import time
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from data_quality import SOURCES, compare, profile
from events.parser import parse_event
from services.metadata import MetadataRateLimiter
from services.metadata_http import MetadataHttpFetcher
from storage.payloads import load_json


def in_window(value, start, end):
    stamp = datetime.fromisoformat(value)
    return stamp.utcoffset() is not None and start <= stamp < end


def collect_snapshot(storage_path, catalog_directory, *, start, end, reference=None,
                     limit=5000, base_url="http://128.2.24.239:8080", fetcher=None):
    if start.utcoffset() is None or end.utcoffset() is None or start >= end or type(limit) is not int or limit < 2:
        raise ValueError("Choose a valid timezone-aware window and a sample limit of at least two")
    connection = sqlite3.connect(Path(storage_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    deadline = time.monotonic() + 30
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    events, coverage = [], {}
    try:
        connection.execute("BEGIN")
        partitions = [row[0] for row in connection.execute("""
            SELECT partition FROM live_event_cursors
            WHERE source_id='cmu-movielog' AND topic='movielog2' ORDER BY partition
        """)]
        if len(partitions) > limit:
            raise ValueError("Sample limit is smaller than the number of Kafka partitions")
        quota = max(1, limit // max(1, len(partitions)))
        for partition in partitions:
            rows = connection.execute("""
                SELECT raw_value,ingested_at FROM kafka_events
                WHERE source_id='cmu-movielog' AND topic='movielog2' AND partition=?
                ORDER BY offset DESC LIMIT ?
            """, (partition, quota + 1)).fetchall()
            coverage[f"kafka_partition_{partition}_tail_capped"] = len(rows) > quota
            events.extend(row for row in rows[:quota] if in_window(row["ingested_at"], start, end))
        rows = connection.execute("""
            SELECT started_at,record_json FROM recommendation_requests ORDER BY rowid DESC LIMIT ?
        """, (limit + 1,)).fetchall()
        coverage["recommendation_tail_capped"] = len(rows) > limit
        requests = [row["record_json"] for row in rows[:limit] if in_window(row["started_at"], start, end)]
    finally:
        connection.close()  # Release the live snapshot before decoding or network calls.
    parsed = [parse_event(row["raw_value"], event_timezone="UTC") for row in events]
    decoded = []
    for row in requests:
        try:
            decoded.append(load_json(row))
        except (ValueError, TypeError):
            decoded.append({"_error": "unreadable_request"})
    if reference is None:
        users = list(dict.fromkeys(str(row["user_id"]) for row in parsed if row.get("user_id")))[:100]
        movies = pd.read_csv(Path(catalog_directory) / "movies.csv.gz", usecols=["movie_id"], dtype=str)
        ids = {"user": users, "movie": sorted(movies.movie_id.unique())[:100]}
    else:
        ids = reference["metadata_ids"]
    sources = {"kafka": profile("kafka", parsed), "recommendation_api": profile("recommendation_api", decoded)}
    client = nullcontext(fetcher) if fetcher is not None else MetadataHttpFetcher(base_url)
    limiter = MetadataRateLimiter(1)
    with client as fetch:
        for kind in ("user", "movie"):
            selected = ids[kind]
            if selected:
                limiter.wait()
                batch = fetch(kind, selected)
                records = [batch.records.get(entity_id, {"_error": batch.error_type or "missing_metadata"})
                           for entity_id in selected]
            else:
                records = []
            sources[kind + "_api"] = profile(kind + "_api", records)
    return {"format_version": 1, "sources": sources, "metadata_ids": ids, "coverage": coverage,
            "window": {"start_inclusive": start.isoformat(), "end_exclusive": end.isoformat(),
                       "selection": "Bounded Kafka ingestion-time and request start-time tails"},
            "metadata_observed_at": datetime.now(timezone.utc).isoformat()}


def check_recent_quality(storage_path, catalog_directory, reference_path, *, start, end, **options):
    raw = Path(reference_path).read_bytes()
    reference = json.loads(raw)
    if reference.get("format_version") != 1 or set(reference.get("sources", {})) != set(SOURCES):
        raise ValueError("Unsupported data-quality reference")
    current = collect_snapshot(storage_path, catalog_directory, start=start, end=end, reference=reference, **options)
    return {**compare(reference, current), "reference_sha256": hashlib.sha256(raw).hexdigest(),
            "reference_window": reference["window"]}


def write_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as output:
        temporary = Path(output.name)
        json.dump(value, output, indent=2, allow_nan=False)
        output.write("\n")
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    parser.add_argument("--catalog-dir", type=Path, default="/app/data")
    parser.add_argument("--base-url", default=os.environ.get("METADATA_BASE_URL", "http://128.2.24.239:8080"))
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--limit", type=int, default=5000)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--reference", type=Path)
    mode.add_argument("--save-reference", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if not math.isfinite(args.hours) or args.hours <= 0 or args.limit < 2:
        parser.error("Hours must be positive and finite, and limit must be at least two")
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=args.hours)
    try:
        if args.save_reference:
            if args.save_reference.exists():
                raise ValueError("Reference already exists; review it before replacing the baseline")
            snapshot = collect_snapshot(args.storage_path, args.catalog_dir, start=start, end=end,
                                        limit=args.limit, base_url=args.base_url)
            report = compare(snapshot, snapshot)
            if report["status"] == "passed":
                write_json(args.save_reference, snapshot)
                report["status"] = "reference_saved"
        else:
            report = check_recent_quality(args.storage_path, args.catalog_dir, args.reference,
                                          start=start, end=end, limit=args.limit, base_url=args.base_url)
        if args.output:
            write_json(args.output, report)
    except (ValueError, OSError, sqlite3.Error, KeyError) as error:
        parser.exit(1, f"Data-quality check failed ({type(error).__name__}): {error}\n")
    print(json.dumps(report, indent=2))
    return 2 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
