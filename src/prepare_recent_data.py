"""Export a bounded production sample in the existing MovieDataset format."""
import argparse
import gzip
import hashlib
import json
import math
import os
import sqlite3
import tempfile
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from dataset import MovieDataset
from recommender import EaseRecommender
from check_data_quality import check_recent_quality


def utc(value):
    if not isinstance(value, str):
        raise ValueError("Missing timestamp")
    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("Data-window timestamps must include a timezone")
    return stamp.astimezone(timezone.utc)


def sha256(path):
    with Path(path).open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write_jsonl(path, rows):
    with gzip.open(path, "wt", encoding="utf-8") as output:
        for row in rows:
            output.write(json.dumps(row, ensure_ascii=False, allow_nan=False) + "\n")


def prepare_recent_data(storage_path, catalog_directory, model_path, output, *,
                        start, end, max_events=200000, max_users=5000, max_movies=2000, quality_reference=None):
    if start.utcoffset() is None or end.utcoffset() is None or start >= end:
        raise ValueError("Choose an increasing timezone-aware data window")
    start, end = start.astimezone(timezone.utc), end.astimezone(timezone.utc)
    if any(type(limit) is not int or limit <= 0 for limit in (max_events, max_users, max_movies)):
        raise ValueError("Sample limits must be positive integers")
    output, storage_path = Path(output), Path(storage_path).resolve()
    if output.exists():
        raise ValueError("Dataset output must be a new directory")
    if not storage_path.is_file():
        raise FileNotFoundError("Production event database is missing")
    quality = None
    if quality_reference is not None:
        quality = check_recent_quality(storage_path, catalog_directory, quality_reference, start=start, end=end,
                                       base_url=os.environ.get("METADATA_BASE_URL", "http://128.2.24.239:8080"))
        if quality["status"] != "passed":
            raise ValueError("Data quality rejected this update: " + json.dumps(quality["findings"]))
    # Fix the candidate pool before looking at recent outcomes. Bound EASE's dense arrays.
    reference_hash = sha256(model_path)
    reference = EaseRecommender.load(model_path)
    catalog = pd.read_csv(Path(catalog_directory) / "movies.csv.gz", dtype=str, keep_default_na=False)
    catalog["prior_popularity"] = catalog.movie_id.map(dict(zip(reference.movie_ids, reference.popularity))).fillna(0)
    movies = catalog.sort_values(["prior_popularity", "movie_id"], ascending=[False, True]).head(max_movies).drop(columns="prior_popularity")
    catalog = catalog.drop(columns="prior_popularity")
    allowed_movies = set(movies.movie_id)
    source_events, training_events, usage, users = [], [], [], []
    counts, partitions = Counter(), {}
    source_id, topic = "cmu-movielog", "movielog2"
    connection = sqlite3.connect(storage_path.as_uri() + "?mode=ro", uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    deadline = time.monotonic() + 90
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    try:
        connection.execute("BEGIN")
        numbers = [row[0] for row in connection.execute(
            "SELECT partition FROM live_event_cursors WHERE source_id=? AND topic=? ORDER BY partition",
            (source_id, topic),
        )]
        if not numbers or max_events < len(numbers):
            raise ValueError("No captured Kafka partitions, or event limit too small")
        quota = max_events // len(numbers)
        for number in numbers:
            # The primary key supports this bounded tail read without scanning the live DB.
            rows = connection.execute("""
                SELECT source_id,topic,partition,offset,event_timestamp,ingested_at,
                       user_id,movie_id,event_type,parse_status,parsed_json
                FROM kafka_events WHERE source_id=? AND topic=? AND partition=?
                ORDER BY offset DESC LIMIT ?
            """, (source_id, topic, number, quota + 1)).fetchall()
            partitions[str(number)] = {
                "tail_capped": len(rows) > quota,
                "highest_offset": rows[0]["offset"] if rows else None,
                "lowest_offset": rows[min(len(rows), quota) - 1]["offset"] if rows else None,
            }
            for row in rows[:quota]:
                counts["source_rows_read"] += 1
                try:
                    stamp = utc(row["event_timestamp"])
                    if not start <= stamp < end or utc(row["ingested_at"]) > end:
                        continue
                    parsed = json.loads(row["parsed_json"])
                    record = {key: row[key] for key in row.keys() if key != "parsed_json"}
                    record["parsed"] = parsed
                    source_events.append(record)
                    if row["parse_status"] != "parsed" or row["event_type"] not in ("watch", "rating"):
                        continue
                    if row["movie_id"] not in allowed_movies:
                        counts["outside_candidate_catalog"] += 1
                        continue
                    rating = parsed["fields"].get("rating") if row["event_type"] == "rating" else ""
                    if row["event_type"] == "rating" and (type(rating) is not int or not 1 <= rating <= 10):
                        raise ValueError("Invalid rating")
                    if type(row["user_id"]) is not int or row["user_id"] <= 0:
                        raise ValueError("Invalid user ID")
                    training_events.append((stamp.isoformat(), row["user_id"], row["event_type"], row["movie_id"], rating))
                except (ValueError, TypeError, KeyError, AttributeError):
                    counts["invalid_rows"] += 1
        active = Counter(row[1] for row in training_events)
        selected = {user for user, _ in sorted(active.items(), key=lambda item: (-item[1], item[0]))[:max_users]}
        training_events = [row for row in training_events if row[1] in selected]
        for user_id in sorted(selected):
            snapshot = connection.execute("""
                SELECT record_json FROM metadata_snapshots
                WHERE source_id='cmu-metadata-api' AND entity_type='user' AND entity_id=? AND fetched_at<=?
                ORDER BY fetched_at DESC,snapshot_id DESC LIMIT 1
            """, (str(user_id), end.isoformat())).fetchone()
            fields = json.loads(snapshot[0])["record"] if snapshot else {}
            descriptions = [fields.get(key) for key in ("self_description_likes", "self_description_dislikes")]
            users.append((user_id, *(value if isinstance(value, str) else "" for value in descriptions)))
            counts["users_with_cached_metadata"] += int(snapshot is not None)
        # Export usage rather than full prompts/descriptions. The cap is explicit for analysts.
        attempts = connection.execute("""
            SELECT attempt_id,started_at,finished_at,status,record_json
            FROM llm_attempts ORDER BY rowid DESC LIMIT 5001
        """).fetchall()
        usage_capped = len(attempts) > 5000
        for row in attempts[:5000]:
            if start <= utc(row["started_at"]) < end:
                saved = json.loads(row["record_json"])
                usage.append({**{key: row[key] for key in ("attempt_id", "started_at", "finished_at", "status")},
                              **{key: saved.get(key) for key in ("model", "endpoint", "usage")}})
    finally:
        connection.close()  # Never hold a live read transaction during file export or training.
    if not training_events:
        raise ValueError("The selected window/sample has no usable production interactions")
    events = pd.DataFrame(training_events, columns=["timestamp", "user_id", "event_type", "movie_id", "rating"])
    events = events.sort_values(["timestamp", "user_id", "movie_id", "event_type"], kind="stable")
    user_table = pd.DataFrame(users, columns=["user_id", "self_description_likes", "self_description_dislikes"])
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".recent-data-", dir=output.parent) as temporary:
        stage = Path(temporary)
        for name, table in (("events", events), ("users", user_table), ("movies", movies), ("catalog", catalog)):
            table.to_csv(stage / f"{name}.csv.gz", index=False, compression={"method": "gzip", "mtime": 0})
        dataset = MovieDataset(stage)  # Reuse existing schema/referential/rating checks.
        write_jsonl(stage / "kafka_events.jsonl.gz", source_events)
        write_jsonl(stage / "llm_usage.jsonl.gz", usage)
        if quality is not None:
            (stage / "data-quality.json").write_text(json.dumps(quality, indent=2) + "\n", encoding="utf-8")
        report = {
            "window_start_inclusive": start.isoformat(), "window_end_exclusive": end.isoformat(),
            "actual_training_start": events.timestamp.min(), "actual_training_end": events.timestamp.max(),
            "reference_model_sha256": reference_hash, "source_id": source_id, "topic": topic,
            "partitions": partitions, "limits": {"events": max_events, "users": max_users, "movies": max_movies},
            "counts": {**counts, "training_events": len(events), "training_users": len(users),
                       "training_movies": len(movies), "interactions": len(dataset.get_interactions()),
                       "source_events_exported": len(source_events), "llm_attempts_exported": len(usage)},
            "llm_tail_capped": usage_capped,
            "selection": "Most active users in a bounded per-partition tail; movie pool fixed using prior model popularity and movie ID. Missing cached descriptions are blank.",
            "limits_note": "A bounded retained-event sample, not a complete stream window. Watch rows retain latest positions, not every minute or every viewing start. Usage is a bounded tail of recorded attempts in their current state; missing usage is not zero cost.",
            "files": {path.name: sha256(path) for path in stage.iterdir()},
        }
        (stage / "dataset-manifest.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        os.replace(stage, output)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    parser.add_argument("--catalog-dir", type=Path, default="/app/data")
    parser.add_argument("--model-path", type=Path, default="/app/models/recommender.npz")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--end", help="Exclusive UTC/offset-aware end; defaults to now")
    parser.add_argument("--hours", type=float, default=24)
    parser.add_argument("--max-events", type=int, default=200000)
    parser.add_argument("--max-users", type=int, default=5000)
    parser.add_argument("--max-movies", type=int, default=2000)
    parser.add_argument("--quality-reference", type=Path, default=os.environ.get("DATA_QUALITY_REFERENCE"))
    args = parser.parse_args()
    if not math.isfinite(args.hours) or args.hours <= 0:
        parser.error("Hours must be positive and finite")
    try:
        end = utc(args.end) if args.end else datetime.now(timezone.utc)
        report = prepare_recent_data(args.storage_path, args.catalog_dir, args.model_path, args.output,
                                     start=end - timedelta(hours=args.hours), end=end,
                                     max_events=args.max_events, max_users=args.max_users, max_movies=args.max_movies,
                                     quality_reference=args.quality_reference)
    except (ValueError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"Preparation failed ({type(error).__name__}): {error}\n")
    print(json.dumps({"data_directory": str(args.output), **report}, indent=2))


if __name__ == "__main__":
    main()
