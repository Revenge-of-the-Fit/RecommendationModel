"""Save course-observed request outcomes around one model activation."""
import argparse
import gzip
import json
import os
import sqlite3
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

from prepare_recent_data import utc
from routing import write_json


def collect_evidence(deployment_path, storage_path, server, *, minutes=5, max_events=200000,
                     submission=None, clock=None):
    deployment_path = Path(deployment_path)
    deployment = json.loads(deployment_path.read_text())
    if deployment["status"] != "deployed":
        raise ValueError("Only a successful activation can supply update evidence")
    if minutes <= 0 or max_events <= 0 or not server.strip():
        raise ValueError("Choose a server and positive window/sample limits")
    switched = utc(deployment["switched_at"])
    start, end = switched - timedelta(minutes=minutes), switched + timedelta(minutes=minutes)
    collected = clock or datetime.now(timezone.utc)
    if collected < end + timedelta(seconds=60):
        raise ValueError(f"Wait until at least {(end + timedelta(seconds=60)).isoformat()} for Kafka follow-up")
    connection = sqlite3.connect(Path(storage_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    connection.row_factory = sqlite3.Row
    deadline = time.monotonic() + 90
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
    requests, coverage = [], {}
    unassigned = 0
    try:
        connection.execute("BEGIN")
        partitions = connection.execute(
            "SELECT partition FROM live_event_cursors WHERE source_id='cmu-movielog' AND topic='movielog2' ORDER BY partition"
        ).fetchall()
        if not partitions or max_events < len(partitions):
            raise ValueError("No captured Kafka partitions, or sample limit too small")
        quota = max_events // len(partitions)
        for partition, in partitions:
            rows = connection.execute("""
                SELECT offset,event_timestamp,event_type,parse_status,parsed_json FROM kafka_events
                WHERE source_id='cmu-movielog' AND topic='movielog2' AND partition=?
                ORDER BY offset DESC LIMIT ?
            """, (partition, quota)).fetchall()
            stamps = []
            for row in rows:
                try:
                    stamp = utc(row["event_timestamp"])
                except ValueError:
                    continue
                stamps.append(stamp)
                if row["event_type"] != "recommendation" or not start <= stamp < end:
                    continue
                fields = json.loads(row["parsed_json"])["fields"]
                if not fields.get("server"):
                    unassigned += 1
                    continue
                if fields["server"] != server:
                    continue
                requests.append({"partition": partition, "offset": row["offset"],
                                 "event_timestamp": stamp.isoformat(), "server": server,
                                 "status": fields.get("status"), "parse_status": row["parse_status"],
                                 "response_time_ms": fields.get("response_time_ms")})
            coverage[str(partition)] = {
                "rows_read": len(rows), "oldest_event": min(stamps).isoformat() if stamps else None,
                "newest_event": max(stamps).isoformat() if stamps else None,
                "lowest_offset": rows[-1]["offset"] if rows else None,
                "highest_offset": rows[0]["offset"] if rows else None,
                "window_bracketed": bool(stamps and min(stamps) <= start and max(stamps) >= end),
            }
    finally:
        connection.close()

    def summary(rows):
        statuses = Counter(str(row["status"]) for row in rows)
        successes = statuses["200"]
        return {"requests": len(rows), "successful": successes, "failed": len(rows) - successes,
                "availability": successes / len(rows) if rows else None, "statuses": dict(statuses)}

    requests.sort(key=lambda row: (row["event_timestamp"], row["partition"], row["offset"]))
    report = {"deployment": str(deployment_path), "collected_at": collected.isoformat(),
              "server": server, "source_id": "cmu-movielog", "topic": "movielog2",
              "start": start.isoformat(), "switch": switched.isoformat(), "end": end.isoformat(),
              "definition": "Course Kafka status 200 / all recommendation requests for this exact server",
              "coverage": coverage, "unattributable_recommendations": unassigned,
              "window_bracketed": all(value["window_bracketed"] for value in coverage.values()),
              "overall": summary(requests),
              "before": summary([row for row in requests if utc(row["event_timestamp"]) < switched]),
              "after": summary([row for row in requests if utc(row["event_timestamp"]) >= switched])}
    report["usable"] = bool(report["window_bracketed"] and not unassigned and
                            report["before"]["requests"] and report["after"]["requests"])
    if submission is not None:
        report["submission"] = submission.isoformat()
        report["in_final_72_hours"] = submission - timedelta(hours=72) <= switched <= submission
    output = deployment_path.parent
    raw_path = output / "availability-requests.jsonl.gz"
    temporary = raw_path.with_suffix(".tmp")
    with gzip.open(temporary, "wt", encoding="utf-8") as target:
        for row in requests:
            target.write(json.dumps(row) + "\n")
    os.replace(temporary, raw_path)
    write_json(output / "availability.json", report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("deployment", type=Path)
    parser.add_argument("--server", required=True, help="Exact server field from the course Kafka log")
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    parser.add_argument("--minutes", type=int, default=5)
    parser.add_argument("--max-events", type=int, default=200000)
    parser.add_argument("--submission", type=utc)
    args = parser.parse_args()
    try:
        report = collect_evidence(args.deployment, args.storage_path, args.server, minutes=args.minutes,
                                  max_events=args.max_events, submission=args.submission)
    except (ValueError, OSError, KeyError, sqlite3.Error) as error:
        parser.exit(1, f"Evidence export failed ({type(error).__name__}): {error}\n")
    print(json.dumps(report, indent=2))
    if not report["usable"]:
        parser.exit(1, "Evidence is incomplete; inspect coverage, wait for ingestion, or increase --max-events.\n")


if __name__ == "__main__":
    main()
