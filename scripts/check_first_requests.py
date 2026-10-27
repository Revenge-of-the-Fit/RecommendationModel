"""Measure real HTTP cold-start responses using pending production user IDs."""

import argparse
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from models.settings import ServingSettings
from recommender import EaseRecommender
from storage.live import merge_history, read_live_user
from storage.payloads import load_json


def logged_record(connection, request_id):
    for _ in range(20):
        row = connection.execute("SELECT record_json FROM recommendation_requests WHERE request_id=?", (request_id,)).fetchone()
        if row:
            return load_json(row[0])
        time.sleep(0.05)
    return {}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8082")
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--user-ids", type=int, nargs="+")
    args = parser.parse_args()
    if not 1 <= args.count <= 100:
        parser.error("count must be between 1 and 100")
    settings = ServingSettings.from_environment()
    model = EaseRecommender.load(settings.model_path)
    connection = sqlite3.connect(settings.storage_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
    try:
        candidates = args.user_ids or [row[0] for row in connection.execute(
            "SELECT user_id FROM live_users WHERE record_json IS NULL AND last_error IS NULL ORDER BY user_id DESC LIMIT 1000"
        ).fetchall()]
        selected = []
        for user in candidates:
            history = read_live_user(settings.storage_path, user, settings.live_source_id, settings.live_topic)
            supported, _ = merge_history(model, user, history)
            if not supported and history["prepared"] is None:
                selected.append(user)
            if len(selected) >= args.count:
                break
        if not selected:
            print("No unprepared users without supported history are available yet.")
            return 2
        results = []
        with httpx.Client(timeout=2, trust_env=False) as client:
            for user in selected:
                for phase in ("first", "later"):
                    if phase == "later":
                        time.sleep(1)
                    started = time.perf_counter()
                    try:
                        response = client.get(args.base_url.rstrip("/") + f"/recommend/{user}")
                        elapsed = (time.perf_counter() - started) * 1000
                        record = logged_record(connection, response.headers.get("x-request-id", ""))
                        ids = response.text.split(",")
                        results.append({
                            "user_id": user, "phase": phase, "status": response.status_code,
                            "http_ms": elapsed, "valid": response.status_code == 200 and 1 <= len(ids) <= 20 and len(ids) == len(set(ids)) and all(movie in model.movie_index for movie in ids),
                            "method": record.get("serving_method"), "fallback_reason": record.get("fallback_reason"),
                            "preparation_attempted": record.get("preparation_attempted"),
                            "preparation_wait_ms": record.get("preparation_wait_ms"),
                            "versions": record.get("versions"),
                        })
                    except httpx.HTTPError as error:
                        results.append({"user_id": user, "phase": phase, "error_type": type(error).__name__, "valid": False})
        first = [row for row in results if row["phase"] == "first"]
        elapsed = [row["http_ms"] for row in first if "http_ms" in row]
        report = {
            "measured_at": datetime.now(timezone.utc).isoformat(),
            "code_revision": os.environ.get("APP_REVISION", "unknown"),
            "first_requests": len(first), "first_personalized": sum(row.get("method") == "llm_cold_start" for row in first),
            "first_fallbacks": sum(row.get("method") == "popularity" for row in first),
            "first_latency_ms": {f"p{p}": float(np.percentile(elapsed, p)) for p in (50, 95, 99)} if elapsed else {},
            "valid_responses": sum(row["valid"] for row in results), "responses": results,
            "note": "Localhost HTTP timings omit course-to-VM network latency. First-request users were unprepared at selection; description availability is not guaranteed.",
        }
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({key: value for key, value in report.items() if key != "responses"}, indent=2))
        return 0 if all(row["valid"] and row.get("http_ms", 2000) < 600 for row in results) else 1
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
