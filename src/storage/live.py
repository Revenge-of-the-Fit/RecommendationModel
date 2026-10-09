import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from storage.database import open_database


def queue_user(connection, user_id, priority=1):
    if user_id is None:
        return
    # Repeated activity raises priority without bypassing retry or refresh deadlines.
    connection.execute("""
        INSERT INTO live_users(user_id, priority) VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET priority=min(priority, excluded.priority)
    """, (user_id, priority))


def project_event(connection, source_id, topic, partition, offset, parsed, broker_timestamp_ms, ingested_at):
    if parsed.get("parse_status") != "parsed" or parsed.get("user_id") is None:
        return
    user_id = parsed["user_id"]
    kind = parsed.get("event_type")
    queue_user(connection, user_id, 0 if kind == "account_created" else 2)
    if kind not in ("watch", "rating") or not parsed.get("movie_id"):
        return
    raw_timestamp = parsed.get("event_timestamp")
    if raw_timestamp:
        stamp = datetime.fromisoformat(raw_timestamp).astimezone(timezone.utc)
    elif broker_timestamp_ms is not None:
        stamp = datetime.fromtimestamp(broker_timestamp_ms / 1000, timezone.utc)
    else:
        stamp = datetime.fromisoformat(ingested_at).astimezone(timezone.utc)
    order = f"{stamp.isoformat(timespec='microseconds')}|{partition:010d}|{offset:020d}"
    rating = parsed.get("fields", {}).get("rating") if kind == "rating" else None
    connection.execute("""
        INSERT INTO live_interactions VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_id, topic, user_id, movie_id) DO UPDATE SET
            watched=max(watched, excluded.watched),
            rating=CASE WHEN excluded.rating IS NOT NULL AND excluded.rating_order>=rating_order
                        THEN excluded.rating ELSE rating END,
            rating_order=CASE WHEN excluded.rating IS NOT NULL AND excluded.rating_order>=rating_order
                              THEN excluded.rating_order ELSE rating_order END
    """, (source_id, topic, user_id, parsed["movie_id"], int(kind == "watch"), rating, order if rating is not None else ""))
    if kind == "rating":
        connection.execute("""
            UPDATE live_users SET next_attempt_at=0 WHERE user_id=? AND last_error IS NULL
                AND record_json IS NOT NULL AND json_extract(record_json, '$.profile') IS NULL
        """, (user_id,))


def read_live_user(path, user_id, source_id="cmu-movielog", topic="movielog2"):
    connection = sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=0.02)
    try:
        # Read the profile and history from the same snapshot without a writer lock.
        connection.execute("BEGIN")
        rows = connection.execute("""
            SELECT movie_id, watched, rating FROM live_interactions
            WHERE source_id=? AND topic=? AND user_id=?
        """, (source_id, topic, user_id)).fetchall()
        prepared = connection.execute("SELECT record_json FROM live_users WHERE user_id=?", (user_id,)).fetchone()
        return {
            "watched": {movie for movie, watched, _ in rows if watched},
            "ratings": {movie: rating for movie, _, rating in rows if rating is not None},
            "seen": {movie for movie, _, _ in rows},
            "prepared": json.loads(prepared[0]) if prepared and prepared[0] else None,
        }
    finally:
        connection.close()


def merge_history(model, user_id, history):
    positive, seen = set(), set(history["seen"])
    rated_negative = set()
    row = model.user_index.get(user_id)
    if row is not None:
        positive.update(model.movie_ids[model.user_profiles[row]])
        static_seen = set(model.movie_ids[model.seen_movies[row]])
        seen.update(static_seen)
        rated_negative = static_seen - positive
    # A watch cannot undo a low rating; an explicit live rating can replace it.
    positive.update(history["watched"] - history["ratings"].keys() - rated_negative)
    positive.difference_update(history["ratings"])
    positive.update(movie for movie, rating in history["ratings"].items() if rating >= model.min_rating)
    supported = {movie for movie in positive if movie in model.movie_index
                 and model.popularity[model.movie_index[movie]] > 0}
    return supported, seen


class LiveStore:
    def __init__(self, path):
        self.connection = open_database(path)
        self.connection.row_factory = sqlite3.Row

    def enqueue(self, user_id, priority=1):
        with self.connection:
            queue_user(self.connection, user_id, priority)

    def ingest(self, source_id, topic, limit=1000):
        cursors = self.connection.execute("""
            SELECT partition, last_offset FROM live_event_cursors WHERE source_id=? AND topic=?
        """, (source_id, topic)).fetchall()
        processed = 0
        # A busy partition must not prevent old signup events in another from being read.
        per_partition = max(1, limit // max(1, len(cursors)))
        for cursor in cursors:
            if processed >= limit:
                break
            rows = self.connection.execute("""
                SELECT partition, offset, parsed_json, broker_timestamp_ms, ingested_at
                FROM kafka_events WHERE source_id=? AND topic=? AND partition=? AND offset>?
                ORDER BY offset LIMIT ?
            """, (source_id, topic, cursor["partition"], cursor["last_offset"], min(per_partition, limit - processed))).fetchall()
            with self.connection:
                for row in rows:
                    project_event(self.connection, source_id, topic, row["partition"], row["offset"],
                                  json.loads(row["parsed_json"]), row["broker_timestamp_ms"], row["ingested_at"])
                if rows:
                    self.connection.execute("""
                        UPDATE live_event_cursors SET last_offset=? WHERE source_id=? AND topic=? AND partition=?
                    """, (rows[-1]["offset"], source_id, topic, cursor["partition"]))
            processed += len(rows)
        return processed

    def due_users(self, now=None, limit=200, interpretation_version=None):
        rows = self.connection.execute("""
            SELECT * FROM live_users WHERE next_attempt_at<=?
            ORDER BY priority, next_attempt_at, user_id LIMIT ?
        """, (time.time() if now is None else now, limit))
        jobs = [dict(row) for row in rows]
        if interpretation_version is not None:
            with self.connection:
                self.connection.executemany("""
                    UPDATE live_users SET interpretation_version=? WHERE user_id=? AND interpretation_version=''
                """, [(interpretation_version, job["user_id"]) for job in jobs])
        return jobs

    def refresh_version(self, version):
        with self.connection:
            self.connection.execute("""
                UPDATE live_users SET next_attempt_at=0, attempts=0, interpretation_version=?
                WHERE interpretation_version<>? AND interpretation_version<>''
            """, (version, version))
            self.connection.execute("UPDATE live_users SET interpretation_version=? WHERE interpretation_version=''", (version,))

    def complete(self, user_id, record, interpretation_version, refresh_seconds, now=None):
        from storage.metadata import prepare_metadata_record
        payload = json.dumps(prepare_metadata_record(record), ensure_ascii=False, allow_nan=False)
        with self.connection:
            self.connection.execute("""
                UPDATE live_users SET record_json=?, interpretation_version=?, next_attempt_at=?,
                    attempts=0, last_error=NULL, priority=2 WHERE user_id=?
            """, (payload, interpretation_version, (time.time() if now is None else now) + refresh_seconds, user_id))

    def fail(self, user_id, error_type, transient, refresh_seconds, now=None):
        row = self.connection.execute("SELECT attempts FROM live_users WHERE user_id=?", (user_id,)).fetchone()
        attempts = row[0] + 1
        delay = min(300, 2 ** min(attempts, 9)) if transient else refresh_seconds
        with self.connection:
            self.connection.execute("""
                UPDATE live_users SET attempts=?, last_error=?, next_attempt_at=? WHERE user_id=?
            """, (attempts, error_type, (time.time() if now is None else now) + delay, user_id))

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
