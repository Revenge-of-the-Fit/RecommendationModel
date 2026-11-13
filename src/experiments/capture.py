"""Save original watch/rating observations before the live watch buffer coalesces them."""
import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from evaluate_online import utc
from routing import read_routing, write_json
from storage.database import CapacityConnection


class OutcomeCapture:
    def __init__(self, directory, routing_path):
        self.directory, self.routing_path = Path(directory), Path(routing_path)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.connection = None
        self.plan = None
        self.last_heartbeat = 0

    def _select(self, plan):
        if self.plan == plan:
            return
        self.close()
        self.plan = plan
        if plan is None:
            return
        directory = self.directory / plan["id"]
        directory.mkdir(parents=True, exist_ok=True)
        saved = directory / "capture-plan.json"
        if saved.exists() and json.loads(saved.read_text()) != plan:
            raise ValueError("An experiment's capture plan cannot change")
        write_json(saved, plan)
        database = directory / "outcomes.sqlite3"
        self.connection = sqlite3.connect(database, timeout=5, factory=CapacityConnection)
        self.connection.storage_path = database
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        page_size = self.connection.execute("PRAGMA page_size").fetchone()[0]
        self.connection.execute(f"PRAGMA max_page_count={8 * 1024**3 // page_size}")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS observations (
                partition INTEGER NOT NULL, offset INTEGER NOT NULL,
                timestamp TEXT NOT NULL, user_id INTEGER NOT NULL,
                movie_id TEXT NOT NULL, kind TEXT NOT NULL, value INTEGER NOT NULL,
                PRIMARY KEY(partition, offset)
            ) WITHOUT ROWID;
            CREATE TABLE IF NOT EXISTS progress (
                partition INTEGER PRIMARY KEY, first_time TEXT, latest_time TEXT,
                last_offset INTEGER NOT NULL, last_ingested_at TEXT NOT NULL,
                invalid_outcomes INTEGER NOT NULL DEFAULT 0
            );
        """)

    def capture(self, records):
        self._select(read_routing(self.routing_path).get("experiment"))
        if self.plan is not None:
            plan = self.plan
            start, end = utc(plan["start"]), utc(plan["observation_end"])
            with self.connection:
                for envelope, parsed in records:
                    if (envelope.source_id, envelope.topic) != (plan["source_id"], plan["topic"]):
                        continue
                    if envelope.partition not in plan["partitions"]:
                        raise ValueError("Undeclared Kafka partition during experiment")
                    try:
                        stamp = utc(parsed.get("event_timestamp"))
                    except (ValueError, TypeError):
                        stamp = None
                    kind = parsed.get("event_type")
                    valid = parsed.get("parse_status") == "parsed" and stamp is not None
                    invalid = int(kind in ("watch", "rating") and not valid)
                    previous = self.connection.execute(
                        "SELECT last_offset FROM progress WHERE partition=?", (envelope.partition,),
                    ).fetchone()
                    # Replayed offsets are harmless; bounds and counters advance only with the stream.
                    if previous is None or envelope.offset > previous[0]:
                        self.connection.execute("""
                            INSERT INTO progress VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(partition) DO UPDATE SET
                              first_time=coalesce(progress.first_time, excluded.first_time),
                              latest_time=coalesce(excluded.latest_time, progress.latest_time),
                              last_offset=excluded.last_offset, last_ingested_at=excluded.last_ingested_at,
                              invalid_outcomes=progress.invalid_outcomes+excluded.invalid_outcomes
                        """, (envelope.partition, stamp.isoformat() if stamp else None,
                              stamp.isoformat() if stamp else None, envelope.offset, envelope.ingested_at, invalid))
                    if not valid or kind not in ("watch", "rating") or not start <= stamp < end:
                        continue
                    value = parsed["fields"]["minute" if kind == "watch" else "rating"]
                    row = (envelope.partition, envelope.offset, stamp.isoformat(), parsed["user_id"],
                           parsed["movie_id"], kind, value)
                    cursor = self.connection.execute("INSERT OR IGNORE INTO observations VALUES (?, ?, ?, ?, ?, ?, ?)", row)
                    if not cursor.rowcount:
                        prior = self.connection.execute("SELECT * FROM observations WHERE partition=? AND offset=?", row[:2]).fetchone()
                        if prior != row:
                            raise ValueError("Conflicting experiment observation at a Kafka offset")
        if time.monotonic() - self.last_heartbeat >= 5:
            write_json(self.directory / "capture-status.json", {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "experiment_id": self.plan["id"] if self.plan else None,
            })
            self.last_heartbeat = time.monotonic()

    def close(self):
        if self.connection is not None:
            self.connection.close()
            self.connection = None
