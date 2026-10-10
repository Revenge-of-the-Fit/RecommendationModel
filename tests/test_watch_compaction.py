import io
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event
from manage_storage import main as manage_storage
from storage.database import DATABASE_SCHEMA_VERSION, StorageError
from storage.events import EventStore, KafkaEnvelope
from storage.live import LiveStore, read_live_user
from storage.metadata import MetadataStore
from storage.operations import compact_watch_events
from storage.profiles import ProfileStore, content_version
from storage.requests import RequestStore


class WatchCompactionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def watch(offset, minute=17, timestamp="2026-10-10T12:00:00+00:00", user_id=42, movie_id="movie_a", **changes):
        return KafkaEnvelope(
            source_id=changes.pop("source_id", "cmu-movielog"),
            topic=changes.pop("topic", "movielog2"),
            partition=changes.pop("partition", 0), offset=offset,
            value=f"{timestamp},{user_id},GET /data/m/{movie_id}/{minute}.mpg".encode(),
            key=f"user-{user_id}".encode(), headers=[("trace", b"first"), ("trace", None)],
            ingested_at=changes.pop("ingested_at", "2026-10-10T13:00:00+00:00"), **changes,
        )

    @staticmethod
    def save(store, envelope):
        return store.save_event(envelope, parse_event(envelope.value))

    @staticmethod
    def get(store, envelope):
        return store.get_event(envelope.source_id, envelope.topic, envelope.partition, envelope.offset)

    def test_latest_watch_can_rewind_and_replay_preserves_scoped_raw_record(self):
        first = self.watch(10, minute=90)
        latest = self.watch(11, minute=3, timestamp="2026-10-10T12:05:00+00:00")
        delayed = self.watch(999, minute=120, timestamp="2026-10-10T12:01:00+00:00", partition=9)
        scopes = (
            self.watch(20, user_id=43), self.watch(21, movie_id="movie_b"),
            self.watch(10, source_id="other-cluster"), self.watch(10, topic="other-topic"),
        )
        with EventStore(self.path) as store:
            self.assertTrue(self.save(store, first))
            self.assertTrue(self.save(store, latest))
            retained = self.get(store, latest)
            self.assertIsNone(self.get(store, first))
            self.assertFalse(self.save(store, delayed))
            self.assertFalse(self.save(store, replace(first, ingested_at="2030-01-01T00:00:00+00:00")))
            self.assertFalse(self.save(store, replace(latest, leader_epoch=99, ingested_at="2030-01-01T00:00:00+00:00")))
            with self.assertRaises(StorageError):
                self.save(store, replace(latest, value=latest.value.replace(b"/3.mpg", b"/4.mpg")))
            self.assertEqual(self.get(store, latest), retained)
            self.assertEqual(retained["raw_value"], latest.value)
            self.assertEqual(retained["headers"], latest.headers)
            self.assertEqual(retained["parsed"]["fields"]["minute"], 3)
            for envelope in scopes:
                self.assertTrue(self.save(store, envelope))
                self.assertEqual(self.get(store, envelope)["raw_value"], envelope.value)
            self.assertEqual(store.connection.execute("SELECT count(*) FROM kafka_events").fetchone()[0], 5)
        with EventStore(self.path) as reopened:
            self.assertEqual(self.get(reopened, latest), retained)
        self.assertEqual(read_live_user(self.path, 42)["watched"], {"movie_a", "movie_b"})

    def test_normalized_event_then_broker_then_partition_offset_order_ignores_ingestion(self):
        clock_records = (
            self.watch(1, timestamp="2026-10-10T14:00:00+02:00", broker_timestamp_ms=4102444800000),
            self.watch(2, timestamp="2026-10-10T12:01:00+00:00"),
            self.watch(1, timestamp="2026-10-10T07:01:00-05:00", partition=2),
            self.watch(2, minute=0, timestamp="2026-10-10T12:01:00Z", partition=2),
        )
        broker_first = self.watch(10, timestamp="2026-10-10T12:00:00", movie_id="broker_movie", broker_timestamp_ms=2000)
        broker_older = self.watch(11, timestamp="2026-10-10T15:00:00", movie_id="broker_movie", broker_timestamp_ms=1000, ingested_at="2030-01-01T00:00:00+00:00")
        broker_latest = self.watch(12, timestamp="2026-10-10T11:00:00", movie_id="broker_movie", broker_timestamp_ms=3000)
        no_clock = self.watch(7, timestamp="2026-10-10T12:00:00", movie_id="no_clock", partition=4)
        no_clock_older = self.watch(999, timestamp="2026-10-10T15:00:00", movie_id="no_clock", partition=3, ingested_at="2030-01-01T00:00:00+00:00")
        no_clock_latest = self.watch(8, minute=0, timestamp="2026-10-10T11:00:00", movie_id="no_clock", partition=4)
        with EventStore(self.path) as store:
            for envelope in clock_records:
                self.assertTrue(self.save(store, envelope))
            self.assertTrue(self.save(store, broker_first))
            self.assertFalse(self.save(store, broker_older))
            self.assertTrue(self.save(store, broker_latest))
            self.assertTrue(self.save(store, no_clock))
            self.assertFalse(self.save(store, no_clock_older))
            self.assertTrue(self.save(store, no_clock_latest))
            self.assertFalse(self.save(store, replace(no_clock, ingested_at="2031-01-01T00:00:00+00:00")))
            self.assertEqual([row["raw_value"] for row in store.list_events(42)], [
                broker_latest.value, no_clock_latest.value, clock_records[-1].value,
            ])
            self.assertIsNone(self.get(store, no_clock_latest)["event_timestamp"])
            self.assertEqual(self.get(store, no_clock_latest)["parsed"]["timestamp_status"], "timezone_missing")

    def test_nonwatch_and_failed_watch_records_remain_byte_exact(self):
        first, latest = self.watch(1), self.watch(2, timestamp="2026-10-10T12:01:00+00:00")
        retained = [self.watch(10, minute=-1), self.watch(11, minute=-2)]
        bodies = ("GET /rate/movie_a=8", "GET /rate/movie_a=4", "future format,extra", "GET /create_account", "recommendation request server, status 200, result: movie_a, 12 ms")
        retained.extend(replace(self.watch(20 + index), value=f"2026-10-10T12:00:00+00:00,42,{body}".encode()) for index, body in enumerate(bodies))
        retained.append(replace(self.watch(30), value=None))
        with EventStore(self.path) as store:
            self.save(store, first)
            self.save(store, latest)
            for envelope in retained:
                self.assertTrue(self.save(store, envelope))
            expected = [self.get(store, envelope) for envelope in retained]
            self.assertEqual(expected[0]["parse_status"], "failed")
            self.assertEqual(expected[0]["movie_id"], "movie_a")
        self.assertEqual(compact_watch_events(self.path), {
            "removed_watch_events": 0, "retained_watch_events": 1, "reclaimed_space": False,
        })
        with EventStore(self.path) as reopened:
            self.assertEqual([self.get(reopened, envelope) for envelope in retained], expected)
            self.assertEqual(len(reopened.list_events(42)), 8)
            self.assertEqual(reopened.connection.execute("SELECT count(*) FROM kafka_events").fetchone()[0], 9)

    def test_missing_clocks_cannot_displace_or_block_dated_watch_progress(self):
        undated = self.watch(1, timestamp="2026-10-10T12:00:00")
        dated = self.watch(2, minute=3, timestamp="2026-10-10T12:01:00Z")
        later_undated = self.watch(999, minute=90, timestamp="2026-10-10T15:00:00", partition=9)
        later_dated = self.watch(3, minute=4, timestamp="2026-10-10T12:02:00Z")
        with EventStore(self.path) as store:
            self.assertTrue(self.save(store, undated))
            self.assertTrue(self.save(store, dated))
            self.assertFalse(self.save(store, later_undated))
            self.assertTrue(self.save(store, later_dated))
            self.assertEqual(store.list_events(42)[0]["raw_value"], later_dated.value)

    def test_failed_projection_rolls_back_watch_replacement(self):
        first = self.watch(1)
        replacement = self.watch(2, minute=18, timestamp="2026-10-10T12:01:00Z")
        with EventStore(self.path) as store:
            self.save(store, first)
            original = self.get(store, first)
            with patch("storage.events.project_event", side_effect=sqlite3.OperationalError("write failed")):
                with self.assertRaises(sqlite3.OperationalError):
                    self.save(store, replacement)
            self.assertEqual(store.list_events(42), [original])
            self.assertIsNone(self.get(store, replacement))

    def test_parallel_writers_leave_one_latest_watch_and_valid_live_history(self):
        with EventStore(self.path):
            pass
        barrier = threading.Barrier(8)
        envelopes = [self.watch(index, minute=0 if index == 7 else 90, timestamp=f"2026-10-10T12:00:{index:02d}+00:00", partition=index % 3) for index in range(8)]

        def write(envelope):
            with EventStore(self.path, busy_timeout=10) as store:
                barrier.wait(timeout=10)
                return self.save(store, envelope)

        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(write, envelopes))
        self.assertTrue(any(results))
        with EventStore(self.path) as store:
            records = store.list_events(42)
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0]["raw_value"], envelopes[-1].value)
            self.assertEqual(records[0]["parsed"]["fields"]["minute"], 0)
            self.assertFalse(self.save(store, replace(envelopes[0], ingested_at="2030-01-01T00:00:00+00:00")))
            self.assertEqual(store.connection.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
        self.assertEqual(read_live_user(self.path, 42), {
            "watched": {"movie_a"}, "ratings": {}, "seen": {"movie_a"}, "prepared": None,
        })

    def test_legacy_cleanup_migrates_preserves_other_state_and_reclaims_idempotently(self):
        latest = self.watch(10, minute=3, timestamp="2026-10-10T12:05:00+00:00", partition=1)
        rating = replace(self.watch(30), value=b"2026-10-10T12:02:00+00:00,42,GET /rate/movie_a=8")
        with EventStore(self.path) as store:
            self.save(store, latest)
            self.save(store, rating)
            retained = self.get(store, latest)
        self.seed_profile_and_request()
        with LiveStore(self.path) as live:
            live.complete(42, {"profile": {"_provenance": {"profile_id": "profile-1"}}}, "version", 86400, now=1791648000)
        history = read_live_user(self.path, 42)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            template = dict(connection.execute("SELECT * FROM kafka_events WHERE partition=1 AND offset=10").fetchone())
            legacy = (
                self.watch(1, minute=90, timestamp="2026-10-10T14:00:00+02:00", partition=1),
                self.watch(999, minute=120, timestamp="2026-10-10T12:01:00Z", partition=9),
                self.watch(7, timestamp="2026-10-10T12:00:00", movie_id="no_clock", partition=3, ingested_at="2030-01-01T00:00:00+00:00"),
                self.watch(1, minute=0, timestamp="2026-10-10T11:00:00", movie_id="no_clock", partition=4),
            )
            for envelope in legacy:
                parsed = parse_event(envelope.value)
                row = {**template, "partition": envelope.partition, "offset": envelope.offset,
                       "raw_value": envelope.value, "ingested_at": envelope.ingested_at,
                       "event_timestamp": parsed["event_timestamp"], "movie_id": parsed["movie_id"],
                       "parsed_json": json.dumps(parsed)}
                columns = ",".join(row)
                connection.execute(f"INSERT INTO kafka_events ({columns}) VALUES ({','.join('?' for _ in row)})", tuple(row.values()))
            connection.execute("DROP INDEX watch_by_user_movie")
            connection.execute("PRAGMA user_version=5")
            connection.commit()
        other_state = self.other_rows()
        self.assertEqual(compact_watch_events(self.path), {
            "removed_watch_events": 3, "retained_watch_events": 2, "reclaimed_space": False,
        })
        self.assertEqual(self.other_rows(), other_state)
        self.assertEqual(read_live_user(self.path, 42), history)
        with EventStore(self.path) as reopened:
            self.assertEqual(self.get(reopened, latest), retained)
            self.assertEqual(self.get(reopened, legacy[-1])["raw_value"], legacy[-1].value)
            self.assertIsNotNone(self.get(reopened, rating))
            self.assertFalse(self.save(reopened, replace(legacy[0], ingested_at="2031-01-01T00:00:00+00:00")))
            self.assertEqual(reopened.connection.execute("PRAGMA user_version").fetchone()[0], DATABASE_SCHEMA_VERSION)
            index = next(row for row in reopened.connection.execute("PRAGMA index_list(kafka_events)") if row[1] == "watch_by_user_movie")
            self.assertEqual((index[2], index[4]), (0, 1))
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(manage_storage(["compact-watches", "--storage-path", str(self.path), "--reclaim-space"]), 0)
        self.assertEqual(json.loads(output.getvalue()), {
            "removed_watch_events": 0, "retained_watch_events": 2, "reclaimed_space": True,
        })
        self.assertEqual(self.other_rows(), other_state)
        self.assertEqual(read_live_user(self.path, 42), history)
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA integrity_check").fetchall(), [("ok",)])
            self.assertEqual(connection.execute("PRAGMA foreign_key_check").fetchall(), [])

    def seed_profile_and_request(self):
        started, finished = "2026-10-10T12:00:00+00:00", "2026-10-10T12:00:01+00:00"
        with MetadataStore(self.path) as metadata:
            metadata.save_fetch({
                "fetch_id": "fetch-1", "source_id": "course-api", "entity_type": "user",
                "requested_ids": [42], "started_at": started, "finished_at": finished,
                "status": "success", "http_status": 200,
            }, [{"snapshot_id": "snapshot-1", "source_id": "course-api", "entity_type": "user", "entity_id": 42,
                 "fetched_at": finished, "record": {"user_id": 42, "self_description_likes": "Drama"}}])
        profile = {"liked_genres": ["Drama"]}
        with ProfileStore(self.path) as profiles:
            attempt = profiles.start_attempt({
                "attempt_id": "attempt-1", "cache_key": "profile-key", "started_at": finished,
                "user_id": 42, "source_snapshot_id": "snapshot-1", "request": {"likes": "Drama"},
            })
            profiles.update_attempt({**attempt, "status": "success", "finished_at": "2026-10-10T12:00:02+00:00"}, {
                "profile_id": "profile-1", "cache_key": "profile-key", "created_at": "2026-10-10T12:00:02+00:00",
                "attempt_id": "attempt-1", "origin": "llm", "profile": profile, "content_version": content_version(profile),
            })
            profiles.record_use({"use_id": "use-1", "profile_id": "profile-1", "user_id": 42,
                                 "source_snapshot_id": "snapshot-1", "used_at": "2026-10-10T12:00:03+00:00"})
        with RequestStore(self.path) as requests:
            requests.save_request({"request_id": "request-1", "user_id": 42, "started_at": "2026-10-10T12:00:03+00:00",
                                   "profile_reference": {"profile_id": "profile-1", "attempt_id": "attempt-1"}, "response_body": "movie_a"})

    def other_rows(self):
        with closing(sqlite3.connect(self.path)) as connection:
            tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'") if row[0] != "kafka_events"]
            result = {table: sorted(connection.execute(f'SELECT * FROM "{table}"').fetchall(), key=repr) for table in tables}
            result["nonwatch"] = connection.execute("SELECT * FROM kafka_events WHERE event_type!='watch' OR parse_status!='parsed' ORDER BY source_id,topic,partition,offset").fetchall()
            return result


if __name__ == "__main__":
    unittest.main()
