import copy
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event
from storage.database import StorageError
from storage.events import EventStore, KafkaEnvelope
from storage.metadata import MetadataStore, normalize_entity_id
from storage.requests import RequestLog, RequestStore


def metadata_fetch(fetch_id="fetch-1", **changes):
    return {
        "fetch_id": fetch_id,
        "source_id": "course-api",
        "entity_type": "user",
        "requested_ids": [42],
        "started_at": "2026-10-09T12:00:00Z",
        "finished_at": "2026-10-09T12:00:01Z",
        "status": "success",
        "http_status": 200,
        "error_type": None,
        "response": {
            "headers": {"content-type": "application/json", "x-trace-id": ["trace-1", "trace-2"]},
            "body": {"users": [{"id": 42, "extra": "retained"}], "pagination": {"done": True}},
            "extension": ["unrecognized", 3],
        },
        "collector_extension": {"retained": True},
        **changes,
    }


def metadata_snapshot(snapshot_id="snapshot-1", **changes):
    return {
        "snapshot_id": snapshot_id,
        "source_id": "course-api",
        "entity_type": "user",
        "entity_id": 42,
        "fetched_at": "2026-10-09T12:00:01Z",
        "record": {"id": 42, "likes": "Crime dramas", "dislikes": "", "unknown": {"retained": [1, 2]}},
        "source_updated_at": "2026-10-01T12:00:00Z",
        **changes,
    }


class MetadataStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def test_fetch_envelope_and_snapshot_are_durable_and_independent(self):
        fetch = metadata_fetch()
        snapshot = metadata_snapshot()
        with MetadataStore(self.path) as store:
            self.assertTrue(store.save_fetch(fetch, [snapshot]))
        with MetadataStore(self.path) as reopened:
            saved_fetch = reopened.get_fetch("fetch-1")
            saved_snapshot = reopened.get_snapshot("snapshot-1")
            self.assertEqual(saved_fetch["response"], fetch["response"])
            self.assertEqual(saved_fetch["collector_extension"], fetch["collector_extension"])
            self.assertEqual(saved_fetch["requested_ids"], ["42"])
            self.assertEqual(saved_fetch["finished_at"], "2026-10-09T12:00:01.000000+00:00")
            self.assertEqual(saved_snapshot["record"], snapshot["record"])
            self.assertEqual(saved_snapshot["fetch_id"], "fetch-1")
            self.assertEqual(saved_snapshot["entity_id"], "42")
            self.assertEqual(saved_snapshot["source_updated_at"], snapshot["source_updated_at"])
            self.assertTrue(saved_snapshot["content_version"].startswith("sha256:"))
            self.assertEqual(reopened.latest_snapshot("course-api", "user", "00042"), saved_snapshot)
            self.assertIsNone(reopened.get_fetch("missing"))
            self.assertIsNone(reopened.get_snapshot("missing"))

    def test_unchanged_observation_is_appended_but_content_version_is_stable(self):
        with MetadataStore(self.path) as store:
            store.save_fetch(metadata_fetch(), [metadata_snapshot()])
            store.save_fetch(metadata_fetch(
                "fetch-2", started_at="2026-10-09T12:00:02Z", finished_at="2026-10-09T12:00:03Z",
            ), [metadata_snapshot("snapshot-2", fetched_at="2026-10-09T12:00:03Z")])
            first = store.get_snapshot("snapshot-1")
            second = store.get_snapshot("snapshot-2")
            self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
            self.assertNotEqual(first["fetched_at"], second["fetched_at"])
            self.assertEqual(first["content_version"], second["content_version"])
            self.assertEqual(store.latest_snapshot("course-api", "user", 42), second)
            store.save_fetch(metadata_fetch(
                "fetch-3", started_at="2026-10-09T12:00:04Z", finished_at="2026-10-09T12:00:05Z",
            ), [metadata_snapshot("snapshot-3", fetched_at="2026-10-09T12:00:05Z", record={"id": 42, "likes": "Comedy"})])
            self.assertNotEqual(store.get_snapshot("snapshot-3")["content_version"], first["content_version"])

    def test_exact_replay_canonicalizes_times_ids_and_snapshot_order(self):
        fetch = metadata_fetch(requested_ids=[42, 43])
        snapshots = [metadata_snapshot("snapshot-b"), metadata_snapshot("snapshot-a", entity_id=43)]
        with MetadataStore(self.path) as store:
            self.assertTrue(store.save_fetch(fetch, snapshots))
            original = store.get_fetch("fetch-1")
            replay = {**fetch, "requested_ids": ["00042", "43"], "started_at": "2026-10-09T08:00:00-04:00"}
            self.assertFalse(store.save_fetch(replay, list(reversed(snapshots))))
            self.assertEqual(store.get_fetch("fetch-1"), original)
            with self.assertRaises(StorageError):
                store.save_fetch({**fetch, "collector_extension": {"retained": False}}, snapshots)
            with self.assertRaises(StorageError):
                store.save_fetch(fetch, [snapshots[0]])
            self.assertEqual(store.get_fetch("fetch-1"), original)

    def test_snapshot_conflict_rolls_back_new_fetch_and_prior_new_snapshot(self):
        with MetadataStore(self.path) as store:
            store.save_fetch(metadata_fetch(), [metadata_snapshot("z-existing")])
            original = store.get_snapshot("z-existing")
            with self.assertRaises(StorageError):
                store.save_fetch(metadata_fetch("fetch-conflict", requested_ids=[42, 43]), [
                    metadata_snapshot("a-new", entity_id=43), metadata_snapshot("z-existing"),
                ])
            self.assertIsNone(store.get_fetch("fetch-conflict"))
            self.assertIsNone(store.get_snapshot("a-new"))
            self.assertEqual(store.get_snapshot("z-existing"), original)

    def test_latest_and_asof_use_availability_and_stable_snapshot_tie(self):
        with MetadataStore(self.path) as store:
            store.save_fetch(metadata_fetch(), [metadata_snapshot("snapshot-a", source_updated_at="2099-01-01T00:00:00Z")])
            store.save_fetch(metadata_fetch("fetch-b"), [metadata_snapshot("snapshot-b", source_updated_at="2000-01-01T00:00:00Z")])
            self.assertEqual(store.latest_snapshot("course-api", "user", 42)["snapshot_id"], "snapshot-b")
            self.assertIsNone(store.latest_snapshot("course-api", "user", 42, as_of="2026-10-09T12:00:00.999999Z"))
            self.assertEqual(store.latest_snapshot("course-api", "user", 42, as_of="2026-10-09T12:00:01Z")["snapshot_id"], "snapshot-b")
            store.save_fetch(metadata_fetch(
                "fetch-later", started_at="2026-10-09T12:00:02Z", finished_at="2026-10-09T12:00:03Z",
            ), [metadata_snapshot("snapshot-later", fetched_at="2026-10-09T12:00:03Z")])
            self.assertEqual(store.latest_snapshot("course-api", "user", 42)["snapshot_id"], "snapshot-later")
            self.assertEqual(store.latest_snapshot("course-api", "user", 42, as_of="2026-10-09T08:00:02-04:00")["snapshot_id"], "snapshot-b")

    def test_failed_fetch_retains_diagnostics_without_replacing_latest_snapshot(self):
        with MetadataStore(self.path) as store:
            store.save_fetch(metadata_fetch(), [metadata_snapshot()])
            original = store.latest_snapshot("course-api", "user", 42)
            self.assertTrue(store.save_fetch(metadata_fetch(
                "fetch-failed", started_at="2026-10-09T12:00:02Z", finished_at="2026-10-09T12:00:03Z",
                status="failed", http_status=503, error_type="HTTPError", response={"body": {"error": "unavailable"}},
            )))
            self.assertEqual(store.get_fetch("fetch-failed")["http_status"], 503)
            self.assertEqual(store.latest_snapshot("course-api", "user", 42), original)
            self.assertTrue(store.save_fetch(metadata_fetch("fetch-partial", status="partial", requested_ids=[42, 43]), [metadata_snapshot("snapshot-partial")]))
            self.assertIsNone(store.latest_snapshot("course-api", "user", 43))

    def test_invalid_writes_are_rejected_before_fetch_or_snapshot_persistence(self):
        cases = [
            ({"source_id": " "}, None), ({"entity_type": "profile"}, None),
            ({"requested_ids": [True]}, None), ({"requested_ids": [0]}, None),
            ({"status": "unknown"}, None), ({"http_status": True}, None),
            ({"http_status": 600}, None), ({"error_type": "password=not-a-category"}, None),
            ({"started_at": "2026-10-09T12:00:00"}, None),
            ({"finished_at": "2026-10-09T11:59:59Z"}, None),
            ({"response": {"number": float("nan")}}, None),
            ({}, [metadata_snapshot(fetched_at="2026-10-09T11:59:59Z")]),
            ({}, [metadata_snapshot(fetched_at="2026-10-09T12:00:02Z")]),
            ({}, [metadata_snapshot(source_id="other-source")]),
            ({}, [metadata_snapshot(entity_type="movie")]),
            ({}, [metadata_snapshot(entity_id=43)]),
            ({}, [metadata_snapshot(fetch_id="different-fetch")]),
            ({}, [metadata_snapshot(record=[])]),
            ({}, [metadata_snapshot(record={"value": float("inf")})]),
            ({}, [metadata_snapshot(), metadata_snapshot()]),
            ({"status": "failed"}, [metadata_snapshot()]),
        ]
        with MetadataStore(self.path) as store:
            for changes, snapshots in cases:
                with self.subTest(changes=changes, snapshots=snapshots):
                    with self.assertRaises((ValueError, TypeError)):
                        store.save_fetch(metadata_fetch(**changes), snapshots)
                    self.assertIsNone(store.get_fetch("fetch-1"))
                    self.assertIsNone(store.get_snapshot("snapshot-1"))

    def test_response_headers_credentials_and_unknown_fields_are_safely_preserved(self):
        fetch = metadata_fetch(response={
            "headers": [["Authorization", "Bearer header-secret"], ["Set-Cookie", "session=cookie-secret"], ["x-trace-id", "trace-1"], ["x-trace-id", None]],
            "request_headers": {"X-API-Key": "api-secret", "x-safe": "visible"},
            "body": {"extension": {"password": "body-secret", "unknown": [1, "retained"]}},
            "url": "https://url-user:url-password@example.com/path?token=url-secret&tag=visible",
        })
        snapshot = metadata_snapshot(record={
            "id": 42, "likes": "Crime dramas", "unknown": {"retained": [1, 2]},
            "source_url": "https://entity-user:entity-password@example.com/?access_token=entity-secret",
        })
        original_fetch = copy.deepcopy(fetch)
        original_snapshot = copy.deepcopy(snapshot)
        with MetadataStore(self.path) as store:
            store.save_fetch(fetch, [snapshot])
            saved_fetch = store.get_fetch("fetch-1")
            saved_snapshot = store.get_snapshot("snapshot-1")
        encoded = json.dumps([saved_fetch, saved_snapshot])
        for secret in ("header-secret", "cookie-secret", "api-secret", "body-secret", "url-user", "url-password", "url-secret", "entity-user", "entity-password", "entity-secret"):
            self.assertNotIn(secret, encoded)
        self.assertEqual(saved_fetch["response"]["headers"][2:], [["x-trace-id", "trace-1"], ["x-trace-id", None]])
        self.assertEqual(saved_fetch["response"]["body"]["extension"]["unknown"], [1, "retained"])
        self.assertEqual(saved_snapshot["record"]["unknown"], {"retained": [1, 2]})
        self.assertEqual(fetch, original_fetch)
        self.assertEqual(snapshot, original_snapshot)

    def test_source_and_kind_isolation_and_id_validation(self):
        with MetadataStore(self.path) as store:
            store.save_fetch(metadata_fetch(), [metadata_snapshot()])
            store.save_fetch(metadata_fetch("fetch-other", source_id="other-api"), [metadata_snapshot("snapshot-other", source_id="other-api")])
            store.save_fetch(metadata_fetch("fetch-movie", entity_type="movie", requested_ids=["42"]), [metadata_snapshot("snapshot-movie", entity_type="movie", entity_id="42", record={"title": "Movie 42"})])
            self.assertEqual(store.latest_snapshot("course-api", "user", 42)["snapshot_id"], "snapshot-1")
            self.assertEqual(store.latest_snapshot("other-api", "user", 42)["snapshot_id"], "snapshot-other")
            self.assertEqual(store.latest_snapshot("course-api", "movie", "42")["snapshot_id"], "snapshot-movie")
        for value in (True, 0, -1, 1.0, "not-a-user", str(2**63), "9" * 5000):
            with self.subTest(value=str(value)[:40]):
                with self.assertRaises(ValueError):
                    normalize_entity_id("user", value)
        self.assertEqual(normalize_entity_id("user", "00042"), "42")
        self.assertEqual(normalize_entity_id("user", 2**63 - 1), str(2**63 - 1))
        self.assertEqual(normalize_entity_id("movie", "movie+a,2026"), "movie+a,2026")
        for value in (None, 42, " "):
            with self.assertRaises(ValueError):
                normalize_entity_id("movie", value)

    def test_cache_freshness_expires_at_boundary_and_rejects_future_availability(self):
        snapshot = metadata_snapshot()
        self.assertTrue(MetadataStore.is_fresh(snapshot, 60, now="2026-10-09T12:01:00.999999Z"))
        self.assertFalse(MetadataStore.is_fresh(snapshot, 60, now="2026-10-09T12:01:01Z"))
        self.assertFalse(MetadataStore.is_fresh(snapshot, 60, now="2026-10-09T12:00:00Z"))
        self.assertTrue(MetadataStore.is_fresh(snapshot, 60, now=datetime(2026, 10, 9, 12, 0, 1, tzinfo=timezone.utc)))
        self.assertFalse(MetadataStore.is_fresh(None, 60))
        for max_age in (0, -1, True, float("nan"), float("inf")):
            with self.subTest(max_age=max_age), self.assertRaises(ValueError):
                MetadataStore.is_fresh(snapshot, max_age)

    def test_schema2_migration_preserves_request_json_and_kafka_bytes(self):
        request_json = '{"request_id":"existing", "schema_version":1,"user_id":42,"started_at":"2026-10-09T12:00:00+00:00","extra":"🎬"}'
        parsed_json = '{"parser_version":1, "parse_status":"unrecognized","fields":{"body":"opaque"}}'
        with closing(sqlite3.connect(self.path)) as connection:
            connection.executescript("""
                CREATE TABLE recommendation_requests (
                    request_id TEXT PRIMARY KEY NOT NULL, started_at TEXT NOT NULL,
                    user_id INTEGER, record_json TEXT NOT NULL
                );
                CREATE INDEX requests_by_user_time
                    ON recommendation_requests(user_id, started_at, request_id);
                CREATE TABLE kafka_events (
                    source_id TEXT NOT NULL, topic TEXT NOT NULL, partition INTEGER NOT NULL,
                    offset INTEGER NOT NULL, source_fingerprint TEXT NOT NULL, raw_key BLOB, raw_value BLOB,
                    headers_json TEXT NOT NULL, raw_redacted INTEGER NOT NULL, broker_timestamp_ms INTEGER,
                    broker_timestamp_type INTEGER NOT NULL, leader_epoch INTEGER, ingested_at TEXT NOT NULL,
                    event_timestamp TEXT, user_id INTEGER, movie_id TEXT, event_type TEXT,
                    parse_status TEXT NOT NULL, parser_version INTEGER NOT NULL, parsed_json TEXT NOT NULL,
                    PRIMARY KEY(source_id, topic, partition, offset)
                ) WITHOUT ROWID;
                CREATE INDEX events_by_user_time
                    ON kafka_events(user_id, event_timestamp, event_type);
                PRAGMA user_version=2;
            """)
            connection.execute("INSERT INTO recommendation_requests VALUES(?,?,?,?)", (
                "existing", "2026-10-09T12:00:00+00:00", 42, request_json,
            ))
            connection.execute("INSERT INTO kafka_events VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                "course", "movielog2", 2, 100, "old-fingerprint", b"\xffkey", b"\x00\xffbody",
                '[["trace", "dHJhY2U="], ["trace", null]]', 0, None, 0, 3,
                "2026-10-09T12:00:01+00:00", None, 42, None, None, "unrecognized", 1, parsed_json,
            ))
            connection.commit()
            request_before = connection.execute("SELECT * FROM recommendation_requests").fetchall()
            kafka_before = connection.execute("SELECT * FROM kafka_events").fetchall()
        with MetadataStore(self.path) as store:
            store.save_fetch(metadata_fetch(), [metadata_snapshot()])
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 4)
            self.assertEqual(connection.execute("SELECT * FROM recommendation_requests").fetchall(), request_before)
            self.assertEqual(connection.execute("SELECT * FROM kafka_events").fetchall(), kafka_before)
        with RequestStore(self.path) as requests:
            self.assertEqual(requests.get_request("existing"), json.loads(request_json))
        with EventStore(self.path) as events:
            self.assertEqual(events.get_event("course", "movielog2", 2, 100)["raw_value"], b"\x00\xffbody")

    def test_metadata_kafka_and_request_writers_use_independent_connections(self):
        request_log = RequestLog(self.path, queue_size=100, busy_timeout=3)
        barrier = threading.Barrier(4)
        errors = []

        def write_metadata(worker_id):
            try:
                with MetadataStore(self.path, busy_timeout=3) as store:
                    barrier.wait(timeout=3)
                    for index in range(10):
                        store.save_fetch(metadata_fetch(f"fetch-{worker_id}-{index}"), [metadata_snapshot(f"snapshot-{worker_id}-{index}")])
            except Exception as error:
                errors.append(error)

        def write_events():
            try:
                with EventStore(self.path, busy_timeout=3) as store:
                    barrier.wait(timeout=3)
                    for index in range(20):
                        envelope = KafkaEnvelope(
                            source_id="course", topic="movielog2", partition=0, offset=index,
                            value=b"2026-10-09T12:00:00Z,42,GET /rate/movie_a=7",
                        )
                        store.save_event(envelope, parse_event(envelope.value))
            except Exception as error:
                errors.append(error)

        workers = [threading.Thread(target=write_metadata, args=(worker_id,)) for worker_id in range(2)]
        workers.append(threading.Thread(target=write_events))
        for worker in workers:
            worker.start()
        try:
            barrier.wait(timeout=3)
            for index in range(20):
                self.assertTrue(request_log.submit({"request_id": f"request-{index}", "started_at": "2026-10-09T12:00:00Z", "user_id": 42}))
            for worker in workers:
                worker.join(timeout=5)
                self.assertFalse(worker.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(request_log.close())
        finally:
            for worker in workers:
                worker.join(timeout=5)
            request_log.close()
        with MetadataStore(self.path) as metadata:
            self.assertEqual(metadata.connection.execute("SELECT COUNT(*) FROM metadata_fetches").fetchone()[0], 20)
            self.assertEqual(metadata.connection.execute("SELECT COUNT(*) FROM metadata_snapshots").fetchone()[0], 20)
        with EventStore(self.path) as events:
            self.assertEqual(len(events.list_events(42)), 20)
        with RequestStore(self.path) as requests:
            self.assertEqual(len(requests.list_requests(42)), 20)


if __name__ == "__main__":
    unittest.main()
