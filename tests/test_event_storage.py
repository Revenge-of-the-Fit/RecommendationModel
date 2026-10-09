import copy
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event
from storage.database import DATABASE_SCHEMA_VERSION, StorageError
from storage.events import EventStore, KafkaEnvelope
from storage.requests import RequestLog, RequestStore


def event_envelope(**changes):
    return KafkaEnvelope(**{
        "source_id": "course-cluster",
        "topic": "movielog2",
        "partition": 2,
        "offset": 100,
        "key": b"user-42",
        "value": b"2026-10-08T12:00:00+00:00,42,GET /data/m/movie_a/17.mpg",
        "headers": [("trace-id", b"trace-1")],
        "broker_timestamp_ms": 1791460800000,
        "broker_timestamp_type": 1,
        "leader_epoch": 3,
        "ingested_at": "2026-10-08T12:00:00.125000+00:00",
        **changes,
    })


class EventStorageTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def get_saved(self, store, envelope):
        return store.get_event(envelope.source_id, envelope.topic, envelope.partition, envelope.offset)

    def test_event_is_durable_with_broker_metadata_and_indexed_fields(self):
        envelope = event_envelope()
        parsed = parse_event(envelope.value)
        with EventStore(self.path) as store:
            self.assertTrue(store.save_event(envelope, parsed))
        with EventStore(self.path) as reopened:
            saved = self.get_saved(reopened, envelope)
            self.assertEqual(saved["source_id"], envelope.source_id)
            self.assertEqual(saved["topic"], envelope.topic)
            self.assertEqual(saved["partition"], envelope.partition)
            self.assertEqual(saved["offset"], envelope.offset)
            self.assertEqual(saved["raw_key"], envelope.key)
            self.assertEqual(saved["raw_value"], envelope.value)
            self.assertEqual(saved["headers"], envelope.headers)
            self.assertFalse(saved["raw_redacted"])
            self.assertEqual(saved["broker_timestamp_ms"], envelope.broker_timestamp_ms)
            self.assertEqual(saved["broker_timestamp_type"], envelope.broker_timestamp_type)
            self.assertEqual(saved["leader_epoch"], envelope.leader_epoch)
            self.assertEqual(saved["ingested_at"], envelope.ingested_at)
            self.assertEqual(saved["event_timestamp"], parsed["event_timestamp"])
            self.assertEqual(saved["user_id"], 42)
            self.assertEqual(saved["movie_id"], "movie_a")
            self.assertEqual(saved["event_type"], "watch")
            self.assertEqual(saved["parse_status"], "parsed")
            self.assertEqual(saved["parser_version"], 1)
            self.assertEqual(saved["parsed"], parsed)
            self.assertEqual(reopened.list_events(42), [saved])
            self.assertIsNone(reopened.get_event("missing", envelope.topic, envelope.partition, envelope.offset))

    def test_v1_database_migrates_without_changing_existing_request_json(self):
        payload = (
            '{"schema_version":1,"request_id":"existing", "started_at":"2026-10-08T12:00:00+00:00",'
            '"user_id":42,"response_body":"movie_a","extra":{"unicode":"🎬","space":" a b "}}'
        )
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("""
                CREATE TABLE recommendation_requests (
                    request_id TEXT PRIMARY KEY NOT NULL,
                    started_at TEXT NOT NULL,
                    user_id INTEGER,
                    record_json TEXT NOT NULL
                )
            """)
            connection.execute("""
                CREATE INDEX requests_by_user_time
                ON recommendation_requests(user_id, started_at, request_id)
            """)
            connection.execute(
                "INSERT INTO recommendation_requests VALUES (?, ?, ?, ?)",
                ("existing", "2026-10-08T12:00:00+00:00", 42, payload),
            )
            connection.execute("PRAGMA user_version=1")
            connection.commit()
        envelope = event_envelope()
        with EventStore(self.path) as store:
            self.assertTrue(store.save_event(envelope, parse_event(envelope.value)))
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], DATABASE_SCHEMA_VERSION)
            saved_json = connection.execute(
                "SELECT record_json FROM recommendation_requests WHERE request_id='existing'"
            ).fetchone()[0]
            self.assertEqual(saved_json, payload)
        with RequestStore(self.path) as requests:
            self.assertEqual(requests.get_request("existing"), json.loads(payload))
            self.assertTrue(requests.save_request({
                "request_id": "new-request",
                "started_at": "2026-10-08T12:00:01+00:00",
                "user_id": 42,
            }))
            self.assertEqual(requests.get_request("new-request")["schema_version"], 1)
        with EventStore(self.path) as reopened:
            self.assertIsNotNone(self.get_saved(reopened, envelope))

    def test_replay_ignores_ingestion_time_and_all_reparsed_fields(self):
        envelope = event_envelope()
        first_parsed = parse_event(envelope.value)
        replay = replace(envelope, ingested_at="2026-10-09T12:00:00+00:00")
        reparsed = {
            "parser_version": 99,
            "parse_status": "unrecognized",
            "error_type": "NewParserCategory",
            "event_type": "rating",
            "event_timestamp": "2026-10-08T18:00:00+00:00",
            "event_timestamp_raw": "changed timestamp interpretation",
            "timestamp_status": "utc",
            "user_id": 43,
            "movie_id": "other_movie",
            "fields": {"new": "parser field"},
        }
        with EventStore(self.path) as store:
            self.assertTrue(store.save_event(envelope, first_parsed))
            original = self.get_saved(store, envelope)
            self.assertFalse(store.save_event(envelope, first_parsed))
            self.assertFalse(store.save_event(replay, reparsed))
            for epoch in (None, -1, 4):
                self.assertFalse(store.save_event(replace(replay, leader_epoch=epoch), reparsed))
            self.assertEqual(self.get_saved(store, envelope), original)
            self.assertEqual(store.list_events(43), [])
            self.assertEqual(self.get_saved(store, envelope)["parsed"], first_parsed)
            self.assertEqual(self.get_saved(store, envelope)["ingested_at"], envelope.ingested_at)

    def test_source_conflicts_are_rejected_and_first_row_is_preserved(self):
        envelope = event_envelope()
        changes = (
            {"value": b"different event"},
            {"key": b"different key"},
            {"headers": [("trace-id", b"different trace")]},
            {"broker_timestamp_ms": envelope.broker_timestamp_ms + 1},
            {"broker_timestamp_type": 2},
        )
        with EventStore(self.path) as store:
            store.save_event(envelope, parse_event(envelope.value))
            original = self.get_saved(store, envelope)
            for change in changes:
                with self.subTest(change=change):
                    conflict = replace(envelope, **change)
                    with self.assertRaises(StorageError):
                        store.save_event(conflict, parse_event(conflict.value))
                    self.assertEqual(self.get_saved(store, envelope), original)

    def test_source_namespace_topic_partition_and_offset_are_independent(self):
        envelope = event_envelope()
        records = (
            envelope,
            replace(envelope, source_id="other-cluster"),
            replace(envelope, topic="movielog-other"),
            replace(envelope, partition=3),
            replace(envelope, offset=101),
        )
        with EventStore(self.path) as store:
            for record in records:
                self.assertTrue(store.save_event(record, parse_event(record.value)))
                self.assertEqual(self.get_saved(store, record)["raw_value"], record.value)
            self.assertEqual(len(store.list_events(42)), len(records))
            self.assertEqual(len(store.list_events(42, limit=2)), 2)
            self.assertEqual(store.list_events(43), [])
            for invalid_limit in (0, 10001):
                with self.assertRaises(ValueError):
                    store.list_events(42, limit=invalid_limit)

    def test_binary_unknown_and_tombstone_values_are_preserved_byte_exactly(self):
        values = (
            b"\x00\xff\x80arbitrary binary",
            b"2026-10-08T12:00:00+00:00,42,GET /future/action,first,second",
            None,
            b"",
            b"[" * 1100 + b"0" + b"]" * 1100,
        )
        with EventStore(self.path) as store:
            for offset, value in enumerate(values):
                with self.subTest(value=value):
                    envelope = event_envelope(offset=offset, key=b"\xff\x00key", value=value)
                    parsed = parse_event(value)
                    self.assertTrue(store.save_event(envelope, parsed))
                    saved = self.get_saved(store, envelope)
                    self.assertEqual(saved["raw_key"], envelope.key)
                    self.assertEqual(saved["raw_value"], value)
                    self.assertFalse(saved["raw_redacted"])
                    self.assertEqual(saved["parsed"], parsed)

    def test_duplicate_header_names_order_empty_bytes_and_nulls_survive_reopen(self):
        headers = [
            ("trace", b"first"), ("trace", b"second"), ("empty", b""),
            ("absent", None), ("binary", b"\xff\x00"),
        ]
        envelope = event_envelope(headers=headers)
        with EventStore(self.path) as store:
            store.save_event(envelope, parse_event(envelope.value))
        with EventStore(self.path) as reopened:
            saved = self.get_saved(reopened, envelope)
            self.assertEqual(saved["headers"], headers)
            self.assertFalse(saved["raw_redacted"])

    def test_source_and_parsed_text_credentials_are_redacted_without_mutating_inputs(self):
        envelope = event_envelope(
            key=b"access_token=key-secret&tag=visible",
            value=(
                b"2026-10-08T12:00:00+00:00,42,GET /future/action?token=value-secret&tag=visible"
                b"&return_to=https://url-user:url-password@example.com/page"
            ),
            headers=[
                ("Authorization", b"Bearer header-secret"),
                ("Cookie", b"session=cookie-secret"),
                ("x-safe", b"password=header-value-secret&tag=visible"),
                ("x-trace-id", b"trace-1"),
                ("Authorization", None),
            ],
        )
        parsed = parse_event(envelope.value)
        original = copy.deepcopy(parsed)
        with EventStore(self.path) as store:
            store.save_event(envelope, parsed)
            saved = self.get_saved(store, envelope)
        self.assertTrue(saved["raw_redacted"])
        saved_bytes = b"\n".join([
            saved["raw_key"], saved["raw_value"],
            *(value for _, value in saved["headers"] if value is not None),
            json.dumps(saved["parsed"]).encode("utf-8"),
        ])
        for secret in (b"key-secret", b"value-secret", b"url-user", b"url-password", b"header-secret", b"cookie-secret", b"header-value-secret"):
            self.assertNotIn(secret, saved_bytes)
        self.assertIn(b"visible", saved_bytes)
        self.assertEqual(saved["headers"][3], ("x-trace-id", b"trace-1"))
        self.assertEqual(saved["headers"][4], ("Authorization", None))
        self.assertEqual(parsed, original)
        self.assertIn(b"value-secret", envelope.value)

    def test_malformed_json_and_suffixed_secret_fields_are_redacted(self):
        values = (
            b'{"password":"prefix\\\"credential-sentinel","broken":}',
            b'password_hint=credential-sentinel&tag=visible',
            b'{"session":"credential-sentinel","score":NaN}',
            b'GET /future?%74oken=credential-sentinel&tag=visible',
            b'Cookie: session=credential-sentinel; other=another-sentinel',
        )
        with EventStore(self.path) as store:
            for offset, value in enumerate(values):
                with self.subTest(value=value):
                    envelope = event_envelope(offset=offset, value=value)
                    store.save_event(envelope, parse_event(value))
                    saved = self.get_saved(store, envelope)
                    self.assertTrue(saved["raw_redacted"])
                    combined = saved["raw_value"] + json.dumps(saved["parsed"]).encode("utf-8")
                    self.assertNotIn(b"credential-sentinel", combined)
                    self.assertNotIn(b"another-sentinel", combined)

    def test_nonsecret_token_usage_survives_source_and_header_redaction(self):
        value = b'{"input_tokens":4,"output_tokens":5,"total_tokens":9}'
        envelope = event_envelope(value=value, headers=[("usage", value)])
        with EventStore(self.path) as store:
            store.save_event(envelope, parse_event(value))
            saved = self.get_saved(store, envelope)
        self.assertEqual(saved["raw_value"], value)
        self.assertEqual(saved["headers"], envelope.headers)
        self.assertFalse(saved["raw_redacted"])

    def test_unsupported_database_versions_are_not_overwritten(self):
        for version in (-1, DATABASE_SCHEMA_VERSION + 1):
            with self.subTest(version=version):
                path = self.path.with_name(f"version-{version}.sqlite3")
                with closing(sqlite3.connect(path)) as connection:
                    connection.execute(f"PRAGMA user_version={version}")
                with self.assertRaises(StorageError):
                    EventStore(path)
                with closing(sqlite3.connect(path)) as connection:
                    self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], version)

    def test_fresh_request_and_event_connections_initialize_concurrently(self):
        barrier = threading.Barrier(4)

        def initialize(index):
            barrier.wait(timeout=5)
            if index % 2:
                envelope = event_envelope(offset=index)
                with EventStore(self.path, busy_timeout=5) as store:
                    return store.save_event(envelope, parse_event(envelope.value))
            with RequestStore(self.path, busy_timeout=5) as store:
                return store.save_request({
                    "request_id": f"startup-{index}",
                    "started_at": "2026-10-08T12:00:00+00:00",
                    "user_id": 42,
                })

        with ThreadPoolExecutor(max_workers=4) as executor:
            self.assertEqual(list(executor.map(initialize, range(4))), [True] * 4)
        with RequestStore(self.path) as requests, EventStore(self.path) as events:
            self.assertEqual(len(requests.list_requests(42)), 2)
            self.assertEqual(len(events.list_events(42)), 2)

    def test_request_writer_and_event_connections_can_write_concurrently(self):
        request_log = RequestLog(self.path, queue_size=100, busy_timeout=3)
        barrier = threading.Barrier(3)
        errors = []

        def write_partition(partition):
            try:
                with EventStore(self.path, busy_timeout=3) as store:
                    barrier.wait(timeout=3)
                    for offset in range(20):
                        envelope = event_envelope(partition=partition, offset=offset)
                        store.save_event(envelope, parse_event(envelope.value))
            except Exception as error:
                errors.append(error)

        writers = [threading.Thread(target=write_partition, args=(partition,)) for partition in (0, 1)]
        for writer in writers:
            writer.start()
        try:
            barrier.wait(timeout=3)
            for index in range(40):
                self.assertTrue(request_log.submit({
                    "request_id": f"request-{index}",
                    "started_at": "2026-10-08T12:00:00+00:00",
                    "user_id": 42,
                    "response_body": "movie_a",
                }))
            for writer in writers:
                writer.join(timeout=5)
                self.assertFalse(writer.is_alive())
            self.assertEqual(errors, [])
            self.assertTrue(request_log.close())
            self.assertEqual(request_log.status()["written"], 40)
            self.assertEqual(request_log.status()["failed"], 0)
        finally:
            for writer in writers:
                writer.join(timeout=5)
            request_log.close()
        with EventStore(self.path) as events:
            self.assertEqual(len(events.list_events(42)), 40)
        with RequestStore(self.path) as requests:
            saved_requests = requests.list_requests(42)
            self.assertEqual(len(saved_requests), 40)
            self.assertTrue(all(record["schema_version"] == 1 for record in saved_requests))


if __name__ == "__main__":
    unittest.main()
