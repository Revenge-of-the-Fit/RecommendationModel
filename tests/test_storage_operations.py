import base64
import gzip
import hashlib
import io
import json
import sqlite3
import sys
import tempfile
import threading
import tracemalloc
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

import manage_storage
import export_observations as observations_cli
import storage.operations as operations
from events.parser import parse_event
from storage.database import StorageError, open_database
from storage.events import EventStore, KafkaEnvelope
from storage.metadata import MetadataStore
from storage.operations import backup_database, capacity_status, export_records, restore_backup, storage_status
from storage.requests import RequestStore


def request_record(number=1, **changes):
    return {
        "request_id": f"request-{number}", "user_id": 42,
        "started_at": "2026-10-08T12:00:00+00:00",
        "finished_at": "2026-10-08T12:00:00.100000+00:00",
        "status": 200, "response_complete": True,
        "recommendations": [{"movie_id": "movie_a", "rank": 1, "score": 0.4}],
        "response_body": "movie_a", **changes,
    }


def envelope(number=1, **changes):
    return KafkaEnvelope(**{
        "source_id": "course-cluster", "topic": "movielog2", "partition": 3,
        "offset": number, "key": b"\x00\xffbinary-key",
        "value": b"2026-10-08T12:01:00+00:00,42,GET /data/m/movie_a/17.mpg",
        "headers": [("trace", b"\xff\x00"), ("trace", b"second"), ("null", None)],
        "ingested_at": "2026-10-08T12:01:01+00:00", "broker_timestamp_ms": 1791460860000,
        "broker_timestamp_type": 1, "leader_epoch": 4, **changes,
    })


class CountingOutput:
    def __init__(self):
        self.lines = 0
        self.max_line = 0

    def write(self, text):
        self.lines += 1
        self.max_line = max(self.max_line, len(text))


class StorageOperationsTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "events.sqlite3"
        self.archives = self.directory / "archives"
        self.initial = envelope()
        with RequestStore(self.path) as requests:
            requests.save_request(request_record())
        with EventStore(self.path) as events:
            events.save_event(self.initial, parse_event(self.initial.value))

    def tearDown(self):
        self.temporary.cleanup()

    def test_status_integrity_and_capacity_include_wal_without_creating_missing_database(self):
        with EventStore(self.path) as events:
            events.save_event(envelope(2), parse_event(self.initial.value))
            report = storage_status(self.path)
            self.assertTrue(report["healthy"])
            self.assertTrue(report["integrity_ok"])
            self.assertEqual(report["foreign_key_violations"], 0)
            self.assertEqual(report["table_counts"]["kafka_events"], 2)
            self.assertGreater(report["wal_bytes"], 0)
            limited = storage_status(self.path, max_database_bytes=report["database_bytes"])
            self.assertFalse(limited["healthy"])
            self.assertIn("maximum_database_size", limited["capacity_reasons"])
        free_limited = storage_status(self.path, min_free_bytes=2**62)
        self.assertFalse(free_limited["capacity_ok"])
        self.assertIn("minimum_free_space", free_limited["capacity_reasons"])
        missing = self.directory / "missing" / "never-created.sqlite3"
        self.assertFalse(storage_status(missing)["healthy"])
        self.assertFalse(missing.exists())
        corrupt = self.directory / "corrupt.sqlite3"
        corrupt.write_bytes(b"not a database")
        self.assertFalse(storage_status(corrupt)["healthy"])

    def test_capacity_health_is_lightweight_and_never_opens_sqlite_or_exposes_os_messages(self):
        with patch("storage.operations._read_database", side_effect=AssertionError("unexpected SQLite scan")):
            report = capacity_status(self.path)
        self.assertTrue(report["healthy"])
        self.assertNotIn("table_counts", report)
        self.assertNotIn("integrity_ok", report)
        with patch("storage.operations._free_space", side_effect=OSError("secret filesystem diagnostic")):
            failed = capacity_status(self.path)
        self.assertFalse(failed["healthy"])
        self.assertEqual(failed["last_error"], "OSError")
        self.assertNotIn("secret", json.dumps(failed))

    def test_online_backup_restores_exact_source_keys_and_replay_while_writes_continue(self):
        entered = threading.Event()
        stop = threading.Event()
        failures = []

        def writer():
            try:
                with RequestStore(self.path, busy_timeout=5.0) as requests, EventStore(self.path, busy_timeout=5.0) as events:
                    number = 2
                    while not stop.is_set():
                        requests.save_request(request_record(number))
                        events.save_event(envelope(number), parse_event(self.initial.value))
                        entered.set()
                        number += 1
            except Exception as error:
                failures.append(error)
                entered.set()

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            self.assertTrue(entered.wait(5))
            manifest = backup_database(self.path, self.archives)
            self.assertTrue(thread.is_alive())
        finally:
            stop.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])
        self.assertEqual(manifest["retained_archives"], 1)
        self.assertEqual(hashlib.sha256(Path(manifest["archive_path"]).read_bytes()).hexdigest(), manifest["archive_sha256"])
        restored = self.directory / "recovered.sqlite3"
        result = restore_backup(manifest["archive_path"], restored)
        self.assertTrue(result["integrity_ok"])
        self.assertEqual(result["table_counts"], manifest["table_counts"])
        with EventStore(restored) as events:
            record = events.get_event("course-cluster", "movielog2", 3, 1)
            self.assertEqual(record["raw_key"], self.initial.key)
            self.assertEqual(record["raw_value"], self.initial.value)
            self.assertEqual(record["headers"], self.initial.headers)
            self.assertFalse(events.save_event(replace(self.initial, leader_epoch=100, ingested_at="2026-10-09T00:00:00+00:00"), {**parse_event(self.initial.value), "parser_version": 2}))
        with RequestStore(restored) as requests:
            self.assertEqual(requests.get_request("request-1")["response_body"], "movie_a")
        with gzip.open(manifest["archive_path"], "rb") as handle:
            self.assertEqual(hashlib.sha256(handle.read()).hexdigest(), manifest["database_sha256"])

    def test_rotation_retains_count_and_respects_budget_without_removing_live_or_unmanaged_data(self):
        self.archives.mkdir()
        unmanaged = self.archives / "someone-elses-backup.sqlite3.gz"
        unmanaged.write_bytes(b"keep")
        for _ in range(4):
            last = backup_database(self.path, self.archives, retain=2)
        self.assertEqual(last["retained_archives"], 2)
        self.assertEqual(len(list(self.archives.glob("*.manifest.json"))), 2)
        self.assertEqual(unmanaged.read_bytes(), b"keep")
        self.assertEqual(storage_status(self.path)["table_counts"]["kafka_events"], 1)
        pair_size = Path(last["archive_path"]).stat().st_size + Path(last["manifest_path"]).stat().st_size
        bounded = backup_database(self.path, self.archives, retain=7, max_archive_bytes=int(pair_size * 1.5))
        self.assertEqual(bounded["retained_archives"], 1)
        self.assertLessEqual(bounded["archive_total_bytes"], bounded["max_archive_bytes"])
        existing = set(self.archives.iterdir())
        with self.assertRaises(StorageError):
            backup_database(self.path, self.archives, max_archive_bytes=1)
        self.assertEqual(set(self.archives.iterdir()), existing)

    def test_parallel_backup_jobs_do_not_race_publication_or_retention(self):
        hashing = threading.Event()
        release = threading.Event()
        outcomes = []
        original = operations._hash_file

        def blocked_hash(path):
            if Path(path).suffix == ".sqlite3":
                hashing.set()
                if not release.wait(5):
                    raise AssertionError("Backup hash synchronization timed out")
            return original(path)

        def first_backup():
            try:
                outcomes.append(backup_database(self.path, self.archives, retain=1))
            except Exception as error:
                outcomes.append(error)

        with patch("storage.operations._hash_file", side_effect=blocked_hash):
            thread = threading.Thread(target=first_backup)
            thread.start()
            try:
                self.assertTrue(hashing.wait(5))
                with self.assertRaises(StorageError):
                    backup_database(self.path, self.archives, retain=1)
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(outcomes), 1)
        self.assertIsInstance(outcomes[0], dict)
        self.assertEqual(outcomes[0]["retained_archives"], 1)
        self.assertEqual(backup_database(self.path, self.archives, retain=1)["retained_archives"], 1)

    def test_capacity_and_invalid_configuration_fail_before_any_backup_rotation(self):
        manifest = backup_database(self.path, self.archives)
        existing = set(self.archives.iterdir())
        for options in ({"min_free_bytes": 2**62}, {"max_database_bytes": 1}):
            with self.subTest(options=options), self.assertRaises(StorageError):
                backup_database(self.path, self.archives, **options)
        for options in ({"retain": 0}, {"retain": True}, {"max_archive_bytes": 0}, {"min_free_bytes": -1}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                backup_database(self.path, self.archives, **options)
        self.assertEqual(set(self.archives.iterdir()), existing)
        self.assertTrue(Path(manifest["archive_path"]).exists())

    def test_restore_refuses_existing_target_and_missing_or_corrupt_archive(self):
        manifest = backup_database(self.path, self.archives)
        archive = Path(manifest["archive_path"])
        target = self.directory / "target.sqlite3"
        target.write_bytes(b"existing")
        with self.assertRaises(StorageError):
            restore_backup(archive, target)
        self.assertEqual(target.read_bytes(), b"existing")
        target.unlink()
        with self.assertRaises(StorageError):
            restore_backup(self.directory / "missing.gz", target)
        archive.write_bytes(archive.read_bytes() + b"corruption")
        with self.assertRaises(StorageError):
            restore_backup(archive, target)
        self.assertFalse(target.exists())

    def test_restore_rejects_missing_or_wrongly_shaped_manifest_without_creating_target(self):
        manifest = backup_database(self.path, self.archives)
        archive = Path(manifest["archive_path"])
        manifest_path = Path(manifest["manifest_path"])
        target = self.directory / "target.sqlite3"
        manifest_path.unlink()
        with self.assertRaises(StorageError):
            restore_backup(archive, target)
        for malformed in ([], None, {"manifest_version": True}, "not a manifest"):
            manifest_path.write_text(json.dumps(malformed), encoding="utf-8")
            with self.subTest(malformed=malformed), self.assertRaises(StorageError):
                restore_backup(archive, target)
            self.assertFalse(target.exists())

    def test_restore_checks_sqlite_after_matching_gzip_hashes_and_handles_publication_race(self):
        manifest = backup_database(self.path, self.archives)
        archive = Path(manifest["archive_path"])
        target = self.directory / "target.sqlite3"
        with patch("storage.operations._publish") as publish:
            def race(temporary, destination):
                destination.write_bytes(b"concurrent creator")
                import os
                os.link(temporary, destination)
            publish.side_effect = race
            with self.assertRaises(StorageError):
                restore_backup(archive, target)
        self.assertEqual(target.read_bytes(), b"concurrent creator")
        target.unlink()
        invalid = b"not sqlite even with matching declared hashes"
        archive.write_bytes(gzip.compress(invalid))
        saved = json.loads(Path(manifest["manifest_path"]).read_text(encoding="utf-8"))
        saved.update(database_bytes=len(invalid), database_sha256=hashlib.sha256(invalid).hexdigest(), archive_bytes=archive.stat().st_size, archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
        Path(manifest["manifest_path"]).write_text(json.dumps(saved), encoding="utf-8")
        with self.assertRaises(StorageError):
            restore_backup(archive, target)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.directory.glob(".observations-*")), [])

    def test_raw_jsonl_round_trip_preserves_binary_unknown_events_and_duplicate_headers(self):
        binary = envelope(2, value=b"\xff\xfe\x00opaque")
        unknown = envelope(3, value=b"2026-10-08T12:02:00+00:00,42,unknown future format")
        tombstone = envelope(4, value=None, key=None)
        with EventStore(self.path) as events:
            for item in (binary, unknown, tombstone):
                events.save_event(item, parse_event(item.value))
        output = io.StringIO()
        counts = export_records(self.path, output)
        self.assertEqual(counts["kafka_events"], 4)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        original_request_json = next(item["row"]["record_json"] for item in records if item["table"] == "recommendation_requests")
        reconstructed = self.directory / "jsonl-recovered.sqlite3"
        with closing(open_database(reconstructed)) as connection:
            for item in records:
                row = {key: base64.b64decode(value["base64"]) if isinstance(value, dict) and value.get("$type") == "bytes" else value for key, value in item["row"].items()}
                names = list(row)
                connection.execute(f'INSERT INTO "{item["table"]}" (' + ",".join('"' + name + '"' for name in names) + ") VALUES (" + ",".join("?" for _ in names) + ")", [row[name] for name in names])
            connection.commit()
            self.assertEqual(connection.execute("SELECT record_json FROM recommendation_requests").fetchone()[0], original_request_json)
        with EventStore(reconstructed) as events:
            saved = events.get_event("course-cluster", "movielog2", 3, 2)
            self.assertEqual(saved["raw_value"], binary.value)
            self.assertEqual(saved["headers"], binary.headers)
            self.assertFalse(events.save_event(binary, parse_event(binary.value)))
            self.assertEqual(events.get_event("course-cluster", "movielog2", 3, 3)["parse_status"], "unrecognized")
            self.assertIsNone(events.get_event("course-cluster", "movielog2", 3, 4)["raw_value"])

    def test_raw_filters_separate_event_time_and_availability_and_preserve_unknown_records(self):
        late = envelope(2, ingested_at="2026-10-08T13:00:00+00:00")
        other = envelope(3, source_id="other-source")
        topic = envelope(4, topic="another-topic")
        naive = envelope(5, value=b"2026-10-08T12:01:00,42,GET /rate/movie_a=9")
        with EventStore(self.path) as events:
            for item in (late, other, topic, naive):
                events.save_event(item, parse_event(item.value))
        output = io.StringIO()
        counts = export_records(self.path, output, source_id="course-cluster", topic="movielog2", start="2026-10-08T12:00:00+00:00", end="2026-10-08T12:02:00+00:00", as_of="2026-10-08T12:30:00+00:00")
        rows = [json.loads(line)["row"] for line in output.getvalue().splitlines()]
        self.assertEqual([row["offset"] for row in rows], [1, 5])
        self.assertEqual(counts["recommendation_requests"], 0)
        self.assertIsNone(rows[1]["event_timestamp"])
        self.assertEqual(json.loads(rows[1]["parsed_json"])["timestamp_status"], "timezone_missing")

    def test_legacy_database_export_and_backup_do_not_migrate_or_rewrite_request_json(self):
        legacy = self.directory / "version1.sqlite3"
        exact = '{"request_id":"old", "user_id":42, "extra":"unchanged spacing"}'
        with closing(sqlite3.connect(legacy)) as connection:
            connection.execute("CREATE TABLE recommendation_requests(request_id TEXT PRIMARY KEY,started_at TEXT,user_id INTEGER,record_json TEXT)")
            connection.execute("INSERT INTO recommendation_requests VALUES (?,?,?,?)", ("old", "2026-10-08T12:00:00+00:00", 42, exact))
            connection.execute("PRAGMA user_version=1")
            connection.commit()
        output = io.StringIO()
        self.assertEqual(export_records(legacy, output), {"recommendation_requests": 1})
        self.assertEqual(json.loads(output.getvalue())["row"]["record_json"], exact)
        filtered = io.StringIO()
        self.assertEqual(export_records(legacy, filtered, as_of="2026-10-09T00:00:00+00:00"), {"recommendation_requests": 0})
        manifest = backup_database(legacy, self.archives)
        self.assertEqual(manifest["schema_version"], 1)
        with closing(sqlite3.connect(legacy)) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT record_json FROM recommendation_requests").fetchone()[0], exact)

    def test_schema4_metadata_and_profile_lineage_export_and_recover_with_asof_boundaries(self):
        with MetadataStore(self.path) as metadata:
            metadata.save_fetch({
                "fetch_id": "fetch", "source_id": "course-api", "entity_type": "user",
                "requested_ids": [42], "started_at": "2026-10-08T12:00:00Z",
                "finished_at": "2026-10-08T12:00:01Z", "status": "success",
                "http_status": 200, "error_type": None,
                "response": {"body": {"user_id": 42, "unknown": {"preserved": None}}},
            }, [{
                "snapshot_id": "snapshot", "source_id": "course-api", "entity_type": "user",
                "entity_id": 42, "fetched_at": "2026-10-08T12:00:01Z",
                "record": {"user_id": 42, "self_description_likes": "crime dramas"},
            }])
        with closing(open_database(self.path)) as connection:
            for attempt_id, status, finished_at, record in (
                ("pending", "pending", None, {}),
                ("responded", "responded", None, {"response_received_at": "2026-10-08T12:05:00Z", "raw_response": {"output": "captured"}}),
                ("unanchored-response", "responded", None, {"raw_response": {"output": "unknown availability"}}),
                ("success", "success", "2026-10-08T12:20:00.000000+00:00", {"raw_response": {"output": "later response"}}),
            ):
                connection.execute("INSERT INTO llm_attempts VALUES (?,?,?,?,?,?,?,?)", (attempt_id, "cache", "2026-10-08T12:01:00.000000+00:00", finished_at, status, 42, "snapshot", json.dumps(record)))
            connection.execute("INSERT INTO preference_profiles VALUES (?,?,?,?,?,?)", ("profile", "cache", "2026-10-08T12:20:00.000000+00:00", "success", "sha256:content", '{"profile":{"likes":["crime"]},"unknown":null}'))
            connection.execute("INSERT INTO profile_uses VALUES (?,?,?,?,?,?)", ("use", "profile", "2026-10-08T12:21:00.000000+00:00", 42, "snapshot", '{"cached":true}'))
            connection.commit()
        output = io.StringIO()
        counts = export_records(self.path, output, as_of="2026-10-08T12:10:00Z")
        self.assertEqual(counts["metadata_snapshots"], 1)
        self.assertEqual(counts["llm_attempts"], 2)
        self.assertEqual(counts["preference_profiles"], 0)
        self.assertEqual(counts["profile_uses"], 0)
        attempts = [item["row"]["attempt_id"] for item in map(json.loads, output.getvalue().splitlines()) if item["table"] == "llm_attempts"]
        self.assertEqual(attempts, ["pending", "responded"])
        self.assertNotIn("later response", output.getvalue())
        manifest = backup_database(self.path, self.archives)
        self.assertEqual(manifest["schema_version"], 5)
        restored = self.directory / "all-lineage.sqlite3"
        self.assertEqual(restore_backup(manifest["archive_path"], restored)["foreign_key_violations"], 0)
        recovered = io.StringIO()
        recovered_counts = export_records(restored, recovered)
        self.assertEqual(recovered_counts["llm_attempts"], 4)
        self.assertEqual(recovered_counts["preference_profiles"], 1)
        self.assertEqual(recovered_counts["profile_uses"], 1)
        self.assertIn("later response", recovered.getvalue())

    def test_large_raw_export_memory_does_not_scale_with_total_blob_volume(self):
        with EventStore(self.path) as events:
            binary = b"\xff" + bytes(range(256)) * 128
            for number in range(2, 514):
                item = envelope(number, value=binary)
                events.save_event(item, parse_event(binary))
        output = CountingOutput()
        tracemalloc.start()
        try:
            counts = export_records(self.path, output, tables=["kafka_events"])
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertEqual(counts["kafka_events"], 513)
        self.assertEqual(output.lines, 513)
        self.assertLess(peak, 4 * 1024 * 1024)
        self.assertLess(output.max_line, 64 * 1024)

    def test_cli_status_export_backup_restore_and_safe_errors(self):
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(manage_storage.main(["status", "--storage-path", str(self.path)]), 0)
        self.assertTrue(json.loads(output.getvalue())["healthy"])
        destination = self.directory / "records.jsonl"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(manage_storage.main(["export", "--storage-path", str(self.path), "--output", str(destination)]), 0)
        self.assertEqual(len(destination.read_text(encoding="utf-8").splitlines()), 5)
        output = io.StringIO()
        with redirect_stdout(output):
            self.assertEqual(manage_storage.main(["backup", "--storage-path", str(self.path), "--archive-dir", str(self.archives)]), 0)
        archive = json.loads(output.getvalue())["archive_path"]
        restored = self.directory / "restored.sqlite3"
        with redirect_stdout(io.StringIO()):
            self.assertEqual(manage_storage.main(["restore", "--archive", archive, "--target", str(restored)]), 0)
        errors = io.StringIO()
        with redirect_stderr(errors):
            self.assertEqual(manage_storage.main(["restore", "--archive", archive, "--target", str(restored)]), 1)
        self.assertNotIn(str(restored), errors.getvalue())
        with redirect_stdout(io.StringIO()):
            self.assertEqual(manage_storage.main(["status", "--storage-path", str(self.directory / "missing.sqlite3")]), 1)
        self.assertIn("never purged", manage_storage.build_parser().format_help())

    def test_cli_uses_storage_environment_and_explicit_path_overrides_it(self):
        alternate = self.directory / "alternate.sqlite3"
        with RequestStore(alternate):
            pass
        with patch.dict("os.environ", {"STORAGE_PATH": str(self.path)}):
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(manage_storage.main(["status"]), 0)
            status = json.loads(output.getvalue())
            self.assertEqual(status["table_counts"]["recommendation_requests"], 1)
            self.assertEqual(status["table_counts"]["kafka_events"], 1)
            output, errors = io.StringIO(), io.StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(manage_storage.main(["export"]), 0)
            raw = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual({item["table"] for item in raw}, {
                "recommendation_requests", "kafka_events", "live_users", "live_interactions", "live_event_cursors",
            })
            self.assertEqual(json.loads(errors.getvalue())["kafka_events"], 1)
            output, errors = io.StringIO(), io.StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(observations_cli.main([]), 0)
            observed = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual({item["record_type"] for item in observed}, {"impression", "observed_event", "candidate_link"})
            self.assertEqual(json.loads(errors.getvalue())["candidate_links"], 1)
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(manage_storage.main(["status", "--storage-path", str(alternate)]), 0)
            self.assertEqual(json.loads(output.getvalue())["table_counts"]["recommendation_requests"], 0)
            output, errors = io.StringIO(), io.StringIO()
            with redirect_stdout(output), redirect_stderr(errors):
                self.assertEqual(observations_cli.main(["--storage-path", str(alternate)]), 0)
            self.assertEqual(output.getvalue(), "")
            self.assertEqual(json.loads(errors.getvalue())["impressions"], 0)

    def test_export_refuses_overwrite_and_validates_filters_before_writing(self):
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            export_records(self.path, self.path)
        self.assertEqual(self.path.read_bytes(), before)
        target = self.directory / "existing.jsonl"
        target.write_text("existing", encoding="utf-8")
        with self.assertRaises(StorageError):
            export_records(self.path, target)
        self.assertEqual(target.read_text(encoding="utf-8"), "existing")
        for filters in ({"start": "2026-10-08T12:00:00"}, {"as_of": "invalid"}, {"start": "2026-10-09T00:00:00Z", "end": "2026-10-08T00:00:00Z"}, {"tables": ["private_table"]}):
            with self.subTest(filters=filters), self.assertRaises(ValueError):
                export_records(self.path, io.StringIO(), **filters)


if __name__ == "__main__":
    unittest.main()
