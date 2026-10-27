import io
import json
import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import compact_storage
import storage_maintenance
import storage.operations as operations
from events.parser import parse_event
from storage.database import StorageError
from storage.events import EventStore, KafkaEnvelope
from storage.live import read_live_user
from storage.metadata import MetadataStore
from storage.operations import backup_database, checkpoint_database, export_observations, export_records
from storage.payloads import PREFIX, load_json, pack_json, unpack_json
from storage.requests import RequestStore
from test_metadata_storage import metadata_fetch, metadata_snapshot


class StorageCompactionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "events.sqlite3"
        self.request = {
            "request_id": "request-1", "user_id": 42,
            "started_at": "2026-10-09T12:00:00Z", "finished_at": "2026-10-09T12:00:00.100000Z",
            "status": 200, "response_complete": True,
            "recommendations": [{"movie_id": "movie_a", "rank": 1, "score": 0.5}],
            "extension": "Repeated audit text with Unicode 🎬. " * 300,
        }
        with RequestStore(self.path) as requests:
            requests.save_request(self.request)
        self.fetch = metadata_fetch(response={"body": "An unchanged metadata response. " * 400})
        with MetadataStore(self.path) as metadata:
            metadata.save_fetch(self.fetch, [metadata_snapshot()])
        event = KafkaEnvelope(source_id="cmu-movielog", topic="movielog2", partition=0, offset=1,
                              value=b"2026-10-09T12:01:00Z,42,GET /data/m/movie_a/1.mpg")
        with EventStore(self.path) as events:
            events.save_event(event, parse_event(event.value))

    def tearDown(self):
        self.temporary.cleanup()

    def exported(self, path, observations=False):
        output = io.StringIO()
        (export_observations if observations else export_records)(path, output)
        return output.getvalue()

    def test_compressed_and_legacy_records_are_interchangeable_and_idempotent(self):
        with RequestStore(self.path) as store:
            raw = store.connection.execute("SELECT record_json FROM recommendation_requests").fetchone()[0]
            self.assertIsInstance(raw, bytes)
            self.assertLess(len(raw), len(unpack_json(raw).encode()) / 2)
            expected = store.get_request("request-1")
            self.assertEqual(expected["extension"], self.request["extension"])
            self.assertFalse(store.save_request(self.request))
            with store.connection:
                store.connection.execute("UPDATE recommendation_requests SET record_json=?", (unpack_json(raw),))
            self.assertFalse(store.save_request(self.request))
            self.assertEqual(store.list_requests(42), [expected])
        with MetadataStore(self.path) as store:
            self.assertIsInstance(store.connection.execute("SELECT record_json FROM metadata_fetches").fetchone()[0], bytes)
            self.assertEqual(store.get_fetch("fetch-1")["response"], self.fetch["response"])
            self.assertFalse(store.save_fetch(self.fetch, [metadata_snapshot()]))
        self.assertEqual(pack_json('{}'), '{}')
        self.assertEqual(load_json(pack_json(json.dumps(self.request))), self.request)
        with self.assertRaises(ValueError):
            load_json(PREFIX + b"broken compressed record")

    def test_offline_conversion_preserves_events_lineage_live_state_and_evaluation_exports(self):
        with closing(sqlite3.connect(self.path)) as connection:
            for table in compact_storage.COMPRESSED_TABLES:
                rows = connection.execute(f"SELECT rowid,record_json FROM {table}").fetchall()
                connection.executemany(f"UPDATE {table} SET record_json=? WHERE rowid=?", [(unpack_json(value), row) for row, value in rows])
            connection.execute("PRAGMA user_version=6")
            connection.execute("INSERT INTO llm_attempts VALUES ('attempt','cache','2026-10-09T12:00:01Z','2026-10-09T12:00:02Z','success',42,'snapshot-1',?)",
                               (json.dumps({"usage": {"input_tokens": 300, "output_tokens": 0}, "model": "test"}),))
            connection.execute("INSERT INTO preference_profiles VALUES ('profile','cache','2026-10-09T12:00:02Z','attempt','version','{}')")
            connection.execute("INSERT INTO profile_uses VALUES ('use','profile','2026-10-09T12:00:03Z',42,'snapshot-1','{}')")
            connection.commit()
        original = self.exported(self.path)
        evaluations = self.exported(self.path, observations=True)
        history = read_live_user(self.path, 42)
        output = self.directory / "compact.sqlite3"
        result = compact_storage.compact_database(self.path, output, min_free_bytes=0)
        self.assertEqual(result["source_schema"], 6)
        self.assertTrue(result["integrity_ok"])
        self.assertEqual(result["foreign_key_violations"], 0)
        self.assertGreater(result["compressed_rows"], 0)
        self.assertLess(result["output_bytes"], result["source_bytes"])
        self.assertEqual(self.exported(output), original)
        self.assertEqual(self.exported(output, observations=True), evaluations)
        self.assertEqual(self.exported(self.path), original)
        self.assertEqual(read_live_user(output, 42), history)
        with RequestStore(output) as store:
            self.assertFalse(store.save_request(self.request))
        second = self.directory / "second.sqlite3"
        compact_storage.compact_database(output, second, min_free_bytes=0)
        self.assertEqual(self.exported(second), original)

    def test_failed_conversion_preserves_source_and_does_not_publish_partial_copy(self):
        original = self.exported(self.path)
        output = self.directory / "compact.sqlite3"
        with self.assertRaises(StorageError):
            compact_storage.compact_database(self.path, output, min_free_bytes=2**62)
        def interrupted(_):
            raise RuntimeError("simulated interrupted conversion")
        with self.assertRaises(RuntimeError):
            compact_storage.compact_database(self.path, output, min_free_bytes=0, progress=interrupted)
        self.assertFalse(output.exists())
        self.assertEqual(list(self.directory.glob("*.partial")), [])
        self.assertEqual(self.exported(self.path), original)
        output.write_bytes(b"existing destination")
        with self.assertRaises(ValueError):
            compact_storage.compact_database(self.path, output, min_free_bytes=0)
        self.assertEqual(output.read_bytes(), b"existing destination")

    def test_long_lived_writer_stops_at_wal_or_disk_limits_and_can_resume(self):
        with RequestStore(self.path) as store:
            store.save_request({**self.request, "request_id": "second"})
            with patch.dict(os.environ, {"STORAGE_MAX_WAL_BYTES": "1"}):
                with self.assertRaises(sqlite3.OperationalError):
                    store.save_request({**self.request, "request_id": "wal-blocked"})
            with patch.dict(os.environ, {"STORAGE_MIN_FREE_BYTES": str(2**62)}):
                with self.assertRaises(sqlite3.OperationalError):
                    store.save_request({**self.request, "request_id": "disk-blocked"})
            self.assertIsNone(store.get_request("wal-blocked"))
            self.assertIsNone(store.get_request("disk-blocked"))
            self.assertTrue(store.save_request({**self.request, "request_id": "resumed"}))

    def test_checkpoint_yields_to_a_reader_and_recovers_after_it_closes(self):
        with RequestStore(self.path) as writer, closing(sqlite3.connect(self.path)) as reader:
            reader.execute("BEGIN")
            reader.execute("SELECT count(*) FROM recommendation_requests").fetchone()
            writer.save_request({**self.request, "request_id": "after-reader"})
            self.assertTrue(checkpoint_database(self.path)["busy"])
            reader.rollback()
            self.assertFalse(checkpoint_database(self.path)["busy"])
            self.assertEqual(Path(str(self.path) + "-wal").stat().st_size, 0)
            self.assertIsNotNone(writer.get_request("after-reader"))

    def test_maintenance_verifies_the_copy_and_never_scans_live_integrity(self):
        with patch("storage.operations.storage_status", side_effect=AssertionError("live integrity scan")), redirect_stdout(io.StringIO()):
            self.assertEqual(storage_maintenance.run_maintenance(self.path, self.directory / "archives", once=True), 1)

    def test_timed_out_backup_retains_previous_verified_archive(self):
        archives = self.directory / "archives"
        backup_database(self.path, archives)
        before = set(archives.iterdir())
        with patch("storage.operations.time.monotonic", side_effect=[0, 601]), self.assertRaises(StorageError):
            backup_database(self.path, archives)
        self.assertEqual(set(archives.iterdir()), before)

    def test_backup_finishes_with_writes_each_step_and_releases_snapshot_before_verification(self):
        # Force many steps without a production-sized database or timing dependence.
        with closing(sqlite3.connect(self.path)) as writer:
            writer.execute("CREATE TABLE backup_test (id INTEGER PRIMARY KEY, payload BLOB)")
            writer.executemany("INSERT INTO backup_test VALUES (?, ?)", [(i, os.urandom(4096)) for i in range(80)])
            writer.commit()
        read_database, report = operations._read_database, operations._report
        callbacks = []

        class CopySource:
            def __init__(proxy, connection):
                proxy.connection = connection

            def execute(proxy, *args):
                return proxy.connection.execute(*args)

            def backup(proxy, destination, *, pages, sleep, progress):
                def after_step(status, remaining, total):
                    callbacks.append(remaining)
                    if len(callbacks) > 100:
                        raise AssertionError("Backup keeps restarting under concurrent writes")
                    with closing(sqlite3.connect(self.path)) as writer:
                        writer.execute("UPDATE backup_test SET payload=? WHERE id=0", (os.urandom(4096),))
                        writer.commit()
                    progress(status, remaining, total)
                proxy.connection.backup(destination, pages=4, sleep=sleep, progress=after_step)

        @contextmanager
        def source(path):
            with read_database(path) as connection:
                yield CopySource(connection)

        def verify(connection):
            wal = Path(str(self.path) + "-wal")
            self.assertTrue(not wal.exists() or wal.stat().st_size == 0,
                            "Clear the live WAL before slow verification so paused writers can resume")
            self.assertFalse(checkpoint_database(self.path)["busy"], "Backup still pins the live WAL during verification")
            return report(connection)

        with closing(sqlite3.connect(self.path)), patch.object(operations, "_read_database", source), \
                patch.object(operations, "_report", verify):
            result = backup_database(self.path, self.directory / "archives")
        self.assertGreater(len(callbacks), 5)
        self.assertEqual(callbacks[-1], 0)
        self.assertTrue(Path(result["archive_path"]).is_file())
