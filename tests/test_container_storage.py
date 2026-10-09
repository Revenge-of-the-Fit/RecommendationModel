import gzip
import io
import json
import os
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from contextlib import closing, redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

import storage_maintenance
from storage.database import open_database


class StorageMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events" / "events.sqlite3"
        self.archives = Path(self.temporary.name) / "archives"

    def tearDown(self):
        self.temporary.cleanup()

    def arguments(self, *more):
        return ["--storage-path", str(self.path), "--archive-directory", str(self.archives),
                "--min-free-bytes", "0", *more]

    def test_once_writes_a_compressed_consistent_database_without_modifying_live_rows(self):
        with closing(open_database(self.path)) as connection:
            connection.execute("CREATE TABLE maintenance_test (id INTEGER PRIMARY KEY, value TEXT)")
            connection.execute("INSERT INTO maintenance_test VALUES (1, 'persistent-record')")
            connection.commit()
            output = io.StringIO()
            with redirect_stdout(output):
                self.assertEqual(storage_maintenance.main(self.arguments("--once")), 0)
            reports = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual([item["operation"] for item in reports], ["storage_check", "storage_backup"])
            self.assertTrue(reports[0]["healthy"])
            archive = Path(reports[1]["archive_path"])
            restored = Path(self.temporary.name) / "restored.sqlite3"
            with gzip.open(archive, "rb") as source, restored.open("wb") as destination:
                shutil.copyfileobj(source, destination)
            with closing(sqlite3.connect(restored)) as reader:
                self.assertEqual(reader.execute("PRAGMA integrity_check").fetchone()[0], "ok")
                self.assertEqual(reader.execute("SELECT value FROM maintenance_test").fetchone()[0], "persistent-record")
            self.assertEqual(connection.execute("SELECT value FROM maintenance_test").fetchone()[0], "persistent-record")

    def test_unhealthy_storage_exits_nonzero_and_never_backs_up(self):
        status = {"healthy": False, "capacity_ok": False, "capacity_reasons": ["min_free_bytes"]}
        output = io.StringIO()
        errors = io.StringIO()
        with patch.object(storage_maintenance, "storage_status", return_value=status), \
                patch.object(storage_maintenance, "backup_database") as backup, \
                redirect_stdout(output), redirect_stderr(errors):
            self.assertEqual(storage_maintenance.main(self.arguments("--once")), 1)
        backup.assert_not_called()
        self.assertFalse(json.loads(output.getvalue())["healthy"])
        self.assertEqual(json.loads(errors.getvalue())["status"], "failed")

    def test_backup_failure_is_visible_without_printing_exception_payload(self):
        errors = io.StringIO()
        with patch.object(storage_maintenance, "storage_status", return_value={"healthy": True}), \
                patch.object(storage_maintenance, "backup_database", side_effect=OSError("secret-sentinel")), \
                redirect_stdout(io.StringIO()), redirect_stderr(errors):
            self.assertEqual(storage_maintenance.main(self.arguments("--once")), 1)
        self.assertEqual(json.loads(errors.getvalue())["error_type"], "OSError")
        self.assertNotIn("secret-sentinel", errors.getvalue())

    def test_waiting_for_next_backup_is_interrupted_by_shutdown(self):
        stopped = Mock(spec=threading.Event)
        stopped.is_set.side_effect = [False, True]
        with patch.object(storage_maintenance, "storage_status", return_value={"healthy": True}), \
                patch.object(storage_maintenance, "backup_database", return_value={"archive_name": "backup.gz"}), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(storage_maintenance.run_maintenance(
                self.path, self.archives, interval_hours=6, stop_event=stopped,
            ), 1)
        stopped.wait.assert_called_once_with(21600)

    def test_environment_sets_retention_and_capacity_plan(self):
        environment = {
            "STORAGE_PATH": str(self.path), "ARCHIVE_DIRECTORY": str(self.archives),
            "STORAGE_BACKUP_INTERVAL_HOURS": "12", "STORAGE_BACKUP_RETAIN": "5",
            "STORAGE_MIN_FREE_BYTES": "123", "STORAGE_MAX_DATABASE_BYTES": "456",
            "STORAGE_MAX_ARCHIVE_BYTES": "789",
        }
        with patch.dict(os.environ, environment, clear=True), \
                patch.object(storage_maintenance, "run_maintenance") as run:
            self.assertEqual(storage_maintenance.main(["--once"]), 0)
        arguments, options = run.call_args
        self.assertEqual(arguments, (self.path, self.archives))
        self.assertEqual(options["retain"], 5)
        self.assertEqual(options["min_free_bytes"], 123)
        self.assertEqual(options["max_database_bytes"], 456)
        self.assertEqual(options["max_archive_bytes"], 789)
        self.assertEqual(options["interval_hours"], 12)
        self.assertTrue(options["once"])

    def test_invalid_capacity_configuration_does_not_open_storage(self):
        cases = [
            ["--retain", "0"], ["--interval-hours", "nan"], ["--interval-hours", "0"],
            ["--min-free-bytes", "-1"], ["--max-database-bytes", "0"], ["--max-archive-bytes", "0"],
        ]
        with patch.object(storage_maintenance, "run_maintenance") as run:
            for arguments in cases:
                with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()), \
                        self.assertRaises(SystemExit) as result:
                    storage_maintenance.main(self.arguments(*arguments))
                self.assertEqual(result.exception.code, 2)
        run.assert_not_called()


@unittest.skipUnless(shutil.which("docker"), "Docker Compose is needed to validate the deployment configuration")
class ContainerStorageTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["docker", "compose", "--profile", "*", "-f", str(ROOT / "compose.yaml"),
             "config", "--format", "json", "--no-interpolate"],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode:
            raise AssertionError("Docker Compose configuration failed validation")
        cls.configuration = json.loads(result.stdout)

    def test_api_kafka_and_operations_share_persistent_database(self):
        for name in ("api", "kafka", "storage-maintenance", "profiles-preparation"):
            with self.subTest(service=name):
                service = self.configuration["services"][name]
                self.assertEqual(service["environment"]["STORAGE_PATH"], "/app/state/events/events.sqlite3")
                mount = next(item for item in service["volumes"] if item["target"] == "/app/state/events")
                self.assertEqual(mount["type"], "volume")
                self.assertEqual(mount["source"], "event_logs")
                self.assertFalse(mount.get("read_only", False))

    def test_api_can_start_without_kafka_or_openai_credentials(self):
        api = self.configuration["services"]["api"]
        self.assertEqual(set(api["depends_on"]), {"provision"})
        self.assertNotIn("OPENAI_API_KEY", api["environment"])
        self.assertNotIn("KAFKA_CONFIG", api["environment"])
        self.assertFalse(api.get("profiles"))

    def test_kafka_can_reach_host_tunnel_and_read_private_config(self):
        kafka = self.configuration["services"]["kafka"]
        self.assertEqual(kafka["network_mode"], "host")
        self.assertEqual(kafka["profiles"], ["kafka"])
        self.assertEqual(kafka["user"], "0:10001")
        mount = next(item for item in kafka["volumes"] if item["type"] == "bind")
        self.assertEqual(mount["target"], "/app/kafka-config/mlip-kafka.conf")
        self.assertTrue(mount["source"].replace("\\", "/").endswith("/mlip-kafka.conf"))
        self.assertTrue(mount["read_only"])
        self.assertFalse(mount["bind"]["create_host_path"])
        self.assertIn("cmu-movielog", kafka["command"])
        self.assertIn("movielog2", kafka["command"])
        self.assertIn('umask 0002; exec "$$@"', kafka["command"])
        self.assertIn("--event-timezone", kafka["command"])
        self.assertIn("--run-as-uid", kafka["command"])
        self.assertIn("--run-as-gid", kafka["command"])

    def test_archives_survive_recreation_and_diagnostics_are_compressed_and_bounded(self):
        maintenance = self.configuration["services"]["storage-maintenance"]
        mount = next(item for item in maintenance["volumes"] if item["target"] == "/app/state/archives")
        self.assertEqual((mount["type"], mount["source"]), ("volume", "event_archives"))
        for name, service in self.configuration["services"].items():
            with self.subTest(service=name):
                self.assertEqual(service["logging"]["driver"], "local")
                self.assertEqual(service["logging"]["options"], {
                    "max-size": "10m", "max-file": "3", "compress": "true",
                })

    def test_profile_preparation_is_an_optional_one_shot_with_shared_cache(self):
        service = self.configuration["services"]["profiles-preparation"]
        self.assertEqual(service["profiles"], ["profiles"])
        self.assertEqual(service["restart"], "no")
        self.assertIn("--storage-path", service["command"])
        self.assertIn("OPENAI_API_KEY", service["environment"])
        self.assertEqual(next(item for item in service["volumes"]
                              if item["target"] == "/app/state/cache")["source"], "cold_start_cache")

    def test_private_configuration_and_runtime_databases_are_excluded_from_build(self):
        patterns = (ROOT / ".dockerignore").read_text().splitlines()
        self.assertIn(".env", patterns)
        self.assertIn(".env.*", patterns)
        self.assertIn("*.sqlite*", patterns)
        self.assertIn("**/.config", patterns)
        dockerfile = (ROOT / "Dockerfile").read_text()
        self.assertNotIn("COPY . ", dockerfile)
        self.assertIn("USER app", dockerfile)


if __name__ == "__main__":
    unittest.main()
