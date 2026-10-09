import os
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi.testclient import TestClient
from api.app import create_app
from models.serving import RecommendationResult
from models.settings import ServingSettings
from storage.requests import RequestStore


class StorageCapacityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"
        self.record = {
            "request_id": "original", "started_at": "2026-10-09T12:00:00+00:00",
            "user_id": 42, "status": 200, "recommendations": [],
        }

    def tearDown(self):
        self.temporary.cleanup()

    def test_page_capacity_rejects_growth_without_losing_existing_records(self):
        with RequestStore(self.path) as store:
            store.save_request(self.record)
            original = store.get_request("original")
        with closing(sqlite3.connect(self.path)) as connection:
            maximum = (connection.execute("PRAGMA page_count").fetchone()[0] + 2) * connection.execute("PRAGMA page_size").fetchone()[0]
        with patch.dict(os.environ, {"STORAGE_MAX_BYTES": str(maximum)}):
            with RequestStore(self.path) as store:
                with self.assertRaises(sqlite3.DatabaseError):
                    store.save_request({**self.record, "request_id": "oversized", "payload": "x" * 250000})
                self.assertEqual(store.get_request("original"), original)
                self.assertIsNone(store.get_request("oversized"))
                self.assertEqual(store.connection.execute("PRAGMA integrity_check").fetchone()[0], "ok")

    def test_capacity_failure_is_visible_while_recommendations_keep_serving(self):
        settings = ServingSettings(storage_path=self.path, storage_max_bytes=1, storage_min_free_bytes=0)
        service = SimpleNamespace(versions={}, recommend=lambda user_id: RecommendationResult(
            user_id=user_id, method="popularity",
            recommendations=[{"movie_id": "movie_a", "title": "Movie A", "score": 1}],
        ))
        with patch("api.app.RecommendationService.load", return_value=service):
            with TestClient(create_app(settings)) as client:
                health = client.get("/health/storage")
                self.assertEqual(health.status_code, 503)
                self.assertIn("maximum_database_size", health.json()["capacity"]["capacity_reasons"])
                response = client.get("/recommend/42")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.text, "movie_a")
        with RequestStore(self.path) as store:
            self.assertIsNotNone(store.get_request(response.headers["x-request-id"]))


if __name__ == "__main__":
    unittest.main()
