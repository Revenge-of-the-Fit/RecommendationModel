import copy
import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from storage.requests import RequestLog, RequestStore, StorageError


def request_record(request_id="request-1", user_id=42):
    return {
        "request_id": request_id,
        "started_at": "2026-10-08T12:00:00+00:00",
        "finished_at": "2026-10-08T12:00:00.012000+00:00",
        "user_id": user_id,
        "path": f"/recommend/{user_id}",
        "method": "GET",
        "query": {"tag": ["first", "second"]},
        "request_metadata": {"headers": {"user-agent": "test-client"}},
        "recommendations": [
            {"movie_id": "movie_b", "rank": 1, "score": 0.75, "title": "Movie B"},
            {"movie_id": "movie_a", "rank": 2, "score": 0.25, "title": "Movie A"},
        ],
        "response_body": "movie_b,movie_a",
        "serving_method": "popularity",
        "fallback_reason": "cold_start_profile_unavailable",
        "status": 200,
        "error_type": None,
        "latency_ms": 12.0,
        "versions": {"code": "revision-1", "model": "model-2", "profile": None},
        "source_extension": {"field": "retained"},
    }


class RequestStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "persistent" / "requests.sqlite3"

    def tearDown(self):
        self.temporary.cleanup()

    def test_committed_request_survives_reopen(self):
        record = request_record()
        with RequestStore(self.path) as store:
            self.assertTrue(store.save_request(record))
            with closing(sqlite3.connect(self.path)) as reader:
                payload = reader.execute(
                    "SELECT record_json FROM recommendation_requests"
                ).fetchone()[0]
            self.assertEqual(json.loads(payload)["response_body"], "movie_b,movie_a")
        with RequestStore(self.path) as reopened:
            self.assertEqual(reopened.get_request("request-1"), {**record, "schema_version": 1})
            self.assertIsNone(reopened.get_request("missing"))

    def test_duplicate_request_id_keeps_original_record(self):
        with RequestStore(self.path) as store:
            self.assertTrue(store.save_request(request_record()))
            replay = request_record()
            replay["status"] = 500
            self.assertFalse(store.save_request(request_record()))
            with self.assertRaises(StorageError):
                store.save_request(replay)
            self.assertEqual(store.get_request("request-1")["status"], 200)
            self.assertEqual(len(store.list_requests(42)), 1)

    def test_query_is_user_scoped_chronological_and_bounded(self):
        later = request_record("later")
        later["started_at"] = "2026-10-08T09:00:00-04:00"
        with RequestStore(self.path) as store:
            store.save_request(later)
            store.save_request(request_record("earlier"))
            store.save_request(request_record("other-user", 43))
            self.assertEqual(
                [item["request_id"] for item in store.list_requests(42)], ["earlier", "later"]
            )
            self.assertEqual(store.list_requests(42, limit=1)[0]["request_id"], "earlier")
            with self.assertRaises(ValueError):
                store.list_requests(42, limit=0)

    def test_redaction_preserves_nonsecret_fields_and_usage(self):
        record = request_record()
        record["query"]["access_token"] = ["query-secret"]
        record["request_metadata"]["headers"].update({
            "Authorization": "Bearer auth-secret",
            "Cookie": "session=cookie-secret",
            "X-API-Key": "header-secret",
            "X-Trace-ID": "trace-1",
        })
        record["source_extension"] = [{"nested": {"password": "password-secret"}}]
        record["password_hint"] = "hint-secret"
        record["usage"] = {"input_tokens": 4, "output_tokens": 5, "total_tokens": 9}
        original = copy.deepcopy(record)
        with RequestStore(self.path) as store:
            store.save_request(record)
            saved = store.get_request("request-1")
        encoded = json.dumps(saved)
        for secret in ("query-secret", "auth-secret", "cookie-secret", "header-secret", "password-secret", "hint-secret"):
            self.assertNotIn(secret, encoded)
        self.assertEqual(saved["request_metadata"]["headers"]["X-Trace-ID"], "trace-1")
        self.assertEqual(saved["query"]["tag"], ["first", "second"])
        self.assertEqual(saved["usage"], record["usage"])
        self.assertEqual(record, original)

    def test_invalid_records_never_reach_storage(self):
        cases = [
            {"request_id": ""}, {"started_at": "not-a-time"},
            {"started_at": "2026-10-08T12:00:00"}, {"user_id": True},
            {"user_id": 2**63}, {"latency_ms": float("nan")},
            {"recommendations": [{"score": float("inf")}]},
        ]
        with RequestStore(self.path) as store:
            for changes in cases:
                with self.subTest(changes=changes), self.assertRaises(ValueError):
                    store.save_request({**request_record(), **changes})
            self.assertEqual(store.list_requests(42), [])

    def test_future_schema_is_rejected(self):
        self.path.parent.mkdir(parents=True)
        with closing(sqlite3.connect(self.path)) as connection:
            connection.execute("PRAGMA user_version=999")
        with self.assertRaises(StorageError):
            RequestStore(self.path)


class RequestLogTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "requests.sqlite3"
        self.logs = []

    def tearDown(self):
        for log in self.logs:
            if log.worker.is_alive():
                log.close()
        self.temporary.cleanup()

    def make_log(self, **kwargs):
        log = RequestLog(self.path, **kwargs)
        self.logs.append(log)
        return log

    def test_shutdown_drains_accepted_requests_and_duplicate_is_visible(self):
        log = self.make_log()
        self.assertTrue(log.submit(request_record()))
        self.assertTrue(log.submit(request_record()))
        self.assertTrue(log.submit(request_record("request-2")))
        self.assertTrue(log.close())
        status = log.status()
        self.assertEqual(status["accepted"], 3)
        self.assertEqual(status["written"], 2)
        self.assertEqual(status["duplicates"], 1)
        self.assertEqual(status["queued"], 0)
        with RequestStore(self.path) as store:
            self.assertEqual(len(store.list_requests(42)), 2)

    def test_concurrent_submissions_are_all_persisted(self):
        log = self.make_log(queue_size=100)
        outcomes = []
        threads = [
            threading.Thread(target=lambda i=i: outcomes.append(log.submit(request_record(f"request-{i}"))))
            for i in range(40)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive())
        self.assertEqual(outcomes, [True] * 40)
        self.assertTrue(log.close())
        with RequestStore(self.path) as store:
            self.assertEqual(len(store.list_requests(42)), 40)

    def test_full_queue_rejects_without_waiting_for_disk(self):
        entered = threading.Event()
        release = threading.Event()
        real_save = RequestStore.save_prepared

        def blocked_save(store, record):
            entered.set()
            if not release.wait(timeout=3):
                raise TimeoutError("test writer was not released")
            return real_save(store, record)

        with patch.object(RequestStore, "save_prepared", blocked_save):
            log = self.make_log(queue_size=1)
            try:
                self.assertTrue(log.submit(request_record("first")))
                self.assertTrue(entered.wait(timeout=2))
                self.assertTrue(log.submit(request_record("second")))
                with self.assertLogs("storage.requests", level="ERROR"):
                    self.assertFalse(log.submit(request_record("overflow")))
                self.assertEqual(log.status()["queued"], 1)
                self.assertEqual(log.status()["rejected"], 1)
                self.assertEqual(log.status()["last_error"], "queue_full")
                self.assertFalse(log.status()["healthy"])
            finally:
                release.set()
                self.assertTrue(log.close())
        with RequestStore(self.path) as store:
            self.assertIsNone(store.get_request("overflow"))
            self.assertEqual(len(store.list_requests(42)), 2)

    def test_write_failure_is_visible_and_writer_keeps_running(self):
        real_save = RequestStore.save_prepared

        def fail_first(store, record):
            if record[0] == "failed":
                raise sqlite3.OperationalError("credential-sentinel")
            return real_save(store, record)

        with patch.object(RequestStore, "save_prepared", fail_first):
            log = self.make_log()
            with self.assertLogs("storage.requests", level="ERROR") as captured:
                self.assertTrue(log.submit(request_record("failed")))
                self.assertTrue(log.submit(request_record("success")))
                self.assertFalse(log.close())
        self.assertEqual(log.status()["failed"], 1)
        self.assertEqual(log.status()["written"], 1)
        self.assertEqual(log.status()["last_error"], "OperationalError")
        self.assertNotIn("credential-sentinel", " ".join(captured.output))
        with RequestStore(self.path) as store:
            self.assertIsNone(store.get_request("failed"))
            self.assertIsNotNone(store.get_request("success"))

    def test_shutdown_rejects_new_work_and_waits_for_accepted_work(self):
        entered = threading.Event()
        release = threading.Event()
        closed = []
        real_save = RequestStore.save_prepared

        def blocked_save(store, record):
            entered.set()
            if not release.wait(timeout=3):
                raise TimeoutError("test writer was not released")
            return real_save(store, record)

        with patch.object(RequestStore, "save_prepared", blocked_save):
            log = self.make_log()
            try:
                log.submit(request_record())
                self.assertTrue(entered.wait(timeout=2))
                closing = threading.Thread(target=lambda: closed.append(log.close()))
                closing.start()
                self.assertTrue(log.closing.wait(timeout=2))
                with self.assertLogs("storage.requests", level="ERROR"):
                    self.assertFalse(log.submit(request_record("too-late")))
                self.assertEqual(closed, [])
            finally:
                release.set()
                closing.join(timeout=3)
                self.assertFalse(closing.is_alive())
        self.assertEqual(closed, [True])
        with RequestStore(self.path) as store:
            self.assertIsNotNone(store.get_request("request-1"))
            self.assertIsNone(store.get_request("too-late"))

    def test_submission_copies_payload_and_rejects_invalid_json(self):
        log = self.make_log()
        record = request_record()
        self.assertTrue(log.submit(record))
        record["recommendations"][0]["score"] = 99
        with self.assertLogs("storage.requests", level="ERROR"):
            self.assertFalse(log.submit({**request_record("invalid"), "latency_ms": float("nan")}))
        self.assertEqual(log.status()["last_error"], "invalid_record")
        self.assertTrue(log.close())
        with RequestStore(self.path) as store:
            self.assertEqual(store.get_request("request-1")["recommendations"][0]["score"], 0.75)

    def test_initialization_failure_has_a_safe_visible_error(self):
        self.path.mkdir()
        with self.assertLogs("storage.requests", level="ERROR"):
            with self.assertRaisesRegex(StorageError, "initialization failed"):
                RequestLog(self.path)


if __name__ == "__main__":
    unittest.main()
