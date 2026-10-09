import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from contextlib import closing
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from uuid import UUID


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from fastapi.testclient import TestClient
from pydantic import ValidationError

from api.app import create_app
from api.request_logging import RequestLoggingMiddleware
from models.serving import RecommendationResult
from models.settings import ServingSettings
from storage.requests import RequestStore


def recommendation_result(user_id=42, **changes):
    return RecommendationResult.model_validate({
        "user_id": user_id,
        "method": "popularity",
        "recommendations": [
            {"movie_id": "movie_b", "title": "Movie B", "score": 0.75, "reason": "Popular"},
            {"movie_id": "movie_a", "title": "Movie A", "score": 0.25},
        ],
        "fallback_reason": "cold_start_profile_unavailable",
        "cached": False,
        "llm_model": None,
        "profile_version": "profile-3",
        **changes,
    })


class ApiRequestLoggingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "requests.sqlite3"
        self.settings = ServingSettings(storage_path=self.path)
        self.service = SimpleNamespace(
            versions={"model": "model-2", "dataset": "dataset-4"},
            recommend=Mock(return_value=recommendation_result()),
        )

    def tearDown(self):
        self.temporary.cleanup()

    def read_request(self, response):
        request_id = response.headers["x-request-id"]
        with RequestStore(self.path) as store:
            record = store.get_request(request_id)
        self.assertIsNotNone(record)
        return record

    def record_count(self):
        with closing(sqlite3.connect(self.path)) as connection:
            return connection.execute("SELECT COUNT(*) FROM recommendation_requests").fetchone()[0]

    def wait_for_storage(self, client, predicate):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            response = client.get("/health/storage")
            if predicate(response.json()):
                return response
            threading.Event().wait(0.01)
        self.fail("Storage did not reach the expected status")

    def test_success_persists_exact_response_order_metadata_and_versions(self):
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings)) as client:
                response = client.get(
                    "/recommend/42?tag=first&tag=second",
                    headers={"x-trace-id": "trace-1", "x-request-id": "caller-request-id"},
                )
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.text, "movie_b,movie_a")
                self.assertNotEqual(response.headers["x-request-id"], "caller-request-id")
                UUID(response.headers["x-request-id"])
        record = self.read_request(response)
        self.service.recommend.assert_called_once_with(42)
        self.assertEqual(record["request_id"], response.headers["x-request-id"])
        self.assertEqual(record["user_id"], 42)
        self.assertEqual(record["path"], "/recommend/42")
        self.assertEqual(record["method"], "GET")
        self.assertEqual(record["query"]["tag"], ["first", "second"])
        self.assertEqual(record["request_metadata"]["headers"]["x-trace-id"], ["trace-1"])
        self.assertEqual(record["status"], 200)
        self.assertIsNone(record["error_type"])
        self.assertEqual(record["response_body"], response.text)
        self.assertTrue(record["response_complete"])
        self.assertEqual(
            [(item["rank"], item["movie_id"], item["score"]) for item in record["recommendations"]],
            [(1, "movie_b", 0.75), (2, "movie_a", 0.25)],
        )
        self.assertEqual(record["recommendations"][0]["reason"], "Popular")
        self.assertEqual(record["serving_method"], "popularity")
        self.assertEqual(record["fallback_reason"], "cold_start_profile_unavailable")
        self.assertFalse(record["cached"])
        self.assertIsNone(record["llm_model"])
        self.assertEqual(record["versions"]["model"], "model-2")
        self.assertEqual(record["versions"]["dataset"], "dataset-4")
        self.assertEqual(record["versions"]["profile"], "profile-3")
        self.assertTrue(record["versions"]["code"])
        self.assertGreaterEqual(record["latency_ms"], 0)
        started = datetime.fromisoformat(record["started_at"])
        finished = datetime.fromisoformat(record["finished_at"])
        self.assertIsNotNone(started.utcoffset())
        self.assertIsNotNone(finished.utcoffset())
        self.assertGreaterEqual(finished, started)

    def test_cached_cold_start_response_retains_profile_and_provider_references(self):
        self.service.recommend.return_value = recommendation_result(
            method="llm_cold_start", cached=True, fallback_reason=None,
            profile_id="profile-1", llm_attempt_id="attempt-1",
            llm_response_id="response-1", llm_model="provider-model-snapshot",
            profile_origin="llm", prompt_version="sha256:prompt", schema_version="sha256:schema",
        )
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings)) as client:
                response = client.get("/recommend/42")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.text, "movie_b,movie_a")
        record = self.read_request(response)
        self.assertEqual(record["serving_method"], "llm_cold_start")
        self.assertTrue(record["cached"])
        self.assertEqual(record["llm_model"], "provider-model-snapshot")
        self.assertEqual(record["profile_reference"], {
            "profile_id": "profile-1", "attempt_id": "attempt-1", "response_id": "response-1",
            "origin": "llm", "prompt_version": "sha256:prompt", "schema_version": "sha256:schema",
        })
        self.assertEqual(record["versions"]["profile"], "profile-3")

    def test_each_request_generates_a_distinct_id(self):
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings)) as client:
                first = client.get("/recommend/42", headers={"x-request-id": "shared"})
                second = client.get("/recommend/42", headers={"x-request-id": "shared"})
        self.assertNotEqual(first.headers["x-request-id"], second.headers["x-request-id"])
        self.read_request(first)
        self.read_request(second)
        self.assertEqual(self.record_count(), 2)

    def test_validated_user_id_is_used_for_noncanonical_paths(self):
        for path in ("/recommend/%2B42", "/recommend/42.0", "/recommend/%2042%20"):
            with self.subTest(path=path):
                with patch("api.app.RecommendationService.load", return_value=self.service):
                    with TestClient(create_app(self.settings)) as client:
                        response = client.get(path)
                        self.assertEqual(response.status_code, 200)
                self.assertEqual(self.read_request(response)["user_id"], 42)

    def test_invalid_user_id_is_logged_without_persisting_error_response(self):
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings)) as client:
                response = client.get("/recommend/not-an-integer")
                self.assertEqual(response.status_code, 422)
        record = self.read_request(response)
        self.service.recommend.assert_not_called()
        self.assertIsNone(record["user_id"])
        self.assertEqual(record["status"], 422)
        self.assertTrue(record["error_type"])
        self.assertIsNone(record.get("response_body"))
        self.assertEqual(record["recommendations"], [])
        self.assertTrue(record["response_complete"])

    def test_service_startup_failure_still_logs_unavailable_requests(self):
        with patch("api.app.RecommendationService.load", side_effect=OSError("startup-secret")):
            with TestClient(create_app(self.settings)) as client:
                self.assertEqual(client.get("/health/ready").status_code, 503)
                response = client.get("/recommend/42")
                self.assertEqual(response.status_code, 503)
                self.assertEqual(client.get("/health/storage").status_code, 200)
        record = self.read_request(response)
        self.assertEqual(record["user_id"], 42)
        self.assertEqual(record["status"], 503)
        self.assertTrue(record["error_type"])
        self.assertNotIn("startup-secret", json.dumps(record))
        self.assertIsNone(record.get("response_body"))
        self.assertEqual(self.record_count(), 1)

    def test_handler_failures_keep_safe_error_categories(self):
        try:
            RecommendationResult.model_validate({"user_id": 42, "method": "popularity", "recommendations": []})
        except ValidationError as error:
            validation_error = error
        for error in (ValueError("value-secret"), OSError("disk-secret"), validation_error):
            with self.subTest(error_type=type(error).__name__):
                self.service.recommend.side_effect = error
                with patch("api.app.RecommendationService.load", return_value=self.service):
                    with TestClient(create_app(self.settings)) as client:
                        with self.assertLogs("api", level="ERROR") as captured:
                            response = client.get("/recommend/42")
                        self.assertEqual(response.status_code, 503)
                record = self.read_request(response)
                self.assertEqual(record["error_type"], type(error).__name__)
                self.assertEqual(record["status"], 503)
                self.assertEqual(record["recommendations"], [])
                self.assertIsNone(record.get("response_body"))
                encoded = json.dumps(record) + " ".join(captured.output)
                self.assertNotIn("value-secret", encoded)
                self.assertNotIn("disk-secret", encoded)

    def test_unexpected_error_records_actual_500_without_exception_message(self):
        self.service.recommend.side_effect = RuntimeError("unexpected-secret")
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings), raise_server_exceptions=False) as client:
                response = client.get("/recommend/42")
                self.assertEqual(response.status_code, 500)
                self.assertEqual(response.text, "Internal Server Error")
        record = self.read_request(response)
        self.assertEqual(record["status"], 500)
        self.assertEqual(record["error_type"], "RuntimeError")
        self.assertEqual(record["recommendations"], [])
        self.assertIsNone(record.get("response_body"))
        self.assertTrue(record["response_complete"])
        self.assertNotIn("unexpected-secret", json.dumps(record))

    def test_credentials_in_headers_query_and_referer_are_redacted(self):
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings)) as client:
                response = client.get(
                    "/recommend/42?access_token=query-secret&tag=visible&redirect=https%3A%2F%2Fexample.com%2F%3Ftoken%3Dredirect-secret",
                    headers={
                        "authorization": "Bearer auth-secret",
                        "cookie": "session=cookie-secret",
                        "x-api-key": "api-secret",
                        "referer": "https://url-user:url-password@example.com/page?token=referer-secret&tag=visible",
                        "x-trace-id": "trace-1",
                        "x-source-url": "https://extra-user:extra-password@example.com/?token=extra-secret",
                    },
                )
        record = self.read_request(response)
        encoded = json.dumps(record)
        for secret in ("query-secret", "auth-secret", "cookie-secret", "api-secret", "url-user", "url-password", "referer-secret", "redirect-secret", "extra-user", "extra-password", "extra-secret"):
            self.assertNotIn(secret, encoded)
        self.assertEqual(record["query"]["tag"], ["visible"])
        self.assertEqual(record["request_metadata"]["headers"]["x-trace-id"], ["trace-1"])
        self.assertEqual(record["response_body"], "movie_b,movie_a")

    def test_health_checks_are_excluded_and_storage_status_is_visible(self):
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with TestClient(create_app(self.settings)) as client:
                self.assertEqual(client.get("/health/ready").json(), {"status": "ready"})
                before = client.get("/health/storage")
                self.assertEqual(before.status_code, 200)
                self.assertTrue(before.json()["healthy"])
                self.assertEqual(before.json()["accepted"], 0)
                response = client.get("/recommend/42")
                after = self.wait_for_storage(client, lambda status: status["written"] == 1)
                self.assertEqual(after.status_code, 200)
                self.assertEqual(after.json()["accepted"], 1)
                self.assertEqual(after.json()["failed"], 0)
                self.assertEqual(after.json()["rejected"], 0)
                self.assertTrue(after.json()["last_write_at"])
        self.read_request(response)
        self.assertEqual(self.record_count(), 1)

    def test_writer_failure_degrades_health_without_changing_recommendations(self):
        with patch.object(RequestStore, "save_prepared", side_effect=sqlite3.OperationalError("writer-secret")):
            with patch("api.app.RecommendationService.load", return_value=self.service):
                with self.assertLogs("storage.requests", level="ERROR") as captured:
                    with TestClient(create_app(self.settings)) as client:
                        response = client.get("/recommend/42")
                        self.assertEqual(response.status_code, 200)
                        self.assertEqual(response.text, "movie_b,movie_a")
                        health = self.wait_for_storage(client, lambda status: status["failed"] == 1)
                        self.assertEqual(health.status_code, 503)
                        self.assertFalse(health.json()["healthy"])
                        self.assertEqual(health.json()["last_error"], "OperationalError")
                self.assertNotIn("writer-secret", " ".join(captured.output))
        with RequestStore(self.path) as store:
            self.assertIsNone(store.get_request(response.headers["x-request-id"]))

    def test_storage_initialization_failure_preserves_recommendation_availability(self):
        self.path.mkdir()
        with patch("api.app.RecommendationService.load", return_value=self.service):
            with self.assertLogs(level="ERROR") as captured:
                with TestClient(create_app(self.settings)) as client:
                    self.assertEqual(client.get("/health/ready").status_code, 200)
                    response = client.get("/recommend/42")
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(response.text, "movie_b,movie_a")
                    self.assertTrue(response.headers["x-request-id"])
                    health = client.get("/health/storage")
                    self.assertEqual(health.status_code, 503)
                    self.assertFalse(health.json()["healthy"])
                    self.assertTrue(health.json()["last_error"])
        self.assertTrue(captured.output)

    def test_blocked_writer_never_holds_response_and_queue_overflow_is_visible(self):
        entered = threading.Event()
        release = threading.Event()
        real_save = RequestStore.save_prepared
        threads = []
        responses = []

        def blocked_save(store, record):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("test writer was not released")
            return real_save(store, record)

        def request_in_background(client):
            done = threading.Event()

            def request():
                try:
                    responses.append(client.get("/recommend/42"))
                finally:
                    done.set()

            thread = threading.Thread(target=request)
            threads.append(thread)
            thread.start()
            return done

        settings = self.settings.model_copy(update={"request_log_queue_size": 1})
        with patch.object(RequestStore, "save_prepared", blocked_save):
            with patch("api.app.RecommendationService.load", return_value=self.service):
                with TestClient(create_app(settings)) as client:
                    try:
                        first_done = request_in_background(client)
                        self.assertTrue(entered.wait(timeout=2))
                        self.assertTrue(first_done.wait(timeout=2), "Response waited for the blocked writer")
                        second_done = request_in_background(client)
                        self.assertTrue(second_done.wait(timeout=2), "Response waited for the blocked writer")
                        with self.assertLogs("storage.requests", level="ERROR"):
                            third_done = request_in_background(client)
                            self.assertTrue(third_done.wait(timeout=2))
                        self.assertEqual([response.status_code for response in responses], [200, 200, 200])
                        health = client.get("/health/storage")
                        self.assertEqual(health.status_code, 503)
                        self.assertEqual(health.json()["accepted"], 2)
                        self.assertEqual(health.json()["rejected"], 1)
                        self.assertEqual(health.json()["queued"], 1)
                        self.assertEqual(health.json()["last_error"], "queue_full")
                    finally:
                        release.set()
                        for thread in threads:
                            thread.join(timeout=3)
                            self.assertFalse(thread.is_alive())
        self.read_request(responses[0])
        self.read_request(responses[1])
        with RequestStore(self.path) as store:
            self.assertIsNone(store.get_request(responses[2].headers["x-request-id"]))


class ResponseHandoffTests(unittest.IsolatedAsyncioTestCase):
    async def run_response(self, fail_send=False, fail_after_response=False):
        records = []
        application_state = SimpleNamespace(
            request_log=SimpleNamespace(submit=lambda record: records.append(record)),
            code_version="code-1", request_log_capture_failed=0,
            request_log_unavailable=0, request_log_error=None,
        )
        scope = {
            "type": "http", "path": "/recommend/42", "method": "GET",
            "path_params": {"userid": "42"}, "state": {},
        }

        async def app(scope, receive, send):
            scope["state"]["recommendation_result"] = recommendation_result()
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b"movie_b,movie_a"})
            if fail_after_response:
                raise RuntimeError("late-secret")

        async def receive():
            return {"type": "http.request", "body": b""}

        async def send(message):
            if fail_send and message["type"] == "http.response.body":
                raise ConnectionError("send-secret")

        middleware = RequestLoggingMiddleware(app, application_state)
        with self.assertRaises(ConnectionError if fail_send else RuntimeError):
            await middleware(scope, receive, send)
        self.assertEqual(len(records), 1)
        self.assertEqual(application_state.request_log_capture_failed, 0)
        return records[0]

    async def test_failed_handoff_is_never_recorded_as_a_complete_response(self):
        record = await self.run_response(fail_send=True)
        self.assertEqual(record["status"], 200)
        self.assertFalse(record["response_complete"])
        self.assertIsNone(record["response_body"])
        self.assertEqual(record["recommendations"], [])
        self.assertEqual(record["error_type"], "ConnectionError")
        self.assertNotIn("send-secret", json.dumps(record))

    async def test_late_failure_preserves_the_status_and_ids_already_sent(self):
        record = await self.run_response(fail_after_response=True)
        self.assertEqual(record["status"], 200)
        self.assertTrue(record["response_complete"])
        self.assertEqual(record["response_body"], "movie_b,movie_a")
        self.assertEqual(record["error_type"], "RuntimeError")
        self.assertNotIn("late-secret", json.dumps(record))


if __name__ == "__main__":
    unittest.main()
