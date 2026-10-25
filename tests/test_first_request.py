import json
import queue
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import test_live_users as fixtures
from preferences import ColdStartError, normalize_description
from storage.live import LiveStore


class FirstRequestTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.LiveUserTests(methodName="runTest")
        self.fixture.setUp()

    def tearDown(self):
        self.fixture.tearDown()

    def start_service(self, fetcher=None):
        f = self.fixture
        service = f.service()
        service.live_worker = f.worker(service.cold_start, fetcher)
        service.start()
        return service

    def test_uncached_first_request_prepares_without_signup_or_poll_delay(self):
        f = self.fixture
        with f.sdk() as create:
            service = self.start_service()
            service.live_worker.poll_seconds = 30
            started = time.monotonic()
            result = service.recommend(42)
            self.assertLess(time.monotonic() - started, 0.5)
            self.assertEqual(result.method, "llm_cold_start")
            self.assertTrue(result.preparation_attempted)
            self.assertNotIn("b", [item.movie_id for item in result.recommendations])
            self.assertEqual(create.call_count, 1)
            service.recommend(42)
            self.assertEqual(create.call_count, 1)

    def test_new_users_get_different_rankings_from_different_tastes(self):
        f = self.fixture
        f.records["43"] = {"user_id": 43, "self_description_likes": "Comedy and jokes", "self_description_dislikes": "Horror"}
        with f.sdk() as create:
            service = self.start_service()
            crime, comedy = service.recommend(42), service.recommend(43)
            self.assertEqual((crime.method, comedy.method), ("llm_cold_start", "llm_cold_start"))
            self.assertNotEqual([r.movie_id for r in crime.recommendations], [r.movie_id for r in comedy.recommendations])
            self.assertEqual(create.call_count, 2)

    def test_urgent_preparation_does_not_wait_for_historical_kafka_replay(self):
        f = self.fixture
        service = f.service()
        worker = f.worker(service.cold_start)
        with f.sdk(), patch.object(LiveStore, "ingest", side_effect=AssertionError("historical replay")):
            worker.run_once(user_id=42)
        self.assertEqual(service.recommend(42).method, "llm_cold_start")

    def test_slow_metadata_falls_back_and_concurrent_requests_share_one_paid_job(self):
        f = self.fixture
        entered, release = threading.Event(), threading.Event()

        def slow_fetch(kind, ids):
            entered.set()
            release.wait(3)
            return f.fetch(kind, ids)

        with f.sdk() as create:
            service = self.start_service(slow_fetch)
            service.settings = service.settings.model_copy(update={"cold_start_wait_seconds": 0.05})
            try:
                with ThreadPoolExecutor(max_workers=5) as pool:
                    futures = [pool.submit(service.recommend, 42) for _ in range(5)]
                    self.assertTrue(entered.wait(1))
                    results = [future.result(timeout=0.4) for future in futures]
                self.assertTrue(all(result.method == "popularity" for result in results))
                self.assertEqual(create.call_count, 0)
                self.assertEqual(len(service.live_worker.pending), 1)
                release.set()
                with service.live_worker.pending_lock:
                    completed = service.live_worker.pending.get(42)
                if completed is not None:
                    self.assertTrue(completed.wait(2))
                self.assertEqual(service.recommend(42).method, "llm_cold_start")
                self.assertEqual(create.call_count, 1)
                self.assertEqual(len(f.fetch_calls), 1)
            finally:
                release.set()

    def test_slow_decisions_and_expired_request_budget_return_fallback(self):
        f = self.fixture
        entered, release = threading.Event(), threading.Event()

        def slow_provider(**request):
            entered.set()
            release.wait(3)
            return f.provider_response(request)

        with f.sdk(slow_provider):
            service = self.start_service()
            service.settings = service.settings.model_copy(update={"cold_start_wait_seconds": 0.05})
            try:
                started = time.monotonic()
                result = service.recommend(42)
                self.assertTrue(entered.wait(1))
                self.assertLess(time.monotonic() - started, 0.4)
                self.assertEqual(result.method, "popularity")
                with patch.object(service.live_worker, "prepare", wraps=service.live_worker.prepare) as prepare:
                    service.recommend(43, deadline=time.monotonic() - 1)
                    self.assertEqual(prepare.call_args.args[1], 0)
            finally:
                release.set()

    def test_refusal_uses_fallback_and_does_not_bypass_persisted_retry_deadline(self):
        f = self.fixture
        with f.sdk(lambda **_: (_ for _ in ()).throw(ColdStartError("Refused"))) as create:
            service = self.start_service()
            self.assertEqual(service.recommend(42).method, "popularity")
            self.assertEqual(service.recommend(42).method, "popularity")
            self.assertEqual(create.call_count, 1)

    def test_queue_capacity_is_bounded_and_unchanged_preferences_normalize(self):
        f = self.fixture
        service = f.service()
        worker = f.worker(service.cold_start)
        worker.thread = threading.Thread()  # Queue behavior without a running consumer.
        worker.urgent = queue.Queue(maxsize=1)
        self.assertFalse(worker.prepare(42, 0.001))
        self.assertFalse(worker.prepare(43, 0.001))
        self.assertEqual(set(worker.pending), {42})
        worker.thread = None
        self.assertEqual(normalize_description("  Crime\n dramas  "), "Crime dramas")
        self.assertEqual(len(normalize_description("x" * 3000)), 2000)

    def test_dynamic_questions_and_named_movie_fallback_validate_answers(self):
        interpreter = self.fixture.service().cold_start.interpreter
        user_input = json.dumps({"likes": "Crime dramas", "dislikes": "Horror"})
        request = interpreter.decision_request(user_input)
        self.assertEqual([q["name"] for q in request["questions"]], ["Crime", "Drama", "Horror"])
        named = json.dumps({"likes": "The Godfather", "dislikes": ""})
        self.assertEqual(len(interpreter.decision_request(named)["questions"]), 2)
        result = interpreter._decision_profile({"answers": [
            {"type": "choice", "name": "liked_genre", "choice": "Crime"},
            {"type": "choice", "name": "excluded_genre", "choice": "none"},
        ]}, named)
        self.assertEqual(result["liked_genres"], ["Crime"])
        for answers in ([], [{"type": "refusal", "name": "Crime"}], [
            {"type": "choice", "name": "Crime", "choice": "like"},
            {"type": "choice", "name": "Crime", "choice": "like"},
            {"type": "choice", "name": "Horror", "choice": "avoid"},
        ]):
            with self.assertRaises(ColdStartError):
                interpreter._decision_profile({"answers": answers}, user_input)


if __name__ == "__main__":
    unittest.main()
