import json
import sqlite3
import sys
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from models.settings import ServingSettings
from preferences import ColdStartError
from services.live_worker import LiveProfileWorker
from services.metadata import MetadataBatch, MetadataRateLimiter
from storage.live import LiveStore
from storage.requests import RequestStore


class LiveWorkerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.path = Path(self.temporary.name) / "events.sqlite3"
        self.now = datetime.now(timezone.utc)
        self.settings = ServingSettings(storage_path=self.path, profile_refresh_seconds=60, live_enabled=True)
        self.interpreter = Mock()
        self.interpreter._response_schema.return_value = {"type": "object", "required": ["liked_genres"]}
        self.saved = {"profile": {"liked_genres": ["Drama"]}, "_provenance": {"profile_id": "profile-1"}}
        self.interpreter.get_profile_record.return_value = (self.saved, False)

    def tearDown(self):
        self.temporary.cleanup()

    def worker(self):
        worker = LiveProfileWorker(
            self.settings, SimpleNamespace(interpreter=self.interpreter, model=None), clock=lambda: self.now,
        )
        worker.limiter = MetadataRateLimiter(0)
        return worker

    def enqueue(self, *users):
        with LiveStore(self.path) as live:
            for user in users:
                live.enqueue(user)

    def user(self, user_id):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            return dict(connection.execute("SELECT * FROM live_users WHERE user_id=?", (user_id,)).fetchone())

    def request(self, user_id, request_id, started_at=None):
        with RequestStore(self.path) as requests:
            requests.save_request({
                "request_id": request_id, "user_id": user_id,
                "started_at": (started_at or self.now).isoformat(),
            })

    @staticmethod
    def batch(likes="Drama", dislikes=None):
        return lambda _, ids: MetadataBatch(200, {
            user: {"user_id": int(user), "self_description_likes": likes, "self_description_dislikes": dislikes}
            for user in ids
        })

    def test_refresh_links_metadata_and_profiles_and_resumes_after_restart(self):
        self.enqueue(42)
        fetch = Mock(side_effect=self.batch())
        self.worker().run_once(fetch)
        first = self.user(42)
        prepared = json.loads(first["record_json"])
        self.assertEqual(prepared["dislikes"], "")
        self.assertEqual(prepared["profile"], self.saved)
        self.assertEqual(self.interpreter.get_profile_record.call_args.kwargs["context"], {
            "user_id": 42, "source_snapshot_id": prepared["snapshot_id"],
        })
        self.worker().run_once(fetch)
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.interpreter.get_profile_record.call_count, 1)
        self.now += timedelta(seconds=60)
        fetch.side_effect = self.batch("Comedy", "Horror")
        self.worker().run_once(fetch)
        refreshed = json.loads(self.user(42)["record_json"])
        self.assertEqual(refreshed["likes"], "Comedy")
        self.assertNotEqual(prepared["snapshot_id"], refreshed["snapshot_id"])
        self.assertNotEqual(prepared["metadata_version"], refreshed["metadata_version"])

    def test_transient_failure_preserves_profile_and_backoff_survives_restart(self):
        self.enqueue(42)
        self.worker().run_once(self.batch())
        original = self.user(42)["record_json"]
        self.now += timedelta(seconds=60)
        fetch = Mock(return_value=MetadataBatch(429, error_type="HttpError"))
        self.worker().run_once(fetch)
        failed = self.user(42)
        self.assertEqual(failed["record_json"], original)
        self.assertEqual(failed["attempts"], 1)
        self.assertEqual(failed["next_attempt_at"], self.now.timestamp() + 2)
        self.worker().run_once(fetch)
        self.assertEqual(fetch.call_count, 1)
        self.now += timedelta(seconds=2)
        fetch.return_value = MetadataBatch(200)
        self.worker().run_once(fetch)
        missing = self.user(42)
        self.assertEqual(missing["last_error"], "MissingMetadata")
        self.assertEqual(missing["next_attempt_at"], self.now.timestamp() + 4)
        self.assertEqual(missing["record_json"], original)
        self.now += timedelta(seconds=4)
        self.worker().run_once(self.batch("Comedy"))
        self.assertEqual(self.user(42)["attempts"], 0)
        self.assertEqual(json.loads(self.user(42)["record_json"])["likes"], "Comedy")

    def test_changed_interpretation_schema_refreshes_profile_with_fresh_cached_metadata(self):
        self.enqueue(42)
        fetch = Mock(side_effect=self.batch())
        self.worker().run_once(fetch)
        first = self.user(42)
        self.interpreter._response_schema.return_value = {"type": "object", "required": ["liked_genres", "excluded_genres"]}
        self.worker().run_once(fetch)
        self.assertNotEqual(first["interpretation_version"], self.user(42)["interpretation_version"])
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(self.interpreter.get_profile_record.call_count, 2)

    def test_null_descriptions_skip_llm_and_malformed_descriptions_use_daily_cooldown(self):
        self.enqueue(1, 2)
        fetch = Mock(return_value=MetadataBatch(200, {
            "1": {"user_id": 1, "self_description_likes": None, "self_description_dislikes": None},
            "2": {"user_id": 2, "self_description_likes": 0, "self_description_dislikes": ""},
        }))
        self.worker().run_once(fetch)
        self.assertIsNone(json.loads(self.user(1)["record_json"])["profile"])
        self.assertEqual(self.user(2)["last_error"], "ValueError")
        self.assertEqual(self.user(2)["next_attempt_at"], self.now.timestamp() + 60)
        self.interpreter.get_profile_record.assert_not_called()
        self.worker().run_once(fetch)
        self.assertEqual(fetch.call_count, 1)

    def test_provider_transient_failure_is_retried_but_authentication_error_is_not(self):
        self.enqueue(1, 2)
        self.interpreter.get_profile_record.side_effect = [
            ColdStartError("provider timeout", {"provider_error_type": "APITimeoutError"}),
            ColdStartError("provider authentication", {"http_status": 401}),
        ]
        self.worker().run_once(self.batch())
        self.assertEqual(self.user(1)["next_attempt_at"], self.now.timestamp() + 2)
        self.assertEqual(self.user(2)["next_attempt_at"], self.now.timestamp() + 60)

    def test_each_iteration_collects_no_more_than_200_users(self):
        self.enqueue(*range(1, 202))
        worker = self.worker()
        fetch = Mock(side_effect=self.batch(None))
        worker.run_once(fetch)
        worker.run_once(fetch)
        self.assertEqual([len(call.args[1]) for call in fetch.call_args_list], [200, 1])
        self.interpreter.get_profile_record.assert_not_called()

    def test_recent_requesters_advance_within_signup_and_request_priorities_without_bypassing_deadlines(self):
        with LiveStore(self.path) as live:
            for user, priority in ((1, 1), (999, 1), (2, 0), (998, 0), (3, 2), (997, 2), (1001, 0), (1002, 1)):
                live.enqueue(user, priority=priority)
            live.fail(1001, "APITimeoutError", True, 60, now=self.now.timestamp())
            live.complete(1002, {"profile": None}, "version", 60, now=self.now.timestamp())
        self.request(999, "recent-high-id")
        self.request(1, "late-inserted-old-request", self.now - timedelta(days=5))
        self.request(998, "active-signup")
        self.request(997, "active-watch-user")
        self.request(1001, "retrying-signup")
        self.request(1002, "refreshing-user")
        with LiveStore(self.path) as live:
            selected = [row["user_id"] for row in live.due_users(self.now.timestamp())]
        self.assertEqual(selected, [998, 2, 997, 999, 1, 3])
        self.assertEqual(self.user(997)["priority"], 1)
        self.assertEqual(self.user(3)["priority"], 2)
        self.assertEqual(self.user(1001)["attempts"], 1)
        self.assertEqual(self.user(1001)["next_attempt_at"], self.now.timestamp() + 2)
        self.assertEqual(self.user(1002)["next_attempt_at"], self.now.timestamp() + 60)

    def test_first_five_interpretations_yield_to_a_request_arriving_during_the_batch(self):
        self.enqueue(*range(1, 8))
        interpreted = []

        def interpret(*_, context, **__):
            interpreted.append(context["user_id"])
            if len(interpreted) == 1:
                self.request(999, "request-arrived-during-interpretation")
            return self.saved, False

        self.interpreter.get_profile_record.side_effect = interpret
        worker = self.worker()
        fetch = Mock(side_effect=self.batch())
        worker.run_once(fetch)
        self.assertEqual(interpreted, [1, 2, 3, 4, 5])
        self.assertIsNone(self.user(6)["record_json"])
        self.assertIsNone(self.user(7)["record_json"])
        worker.run_once(fetch)
        self.assertEqual(interpreted, [1, 2, 3, 4, 5, 999, 6, 7])
        self.assertEqual(fetch.call_args_list[0].args[1], [str(user) for user in range(1, 8)])
        self.assertEqual(fetch.call_args_list[1].args[1], ["999"])
        self.assertIsNotNone(self.user(999)["record_json"])

    def test_failed_provider_calls_consume_the_five_call_budget_and_later_users_stay_due(self):
        self.enqueue(*range(1, 8))
        self.interpreter.get_profile_record.side_effect = ColdStartError(
            "provider timeout", {"provider_error_type": "APITimeoutError"},
        )
        worker = self.worker()
        worker.run_once(self.batch())
        self.assertEqual(self.interpreter.get_profile_record.call_count, 5)
        for user in range(1, 6):
            self.assertEqual(self.user(user)["attempts"], 1)
            self.assertEqual(self.user(user)["next_attempt_at"], self.now.timestamp() + 2)
        for user in (6, 7):
            self.assertEqual(self.user(user)["attempts"], 0)
            self.assertIsNone(self.user(user)["last_error"])
            self.assertIsNone(self.user(user)["record_json"])
        worker.run_once(self.batch())
        selected = [call.kwargs["context"]["user_id"] for call in self.interpreter.get_profile_record.call_args_list]
        self.assertEqual(selected, list(range(1, 8)))

    def test_users_without_descriptions_do_not_consume_interpretation_budget(self):
        self.enqueue(*range(1, 9))

        def mixed_batch(_, ids):
            return MetadataBatch(200, {
                user: {"user_id": int(user), "self_description_likes": None if int(user) < 4 else "Drama", "self_description_dislikes": None}
                for user in ids
            })

        self.worker().run_once(mixed_batch)
        selected = [call.kwargs["context"]["user_id"] for call in self.interpreter.get_profile_record.call_args_list]
        self.assertEqual(selected, [4, 5, 6, 7, 8])
        for user in range(1, 9):
            self.assertIsNotNone(self.user(user)["record_json"])

    def test_one_background_thread_and_shutdown_wait_is_bounded(self):
        entered, release = threading.Event(), threading.Event()
        worker = self.worker()
        worker.poll_seconds = 0.01

        def blocked_iteration():
            entered.set()
            release.wait(2)

        with patch.object(worker, "run_once", side_effect=blocked_iteration) as run:
            try:
                worker.start()
                self.assertTrue(entered.wait(1))
                worker.start()
                self.assertEqual(run.call_count, 1)
                self.assertFalse(worker.close(0.01))
            finally:
                release.set()
                self.assertTrue(worker.close(1))


if __name__ == "__main__":
    unittest.main()
