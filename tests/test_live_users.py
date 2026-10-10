import copy
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import numpy as np
import pandas as pd
from fastapi.testclient import TestClient
from openai import RateLimitError


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from api.app import create_app
from events.parser import parse_event
from models.settings import ServingSettings
from recommender import EaseRecommender
from services.metadata import MetadataBatch, MetadataRateLimiter
from services.recommendation import RecommendationService
from storage.events import EventStore, KafkaEnvelope
from storage.live import LiveStore, read_live_user
from storage.metadata import MetadataStore
from storage.profiles import ProfileStore
from storage.requests import RequestStore


class MutableClock:
    def __init__(self):
        self.current = datetime(2026, 10, 10, 12, tzinfo=timezone.utc)
        self.lock = threading.Lock()

    def __call__(self):
        with self.lock:
            return self.current

    def advance(self, seconds):
        with self.lock:
            self.current += timedelta(seconds=seconds)

    def iso(self):
        return self().isoformat(timespec="microseconds")


class ProviderResponse(SimpleNamespace):
    def model_dump(self, **_):
        return copy.deepcopy(self.envelope)


class LiveUserTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.path = self.directory / "live.sqlite3"
        self.clock = MutableClock()
        self.settings = ServingSettings(
            data_directory=self.directory, model_path=self.directory / "model.npz",
            cache_directory=self.directory / "profiles", storage_path=self.path,
            storage_min_free_bytes=0, live_enabled=True, profile_refresh_seconds=10,
        )
        movies = pd.DataFrame([
            ("a", "Movie A", "Crime|Drama", "A detective investigates a crime."),
            ("b", "Movie B", "Horror", "A frightening ghost story."),
            ("c", "Movie C", "Crime", "A crime investigation and detectives."),
            ("d", "Movie D", "Comedy", "A clever comedy and jokes."),
            ("e", "Movie E", "Drama", "A thoughtful family drama."),
            ("f", "Movie F", "Documentary", "A nature documentary."),
        ], columns=["movie_id", "title", "genres", "overview"])
        movies["runtime"] = "90"
        movies["license_cost"] = "1"
        users = pd.DataFrame([(1, "", ""), (2, "", ""), (3, "", "")], columns=["user_id", "self_description_likes", "self_description_dislikes"])
        ratings = [(1, "a", 8), (1, "b", 4), (2, "a", 8), (2, "c", 9), (2, "e", 6), (3, "b", 7), (3, "d", 8), (3, "f", 8)]
        events = pd.DataFrame([
            (f"2026-10-09T12:00:{number:02d}Z", user, "rating", movie, rating)
            for number, (user, movie, rating) in enumerate(ratings)
        ], columns=["timestamp", "user_id", "event_type", "movie_id", "rating"])
        for name, table in (("movies", movies), ("users", users), ("events", events)):
            table.to_csv(self.directory / f"{name}.csv.gz", index=False)
        interactions = pd.DataFrame([(user, movie, rating, 0) for user, movie, rating in ratings], columns=["user_id", "movie_id", "rating", "watch_count"])
        self.model = EaseRecommender(regularization=2)
        self.model.fit(interactions, movies)
        self.model.save(self.settings.model_path)
        with LiveStore(self.path):
            pass
        self.records = {
            "42": {"user_id": 42, "age": "30", "occupation": "engineer", "gender": "F", "self_description_likes": "Crime dramas", "self_description_dislikes": "Horror"},
            "1": {"user_id": 1, "self_description_likes": None, "self_description_dislikes": None},
        }
        self.fetch_calls = []
        self.workers = []

    def tearDown(self):
        for worker in self.workers:
            worker.close()
        self.temporary.cleanup()

    def fetch(self, kind, ids):
        self.fetch_calls.append((kind, list(ids)))
        return MetadataBatch(200, records={value: copy.deepcopy(self.records[value]) for value in ids if value in self.records}, response={"body": [copy.deepcopy(self.records[value]) for value in ids if value in self.records], "headers": [["content-type", "application/json"]]})

    def provider_response(self, request, response_id="provider-response"):
        descriptions = json.loads(request["input"])
        comedy = "comedy" in descriptions["likes"].lower()
        profile = {
            "liked_genres": ["Comedy"] if comedy else ["Crime", "Drama"],
            "excluded_genres": ["Horror"], "liked_titles": [], "disliked_titles": [],
            "likes_summary": "Comedy and jokes" if comedy else "Crime dramas and detectives",
            "dislikes_summary": "Horror",
        }
        text = json.dumps(profile)
        usage = {"input_tokens": 10, "output_tokens": 20, "total_tokens": 30}
        envelope = {
            "id": response_id, "model": "provider-model-snapshot", "status": "completed",
            "output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}],
            "usage": usage, "provider_extension": {"nullable": None},
        }
        return ProviderResponse(id=response_id, model=envelope["model"], status="completed", output_text=text, usage=SimpleNamespace(model_dump=lambda **_: copy.deepcopy(usage)), envelope=envelope)

    @contextmanager
    def sdk(self, callback=None):
        client = MagicMock()
        client.__enter__.return_value = client
        create = client.responses.create
        create.side_effect = callback or (lambda **request: self.provider_response(request, f"provider-response-{create.call_count}"))
        with patch.dict("os.environ", {"OPENAI_API_KEY": "test-provider-key"}):
            with patch("preferences.dotenv_values", return_value={}):
                with patch("preferences.utc_timestamp", side_effect=self.clock.iso):
                    with patch("preferences.OpenAI", return_value=client):
                        yield create

    def event(self, offset, body, *, user=42, source="cmu-movielog", topic="movielog2", timestamp=None):
        value = f"{timestamp or self.clock.iso()},{user},{body}".encode("utf-8")
        envelope = KafkaEnvelope(source, topic, 0, offset, value, ingested_at=self.clock.iso())
        with EventStore(self.path) as events:
            inserted = events.save_event(envelope, parse_event(value))
        return envelope, inserted

    def worker(self, cold_start, fetcher=None):
        from services.live_worker import LiveProfileWorker
        worker = LiveProfileWorker(self.settings, cold_start, fetcher=fetcher or self.fetch, clock=self.clock, poll_seconds=0.01)
        worker.limiter = MetadataRateLimiter(0)
        self.workers.append(worker)
        return worker

    def service(self):
        service = RecommendationService.load(self.settings)
        if service.live_worker is not None:
            service.live_worker.close()
        service.live_worker = None
        return service

    def state(self, user=42):
        return read_live_user(self.path, user, self.settings.live_source_id, self.settings.live_topic)

    def queue(self, user=42):
        with closing(sqlite3.connect(self.path)) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM live_users WHERE user_id=?", (user,)).fetchone()
        return dict(row) if row else None

    def rows(self, table):
        with closing(sqlite3.connect(self.path)) as connection:
            return [json.loads(row[0]) for row in connection.execute(f"SELECT record_json FROM {table}")]

    def wait_until(self, predicate):
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            if predicate():
                return
            threading.Event().wait(0.01)
        self.fail("Background preparation did not reach the expected state")

    def expected_ease(self, service, positive, seen):
        model = service.model
        columns = [model.movie_index[movie] for movie in positive]
        scores = model.weights[columns].astype(np.float64).sum(axis=0)
        eligible = [index for index, movie in enumerate(model.movie_ids) if movie not in seen and model.popularity[index] > 0]
        ranked = sorted(eligible, key=lambda index: (-scores[index], -model.popularity[index], model.movie_ids[index]))[:20]
        return [model.movie_ids[index] for index in ranked], [scores[index] for index in ranked]

    def test_signup_prepares_without_http_and_later_response_keeps_audited_profile(self):
        service = self.service()
        worker = self.worker(service.cold_start)
        self.event(1, "GET /create_account")
        with self.sdk() as create:
            worker.run_once()
            self.assertEqual(create.call_count, 1)
        prepared = self.state()["prepared"]
        self.assertEqual(prepared["likes"], "Crime dramas")
        self.assertEqual(prepared["dislikes"], "Horror")
        saved = prepared["profile"]
        reference = saved["_provenance"]
        with ProfileStore(self.path) as profiles:
            attempt = profiles.get_attempt(reference["attempt_id"])
            profile = profiles.get_profile(reference["profile_id"])
        self.assertEqual(attempt["status"], "success")
        self.assertEqual(attempt["user_id"], 42)
        self.assertEqual(attempt["source_snapshot_id"], prepared["snapshot_id"])
        self.assertEqual(attempt["response"]["model"], "provider-model-snapshot")
        self.assertEqual(attempt["usage"]["total_tokens"], 30)
        self.assertEqual(profile["profile"], saved["profile"])
        with patch("preferences.PreferenceInterpreter._request", side_effect=AssertionError("HTTP path called provider")):
            with patch("api.app.RecommendationService.load", return_value=service):
                with TestClient(create_app(self.settings)) as client:
                    response = client.get("/recommend/42")
        self.assertEqual(response.status_code, 200)
        self.assertNotIn("b", response.text.split(","))
        with RequestStore(self.path) as requests:
            logged = requests.get_request(response.headers["x-request-id"])
        self.assertEqual(logged["serving_method"], "llm_cold_start")
        self.assertTrue(logged["cached"])
        self.assertEqual(logged["response_body"], response.text)
        self.assertEqual(logged["profile_reference"]["profile_id"], reference["profile_id"])
        self.assertEqual(logged["profile_reference"]["attempt_id"], reference["attempt_id"])
        self.assertEqual(logged["profile_reference"]["response_id"], saved["response_id"])
        self.assertTrue(logged["versions"]["profile"])

    def test_pending_metadata_and_provider_leave_concurrent_http_requests_under_budget(self):
        service = self.service()
        metadata_started, release_metadata = threading.Event(), threading.Event()
        provider_started, release_provider = threading.Event(), threading.Event()
        metadata_calls = []

        def blocked_fetch(kind, ids):
            metadata_calls.append((kind, list(ids)))
            if "42" in ids:
                metadata_started.set()
                if not release_metadata.wait(5):
                    raise AssertionError("Metadata synchronization timed out")
            return self.fetch(kind, ids)

        def blocked_provider(**request):
            provider_started.set()
            if not release_provider.wait(5):
                raise AssertionError("Provider synchronization timed out")
            return self.provider_response(request)

        service.live_worker = self.worker(service.cold_start, blocked_fetch)

        def timed_get(client, user):
            started = time.perf_counter()
            response = client.get(f"/recommend/{user}")
            return response, time.perf_counter() - started

        with self.sdk(blocked_provider) as create:
            with patch("api.app.RecommendationService.load", return_value=service):
                with TestClient(create_app(self.settings)) as client:
                    try:
                        first, elapsed = timed_get(client, 42)
                        self.assertEqual(first.status_code, 200)
                        self.assertLess(elapsed, 0.6)
                        self.assertTrue(metadata_started.wait(3))
                        with ThreadPoolExecutor(max_workers=8) as executor:
                            futures = [executor.submit(timed_get, client, user) for user in (42, 1, 42, 1, 42, 1, 42, 1)]
                            for future in futures:
                                response, elapsed = future.result(timeout=0.6)
                                self.assertEqual(response.status_code, 200)
                                self.assertLess(elapsed, 0.6)
                                identifiers = response.text.split(",")
                                self.assertTrue(1 <= len(identifiers) <= 20)
                                self.assertEqual(len(identifiers), len(set(identifiers)))
                        release_metadata.set()
                        self.assertTrue(provider_started.wait(3))
                        during_provider, elapsed = timed_get(client, 42)
                        self.assertEqual(during_provider.status_code, 200)
                        self.assertLess(elapsed, 0.6)
                        self.assertEqual(create.call_count, 1)
                        release_provider.set()
                        self.wait_until(lambda: self.state()["prepared"] is not None)
                        ready, elapsed = timed_get(client, 42)
                        self.assertEqual(ready.status_code, 200)
                        self.assertLess(elapsed, 0.6)
                    finally:
                        release_metadata.set()
                        release_provider.set()
        self.assertEqual(sum("42" in ids for _, ids in metadata_calls), 1)
        with closing(sqlite3.connect(self.path)) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM live_users WHERE user_id=42").fetchone()[0], 1)
        with RequestStore(self.path) as requests:
            before = requests.get_request(first.headers["x-request-id"])
            after = requests.get_request(ready.headers["x-request-id"])
        self.assertEqual(before["serving_method"], "popularity")
        self.assertEqual(after["serving_method"], "llm_cold_start")
        self.assertTrue(after["profile_reference"]["profile_id"])

    def test_live_history_changes_ease_ranking_with_latest_ratings_source_isolation_and_replay(self):
        service = self.service()
        unchanged = service.model.user_profiles.copy()
        original, inserted = self.event(1, "GET /data/m/a/17.mpg")
        self.assertTrue(inserted)
        first = service.recommend(42)
        self.assertEqual(first.method, "ease")
        identifiers, scores = self.expected_ease(service, {"a"}, {"a"})
        self.assertEqual([item.movie_id for item in first.recommendations], identifiers)
        np.testing.assert_allclose([item.score for item in first.recommendations], scores)
        self.event(2, "GET /data/m/a/18.mpg")
        with EventStore(self.path) as store:
            self.assertFalse(store.save_event(original, parse_event(original.value)))
        repeated = service.recommend(42)
        self.assertEqual(first, repeated)
        self.event(3, "GET /rate/a=9", timestamp="2026-10-10T12:01:00Z")
        self.event(4, "GET /rate/a=1", timestamp="2026-10-10T12:02:00Z")
        self.event(5, "GET /rate/a=8", timestamp="2026-10-10T12:00:00Z")
        self.event(6, "GET /data/m/b/1.mpg", source="different-cluster")
        self.event(7, "GET /data/m/c/1.mpg", topic="different-topic")
        self.event(8, "GET /data/m/d/1.mpg")
        state = self.state()
        self.assertEqual(state["watched"], {"a", "d"})
        self.assertEqual(state["ratings"], {"a": 1})
        identifiers, scores = self.expected_ease(service, {"d"}, {"a", "d"})
        restarted = self.service().recommend(42)
        self.assertEqual(restarted.method, "ease")
        self.assertEqual([item.movie_id for item in restarted.recommendations], identifiers)
        np.testing.assert_allclose([item.score for item in restarted.recommendations], scores)
        self.event(9, "GET /rate/a=1", user=1)
        self.event(10, "GET /data/m/c/1.mpg", user=1)
        overridden = service.recommend(1)
        identifiers, scores = self.expected_ease(service, {"c"}, {"a", "b", "c"})
        self.assertEqual([item.movie_id for item in overridden.recommendations], identifiers)
        np.testing.assert_allclose([item.score for item in overridden.recommendations], scores)
        np.testing.assert_array_equal(service.model.user_profiles, unchanged)

    def test_live_watch_preserves_static_negative_rating_until_explicit_rerating(self):
        service = self.service()
        self.event(1, "GET /data/m/b/1.mpg", user=1)
        watched = service.recommend(1)
        identifiers, scores = self.expected_ease(service, {"a"}, {"a", "b"})
        self.assertEqual([item.movie_id for item in watched.recommendations], identifiers)
        np.testing.assert_allclose([item.score for item in watched.recommendations], scores)
        self.event(2, "GET /rate/b=9", user=1)
        rerated = service.recommend(1)
        identifiers, scores = self.expected_ease(service, {"a", "b"}, {"a", "b"})
        self.assertEqual([item.movie_id for item in rerated.recommendations], identifiers)
        np.testing.assert_allclose([item.score for item in rerated.recommendations], scores)

    def test_metadata_and_provider_retry_survive_restart_and_permanent_failure_cools_down(self):
        self.settings = self.settings.model_copy(update={"profile_refresh_seconds": 1000})
        service = self.service()
        calls = []

        def flaky_metadata(kind, ids):
            calls.append(list(ids))
            if len(calls) == 1:
                return MetadataBatch(429, response={"body": {"error": "temporary throttle"}})
            if "43" in ids:
                return MetadataBatch(400, response={"body": {"error": "permanent request failure"}})
            return self.fetch(kind, ids)

        def flaky_provider(**request):
            if create.call_count == 1:
                response = httpx.Response(429, json={"error": {"message": "private provider diagnostic"}}, request=httpx.Request("POST", "https://provider.invalid/v1/responses"))
                raise RateLimitError("private provider diagnostic", response=response, body=response.json())
            return self.provider_response(request)

        worker = self.worker(service.cold_start, flaky_metadata)
        self.event(1, "GET /create_account")
        with self.sdk(flaky_provider) as create:
            worker.run_once()
            first_failure = self.queue()
            self.assertEqual(first_failure["attempts"], 1)
            self.assertEqual(first_failure["next_attempt_at"], self.clock().timestamp() + 2)
            worker.run_once()
            self.assertEqual(len(calls), 1)
            self.clock.advance(2)
            worker.run_once()
            second_failure = self.queue()
            self.assertEqual(second_failure["attempts"], 2)
            self.assertEqual(second_failure["last_error"], "ColdStartError")
            self.assertEqual(create.call_count, 1)
            worker.close()
            replacement = self.worker(service.cold_start, flaky_metadata)
            replacement.run_once()
            self.assertEqual(create.call_count, 1)
            self.clock.advance(second_failure["next_attempt_at"] - self.clock().timestamp())
            replacement.run_once()
            self.assertEqual(create.call_count, 2)
            self.assertEqual(len(calls), 2)
            self.assertEqual(self.queue()["attempts"], 0)
            self.assertIsNotNone(self.state()["prepared"]["profile"])
            self.event(2, "GET /create_account", user=43)
            replacement.run_once()
            permanent = self.queue(43)
            self.assertEqual(permanent["attempts"], 1)
            self.assertEqual(permanent["next_attempt_at"], self.clock().timestamp() + 1000)
            with RequestStore(self.path) as requests:
                requests.save_request({"request_id": "during-cooldown", "user_id": 43, "started_at": self.clock.iso()})
            self.clock.advance(10)
            replacement.run_once()
            self.assertEqual(sum("43" in ids for ids in calls), 1)
            self.assertEqual(create.call_count, 2)
        attempts = self.rows("llm_attempts")
        self.assertEqual([row["status"] for row in attempts], ["failed", "success"])
        self.assertEqual(len(self.rows("metadata_fetches")), 3)
        self.assertEqual(service.recommend(43).method, "popularity")
        self.assertEqual(service.recommend(42).method, "llm_cold_start")

    def test_changed_descriptions_refresh_profile_and_null_descriptions_skip_provider(self):
        service = self.service()
        worker = self.worker(service.cold_start)
        self.event(1, "GET /create_account")
        with self.sdk() as create:
            worker.run_once()
            first = self.state()["prepared"]
            first_result = service.recommend(42)
            self.records["42"]["self_description_likes"] = "Comedy and jokes"
            self.clock.advance(11)
            worker.run_once()
            changed = self.state()["prepared"]
            changed_result = service.recommend(42)
            self.assertEqual(create.call_count, 2)
            self.assertNotEqual(first["snapshot_id"], changed["snapshot_id"])
            self.assertNotEqual(first["metadata_version"], changed["metadata_version"])
            self.assertNotEqual(first_result.profile_id, changed_result.profile_id)
            self.assertNotEqual(first_result.profile_version, changed_result.profile_version)
            self.assertEqual(changed_result.recommendations[0].movie_id, "d")
            self.records["42"]["self_description_likes"] = None
            self.records["42"]["self_description_dislikes"] = None
            self.clock.advance(11)
            worker.run_once()
            cleared = self.state()["prepared"]
            self.assertEqual(cleared["likes"], "")
            self.assertEqual(cleared["dislikes"], "")
            self.assertIsNone(cleared["profile"])
            self.assertEqual(service.recommend(42).method, "popularity")
            worker.run_once()
            self.assertEqual(create.call_count, 2)
        with MetadataStore(self.path) as metadata:
            raw = metadata.get_snapshot(cleared["snapshot_id"])["record"]
            self.assertIsNone(raw["self_description_likes"])
            self.assertIsNone(raw["self_description_dislikes"])
        self.assertEqual(len(self.rows("preference_profiles")), 2)
        self.assertEqual(len(self.rows("metadata_snapshots")), 3)

    def test_restart_reuses_completed_profile_without_refetch_or_paid_duplicate(self):
        service = self.service()
        worker = self.worker(service.cold_start)
        self.event(1, "GET /create_account")
        with self.sdk() as create:
            worker.run_once()
            original = service.recommend(42)
            worker.close()
            restarted = self.service()
            replacement = self.worker(restarted.cold_start)
            replacement.run_once()
            self.assertEqual(len(self.fetch_calls), 1)
            self.assertEqual(create.call_count, 1)
            after = restarted.recommend(42)
            self.assertEqual(after.profile_id, original.profile_id)
            self.assertEqual(after.llm_attempt_id, original.llm_attempt_id)
            self.assertEqual(after.llm_response_id, original.llm_response_id)
            self.assertEqual(after.recommendations, original.recommendations)
            with RequestStore(self.path) as requests:
                requests.save_request({"request_id": "repeat-request", "user_id": 42, "started_at": self.clock.iso()})
            replacement.run_once()
            self.assertEqual(create.call_count, 1)
            self.assertEqual(len(self.rows("llm_attempts")), 1)

    def test_existing_issue4_events_backfill_without_changing_raw_rows_or_replay_identity(self):
        first, _ = self.event(1, "GET /data/m/a/17.mpg")
        self.event(2, "GET /rate/a=1")
        self.event(3, "GET /data/m/d/18.mpg")
        self.event(1, "GET /data/m/b/1.mpg", source="another-cluster")
        with closing(sqlite3.connect(self.path)) as connection:
            exact = connection.execute("SELECT source_id,topic,partition,offset,source_fingerprint,raw_value,parsed_json FROM kafka_events ORDER BY source_id,offset").fetchall()
            for table in ("live_event_cursors", "live_interactions", "live_users"):
                connection.execute(f"DROP TABLE {table}")
            connection.execute("DROP INDEX watch_by_user_movie")
            connection.execute("PRAGMA user_version=4")
            connection.commit()
        with LiveStore(self.path) as live:
            self.assertEqual(live.ingest("cmu-movielog", "movielog2", limit=2), 2)
            self.assertEqual(live.ingest("cmu-movielog", "movielog2", limit=2), 1)
            self.assertEqual(live.ingest("cmu-movielog", "movielog2", limit=2), 0)
        state = self.state()
        self.assertEqual(state["watched"], {"a", "d"})
        self.assertEqual(state["ratings"], {"a": 1})
        with closing(sqlite3.connect(self.path)) as connection:
            after = connection.execute("SELECT source_id,topic,partition,offset,source_fingerprint,raw_value,parsed_json FROM kafka_events ORDER BY source_id,offset").fetchall()
        self.assertEqual(after, exact)
        with EventStore(self.path) as events:
            self.assertFalse(events.save_event(first, parse_event(first.value)))
        self.assertEqual(self.service().recommend(42).method, "ease")


if __name__ == "__main__":
    unittest.main()
