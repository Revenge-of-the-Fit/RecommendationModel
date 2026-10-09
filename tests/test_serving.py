import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from fastapi.testclient import TestClient


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cold_start import ColdStartRecommender
from api.app import create_app
from dataset import MovieDataset
from models.settings import ServingSettings
from recommender import EaseRecommender
from services.recommendation import RecommendationService
from services.versions import code_version


class ServingProvenanceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.settings = ServingSettings(
            data_directory=self.directory,
            model_path=self.directory / "model.npz",
            cache_directory=self.directory / "profiles",
            storage_path=self.directory / "requests.sqlite3",
        )
        self.movies = pd.DataFrame([
            ("a", "Movie A", "Drama", "A thoughtful family drama.", "90", "1"),
            ("b", "Movie B", "Horror", "A ghost story.", "90", "1"),
            ("c", "Movie C", "Drama", "A detective investigates a crime.", "90", "1"),
            ("d", "Movie D", "Comedy", "A clever comedy.", "90", "1"),
        ], columns=["movie_id", "title", "genres", "overview", "runtime", "license_cost"])
        users = pd.DataFrame([
            (1, "", ""), (2, "", ""), (42, "Drama", "Horror"), (43, "", ""),
        ], columns=["user_id", "self_description_likes", "self_description_dislikes"])
        events = pd.DataFrame([
            ("2026-10-08T12:00:00Z", 1, "rating", "a", "8"),
            ("2026-10-08T12:01:00Z", 1, "rating", "b", "7"),
            ("2026-10-08T12:02:00Z", 2, "rating", "b", "8"),
            ("2026-10-08T12:03:00Z", 2, "rating", "c", "8"),
            ("2026-10-08T12:04:00Z", 42, "account_created", "", ""),
            ("2026-10-08T12:05:00Z", 43, "account_created", "", ""),
        ], columns=["timestamp", "user_id", "event_type", "movie_id", "rating"])
        for filename, table in (("movies", self.movies), ("users", users), ("events", events)):
            table.to_csv(self.directory / f"{filename}.csv.gz", index=False)
        self.dataset = MovieDataset(self.directory)
        self.model = EaseRecommender()
        self.model.fit(self.dataset.get_interactions(), self.movies)
        self.model.save(self.settings.model_path)

    def tearDown(self):
        self.temporary.cleanup()

    def test_loaded_service_fingerprints_model_and_preserves_warm_ranking(self):
        service = RecommendationService.load(self.settings)
        expected_version = "sha256:" + hashlib.sha256(self.settings.model_path.read_bytes()).hexdigest()
        self.assertEqual(service.versions["model"], expected_version)
        self.assertTrue(service.versions["dataset"].startswith("sha256:"))
        with patch("services.recommendation.file_version", side_effect=AssertionError("request rehashed model")):
            result = service.recommend(1)
        expected = self.model.recommend(1, 20)
        self.assertEqual(result.method, "ease")
        self.assertIsNone(result.fallback_reason)
        self.assertEqual([item.movie_id for item in result.recommendations], expected["movie_id"].tolist())
        self.assertEqual([item.score for item in result.recommendations], expected["score"].tolist())

    def test_missing_cache_and_ordinary_popularity_have_distinct_reasons(self):
        service = RecommendationService.load(self.settings)
        with patch("preferences.PreferenceInterpreter._request", side_effect=AssertionError("serving called LLM")):
            missing_cache = service.recommend(42)
            no_description = service.recommend(43)
            unknown_user = service.recommend(999)
        self.assertEqual(missing_cache.method, "popularity")
        self.assertEqual(missing_cache.fallback_reason, "cold_start_profile_unavailable")
        self.assertEqual(no_description.fallback_reason, "no_description_or_positive_history")
        self.assertEqual(unknown_user.fallback_reason, "user_not_in_dataset")

    def test_cached_profile_details_survive_service_validation(self):
        profile = {
            "liked_genres": ["Drama"], "excluded_genres": ["Horror"],
            "liked_titles": [], "disliked_titles": [],
            "likes_summary": "Family drama", "dislikes_summary": "Ghost stories",
        }
        preparer = ColdStartRecommender(
            self.movies, self.settings.cache_directory, model=self.model, storage_path=self.settings.storage_path,
        )
        with patch.object(preparer.interpreter, "_request", return_value={"profile": profile}):
            prepared = preparer.recommend("Drama", "Horror")
        service = RecommendationService.load(self.settings)
        with patch("preferences.PreferenceInterpreter._request", side_effect=AssertionError("serving called LLM")):
            result = service.recommend(42)
        self.assertEqual(result.method, "llm_cold_start")
        self.assertTrue(result.cached)
        self.assertTrue(result.llm_model)
        self.assertEqual(result.profile_version, prepared["profile_version"])
        self.assertTrue(all(item.reason for item in result.recommendations))
        cache_file = next(self.settings.cache_directory.glob("*.json"))
        saved = json.loads(cache_file.read_text(encoding="utf-8"))
        saved["profile"]["likes_summary"] = "Detective investigations"
        cache_file.write_text(json.dumps(saved), encoding="utf-8")
        fallback = service.recommend(42)
        self.assertEqual(fallback.method, "popularity")
        self.assertEqual(fallback.fallback_reason, "cold_start_profile_unavailable")
        self.assertIsNone(fallback.profile_version)
        self.assertIsNone(fallback.profile_id)

    def test_profile_import_failure_preserves_warm_serving_and_is_visible_in_storage_health(self):
        app = create_app(self.settings)
        with patch("preferences.PreferenceInterpreter.import_cache_profiles", side_effect=OSError("password=private-import-secret")):
            with patch("preferences.PreferenceInterpreter._request", side_effect=AssertionError("serving called LLM")):
                with self.assertLogs("services.recommendation", level="ERROR") as logged:
                    with TestClient(app) as client:
                        self.assertEqual(client.get("/health/ready").status_code, 200)
                        response = client.get("/recommend/1")
                        health = client.get("/health/storage")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.text.split(","), self.model.recommend(1, 20)["movie_id"].tolist())
        self.assertEqual(health.status_code, 503)
        self.assertFalse(health.json()["healthy"])
        self.assertEqual(health.json()["profile_import_error"], "OSError")
        self.assertNotIn("private-import-secret", health.text)
        self.assertNotIn("private-import-secret", "\n".join(logged.output))

    def test_source_version_handles_line_endings_and_detects_changes(self):
        source = self.directory / "source"
        source.mkdir()
        path = source / "example.py"
        path.write_bytes(b"value = 1\n")
        original = code_version(source)
        path.write_bytes(b"value = 1\r\n")
        self.assertEqual(code_version(source), original)
        path.write_bytes(b"value = 2\n")
        self.assertNotEqual(code_version(source), original)


if __name__ == "__main__":
    unittest.main()
