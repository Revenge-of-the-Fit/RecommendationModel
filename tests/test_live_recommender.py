import copy
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cold_start import ColdStartRecommender
from preferences import ColdStartError
from recommender import EaseRecommender
from storage.profiles import content_version


class SparseLiveRecommendationTests(unittest.TestCase):
    def setUp(self):
        self.movies = pd.DataFrame({
            "movie_id": list("abcdef"), "title": [f"Movie {letter.upper()}" for letter in "abcdef"],
            "genres": ["Crime", "Horror", "Crime|Drama", "Comedy", "Drama", "Crime"],
            "overview": ["A detective story.", "A ghost story.", "A crime investigation.", "A comedy.", "A family drama.", "A crime story."],
        })
        interactions = pd.DataFrame([
            (1, "a", 8, 1), (1, "b", 8, 1), (2, "a", 9, 1), (2, "b", 9, 1),
            (3, "c", 8, 1), (3, "d", 8, 1), (4, "c", 9, 1), (4, "d", 9, 1),
            (5, "e", 8, 1), (6, "a", 3, 1),
        ], columns=["user_id", "movie_id", "rating", "watch_count"])
        self.model = EaseRecommender(regularization=2)
        self.model.fit(interactions, self.movies)

    def test_sparse_profile_preserves_existing_static_scores_and_rankings(self):
        for user_id in (1, 3, 5, 6, 999):
            row = self.model.user_index.get(user_id)
            positive = [] if row is None else self.model.movie_ids[self.model.user_profiles[row]]
            seen = [] if row is None else self.model.movie_ids[self.model.seen_movies[row]]
            for popularity_only in (False, True):
                with self.subTest(user_id=user_id, popularity_only=popularity_only):
                    expected = self.model.recommend(user_id, 20, popularity_only)
                    actual = self.model.recommend_from_profile(positive, seen, 20, popularity_only)
                    pd.testing.assert_frame_equal(actual, expected)

    def test_repeated_and_unsupported_seeds_do_not_amplify_scores_or_mutate_dense_model(self):
        before = {
            name: getattr(self.model, name).copy()
            for name in ("user_ids", "user_profiles", "seen_movies", "weights", "popularity")
        }
        expected = self.model.recommend_from_profile({"a", "c"}, {"a", "b", "c"})
        actual = self.model.recommend_from_profile(
            ["c", "a", "a", "c", "a", "unavailable", "f"], ["a", "b", "c", "unavailable"],
        )
        pd.testing.assert_frame_equal(actual, expected)
        scores = self.model.weights[[self.model.movie_index["a"], self.model.movie_index["c"]]].sum(axis=0, dtype=np.float64)
        np.testing.assert_array_equal(actual["score"].to_numpy(), scores[[self.model.movie_index[value] for value in actual["movie_id"]]])
        self.assertTrue(set(actual["movie_id"]).isdisjoint({"a", "b", "c", "f", "unavailable"}))
        for name, value in before.items():
            np.testing.assert_array_equal(getattr(self.model, name), value)

    def test_distinct_supported_histories_change_rankings_for_the_same_eligible_movies(self):
        first = self.model.recommend_from_profile({"a"}, {"a", "c"})
        second = self.model.recommend_from_profile({"c"}, {"a", "c"})
        self.assertEqual(first.iloc[0]["movie_id"], "b")
        self.assertEqual(second.iloc[0]["movie_id"], "d")
        self.assertEqual(set(first["movie_id"]), set(second["movie_id"]))
        self.assertNotEqual(first.set_index("movie_id").loc["b", "score"], second.set_index("movie_id").loc["b", "score"])

    def test_unknown_and_zero_popularity_history_uses_popularity_with_seen_filtering(self):
        actual = self.model.recommend_from_profile(["unavailable", "f", "f"], {"a", "b"})
        expected = self.model.recommend(999, 20)
        expected = expected.loc[~expected["movie_id"].isin({"a", "b"})].reset_index(drop=True)
        pd.testing.assert_frame_equal(actual, expected)
        self.assertEqual(actual["movie_id"].tolist(), ["c", "d", "e"])
        self.assertEqual(actual["score"].tolist(), [1 / 3, 1 / 3, 1 / 6])

    def test_ties_keep_popularity_then_movie_id_order_and_top_k_is_bounded(self):
        full = self.model.recommend_from_profile({"e"}, {"e"})
        self.assertEqual(full["movie_id"].tolist(), ["a", "b", "c", "d"])
        np.testing.assert_array_equal(full["score"].to_numpy(), np.zeros(4))
        pd.testing.assert_frame_equal(self.model.recommend_from_profile({"e"}, {"e"}, 2), full.head(2))
        with self.assertRaises(ValueError):
            self.model.recommend_from_profile({"a"}, {"a"}, 0)
        with self.assertRaises(ValueError):
            EaseRecommender().recommend_from_profile(set(), set())


class SavedColdProfileTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.directory = Path(self.temporary.name)
        self.movies = pd.DataFrame([
            ("a", "Movie A", "Crime", "A detective story."),
            ("b", "Movie B", "Horror", "A ghost story."),
            ("c", "Movie C", "Crime|Drama", "A crime investigation."),
            ("d", "Movie D", "Comedy", "A comedy."),
            ("e", "Movie E", "Drama", "A family drama."),
            ("f", "Movie F", "Crime", "A crime story."),
        ], columns=["movie_id", "title", "genres", "overview"])
        self.recommender = ColdStartRecommender(
            self.movies, self.directory / "missing-cache", env_file=self.directory / "unused.env",
            storage_path=self.directory / "events.sqlite3",
        )
        self.profile = {
            "liked_genres": ["Crime"], "excluded_genres": ["Horror"],
            "liked_titles": ["Movie A"], "disliked_titles": ["Movie D"],
            "likes_summary": "Crime investigations.", "dislikes_summary": "Horror and silly comedies.",
        }
        self.saved = {
            "profile": self.profile, "model": "provider-model-snapshot", "response_id": "response-21",
            "provider_status": "completed",
            "_provenance": {
                "profile_id": "profile-21", "attempt_id": "attempt-21", "origin": "llm",
                "profile_version": content_version(self.profile),
                "prompt_version": "sha256:" + "1" * 64, "schema_version": "sha256:" + "2" * 64,
            },
        }

    def tearDown(self):
        self.temporary.cleanup()

    def test_in_memory_profile_preserves_exclusions_provenance_and_reads_nothing(self):
        original = copy.deepcopy(self.saved)
        with patch.object(self.recommender.interpreter, "get_profile_record", side_effect=AssertionError("read profile cache")):
            with patch.object(self.recommender.interpreter, "_request", side_effect=AssertionError("called API")):
                with patch("preferences.ProfileStore", side_effect=AssertionError("opened database")):
                    with patch.object(Path, "read_text", side_effect=AssertionError("read file")):
                        result = self.recommender.recommend_profile(self.saved, seen_movie_ids={"e", "unavailable"})
        self.assertEqual(self.saved, original)
        self.assertEqual({value["movie_id"] for value in result["recommendations"]}, {"c", "f"})
        self.assertEqual([value["rank"] for value in result["recommendations"]], [1, 2])
        self.assertTrue(result["cached"])
        self.assertEqual(result["llm_model"], "provider-model-snapshot")
        self.assertEqual(result["llm_response_id"], "response-21")
        self.assertEqual(result["profile_id"], "profile-21")
        self.assertEqual(result["llm_attempt_id"], "attempt-21")
        self.assertEqual(result["profile_version"], self.saved["_provenance"]["profile_version"])
        self.assertEqual(result["prompt_version"], self.saved["_provenance"]["prompt_version"])
        self.assertEqual(result["schema_version"], self.saved["_provenance"]["schema_version"])
        self.assertEqual(result["profile_origin"], "llm")
        self.assertFalse((self.directory / "events.sqlite3").exists())
        self.assertFalse((self.directory / "missing-cache").exists())

    def test_legacy_cache_envelope_keeps_its_observed_provider_lineage(self):
        legacy = {
            "profile": self.profile, "model": "historical-model", "response_id": "historical-response",
            "_provenance": {
                "profile_id": "profile-legacy", "attempt_id": None, "origin": "legacy_cache",
                "profile_version": content_version(self.profile), "prompt_version": None, "schema_version": None,
            },
        }
        result = self.recommender.recommend_profile(legacy)
        self.assertEqual(result["llm_model"], "historical-model")
        self.assertEqual(result["llm_response_id"], "historical-response")
        self.assertEqual(result["profile_id"], "profile-legacy")
        self.assertEqual(result["profile_origin"], "legacy_cache")
        self.assertIsNone(result["llm_attempt_id"])
        self.assertIsNone(result["prompt_version"])
        self.assertIsNone(result["schema_version"])

    def test_existing_recommend_delegates_while_preserving_generated_and_cached_flags(self):
        expected = self.recommender.recommend_profile(self.saved, 2, {"e"})
        for cached in (False, True):
            with self.subTest(cached=cached):
                with patch.object(self.recommender.interpreter, "get_profile_record", return_value=(self.saved, cached)) as lookup:
                    actual = self.recommender.recommend("Crime", "Horror", 2, {"e"}, True)
                lookup.assert_called_once_with("Crime", "Horror", True, context=None)
                self.assertEqual(actual, {**expected, "cached": cached})

    def test_invalid_or_unfinished_saved_profiles_are_rejected_before_ranking(self):
        invalid = copy.deepcopy(self.saved)
        invalid["profile"]["excluded_genres"] = ["Unknown"]
        changed = copy.deepcopy(self.saved)
        changed["profile"]["likes_summary"] = "Changed interpretation."
        unfinished = {**self.saved, "provider_status": "incomplete"}
        for record in (invalid, changed, unfinished):
            with self.subTest(record=record):
                with patch.object(self.recommender, "_rank_movies", side_effect=AssertionError("ranked invalid profile")):
                    with self.assertRaises(ColdStartError):
                        self.recommender.recommend_profile(record)


if __name__ == "__main__":
    unittest.main()
