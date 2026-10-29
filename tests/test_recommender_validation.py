"""Input validation and failure handling of EaseRecommender (not the EASE math)."""
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from recommender import EaseRecommender


class RecommenderValidationTests(unittest.TestCase):
    def setUp(self):
        self.movies = pd.DataFrame(
            {"movie_id": list("abcdef"), "title": [f"Movie {c}" for c in "abcdef"]}
        )
        self.interactions = pd.DataFrame(
            [
                (1, "a", 8, 1), (1, "b", 9, 1), (2, "a", 7, 1), (2, "b", 8, 1),
                (2, "c", 9, 1), (3, "c", 8, 1), (3, "d", 3, 1),
            ],
            columns=["user_id", "movie_id", "rating", "watch_count"],
        )
        self.model = EaseRecommender()
        self.model.fit(self.interactions, self.movies)

    def test_constructor_rejects_bad_hyperparameters(self):
        for regularization in [0, -1, float("nan"), float("inf")]:
            with self.subTest(regularization=regularization):
                with self.assertRaisesRegex(ValueError, "positive finite"):
                    EaseRecommender(regularization=regularization)
        for min_rating in [0, 11]:
            with self.subTest(min_rating=min_rating):
                with self.assertRaisesRegex(ValueError, "between 1 and 10"):
                    EaseRecommender(min_rating=min_rating)

    def test_fit_rejects_inconsistent_training_data(self):
        cases = {
            "no interactions": (self.interactions.iloc[0:0], self.movies, "requires interactions"),
            "no movies": (self.interactions, self.movies.iloc[0:0], "requires interactions"),
            "duplicate pair": (pd.concat([self.interactions, self.interactions.iloc[:1]]), self.movies, "one row"),
            "duplicate movie": (self.interactions, pd.concat([self.movies, self.movies.iloc[:1]]), "unique"),
            "unknown movie": (self.interactions.assign(movie_id=["zzz", *self.interactions["movie_id"][1:]]), self.movies, "unknown movies"),
            "nothing positive": (self.interactions.assign(rating=1), self.movies, "positive interaction"),
        }
        for name, (interactions, movies, message) in cases.items():
            with self.subTest(name):
                with self.assertRaisesRegex(ValueError, message):
                    EaseRecommender().fit(interactions, movies)

    def test_failed_refit_does_not_leave_a_usable_model(self):
        with self.assertRaises(ValueError):
            self.model.fit(self.interactions.assign(rating=1), self.movies)
        with self.assertRaisesRegex(ValueError, "Train or load"):
            self.model.recommend(1)

    def test_untrained_model_cannot_score_or_save(self):
        untrained = EaseRecommender()
        with self.assertRaisesRegex(ValueError, "Train or load"):
            untrained.get_scores(1)
        with self.assertRaisesRegex(ValueError, "Train or load"):
            untrained.recommend_from_profile(["a"], [])
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, "Train a model"):
                untrained.save(Path(directory) / "model.npz")

    def test_requests_must_be_positive(self):
        with self.assertRaisesRegex(ValueError, "User ID must be positive"):
            self.model.get_scores(0)
        with self.assertRaisesRegex(ValueError, "must be positive"):
            self.model.recommend(1, top_k=0)
        with self.assertRaisesRegex(ValueError, "must be positive"):
            self.model.recommend_from_profile(["a"], [], top_k=0)

    def test_user_with_only_low_ratings_gets_popularity(self):
        low = pd.concat([
            self.interactions,
            pd.DataFrame([(9, "e", 2, 1)], columns=self.interactions.columns),
        ])
        model = EaseRecommender()
        model.fit(low, self.movies)
        self.assertFalse(model.has_positive_history(9))
        np.testing.assert_array_equal(model.get_scores(9), model.popularity)

    def test_recommendations_exclude_seen_and_never_liked_movies(self):
        # Movies e and f have no positive interactions; d only has a low rating
        for user in [1, 2, 3, 12345]:
            with self.subTest(user=user):
                result = self.model.recommend(user, 10)
                self.assertTrue(set(result["movie_id"]).isdisjoint({"d", "e", "f"}))
                seen = set(self.interactions.loc[self.interactions["user_id"].eq(user), "movie_id"])
                self.assertTrue(set(result["movie_id"]).isdisjoint(seen))

    def test_top_k_larger_than_catalog_returns_only_eligible_movies(self):
        self.assertEqual(set(self.model.recommend(1, 100)["movie_id"]), {"c"})

    def test_ranking_is_deterministic(self):
        first = self.model.recommend(12345, 10)["movie_id"].tolist()
        self.assertEqual(first, self.model.recommend(12345, 10)["movie_id"].tolist())

    def test_recommend_from_profile_ignores_unknown_movies_and_excludes_seen(self):
        result = self.model.recommend_from_profile(["a", "not-a-movie"], ["a", "also-unknown"], 5)
        self.assertNotIn("a", result["movie_id"].tolist())
        self.assertIn("b", result["movie_id"].tolist())

    def test_recommend_from_profile_falls_back_to_popularity(self):
        popular = self.model.recommend_from_profile([], [], 3)
        unknown = self.model.recommend_from_profile(["nope"], [], 3)
        forced = self.model.recommend_from_profile(["a"], [], 3, popularity_only=True)
        for result in (unknown, forced):
            self.assertEqual(result["movie_id"].tolist(), popular["movie_id"].tolist())

    def test_load_failures(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(FileNotFoundError, "src/train.py"):
                EaseRecommender.load(Path(directory) / "missing.npz")
            path = Path(directory) / "model.npz"
            self.model.save(path)
            with np.load(path) as saved:
                contents = dict(saved)
            contents["format_version"] = np.array(2)
            np.savez_compressed(path, **contents)
            with self.assertRaisesRegex(ValueError, "Unsupported model file version"):
                EaseRecommender.load(path)


if __name__ == "__main__":
    unittest.main()
