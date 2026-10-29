import contextlib
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from evaluation import InteractionSplitter, RankingEvaluator
from evaluate import main, select_model
from recommender import EaseRecommender


class FixedRecommender:
    def __init__(self):
        self.movie_ids = np.array(["a", "b", "c", "d"])
        self.popularity = np.array([0.5, 0.25, 0.25, 0.0])

    def recommend(self, user_id, top_k, popularity_only=False):
        return pd.DataFrame({"movie_id": ["a", "c"][:top_k]})


class EvaluationTests(unittest.TestCase):
    def setUp(self):
        rows = []
        for user_id in [1, 2, 3]:
            for movie_number in range(10):
                rows.append((user_id, f"movie-{movie_number}", 8, 1))

        self.interactions = pd.DataFrame(
            rows, columns=["user_id", "movie_id", "rating", "watch_count"]
        )

    def test_split_keeps_pairs_separate_and_preserves_all_rows(self):
        training, validation, test = InteractionSplitter().split(self.interactions)
        pairs = []
        for part in [training, validation, test]:
            pairs.append(set(zip(part["user_id"], part["movie_id"])))

        self.assertTrue(pairs[0].isdisjoint(pairs[1]))
        self.assertTrue(pairs[0].isdisjoint(pairs[2]))
        self.assertTrue(pairs[1].isdisjoint(pairs[2]))
        self.assertEqual(len(pairs[0] | pairs[1] | pairs[2]), 30)
        self.assertEqual(training.groupby("user_id").size().tolist(), [6, 6, 6])
        self.assertEqual(validation.groupby("user_id").size().tolist(), [2, 2, 2])
        self.assertEqual(test.groupby("user_id").size().tolist(), [2, 2, 2])

    def test_split_is_reproducible_after_input_shuffle(self):
        expected = InteractionSplitter(seed=42).split(self.interactions)
        shuffled = self.interactions.sample(frac=1, random_state=10)
        actual = InteractionSplitter(seed=42).split(shuffled)

        for expected_part, actual_part in zip(expected, actual):
            pd.testing.assert_frame_equal(expected_part, actual_part)

    def test_short_histories_stay_in_training(self):
        short = pd.DataFrame(
            [(10, "a", 8, 1), (10, "b", 8, 1)],
            columns=self.interactions.columns,
        )
        training, validation, test = InteractionSplitter().split(short)
        self.assertEqual(len(training), 2)
        self.assertTrue(validation.empty)
        self.assertTrue(test.empty)

    def test_duplicate_pairs_are_rejected(self):
        duplicated = pd.concat([self.interactions, self.interactions.iloc[[0]]])
        with self.assertRaises(ValueError):
            InteractionSplitter().split(duplicated)

    def test_invalid_split_parameters_are_rejected(self):
        for validation, test in [(0, 0.2), (0.5, 0.5), (-0.1, 0.2)]:
            with self.subTest(validation=validation, test=test):
                with self.assertRaises(ValueError):
                    InteractionSplitter(validation, test)

    def test_metrics_keep_unsupported_targets_in_denominator(self):
        held_out = pd.DataFrame(
            [(1, "a", 8), (1, "d", 9), (1, "c", 2), (2, "a", 3)],
            columns=["user_id", "movie_id", "rating"],
        )
        result = RankingEvaluator(top_k=2).evaluate(FixedRecommender(), held_out)

        self.assertEqual(result["evaluated_users"], 1)
        self.assertEqual(result["users_without_relevant_ratings"], 1)
        self.assertEqual(result["relevant_interactions"], 2)
        self.assertEqual(result["precision_at_k"], 0.5)
        self.assertEqual(result["recall_at_k"], 0.5)
        self.assertAlmostEqual(result["ndcg_at_k"], 1.0 / (1.0 + 1.0 / np.log2(3)))
        self.assertEqual(result["hit_rate_at_k"], 1.0)
        self.assertEqual(result["catalog_coverage"], 0.5)
        self.assertEqual(result["supported_target_fraction"], 0.5)

    def test_metrics_discount_later_hits(self):
        held_out = pd.DataFrame(
            [(1, "c", 8)], columns=["user_id", "movie_id", "rating"]
        )
        result = RankingEvaluator(top_k=2).evaluate(FixedRecommender(), held_out)

        self.assertEqual(result["recall_at_k"], 1.0)
        self.assertEqual(result["precision_at_k"], 0.5)
        self.assertAlmostEqual(result["ndcg_at_k"], 1.0 / np.log2(3))

    def test_metrics_average_users_equally(self):
        held_out = pd.DataFrame(
            [(1, "a", 8), (1, "d", 9), (2, "c", 8)],
            columns=["user_id", "movie_id", "rating"],
        )
        result = RankingEvaluator(top_k=2).evaluate(FixedRecommender(), held_out)

        self.assertEqual(result["evaluated_users"], 2)
        self.assertEqual(result["recall_at_k"], 0.75)

    def test_popularity_uses_training_data_and_same_candidates(self):
        from recommender import EaseRecommender

        training, validation, test = InteractionSplitter().split(self.interactions)
        movies = pd.DataFrame(
            {
                "movie_id": [f"movie-{number}" for number in range(10)],
                "title": [f"Movie {number}" for number in range(10)],
            }
        )
        model = EaseRecommender()
        model.fit(training, movies)
        counts = training.groupby("movie_id")["user_id"].nunique()
        expected = counts.reindex(model.movie_ids, fill_value=0).to_numpy() / 3

        np.testing.assert_array_equal(model.popularity, expected)
        self.assertEqual(int(model.seen_movies.sum()), len(training))
        self.assertEqual(int(model.user_profiles.sum()), len(training))

        for user_id in [1, 2, 3]:
            seen = set(training.loc[training["user_id"].eq(user_id), "movie_id"])
            learned = set(model.recommend(user_id, 10)["movie_id"])
            popular = set(model.recommend(user_id, 10, popularity_only=True)["movie_id"])
            self.assertTrue(learned.isdisjoint(seen))
            self.assertEqual(learned, popular)
            for part in [validation, test]:
                for row in part.loc[part["user_id"].eq(user_id)].itertuples():
                    self.assertFalse(
                        model.seen_movies[model.user_index[user_id], model.movie_index[row.movie_id]]
                    )

    def test_training_threshold_does_not_change_validation_relevance(self):
        training, validation, _ = InteractionSplitter().split(self.interactions)
        training.loc[training.index[::2], "rating"] = 6
        validation.loc[validation.index[::2], "rating"] = 6
        movies = pd.DataFrame(
            {"movie_id": [f"movie-{number}" for number in range(10)], "title": "Movie"}
        )
        evaluator = RankingEvaluator(relevance_rating=7)
        with contextlib.redirect_stdout(io.StringIO()):
            best, results = select_model(training, validation, movies, evaluator, [100], [6, 7])

        self.assertEqual(evaluator.relevance_rating, 7)
        self.assertEqual(len(results), 2)
        for result in results:
            self.assertEqual(result["metrics"]["relevant_interactions"], 3)
            self.assertEqual(result["metrics"]["evaluated_users"], 3)

    def test_selection_prioritizes_validation_ndcg(self):
        training, validation, _ = InteractionSplitter().split(self.interactions)
        movies = pd.DataFrame(
            {"movie_id": [f"movie-{number}" for number in range(10)], "title": "Movie"}
        )
        evaluator = RankingEvaluator()
        metrics = [
            {"ndcg_at_k": 0.1, "recall_at_k": 0.9, "precision_at_k": 0.5},
            {"ndcg_at_k": 0.2, "recall_at_k": 0.3, "precision_at_k": 0.2},
        ]
        with patch.object(evaluator, "evaluate", side_effect=metrics) as evaluate:
            with contextlib.redirect_stdout(io.StringIO()):
                best, results = select_model(training, validation, movies, evaluator, [100], [6, 7])

        self.assertEqual(best["min_rating"], 7)
        for call in evaluate.call_args_list:
            model, held_out = call.args
            pd.testing.assert_frame_equal(held_out, validation)
            self.assertEqual(int(model.seen_movies.sum()), len(training))
            for row in validation.itertuples():
                self.assertFalse(model.seen_movies[model.user_index[row.user_id], model.movie_index[row.movie_id]])

    def test_validation_only_does_not_evaluate_final_test(self):
        movies = pd.DataFrame(
            {"movie_id": [f"movie-{number}" for number in range(10)], "title": "Movie"}
        )
        dataset = SimpleNamespace(
            movies=movies,
            get_interactions=lambda: self.interactions,
            get_cold_start_users=lambda: [],
        )
        _, validation, _ = InteractionSplitter().split(self.interactions)
        evaluator = RankingEvaluator()
        original_evaluate = evaluator.evaluate
        evaluated_parts = []

        def record_evaluation(model, held_out, popularity_only=False):
            evaluated_parts.append(held_out.copy())
            return original_evaluate(model, held_out, popularity_only)

        with tempfile.TemporaryDirectory() as directory:
            data_directory = Path(directory)
            output = data_directory / "evaluation.json"
            for filename in ["events.csv.gz", "users.csv.gz", "movies.csv.gz"]:
                (data_directory / filename).write_bytes(b"fixture")
            arguments = [
                "evaluate.py", "--validation-only", "--regularizations", "100",
                "--min-ratings", "6", "7", "--data-dir", str(data_directory),
                "--output", str(output),
            ]
            with patch("sys.argv", arguments), patch("evaluate.MovieDataset", return_value=dataset):
                with patch("evaluate.RankingEvaluator", return_value=evaluator):
                    with patch.object(evaluator, "evaluate", side_effect=record_evaluation):
                        with contextlib.redirect_stdout(io.StringIO()):
                            main()
            saved = json.loads(output.read_text(encoding="utf-8"))

        self.assertFalse(saved["test_evaluated"])
        self.assertNotIn("test_model", saved)
        self.assertEqual(saved["relevance_rating"], 7)
        self.assertEqual(saved["baseline_parameters"], {"regularization": 100, "min_rating": 7})
        self.assertEqual(len(evaluated_parts), 4)
        for held_out in evaluated_parts:
            pd.testing.assert_frame_equal(held_out, validation)

    def run_evaluate(self, *extra, dataset=None):
        movies = pd.DataFrame(
            {"movie_id": [f"movie-{number}" for number in range(10)], "title": "Movie"}
        )
        dataset = dataset or SimpleNamespace(
            movies=movies,
            get_interactions=lambda: self.interactions,
            get_cold_start_users=lambda: [1001, 1002],
        )
        with tempfile.TemporaryDirectory() as directory:
            data_directory = Path(directory)
            output = data_directory / "nested" / "evaluation.json"
            for filename in ["events.csv.gz", "users.csv.gz", "movies.csv.gz"]:
                (data_directory / filename).write_bytes(b"fixture")
            arguments = [
                "evaluate.py", "--regularizations", "100", "--min-ratings", "6", "7",
                "--data-dir", str(data_directory), "--output", str(output), *extra,
            ]
            fits = []
            original_fit = EaseRecommender.fit

            def record_fit(model, interactions, catalog):
                fits.append(interactions.copy())
                return original_fit(model, interactions, catalog)

            stderr = io.StringIO()
            code = 0
            with patch("sys.argv", arguments), patch("evaluate.MovieDataset", return_value=dataset):
                with patch.object(EaseRecommender, "fit", record_fit):
                    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(stderr):
                        try:
                            main()
                        except SystemExit as stop:
                            code = stop.code
            saved = json.loads(output.read_text(encoding="utf-8")) if output.exists() else None
        return code, saved, fits, stderr.getvalue()

    def test_final_test_evaluation_trains_on_train_and_validation_only(self):
        code, saved, fits, _ = self.run_evaluate()
        training, validation, test = InteractionSplitter().split(self.interactions)

        self.assertEqual(code, 0)
        self.assertTrue(saved["test_evaluated"])
        self.assertEqual(saved["test_training_data"], "training and validation interactions")
        for key in ["test_popularity", "test_baseline", "test_model"]:
            self.assertIn("ndcg_at_k", saved[key])
        self.assertEqual(saved["cold_start_users_not_evaluated"], 2)
        self.assertEqual(
            saved["interaction_counts"],
            {"training": len(training), "validation": len(validation), "test": len(test)},
        )
        self.assertEqual(set(saved["data_sha256"]), {"events.csv.gz", "users.csv.gz", "movies.csv.gz"})
        # The last two fits (selected model and baseline) are the final test-time fits
        test_pairs = set(zip(test["user_id"], test["movie_id"]))
        for final in fits[-2:]:
            self.assertEqual(len(final), len(training) + len(validation))
            self.assertTrue(test_pairs.isdisjoint(zip(final["user_id"], final["movie_id"])))

    def test_invalid_split_fractions_exit_with_an_error(self):
        code, saved, _, error = self.run_evaluate("--validation-fraction", "0.9", "--test-fraction", "0.9")
        self.assertEqual(code, 1)
        self.assertIsNone(saved)
        self.assertIn("Error:", error)

    def test_no_relevant_ratings_are_rejected(self):
        held_out = pd.DataFrame(
            [(1, "a", 2)], columns=["user_id", "movie_id", "rating"]
        )
        with self.assertRaises(ValueError):
            RankingEvaluator().evaluate(FixedRecommender(), held_out)


if __name__ == "__main__":
    unittest.main()
