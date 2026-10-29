"""Command-line steps of the learning pipeline, run on tiny synthetic data."""
import contextlib
import io
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import prepare_cold_start
import prepare_data
import recommend
import train
from recommender import EaseRecommender
from sample_data import sample_events, sample_users, write_sample_data


def run_main(module, *arguments):
    """Run a script's main() with argv set; returns (exit_code, stdout, stderr)."""
    out, err = io.StringIO(), io.StringIO()
    code = 0
    with patch.object(sys, "argv", [module.__name__, *map(str, arguments)]):
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                module.main()
            except SystemExit as stop:
                code = stop.code
    return code, out.getvalue(), err.getvalue()


class PipelineScriptTests(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.root = Path(self._directory.name)
        self.data = write_sample_data(self.root / "data")
        self.model_path = self.root / "models" / "recommender.npz"

    def train_model(self):
        code, *_ = run_main(train, "--data-dir", self.data, "--model-path", self.model_path)
        self.assertEqual(code, 0)

    # prepare_data

    def test_prepare_data_writes_interactions_and_cold_start_users(self):
        code, out, _ = run_main(prepare_data, "--data-dir", self.data)
        self.assertEqual(code, 0)
        processed = self.data / "processed"
        interactions = pd.read_csv(processed / "interactions.csv.gz")
        cold = pd.read_csv(processed / "cold_start_users.csv.gz")
        self.assertEqual(len(interactions), 9)
        self.assertEqual(cold["user_id"].tolist(), [1001, 1002])
        self.assertIn("Cold-start users: 2", out)
        self.assertIn("Cold-start users with descriptions: 1", out)

    # train

    def test_train_saves_a_loadable_model_with_requested_hyperparameters(self):
        code, out, _ = run_main(
            train, "--data-dir", self.data, "--model-path", self.model_path,
            "--regularization", "25", "--min-rating", "7",
        )
        self.assertEqual(code, 0)
        self.assertIn("Regularization: 25", out)
        model = EaseRecommender.load(self.model_path)
        self.assertEqual((model.regularization, model.min_rating), (25, 7))
        self.assertEqual(model.user_ids.tolist(), [1, 2, 3, 4])
        self.assertEqual(len(model.movie_ids), 6)

    def test_train_fails_cleanly_on_bad_input(self):
        cases = {
            "missing data": ["--data-dir", self.root / "nowhere"],
            "invalid hyperparameter": ["--data-dir", self.data, "--regularization", "-1"],
        }
        for name, arguments in cases.items():
            with self.subTest(name):
                code, _, err = run_main(train, *arguments, "--model-path", self.model_path)
                self.assertEqual(code, 1)
                self.assertIn("Error:", err)
        self.assertFalse(self.model_path.exists())

    def test_train_refuses_invalid_events_without_writing_a_model(self):
        events = sample_events()
        events.loc[1, "rating"] = "99"
        write_sample_data(self.data, events=events)
        code, _, err = run_main(train, "--data-dir", self.data, "--model-path", self.model_path)
        self.assertEqual(code, 1)
        self.assertIn("between 1 and 10", err)
        self.assertFalse(self.model_path.exists())

    # recommend

    def test_recommend_prints_ease_results_for_users_with_history(self):
        self.train_model()
        code, out, _ = run_main(
            recommend, "--user-id", "1", "--top-k", "2",
            "--model-path", self.model_path, "--data-dir", self.data,
        )
        self.assertEqual(code, 0)
        self.assertIn('"method": "ease"', out)

    def test_recommend_reports_errors_for_invalid_requests(self):
        self.train_model()
        for name, arguments in {
            "non-positive user": ["--user-id", "0"],
            "non-positive top-k": ["--user-id", "1", "--top-k", "0"],
        }.items():
            with self.subTest(name):
                code, _, err = run_main(
                    recommend, *arguments, "--model-path", self.model_path, "--data-dir", self.data
                )
                self.assertEqual(code, 1)
                self.assertIn("must be positive", err)

    def test_recommend_requires_a_trained_model(self):
        code, _, err = run_main(
            recommend, "--user-id", "1", "--model-path", self.model_path, "--data-dir", self.data
        )
        self.assertEqual(code, 1)
        self.assertIn("src/train.py", err)

    def test_unknown_users_get_popularity_without_calling_the_llm(self):
        self.train_model()
        model = EaseRecommender.load(self.model_path)
        with patch("recommend.ColdStartRecommender") as cold_class:
            result = recommend.recommend_for_user(model, 424242, data_directory=self.data)
        cold_class.assert_not_called()
        self.assertEqual(result["method"], "popularity")
        self.assertEqual(result["fallback_reason"], "user_not_in_dataset")
        self.assertTrue(result["recommendations"])

    def test_user_with_description_goes_through_cold_start_not_ease(self):
        self.train_model()
        model = EaseRecommender.load(self.model_path)
        cold = MagicMock()
        cold.recommend.return_value = {"recommendations": [], "cached": False}
        result = recommend.recommend_for_user(model, 1001, data_directory=self.data, cold_start=cold)
        self.assertEqual(result["method"], "llm_cold_start")
        likes, dislikes = cold.recommend.call_args.args[:2]
        self.assertEqual((likes, dislikes), ("quiet dramas", "gore"))

    def test_user_without_description_falls_back_to_popularity(self):
        self.train_model()
        model = EaseRecommender.load(self.model_path)
        cold = MagicMock()
        result = recommend.recommend_for_user(model, 1002, data_directory=self.data, cold_start=cold)
        cold.recommend.assert_not_called()
        self.assertEqual(result["method"], "popularity")
        self.assertEqual(result["fallback_reason"], "no_description_or_positive_history")

    # prepare_cold_start

    def run_prepare_cold_start(self, *extra, cached=False):
        self.train_model()
        with patch("prepare_cold_start.ColdStartRecommender") as cold_class:
            cold_class.return_value.recommend.return_value = {
                "recommendations": [{"movie_id": "d"}], "cached": cached,
            }
            result = run_main(
                prepare_cold_start,
                "--data-dir", self.data, "--model-path", self.model_path,
                "--cache-dir", self.root / "cache", "--storage-path", self.root / "storage.sqlite3",
                *extra,
            )
        return (*result, cold_class.return_value.recommend)

    def cold_users(self):
        """User IDs prepare_cold_start visits, in users.csv.gz order, and who has a description."""
        model = EaseRecommender.load(self.model_path)
        users = sample_users()
        users = users[~users["user_id"].astype(int).map(model.has_positive_history)]
        described = users[users["self_description_likes"].ne("") | users["self_description_dislikes"].ne("")]
        return users["user_id"].tolist(), described["user_id"].tolist()

    def test_prepare_cold_start_only_calls_provider_for_described_users(self):
        code, out, _, recommend_call = self.run_prepare_cold_start()
        visited, described = self.cold_users()
        self.assertEqual(code, 0)
        # User 4 has only a below-threshold rating, so they count as cold but have no description
        self.assertIn("4", visited)
        self.assertEqual(recommend_call.call_count, len(described))
        self.assertEqual(recommend_call.call_args.args[:2], ("quiet dramas", "gore"))
        for user in visited:
            if user not in described:
                self.assertIn(f"User {user}: no description; popularity fallback", out)
        self.assertNotIn("User 1:", out)
        self.assertIn(f"Generated: {len(described)}; reused from cache: 0", out)

    def test_prepare_cold_start_counts_cache_hits(self):
        _, out, _, _ = self.run_prepare_cold_start(cached=True)
        _, described = self.cold_users()
        self.assertIn(f"Generated: 0; reused from cache: {len(described)}", out)

    def test_prepare_cold_start_limit_caps_the_number_of_users(self):
        _, out, _, recommend_call = self.run_prepare_cold_start("--limit", "2")
        visited, described = self.cold_users()
        kept = visited[:2]
        self.assertEqual(recommend_call.call_count, len([user for user in kept if user in described]))
        for user in visited[2:]:
            self.assertNotIn(f"User {user}:", out)
        self.assertGreater(len(visited), 2)

    def test_prepare_cold_start_rejects_invalid_options(self):
        self.train_model()
        for option in (["--limit", "0"], ["--top-k", "0"]):
            with self.subTest(option=option):
                with patch("prepare_cold_start.ColdStartRecommender") as cold_class:
                    code, _, err = run_main(
                        prepare_cold_start, "--data-dir", self.data, "--model-path", self.model_path,
                        "--storage-path", self.root / "storage.sqlite3", *option,
                    )
                self.assertEqual(code, 1)
                self.assertIn("must be positive", err)
                cold_class.assert_not_called()


if __name__ == "__main__":
    unittest.main()
