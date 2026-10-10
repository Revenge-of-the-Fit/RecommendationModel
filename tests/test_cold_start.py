import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from cold_start import ColdStartRecommender
from preferences import ColdStartError, LLM_MODEL
from recommend import recommend_for_user
from recommender import EaseRecommender


class ColdStartTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.cache_directory = Path(self.temporary.name)
        self.movies = pd.DataFrame(
            [
                ("a", "Movie A", "Drama", "A thoughtful drama."),
                ("b", "Movie B", "Horror", "A frightening ghost story."),
                ("c", "Movie C", "Crime|Drama", "A detective investigates a crime."),
                ("d", "Movie D", "Comedy", "A clever comedy."),
                ("e", "Movie E", "Thriller", "A tense mystery."),
            ],
            columns=["movie_id", "title", "genres", "overview"],
        )
        self.recommender = ColdStartRecommender(
            self.movies, self.cache_directory, self.cache_directory / "unused.env",
            storage_path=self.cache_directory / "records.sqlite3",
        )
        self.interpreter = self.recommender.interpreter
        self.profile = {
            "liked_genres": ["Crime", "Drama"],
            "excluded_genres": ["Horror"],
            "liked_titles": ["Movie A"],
            "disliked_titles": ["Movie D"],
            "likes_summary": "Detective investigations and crime dramas.",
            "dislikes_summary": "Horror and silly comedy.",
        }
        self.saved = {"model": LLM_MODEL, "profile": self.profile}

    def tearDown(self):
        self.temporary.cleanup()

    def test_filters_seen_movies_examples_and_excluded_genres(self):
        result = self.recommender._rank_movies(self.profile, {"e"}, 10)

        self.assertEqual([item["movie_id"] for item in result], ["c"])
        self.assertEqual(result[0]["title"], "Movie C")
        self.assertEqual(result[0]["rank"], 1)
        self.assertTrue(np.isfinite(result[0]["score"]))

    def test_unknown_example_titles_cannot_create_movie_ids(self):
        profile = copy.deepcopy(self.profile)
        profile["liked_titles"].append("A Film That Does Not Exist")
        result = self.recommender._rank_movies(profile, set(), 10)

        self.assertTrue({item["movie_id"] for item in result}.issubset(set("abcde")))
        self.assertEqual(len(result), len({item["movie_id"] for item in result}))

    def test_title_matching_handles_articles_punctuation_and_years(self):
        matches = self.recommender._match_titles(["The Movie A (1995)", "MOVIE-A"])
        self.assertEqual(matches, [0])

    def test_title_matching_handles_stylized_cent_signs(self):
        stylized = "Ri" + chr(162) + "hie Ri" + chr(162) + "h"
        self.assertEqual(
            self.recommender._normalize_title(stylized),
            self.recommender._normalize_title("Richie Rich"),
        )

    def test_unknown_genres_and_wrong_field_types_are_rejected(self):
        for field, value in [
            ("excluded_genres", ["Unknown"]),
            ("liked_titles", "Movie A"),
            ("likes_summary", None),
        ]:
            profile = copy.deepcopy(self.profile)
            profile[field] = value
            with self.subTest(field=field):
                with self.assertRaises(ColdStartError):
                    self.interpreter._validate_profile(profile)

    def test_cache_reuses_profile_without_api_access(self):
        with patch.object(self.interpreter, "_request", return_value=self.saved) as request:
            first = self.recommender.recommend("Crime dramas", "Horror", top_k=2)
            second = self.recommender.recommend("Crime dramas", "Horror", top_k=1, offline=True)

        self.assertEqual(request.call_count, 1)
        self.assertFalse(first["cached"])
        self.assertTrue(second["cached"])
        self.assertEqual(second["recommendations"], first["recommendations"][:1])
        prompt = json.loads(request.call_args.args[0])
        self.assertEqual(prompt, {"likes": "Crime dramas", "dislikes": "Horror"})

    def test_changed_preferences_refresh_profile(self):
        with patch.object(self.interpreter, "_request", return_value=self.saved) as request:
            self.recommender.recommend("Crime", "Horror")
            self.recommender.recommend("Drama", "Horror")
            self.assertEqual(request.call_count, 2)

    def test_seen_movies_are_filtered_again_using_cached_profile(self):
        with patch.object(self.interpreter, "_request", return_value=self.saved) as request:
            self.recommender.recommend("Crime", "Horror")
            result = self.recommender.recommend("Crime", "Horror", seen_movie_ids={"c"}, offline=True)

        self.assertEqual(request.call_count, 1)
        self.assertEqual([item["movie_id"] for item in result["recommendations"]], ["e"])

    def test_catalog_changes_use_cached_preferences_and_new_metadata(self):
        with patch.object(self.interpreter, "_request", return_value=self.saved):
            self.recommender.recommend("Crime", "Horror")

        changed_movies = self.movies.copy()
        changed_movies.loc[2, "genres"] = "Crime|Horror"
        changed = ColdStartRecommender(changed_movies, self.cache_directory)
        with patch.object(changed.interpreter, "_request") as request:
            result = changed.recommend("Crime", "Horror", offline=True)
            request.assert_not_called()

        self.assertNotIn("c", [item["movie_id"] for item in result["recommendations"]])

    def test_offline_cache_miss_never_calls_api(self):
        with patch.object(self.interpreter, "_request") as request:
            with self.assertRaises(ColdStartError):
                self.recommender.recommend("Crime", "Horror", offline=True)
            request.assert_not_called()

    def test_corrupt_cache_is_rejected_offline(self):
        with patch.object(self.interpreter, "_request", return_value=self.saved):
            self.recommender.recommend("Crime", "Horror")
        next(self.cache_directory.glob("*.json")).write_text("{invalid", encoding="utf-8")

        with patch.object(self.interpreter, "_request") as request:
            with self.assertRaises(ColdStartError):
                self.recommender.recommend("Crime", "Horror", offline=True)
            request.assert_not_called()

    def test_blank_descriptions_and_invalid_counts_are_rejected(self):
        with patch.object(self.interpreter, "_request") as request:
            for likes, dislikes, count in [("", " ", 10), ("Crime", "", 0)]:
                with self.assertRaises(ValueError):
                    self.recommender.recommend(likes, dislikes, count)
            request.assert_not_called()

    def test_exhausted_catalog_returns_an_empty_list(self):
        result = self.recommender._rank_movies(self.profile, set("abcde"), 10)
        self.assertEqual(result, [])

    def test_movie_examples_use_learned_relationships(self):
        interactions = pd.DataFrame(
            [(1, "a", 8, 1), (1, "e", 8, 1), (2, "a", 8, 1), (2, "e", 8, 1), (3, "c", 8, 1)],
            columns=["user_id", "movie_id", "rating", "watch_count"],
        )
        model = EaseRecommender()
        model.fit(interactions, self.movies)
        recommender = ColdStartRecommender(self.movies, self.cache_directory, model=model)
        scores = recommender._collaborative_scores([0], [])

        self.assertGreater(scores[4], scores[2])
        self.assertGreater(scores[4], 0)

    def test_api_request_uses_luna_with_a_small_payload(self):
        response = SimpleNamespace(
            status="completed", output_text=json.dumps(self.profile),
            model=LLM_MODEL, id="test-response", usage=None,
        )
        with patch.dict("os.environ", {}, clear=True):
            with patch("preferences.dotenv_values", return_value={"OPENAI_TOKEN": "test-token"}):
                with patch("preferences.OpenAI") as client_class:
                    client = client_class.return_value.__enter__.return_value
                    client.responses.create.return_value = response
                    result = self.interpreter._request("preferences", self.interpreter._response_schema())

        arguments = client.responses.create.call_args.kwargs
        self.assertEqual(arguments["model"], "gpt-6-luna")
        self.assertEqual(arguments["reasoning"], {"effort": "none"})
        self.assertFalse(arguments["store"])
        self.assertTrue(arguments["text"]["format"]["strict"])
        self.assertLess(len(arguments["instructions"]), 2000)
        self.assertEqual(result["profile"], self.profile)
        self.assertEqual(client_class.call_args.kwargs["base_url"], "https://api.openai.com/v1")

    def test_missing_key_has_a_clear_error(self):
        with patch.dict("os.environ", {}, clear=True):
            with patch("preferences.dotenv_values", return_value={}):
                with self.assertRaisesRegex(ColdStartError, "OPENAI_TOKEN"):
                    self.interpreter._request("preferences", {})

    def test_partial_azure_configuration_does_not_fall_back_to_openai(self):
        for name in ("AZURE_OPENAI_ENDPOINT", "AZURE_OPENAI_DEPLOYMENT", "AZURE_OPENAI_API_KEY"):
            with self.subTest(name=name), patch.dict("os.environ", {}, clear=True):
                with patch("preferences.dotenv_values", return_value={name: "set", "OPENAI_TOKEN": "direct-key"}):
                    with patch("preferences.OpenAI") as client:
                        with self.assertRaisesRegex(ColdStartError, "AZURE_OPENAI"):
                            self.interpreter._request("preferences", {})
                        client.assert_not_called()

    def test_azure_project_url_is_not_used_as_the_model_endpoint(self):
        settings = {
            "AZURE_OPENAI_ENDPOINT": "https://example.services.ai.azure.com/api/projects/proj-default",
            "AZURE_OPENAI_DEPLOYMENT": "gpt-5-mini", "AZURE_OPENAI_API_KEY": "azure-test-key",
        }
        with patch.dict("os.environ", {}, clear=True), patch("preferences.dotenv_values", return_value=settings):
            with patch("preferences.OpenAI") as client:
                with self.assertRaisesRegex(ColdStartError, "resource endpoint"):
                    self.interpreter._request("preferences", {})
                client.assert_not_called()

    def test_api_errors_do_not_expose_the_key(self):
        class RequestFailure(Exception):
            status_code = 401

        with patch.dict("os.environ", {}, clear=True):
            with patch("preferences.dotenv_values", return_value={"OPENAI_TOKEN": "test-secret"}):
                with patch("preferences.APIError", RequestFailure):
                    with patch("preferences.OpenAI", side_effect=RequestFailure("test-secret")):
                        with self.assertRaises(ColdStartError) as error:
                            self.interpreter._request("preferences", {})

        self.assertIn("HTTP 401", str(error.exception))
        self.assertNotIn("test-secret", str(error.exception))

    def test_warm_users_do_not_load_cold_start_data(self):
        model = Mock()
        model.has_positive_history.return_value = True
        model.recommend.return_value = pd.DataFrame(
            [{"movie_id": "c", "title": "Movie C", "score": 0.5}]
        )
        with patch("recommend.MovieDataset") as dataset:
            result = recommend_for_user(model, 1)
            dataset.assert_not_called()
        self.assertEqual(result["method"], "ease")

    def test_descriptions_route_to_cold_start(self):
        model = Mock()
        model.has_positive_history.return_value = False
        model.user_index = {}
        users = pd.DataFrame(
            [(1001, "Crime", "Horror")],
            columns=["user_id", "self_description_likes", "self_description_dislikes"],
        )
        with patch("recommend.MovieDataset", return_value=SimpleNamespace(users=users, movies=self.movies)):
            with patch("recommend.ColdStartRecommender") as cold_class:
                cold_class.return_value.recommend.return_value = {
                    "recommendations": [], "cached": True, "llm_model": LLM_MODEL
                }
                result = recommend_for_user(model, 1001, offline=True)

        self.assertEqual(result["method"], "llm_cold_start")
        cold_class.return_value.recommend.assert_called_once_with("Crime", "Horror", 10, set(), True)

    def test_missing_description_uses_popularity(self):
        model = Mock()
        model.has_positive_history.return_value = False
        model.recommend.return_value = pd.DataFrame(
            [{"movie_id": "c", "title": "Movie C", "score": 0.5}]
        )
        users = pd.DataFrame(
            [(1001, "", "")],
            columns=["user_id", "self_description_likes", "self_description_dislikes"],
        )
        with patch("recommend.MovieDataset", return_value=SimpleNamespace(users=users, movies=self.movies)):
            with patch("recommend.ColdStartRecommender") as cold_class:
                result = recommend_for_user(model, 1001)
                cold_class.assert_not_called()
        self.assertEqual(result["method"], "popularity")


if __name__ == "__main__":
    unittest.main()
