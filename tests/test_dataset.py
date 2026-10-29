import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from dataset import DatasetError, MovieDataset
from sample_data import sample_events, sample_movies, sample_users, write_sample_data


class DatasetLoadingTests(unittest.TestCase):
    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.directory = Path(self._directory.name)

    def load(self, **tables):
        return MovieDataset(write_sample_data(self.directory, **tables))

    def test_loads_typed_tables(self):
        dataset = self.load()
        self.assertEqual(dataset.events["user_id"].dtype, "int64")
        self.assertEqual(str(dataset.events["timestamp"].dt.tz), "UTC")
        self.assertEqual(str(dataset.events["rating"].dtype), "Int64")
        self.assertEqual(dataset.events["rating"].isna().sum(), 6)
        self.assertEqual(len(dataset.users), 6)

    def test_interactions_combine_watches_and_keep_the_latest_rating(self):
        interactions = self.load().get_interactions().set_index(["user_id", "movie_id"])

        # User 3 rated movie d twice (7 then 9) and watched it twice
        self.assertEqual(interactions.loc[(3, "d"), "rating"], 9)
        self.assertEqual(interactions.loc[(3, "d"), "watch_count"], 2)
        # A watch followed by a rating keeps both signals
        self.assertEqual(interactions.loc[(1, "a"), "watch_count"], 1)
        self.assertEqual(interactions.loc[(1, "a"), "rating"], 6)
        # An unrated watch has no rating; a rating alone has no watches
        self.assertTrue(pd.isna(interactions.loc[(1, "c"), "rating"]))
        self.assertEqual(interactions.loc[(2, "a"), "watch_count"], 0)
        self.assertEqual(
            interactions.loc[(3, "d"), "first_timestamp"], pd.Timestamp("2026-01-04T10:10:00Z")
        )
        self.assertEqual(
            interactions.loc[(3, "d"), "last_timestamp"], pd.Timestamp("2026-01-05T11:00:00Z")
        )

    def test_interactions_have_one_row_per_pair_and_ignore_account_events(self):
        interactions = self.load().get_interactions()
        self.assertFalse(interactions.duplicated(["user_id", "movie_id"]).any())
        self.assertTrue(interactions["user_id"].isin([1, 2, 3, 4]).all())
        self.assertEqual(len(interactions), 9)

    def test_interactions_can_be_computed_for_a_subset_of_events(self):
        dataset = self.load()
        early = dataset.events[dataset.events["timestamp"] < pd.Timestamp("2026-01-03T00:00:00Z")]
        interactions = dataset.get_interactions(early)
        self.assertEqual(set(interactions["user_id"]), {1})

    def test_cold_start_users_have_no_movie_events(self):
        cold = self.load().get_cold_start_users()
        self.assertEqual(cold["user_id"].tolist(), [1001, 1002])

    def test_cold_start_users_include_users_who_never_appear_in_events(self):
        users = pd.concat(
            [sample_users(), pd.DataFrame([("7", "x", "y")], columns=sample_users().columns)]
        )
        cold = self.load(users=users).get_cold_start_users()
        self.assertEqual(cold["user_id"].tolist(), [7, 1001, 1002])


class DatasetValidationTests(unittest.TestCase):
    """Malformed inputs must fail loudly instead of reaching training."""

    def setUp(self):
        self._directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._directory.cleanup)
        self.directory = Path(self._directory.name)

    def assertRejected(self, message, exception=DatasetError, **tables):
        write_sample_data(self.directory, **tables)
        with self.assertRaisesRegex(exception, message):
            MovieDataset(self.directory)

    def with_event(self, **changes):
        events = sample_events()
        events.loc[0, list(changes)] = list(changes.values())
        return events

    def test_dataset_errors_remain_value_errors_for_existing_callers(self):
        self.assertTrue(issubclass(DatasetError, ValueError))

    def test_missing_file(self):
        with self.assertRaisesRegex(FileNotFoundError, "download_data"):
            MovieDataset(self.directory)

    def test_missing_columns(self):
        self.assertRejected("missing columns: rating", events=sample_events().drop(columns="rating"))
        self.assertRejected("missing columns: license_cost", movies=sample_movies().drop(columns="license_cost"))

    def test_empty_tables(self):
        self.assertRejected("contains no rows", users=sample_users().iloc[0:0])

    def test_user_problems(self):
        users = sample_users()
        duplicated = pd.concat([users, users.iloc[:1]])
        self.assertRejected("User IDs must be unique", users=duplicated)
        nonpositive = users.copy()
        nonpositive.loc[0, "user_id"] = "0"
        self.assertRejected("User IDs must be positive", users=nonpositive, events=sample_events().iloc[1:])
        self.assertRejected(
            "users.csv.gz has invalid values in column user_id",
            users=users.assign(user_id=["one", *users["user_id"][1:]]),
        )
        self.assertRejected(
            "users.csv.gz has invalid values in column user_id",
            users=users.assign(user_id=["9" * 30, *users["user_id"][1:]]),
        )

    def test_movie_problems(self):
        movies = sample_movies()
        self.assertRejected("Movie IDs must be unique", movies=pd.concat([movies, movies.iloc[:1]]))
        self.assertRejected("Movie IDs cannot be empty", movies=movies.assign(movie_id=["", *movies["movie_id"][1:]]))

    def test_event_type_problems(self):
        self.assertRejected("unsupported event type", events=self.with_event(event_type="purchase"))

    def test_events_must_refer_to_known_users_and_movies(self):
        self.assertRejected("users missing", events=self.with_event(user_id="999"))
        self.assertRejected("movies missing", events=self.with_event(movie_id="zzz"))

    def test_unparseable_timestamps(self):
        self.assertRejected(
            "events.csv.gz has invalid values in column timestamp",
            events=self.with_event(timestamp="yesterday"),
        )

    def test_rating_problems(self):
        for bad in ["0", "11", "-3"]:
            with self.subTest(rating=bad):
                events = sample_events()
                events.loc[1, "rating"] = bad
                self.assertRejected("between 1 and 10", events=events)
        events = sample_events()
        events.loc[1, "rating"] = ""
        self.assertRejected("between 1 and 10", events=events)
        events.loc[1, "rating"] = "great"
        self.assertRejected("events.csv.gz has invalid values in column rating", events=events)

    def test_ratings_only_belong_on_rating_events(self):
        self.assertRejected("Only rating events", events=self.with_event(rating="5"))

    def test_account_events_cannot_carry_movies(self):
        events = sample_events()
        events.loc[events["event_type"].eq("account_created").idxmax(), "movie_id"] = "a"
        self.assertRejected("cannot contain movies", events=events)


if __name__ == "__main__":
    unittest.main()
