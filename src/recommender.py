from pathlib import Path

import numpy as np
import pandas as pd


MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "recommender.npz"
DEFAULT_REGULARIZATION = 100.0
DEFAULT_MIN_RATING = 6


class EaseRecommender:
    """Learn movie-to-movie weights from binary user profiles using EASE."""

    def __init__(
        self,
        regularization: float = DEFAULT_REGULARIZATION,
        min_rating: int = DEFAULT_MIN_RATING,
    ):
        if not np.isfinite(regularization) or regularization <= 0:
            raise ValueError("Regularization must be a positive finite number")

        if min_rating < 1 or min_rating > 10:
            raise ValueError("Minimum rating must be between 1 and 10")

        self.regularization = regularization
        self.min_rating = min_rating
        self.weights = None
        self.user_ids = np.array([], dtype=np.int64)
        self.movie_ids = np.array([], dtype=str)
        self.movie_titles = np.array([], dtype=str)
        self.user_profiles = np.empty((0, 0), dtype=bool)
        self.seen_movies = np.empty((0, 0), dtype=bool)
        self.popularity = np.array([], dtype=np.float64)
        self.user_index = {}
        self.movie_index = {}

    def fit(self, interactions: pd.DataFrame, movies: pd.DataFrame) -> None:
        if interactions.empty or movies.empty:
            raise ValueError("Training requires interactions and movie metadata")

        if interactions.duplicated(["user_id", "movie_id"]).any():
            raise ValueError("Training requires one row per user and movie")

        if movies["movie_id"].duplicated().any():
            raise ValueError("Movie IDs must be unique")

        if not interactions["movie_id"].isin(movies["movie_id"]).all():
            raise ValueError("Training interactions contain unknown movies")

        self.weights = None
        catalog = movies.sort_values("movie_id")
        self.user_ids = np.sort(interactions["user_id"].unique()).astype(np.int64)
        self.movie_ids = catalog["movie_id"].to_numpy(dtype=str)
        self.movie_titles = catalog["title"].to_numpy(dtype=str)
        self._build_indices()

        shape = (len(self.user_ids), len(self.movie_ids))
        self.user_profiles = np.zeros(shape, dtype=bool)
        self.seen_movies = np.zeros(shape, dtype=bool)

        user_rows = interactions["user_id"].map(self.user_index).to_numpy(dtype=np.int64)
        movie_columns = interactions["movie_id"].map(self.movie_index).to_numpy(dtype=np.int64)
        # Seen movies include low ratings, even though they do not count as positive
        self.seen_movies[user_rows, movie_columns] = True

        liked = interactions["rating"].ge(self.min_rating).fillna(False)
        # A rating takes precedence over the watch event
        unrated_watches = (
            interactions["rating"].isna() & interactions["watch_count"].gt(0)
        )
        positive = (liked | unrated_watches).to_numpy(dtype=bool)
        self.user_profiles[user_rows[positive], movie_columns[positive]] = True

        if not self.user_profiles.any():
            raise ValueError("Training requires at least one positive interaction")

        training_matrix = self.user_profiles.astype(np.float64)
        gram_matrix = training_matrix.T @ training_matrix
        diagonal = np.diag_indices_from(gram_matrix)
        gram_matrix[diagonal] += self.regularization

        inverse = np.linalg.solve(gram_matrix, np.eye(len(self.movie_ids)))
        # Normalize each target column using the EASE closed-form solution
        self.weights = -inverse / np.diag(inverse)
        # Prevent a movie from predicting itself
        self.weights[diagonal] = 0.0
        self.weights = self.weights.astype(np.float32)
        self.popularity = self.user_profiles.mean(axis=0)

    def _build_indices(self) -> None:
        self.user_index = {
            int(user_id): index for index, user_id in enumerate(self.user_ids)
        }
        self.movie_index = {
            movie_id: index for index, movie_id in enumerate(self.movie_ids)
        }

    def has_positive_history(self, user_id: int) -> bool:
        user_row = self.user_index.get(user_id)

        if user_row is None:
            return False

        return bool(self.user_profiles[user_row].any())

    def get_scores(self, user_id: int, popularity_only: bool = False) -> np.ndarray:
        if self.weights is None:
            raise ValueError("Train or load a model before requesting recommendations")

        if user_id <= 0:
            raise ValueError("User ID must be positive")

        if popularity_only or not self.has_positive_history(user_id):
            return self.popularity.copy()

        user_row = self.user_index[user_id]
        liked_movies = np.flatnonzero(self.user_profiles[user_row])

        return self.weights[liked_movies].sum(axis=0, dtype=np.float64)

    def recommend(
        self, user_id: int, top_k: int = 10, popularity_only: bool = False
    ) -> pd.DataFrame:
        if top_k <= 0:
            raise ValueError("The number of recommendations must be positive")

        scores = self.get_scores(user_id, popularity_only)
        user_row = self.user_index.get(user_id)
        seen_columns = np.flatnonzero(self.seen_movies[user_row]) if user_row is not None else []
        return self._recommend_scores(scores, seen_columns, top_k)

    def recommend_from_profile(
        self,
        positive_movie_ids,
        seen_movie_ids,
        top_k: int = 20,
        popularity_only: bool = False,
    ) -> pd.DataFrame:
        if self.weights is None:
            raise ValueError("Train or load a model before requesting recommendations")
        if top_k <= 0:
            raise ValueError("The number of recommendations must be positive")

        positive_columns = sorted({
            self.movie_index[movie_id]
            for movie_id in positive_movie_ids
            if movie_id in self.movie_index and self.popularity[self.movie_index[movie_id]] > 0
        })
        scores = self.popularity.copy() if popularity_only or not positive_columns else (
            self.weights[positive_columns].sum(axis=0, dtype=np.float64)
        )
        seen_columns = [
            self.movie_index[movie_id] for movie_id in set(seen_movie_ids)
            if movie_id in self.movie_index
        ]
        return self._recommend_scores(scores, seen_columns, top_k)

    def _recommend_scores(self, scores, seen_columns, top_k):
        eligible = self.popularity > 0
        eligible[seen_columns] = False

        candidates = np.flatnonzero(eligible)
        order = np.lexsort(
            (
                self.movie_ids[candidates],
                -self.popularity[candidates],
                -scores[candidates],
            )
        )
        selected = candidates[order[:top_k]]

        return pd.DataFrame(
            {
                "movie_id": self.movie_ids[selected],
                "title": self.movie_titles[selected],
                "score": scores[selected],
            }
        )

    def save(self, path: Path = MODEL_PATH) -> None:
        if self.weights is None:
            raise ValueError("Train a model before saving it")

        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("wb") as output:
            np.savez_compressed(
                output,
                format_version=np.array(1),
                regularization=np.array(self.regularization),
                min_rating=np.array(self.min_rating),
                user_ids=self.user_ids,
                movie_ids=self.movie_ids,
                movie_titles=self.movie_titles,
                user_profiles=self.user_profiles,
                seen_movies=self.seen_movies,
                weights=self.weights,
                popularity=self.popularity,
            )

    @classmethod
    def load(cls, path: Path = MODEL_PATH) -> "EaseRecommender":
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing {path}. Run python src/train.py first."
            )

        with np.load(path, allow_pickle=False) as saved:
            if int(saved["format_version"]) != 1:
                raise ValueError("Unsupported model file version")

            model = cls(
                regularization=float(saved["regularization"]),
                min_rating=int(saved["min_rating"]),
            )
            model.user_ids = saved["user_ids"]
            model.movie_ids = saved["movie_ids"]
            model.movie_titles = saved["movie_titles"]
            model.user_profiles = saved["user_profiles"]
            model.seen_movies = saved["seen_movies"]
            model.weights = saved["weights"]
            model.popularity = saved["popularity"]

        model._build_indices()

        return model
