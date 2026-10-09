import hashlib
import json
import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer

from preferences import CACHE_DIRECTORY, ENV_FILE, LLM_MODEL, PreferenceInterpreter
from recommender import EaseRecommender


class ColdStartRecommender:
    """Rank catalog movies using a user's descriptions and movie examples."""

    def __init__(
        self,
        movies: pd.DataFrame,
        cache_directory: Path = CACHE_DIRECTORY,
        env_file: Path = ENV_FILE,
        model: EaseRecommender | None = None,
    ):
        self.movies = movies.sort_values("movie_id").reset_index(drop=True)
        self.movie_ids = self.movies["movie_id"].to_numpy(dtype=str)
        self.movie_genres = [set(value.split("|")) for value in self.movies["genres"]]
        self.model = model
        genres = sorted(set().union(*self.movie_genres))
        self.interpreter = PreferenceInterpreter(genres, cache_directory, env_file)
        self.title_index = {}

        for index, title in enumerate(self.movies["title"]):
            key = self._normalize_title(title)
            self.title_index.setdefault(key, []).append(index)

        descriptions = (
            self.movies["title"] + " "
            + self.movies["genres"].str.replace("|", " ", regex=False) + " "
            + self.movies["overview"]
        )
        self.vectorizer = TfidfVectorizer(
            stop_words="english", ngram_range=(1, 2), max_features=30000
        )
        self.movie_features = self.vectorizer.fit_transform(descriptions)
        self.popularity = np.zeros(len(self.movies))

        if model is not None:
            if model.weights is None:
                raise ValueError("Cold-start recommendations require a trained model")

            for index, movie_id in enumerate(self.movie_ids):
                model_index = model.movie_index.get(movie_id)
                if model_index is not None:
                    self.popularity[index] = model.popularity[model_index]

    def recommend(
        self,
        likes: str,
        dislikes: str,
        top_k: int = 10,
        seen_movie_ids: set[str] | None = None,
        offline: bool = False,
    ) -> dict:
        if top_k < 1:
            raise ValueError("The number of recommendations must be positive")

        # Reuse the interpretation, but rank again with the current model and seen movies
        profile, cached = self.interpreter.get_profile(likes, dislikes, offline)
        seen = set() if seen_movie_ids is None else set(seen_movie_ids)
        recommendations = self._rank_movies(profile, seen, top_k)

        return {
            "recommendations": recommendations,
            "llm_model": LLM_MODEL,
            "cached": cached,
            "profile_version": "sha256:" + hashlib.sha256(
                json.dumps(profile, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            ).hexdigest(),
        }

    @staticmethod
    def _normalize_title(title: str) -> str:
        title = re.sub(r"\s*\(\d{4}\)\s*$", "", title)
        title = title.replace(chr(162), "c")
        title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
        title = re.sub(r"^(the|an|a)\s+", "", title.casefold().strip())
        return re.sub(r"[^a-z0-9]", "", title)

    def _match_titles(self, titles: list[str]) -> list[int]:
        """Match normalized titles without guessing at similar names."""
        matches = set()
        for title in titles:
            matches.update(self.title_index.get(self._normalize_title(title), []))
        return sorted(matches)

    def _text_scores(self, summary: str, movie_indices: list[int]) -> np.ndarray:
        profile = self.vectorizer.transform([summary]).toarray()[0]
        if movie_indices:
            profile += np.asarray(self.movie_features[movie_indices].mean(axis=0)).ravel()

        length = np.linalg.norm(profile)
        if length > 0:
            profile /= length
        return np.asarray(self.movie_features @ profile).ravel()

    @staticmethod
    def _scale_positive(scores: np.ndarray) -> np.ndarray:
        positive = np.maximum(scores, 0.0)
        maximum = positive.max(initial=0.0)
        return positive / maximum if maximum > 0 else positive

    def _collaborative_scores(self, liked: list[int], disliked: list[int]) -> np.ndarray:
        scores = np.zeros(len(self.movies))
        if self.model is None:
            return scores

        model_scores = np.zeros(len(self.model.movie_ids))
        for indices, weight in [(liked, 1.0), (disliked, -0.5)]:
            seed_rows = [
                self.model.movie_index[self.movie_ids[index]]
                for index in indices if self.movie_ids[index] in self.model.movie_index
            ]
            if seed_rows:
                model_scores += weight * self.model.weights[seed_rows].mean(axis=0)

        for index, movie_id in enumerate(self.movie_ids):
            model_index = self.model.movie_index.get(movie_id)
            if model_index is not None:
                scores[index] = model_scores[model_index]

        return self._scale_positive(scores)

    def _rank_movies(self, profile: dict, seen: set[str], top_k: int) -> list[dict]:
        liked = self._match_titles(profile["liked_titles"])
        disliked = self._match_titles(profile["disliked_titles"])
        preferred_genres = set(profile["liked_genres"])
        excluded_genres = set(profile["excluded_genres"])
        # Treat recognized examples as movies the user already knows
        excluded_ids = seen | set(self.movie_ids[liked]) | set(self.movie_ids[disliked])

        likes_text = profile["likes_summary"] + " " + " ".join(sorted(preferred_genres))
        positive_text = self._text_scores(likes_text, liked)
        negative_text = self._text_scores(profile["dislikes_summary"], disliked)
        genre_scores = np.array(
            [len(genres & preferred_genres) / max(1, len(preferred_genres))
             for genres in self.movie_genres]
        )
        collaborative = self._collaborative_scores(liked, disliked)
        # Hand-set weights for the cold-start blend
        scores = (
            0.50 * positive_text
            + 0.25 * genre_scores
            + 0.20 * collaborative
            + 0.05 * self._scale_positive(self.popularity)
            - 0.35 * negative_text
        )

        candidates = np.array(
            [index for index, movie_id in enumerate(self.movie_ids)
             if movie_id not in excluded_ids and not self.movie_genres[index] & excluded_genres],
            dtype=np.int64,
        )
        order = np.lexsort(
            (self.movie_ids[candidates], -self.popularity[candidates], -scores[candidates])
        )
        selected = candidates[order[:top_k]]
        recommendations = []

        for index in selected:
            matched_genres = sorted(self.movie_genres[index] & preferred_genres)
            if matched_genres:
                reason = "Matches preferred genres: " + ", ".join(matched_genres) + "."
            elif positive_text[index] > 0:
                reason = "Shares themes or descriptions with your stated preferences."
            elif collaborative[index] > 0:
                reason = "Related to your movie examples through other users' preferences."
            else:
                reason = "A popular available movie outside your exclusions."

            recommendations.append(
                {
                    "movie_id": self.movie_ids[index],
                    "title": self.movies.iloc[index]["title"],
                    "rank": len(recommendations) + 1,
                    "score": float(scores[index]),
                    "reason": reason,
                }
            )

        return recommendations
