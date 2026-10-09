"""Reuse the existing recommendation entry point with persistent resources."""

import logging
import sqlite3

from cold_start import ColdStartRecommender
from dataset import MovieDataset
from models.serving import RecommendationResult
from models.settings import ServingSettings
from preferences import ColdStartError
from recommend import recommend_for_user
from recommender import EaseRecommender
from services.versions import dataset_version, file_version
from storage.live import LiveStore, merge_history, read_live_user
from storage.profiles import content_version


LOGGER = logging.getLogger(__name__)


class RecommendationService:
    def __init__(
        self,
        model: EaseRecommender,
        dataset: MovieDataset,
        cold_start: ColdStartRecommender,
        versions: dict[str, str] | None = None,
    ):
        self.model = model
        self.dataset = dataset
        self.cold_start = cold_start
        self.versions = versions or {}
        self.profile_import_error = None
        self.settings = None
        self.live_worker = None

    @classmethod
    def load(cls, settings: ServingSettings) -> "RecommendationService":
        model = EaseRecommender.load(settings.model_path)
        dataset = MovieDataset(settings.data_directory)
        if not set(model.movie_ids).issubset(set(dataset.movies["movie_id"])):
            raise ValueError("The trained model contains movies absent from the catalog")
        cold_start = ColdStartRecommender(
            dataset.movies, settings.cache_directory, model=model, storage_path=settings.storage_path
        )
        profile_import_error = None
        try:
            cold_start.interpreter.import_cache_profiles()
        except Exception as error:
            profile_import_error = type(error).__name__
            LOGGER.error("Profile provenance import unavailable (%s)", profile_import_error)
        service = cls(model, dataset, cold_start, versions={
            "model": file_version(settings.model_path),
            "dataset": dataset_version(settings.data_directory),
        })
        service.profile_import_error = profile_import_error
        service.settings = settings
        if settings.live_enabled:
            from services.live_worker import LiveProfileWorker
            with LiveStore(settings.storage_path):
                pass
            service.live_worker = LiveProfileWorker(settings, cold_start)
        return service

    def start(self):
        if self.live_worker is not None:
            self.live_worker.start()

    def close(self):
        if self.live_worker is not None:
            return self.live_worker.close()
        return True

    def recommend(self, user_id: int) -> RecommendationResult:
        if self.settings is not None and self.settings.live_enabled:
            try:
                history = read_live_user(self.settings.storage_path, user_id,
                                         self.settings.live_source_id, self.settings.live_topic)
            except (sqlite3.Error, OSError, ValueError) as error:
                LOGGER.error("Live state unavailable (%s)", type(error).__name__)
            else:
                return self._recommend_live(user_id, history)
        try:
            result = recommend_for_user(
                self.model,
                user_id,
                top_k=20,
                offline=True,
                dataset=self.dataset,
                cold_start=self.cold_start,
            )
        except ColdStartError:
            # A missing/invalid cached profile uses the existing popularity fallback.
            result = {
                "user_id": user_id,
                "method": "popularity",
                "fallback_reason": "cold_start_profile_unavailable",
                "recommendations": self.model.recommend(user_id, 20).to_dict(orient="records"),
            }
        return RecommendationResult.model_validate(result)

    def _recommend_live(self, user_id, history):
        supported, seen = merge_history(self.model, user_id, history)
        prepared = history["prepared"]
        saved = prepared.get("profile") if prepared else None
        if saved:
            profile = saved["profile"]
            excluded_genres = set(profile["excluded_genres"])
            seen.update(movie for movie, genres in zip(self.cold_start.movie_ids, self.cold_start.movie_genres)
                        if genres & excluded_genres)
            seen.update(self.cold_start.movie_ids[index] for index in
                        self.cold_start._match_titles(profile["liked_titles"] + profile["disliked_titles"]))
        if supported:
            result = {"user_id": user_id, "method": "ease", "recommendations":
                      self.model.recommend_from_profile(supported, seen, 20).to_dict(orient="records")}
            if saved:
                reference = saved.get("_provenance") or {}
                result.update(
                    profile_version=content_version(saved["profile"]), profile_id=reference.get("profile_id"),
                    llm_attempt_id=reference.get("attempt_id"), llm_response_id=saved.get("response_id"),
                    llm_model=saved.get("model"), profile_origin=reference.get("origin"),
                    prompt_version=reference.get("prompt_version"), schema_version=reference.get("schema_version"),
                )
        elif saved:
            result = {"user_id": user_id, "method": "llm_cold_start",
                      **self.cold_start.recommend_profile(saved, 20, seen)}
        elif prepared is None:
            try:
                users = self.dataset.users.loc[self.dataset.users["user_id"].eq(user_id)]
                user = users.iloc[0] if not users.empty else None
                if user is not None and (user.self_description_likes.strip() or user.self_description_dislikes.strip()):
                    result = {"user_id": user_id, "method": "llm_cold_start",
                              **self.cold_start.recommend(user.self_description_likes, user.self_description_dislikes,
                                                          20, seen, offline=True)}
                else:
                    result = self._live_fallback(user_id, seen, "live_profile_pending")
            except ColdStartError:
                result = self._live_fallback(user_id, seen, "live_profile_pending")
        else:
            result = self._live_fallback(user_id, seen, "no_description_or_positive_history")
        return RecommendationResult.model_validate(result)

    def _live_fallback(self, user_id, seen, reason):
        return {"user_id": user_id, "method": "popularity", "fallback_reason": reason,
                "recommendations": self.model.recommend_from_profile(set(), seen, 20, popularity_only=True).to_dict(orient="records")}
