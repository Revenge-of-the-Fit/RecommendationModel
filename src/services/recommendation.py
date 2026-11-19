"""Reuse the existing recommendation entry point with persistent resources."""

import logging
import os
import sqlite3
import time
from pathlib import Path

from cold_start import ColdStartRecommender
from dataset import MovieDataset
from models.serving import RecommendationResult
from models.settings import ServingSettings
from preferences import ColdStartError
from provenance import register_serving
from recommend import recommend_for_user
from recommender import EaseRecommender
from services.versions import code_version, dataset_version, file_version
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
        self.provenance_error = None
        self.settings = None
        self.live_worker = None

    @classmethod
    def load(cls, settings: ServingSettings) -> "RecommendationService":
        original = settings.model_path.stat()
        model = EaseRecommender.load(settings.model_path)
        artifact_version = file_version(settings.model_path)
        current = settings.model_path.stat()
        if (original.st_dev, original.st_ino, original.st_size, original.st_mtime_ns) != (
            current.st_dev, current.st_ino, current.st_size, current.st_mtime_ns,
        ):
            raise ValueError("Model artifact changed while loading; retry startup")
        dataset = MovieDataset(settings.data_directory)
        if not set(model.movie_ids).issubset(set(dataset.movies["movie_id"])):
            raise ValueError("The trained model contains movies absent from the catalog")
        if settings.licensing_penalty:
            model.configure_licensing_penalty(settings.licensing_fees_path, settings.licensing_penalty)
            LOGGER.info("Loaded licensing ranking policy: %s", model.licensing_policy)
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
            "model": artifact_version,
            "dataset": dataset_version(settings.data_directory),
        })
        service.profile_import_error = profile_import_error
        service.settings = settings
        if settings.provenance_directory is not None:
            try:
                served, training = register_serving(
                    settings.provenance_directory, model_version=service.versions["model"],
                    catalog_version=service.versions["dataset"], ranking=service.ranking_configuration(),
                    source_version=code_version(Path(__file__).resolve().parents[1]),
                    source_revision=os.environ.get("APP_REVISION", "unknown"),
                    model_path=settings.model_path, model=model,
                )
                service.versions.update(serving=served)
                if training:
                    service.versions.update(training_dataset=training)
            except (OSError, ValueError, KeyError, TypeError) as error:
                # Leave serving available; do not advertise a receipt we could not write.
                service.provenance_error = type(error).__name__
                LOGGER.error("Serving provenance unavailable (%s)", type(error).__name__)
        if settings.live_enabled:
            from services.live_worker import LiveProfileWorker
            with LiveStore(settings.storage_path):
                pass
            service.live_worker = LiveProfileWorker(settings, cold_start)
        return service

    def ranking_configuration(self) -> dict:
        """Include any ranking policy parameters used by this service in its identity."""
        return {
            "policy": "ease-with-profile-and-popularity-fallback",
            "top_k": 20,
            **({"licensing": self.model.licensing_policy} if self.model.licensing_policy else {}),
            **self.settings.model_dump(include={
                "live_enabled", "live_source_id", "live_topic", "cold_start_wait_seconds",
                "recommendation_budget_seconds",
            }),
        }

    def start(self):
        if self.live_worker is not None:
            self.live_worker.start()

    def close(self):
        if self.live_worker is not None:
            return self.live_worker.close()
        return True

    def recommend(self, user_id: int, *, deadline=None) -> RecommendationResult:
        started = time.monotonic()
        budget = self.settings.recommendation_budget_seconds if self.settings is not None else 0.5
        deadline = min(deadline if deadline is not None else started + budget, started + budget)
        if self.settings is not None and self.settings.live_enabled:
            try:
                history = read_live_user(self.settings.storage_path, user_id,
                                         self.settings.live_source_id, self.settings.live_topic)
            except (sqlite3.Error, OSError, ValueError) as error:
                LOGGER.error("Live state unavailable (%s)", type(error).__name__)
            else:
                supported, _ = merge_history(self.model, user_id, history)
                prepared = history["prepared"]
                if (not supported and prepared is None and self.live_worker is not None):
                    fallback = self._recommend_live(user_id, history)
                    if fallback.method == "llm_cold_start":
                        return fallback
                    wait = max(0, min(
                        started + self.settings.cold_start_wait_seconds,
                        deadline - 0.05,
                    ) - time.monotonic())
                    waiting = time.monotonic()
                    completed = self.live_worker.prepare(user_id, wait)
                    waited_ms = (time.monotonic() - waiting) * 1000
                    if completed and time.monotonic() < deadline - 0.01:
                        try:
                            history = read_live_user(self.settings.storage_path, user_id,
                                                     self.settings.live_source_id, self.settings.live_topic)
                        except (sqlite3.Error, OSError, ValueError):
                            pass
                    result = self._recommend_live(user_id, history) if completed else fallback
                    return result.model_copy(update={
                        "preparation_attempted": wait > 0,
                        "preparation_wait_ms": waited_ms,
                    })
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
