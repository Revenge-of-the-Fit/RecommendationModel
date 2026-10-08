"""Reuse the existing recommendation entry point with persistent resources."""

from cold_start import ColdStartRecommender
from dataset import MovieDataset
from models.serving import RecommendationResult
from models.settings import ServingSettings
from preferences import ColdStartError
from recommend import recommend_for_user
from recommender import EaseRecommender


class RecommendationService:
    def __init__(
        self,
        model: EaseRecommender,
        dataset: MovieDataset,
        cold_start: ColdStartRecommender,
    ):
        self.model = model
        self.dataset = dataset
        self.cold_start = cold_start

    @classmethod
    def load(cls, settings: ServingSettings) -> "RecommendationService":
        model = EaseRecommender.load(settings.model_path)
        dataset = MovieDataset(settings.data_directory)
        if not set(model.movie_ids).issubset(set(dataset.movies["movie_id"])):
            raise ValueError("The trained model contains movies absent from the catalog")
        cold_start = ColdStartRecommender(
            dataset.movies, settings.cache_directory, model=model
        )
        return cls(model, dataset, cold_start)

    def recommend(self, user_id: int) -> RecommendationResult:
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
                "recommendations": self.model.recommend(user_id, 20).to_dict(orient="records"),
            }
        return RecommendationResult.model_validate(result)
