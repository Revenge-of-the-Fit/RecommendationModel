"""Pydantic schemas for data passed between the API and serving service."""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, PositiveInt, field_validator


class Recommendation(BaseModel):
    model_config = ConfigDict(allow_inf_nan=False)

    movie_id: str = Field(min_length=1, pattern=r"^[^,\s]+$")
    title: str
    score: float
    reason: str | None = None


class RecommendationResult(BaseModel):
    user_id: PositiveInt
    method: Literal["ease", "llm_cold_start", "popularity"]
    recommendations: list[Recommendation] = Field(min_length=1, max_length=20)
    fallback_reason: str | None = None
    cached: bool | None = None
    llm_model: str | None = None
    profile_version: str | None = None

    @field_validator("recommendations")
    @classmethod
    def unique_movie_ids(cls, recommendations: list[Recommendation]) -> list[Recommendation]:
        movie_ids = [item.movie_id for item in recommendations]
        if len(movie_ids) != len(set(movie_ids)):
            raise ValueError("Recommendation IDs must be unique")
        return recommendations
