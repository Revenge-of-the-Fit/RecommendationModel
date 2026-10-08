"""Helixan's EASE recommender, used unmodified from external/helixan/src."""
import sys
from pathlib import Path

from model_comparison.job import Job, JobResult

NAME = "helixan"
REPO = "helixan"
FULL_POPULATION = True
PARAM_GRID = [
    {"regularization": regularization, "min_rating": min_rating}
    for min_rating in (6, 7, 8)
    for regularization in (10, 50, 100, 250, 500)
]


def recommend(job: Job) -> JobResult:
    sys.path.insert(0, str(Path(job.repo_dir) / "src"))
    from recommender import EaseRecommender  # Helixan's module

    model = EaseRecommender(job.params["regularization"], job.params["min_rating"])
    model.fit(job.interactions, job.movies)
    recommendations = {
        int(user_id): model.recommend(int(user_id), job.k)["movie_id"].tolist()
        for user_id in job.user_ids
    }
    return JobResult(recommendations)
