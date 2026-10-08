"""Baseline: most-liked movies the user has not seen. Runs inside the harness."""
from model_comparison.data import seen_by_user
from model_comparison.job import Job, JobResult

NAME = "popularity"
REPO = None
PARAM_GRID = [{}]
FULL_POPULATION = True
MIN_RATING = 7


def recommend(job: Job) -> JobResult:
    interactions = job.interactions
    # Same notion of "positive" as Helixan: rated >= 7, or watched and never rated
    positive = (interactions["rating"] >= MIN_RATING) | (
        interactions["rating"].isna() & (interactions["watch_count"] > 0)
    )
    counts = interactions[positive].groupby("movie_id").size().rename("n").reset_index()
    ranking = counts.sort_values(["n", "movie_id"], ascending=[False, True])["movie_id"].tolist()
    seen = seen_by_user(interactions)

    recommendations = {}
    for user_id in job.user_ids:
        already = seen.get(int(user_id), set())
        recommendations[int(user_id)] = [m for m in ranking if m not in already][: job.k]
    return JobResult(recommendations)
