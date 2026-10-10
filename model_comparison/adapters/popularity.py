"""Baseline: most-liked movies the user has not seen. Runs inside the harness."""
from model_comparison.costs import CostRecorder
from model_comparison.data import seen_by_user
from model_comparison.job import Job, JobResult

NAME = "popularity"
REPO = None
PARAM_GRID = [{}]
FULL_POPULATION = True
MIN_RATING = 7


def rank_movies(interactions) -> list[str]:
    # Same notion of "positive" as Helixan: rated >= 7, or watched and never rated
    positive = (interactions["rating"] >= MIN_RATING) | (
        interactions["rating"].isna() & (interactions["watch_count"] > 0)
    )
    counts = interactions[positive].groupby("movie_id").size().rename("n").reset_index()
    return counts.sort_values(["n", "movie_id"], ascending=[False, True])["movie_id"].tolist()


def recommend(job: Job) -> JobResult:
    costs = CostRecorder()
    ranking = costs.fit(rank_movies, job.interactions)
    costs.measure_size(ranking)
    seen = seen_by_user(job.interactions)

    def top_unseen(user_id: int) -> list[str]:
        already = seen.get(user_id, set())
        return [m for m in ranking if m not in already][: job.k]

    recommendations = {int(u): costs.request(top_unseen, int(u)) for u in job.user_ids}
    return JobResult(recommendations, notes={"costs": costs.summary()})
