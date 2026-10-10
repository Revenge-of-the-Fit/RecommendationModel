"""d-urbonas's LLM + embeddings recommender. Uses each user's self-description, never their history.

The model's code is not edited: its private OpenAI helpers are wrapped with backoff at runtime,
and its output is cached per user so reruns make no requests for completed users.
"""
import json
import os
import shutil
import sys
import time
from pathlib import Path

import pandas as pd

from model_comparison.backoff import with_backoff
from model_comparison.costs import CostRecorder
from model_comparison.job import Job, JobResult

NAME = "d-urbonas"
REPO = "d-urbonas"
FULL_POPULATION = False
PARAM_GRID = [{}]
PROJECT_ROOT = Path(__file__).resolve().parents[2]
QUOTA_MARKERS = {"insufficient_quota", "credit_balance_exhausted"}


def require_api_key() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("OPENAI_API_KEY is not set; add it to .env before running d-urbonas")


def is_quota_exhausted(error: BaseException) -> bool:
    """An empty OpenAI account is a 429 (type 'insufficient_quota', code 'credit_balance_exhausted'); retrying cannot help."""
    return {getattr(error, "code", None), getattr(error, "type", None)} & QUOTA_MARKERS != set()


def build_title_index(movies: pd.DataFrame) -> tuple[dict[tuple[str, str], str], int]:
    """(title, year) -> movie_id. Keys shared by several movies are left out and counted."""
    keyed = pd.DataFrame(
        {
            "title": movies["title"].to_numpy(),
            "year": movies["release_date"].fillna("").astype(str).str[:4].to_numpy(),
            "movie_id": movies["movie_id"].to_numpy(),
        }
    )
    sizes = keyed.groupby(["title", "year"])["movie_id"].transform("size")
    unique = keyed[sizes == 1]
    index = {
        (title, year): movie_id
        for title, year, movie_id in zip(unique["title"], unique["year"], unique["movie_id"])
    }
    ambiguous = len(keyed[sizes > 1][["title", "year"]].drop_duplicates())
    return index, ambiguous


def map_titles(entries: list[dict], index: dict) -> tuple[list[str], int]:
    ids, unmapped = [], 0
    for entry in entries:
        movie_id = index.get((entry["title"], entry["year"]))
        if movie_id is None:
            unmapped += 1
        else:
            ids.append(movie_id)
    return ids, unmapped


class JsonCache:
    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def get(self, user_id: int):
        path = self.directory / f"{user_id}.json"
        return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None

    def put(self, user_id: int, value: dict) -> None:
        (self.directory / f"{user_id}.json").write_text(json.dumps(value), encoding="utf-8")


def run_with_module(job: Job, module, retriable: tuple, is_fatal=lambda error: False,
                    costs: CostRecorder | None = None) -> JobResult:
    costs = costs or CostRecorder()
    cache = JsonCache(Path(job.cache_dir) / "d-urbonas")
    index, ambiguous = build_title_index(job.movies)
    training_counts = job.interactions.groupby("user_id").size().to_dict()
    recommendations, failed, unmapped_total = {}, {}, 0

    for user_id in job.user_ids:
        user_id = int(user_id)
        # The model ignores history, so ask for enough to survive removing seen movies
        needed = job.k + int(training_counts.get(user_id, 0))
        cached = cache.get(user_id)
        if cached is None or len(cached["movies"]) < needed:
            started = time.perf_counter()
            try:
                profile, movies = module.recommend_movies(str(user_id), top_k=needed)
            except Exception as error:  # recorded per user, never silently dropped
                if is_fatal(error):
                    # An empty account fails every remaining user; stop rather than report zeros
                    raise RuntimeError(
                        f"OpenAI account has no credits remaining ({error}); "
                        "add credits and rerun - completed users are cached"
                    ) from error
                failed[user_id] = f"{type(error).__name__}: {error}"
                recommendations[user_id] = []
                continue
            cached = {
                "profile": profile.model_dump(),
                "movies": [{"title": m["title"], "year": m["year"]} for m in movies],
                # The real request time (LLM + embeddings), kept so cached reruns still report it
                "latency_s": time.perf_counter() - started,
            }
            cache.put(user_id, cached)
        if "latency_s" in cached:
            costs.add_request_latency(cached["latency_s"])
        ids, unmapped = map_titles(cached["movies"], index)
        unmapped_total += unmapped
        recommendations[user_id] = ids

    return JobResult(
        recommendations, failed,
        {"unmapped_titles": unmapped_total, "ambiguous_title_years_in_catalog": ambiguous,
         "costs": costs.summary()},
    )


def _embeddings_match(repo_dir: Path, movies_sorted: pd.DataFrame) -> bool:
    import numpy as np

    repo_csv, repo_npy = repo_dir / "data" / "movies.csv", repo_dir / "movie_embeddings.npy"
    if not (repo_csv.exists() and repo_npy.exists()):
        return False
    repo_ids = pd.read_csv(repo_csv, usecols=["movie_id"])["movie_id"].tolist()
    return repo_ids == movies_sorted["movie_id"].tolist() and np.load(repo_npy).shape[0] == len(repo_ids)


def recommend(job: Job) -> JobResult:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")  # the model's own load_dotenv only looks in its repo
    require_api_key()

    # The model needs the working directory changed, so make every path absolute first
    job.cache_dir = str(Path(job.cache_dir).resolve())
    repo_dir, work = Path(job.repo_dir).resolve(), Path(job.work_dir).resolve() / "d-urbonas"
    (work / "data").mkdir(parents=True)
    movies_sorted = job.movies.sort_values("movie_id").reset_index(drop=True)
    movies_sorted.to_csv(work / "data" / "movies.csv", index=False)
    job.users.to_csv(work / "data" / "users.csv", index=False)

    sys.path.insert(0, str(repo_dir))
    os.chdir(work)  # the model reads data/ and movie_embeddings.npy relative to the cwd
    import model as d_urbonas_model  # d-urbonas's model.py
    import openai

    retriable = (openai.RateLimitError, openai.APIConnectionError, openai.APITimeoutError,
                 openai.InternalServerError)
    d_urbonas_model._embed = with_backoff(d_urbonas_model._embed, retriable, give_up=is_quota_exhausted)
    d_urbonas_model._get_preferences = with_backoff(
        d_urbonas_model._get_preferences, retriable, give_up=is_quota_exhausted
    )

    costs = CostRecorder()
    if _embeddings_match(repo_dir, movies_sorted):
        # Precomputed catalog embeddings are reused, so there is no fit to time
        shutil.copy(repo_dir / "movie_embeddings.npy", work / "movie_embeddings.npy")
    else:
        costs.fit(d_urbonas_model.train_model)  # one-time catalog embedding (~catalog size / 128 requests)
    # Its "model" is the catalog embedding matrix that every request loads
    costs.set_size_bytes((work / "movie_embeddings.npy").stat().st_size, "size of movie_embeddings.npy")

    return run_with_module(job, d_urbonas_model, retriable, is_fatal=is_quota_exhausted, costs=costs)
