import argparse
from pathlib import Path

import pandas as pd
import pytest

from model_comparison import compare
from model_comparison.data import KEYS
from model_comparison.job import JobResult


def synthetic_tables():
    movies = pd.DataFrame({"movie_id": [f"m{i}" for i in range(12)]})
    movies["title"] = movies["movie_id"]
    users = pd.DataFrame(
        {"user_id": range(1, 21),
         "self_description_likes": ["likes things"] * 14 + [""] * 6,
         "self_description_dislikes": [""] * 20}
    )
    rows = []
    for user in range(1, 21):
        for i in range(10):
            rows.append((f"2025-01-01T00:{i:02d}:00", user, "watch", f"m{(user + i) % 12}", None))
            rows.append((f"2025-01-01T00:{i:02d}:30", user, "rating", f"m{(user + i) % 12}", float(5 + (user * i) % 5)))
    events = pd.DataFrame(rows, columns=["timestamp", "user_id", "event_type", "movie_id", "rating"])
    return compare.Tables(events=events, users=users, movies=movies)


class RecordingRunner:
    """Returns the most popular training movie to every user and records each job."""

    def __init__(self):
        self.jobs = []

    def __call__(self, job):
        self.jobs.append(job)
        top = job.interactions["movie_id"].value_counts().index.tolist()
        return JobResult({int(u): top[: job.k + 3] for u in job.user_ids})


def args(**overrides):
    values = dict(seed=42, top_k=3, relevance_rating=7, validation_fraction=0.2, test_fraction=0.2,
                  tuning_users=5, d_urbonas_sample=4, models=["popularity", "Helixan"],
                  data_dir=Path("data"), results_dir=Path("results"))
    values.update(overrides)
    return argparse.Namespace(**values)


@pytest.fixture
def patched(monkeypatch):
    monkeypatch.setattr(compare, "load_tables", lambda _: synthetic_tables())
    monkeypatch.setattr(compare, "file_sha256", lambda _: {})
    monkeypatch.setattr(compare, "write_report", lambda *a, **k: None)  # keep tests out of results/
    monkeypatch.setattr(compare, "adapter_repo_dir", lambda name, external: "")


def test_tuning_jobs_never_contain_test_data_and_final_jobs_exclude_test(patched):
    from model_comparison.data import build_interactions
    from model_comparison.split import InteractionSplitter

    runner = RecordingRunner()
    compare.run_comparison(args(), runner=runner)

    tables = synthetic_tables()
    train, validation, test = InteractionSplitter(0.2, 0.2, 42).split(build_interactions(tables.events))
    pairs = lambda frame: set(map(tuple, frame[KEYS].to_numpy()))
    tuning_jobs = [j for j in runner.jobs if len(j.interactions) == len(train) and j.adapter == "Helixan"]
    assert tuning_jobs, "Helixan has a grid, so there must be tuning jobs"
    for job in runner.jobs:
        assert not (pairs(job.interactions) & pairs(test))      # test never trains or tunes
    for job in tuning_jobs:
        assert set(job.user_ids) <= set(validation["user_id"])  # tuned on validation users only
    final_jobs = [j for j in runner.jobs if len(j.interactions) == len(train) + len(validation)]
    assert {j.adapter for j in final_jobs} == {"popularity", "Helixan"}


def test_users_without_recommendations_are_scored_zero_and_counted(patched):
    class Silent:
        def __call__(self, job):
            return JobResult({}, {})

    payload = compare.run_comparison(args(models=["popularity"]), runner=Silent())
    metrics = payload["models"]["popularity"]["full_population"]
    assert metrics["hit_rate_at_k"] == 0.0
    assert metrics["users_without_recommendations"] == metrics["evaluated_users"]


def test_sample_only_includes_users_with_a_description(patched):
    payload = compare.run_comparison(args(models=["popularity"], d_urbonas_sample=100), runner=RecordingRunner())
    assert payload["split"]["sample_users"] <= 14
    assert payload["split"]["test_users_without_description"] >= 0


def test_sample_users_is_seeded_and_capped():
    users = list(range(100))
    assert compare.sample_users(users, 10, 1) == compare.sample_users(users, 10, 1)
    assert len(compare.sample_users(users, 10, 1)) == 10
    assert compare.sample_users(users, 500, 1) == users


def test_prerequisites_fail_clearly(tmp_path):
    with pytest.raises(SystemExit, match="OPENAI_API_KEY"):
        compare.check_prerequisites(["d-urbonas"], tmp_path, env={})
    with pytest.raises(SystemExit, match="setup_external"):
        compare.check_prerequisites(["MuhammadDF"], tmp_path, env={})
    compare.check_prerequisites(["popularity"], tmp_path, env={})  # needs nothing
