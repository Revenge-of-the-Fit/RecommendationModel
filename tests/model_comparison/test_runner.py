import pandas as pd

from model_comparison.job import Job, JobResult
from model_comparison.metrics import finalize_recommendations
from model_comparison.runner import run_job

from conftest import make_interactions


def popularity_job(tmp_path, **overrides):
    interactions = make_interactions(
        [(1, "a", 9.0, 1), (2, "a", 8.0, 1), (2, "b", 9.0, 1), (3, "a", 7.0, 1),
         (3, "b", 3.0, 1), (3, "c", None, 1), (4, "c", 9.0, 1)]
    )
    values = dict(
        adapter="popularity", params={}, interactions=interactions,
        events=pd.DataFrame(), movies=pd.DataFrame({"movie_id": ["a", "b", "c", "d"]}),
        users=pd.DataFrame(), user_ids=[1, 5], k=3, repo_dir="", data_dir="",
        cache_dir=str(tmp_path / "cache"),
    )
    values.update(overrides)
    return Job(**values)


def test_popularity_runs_in_subprocess_and_excludes_seen(tmp_path):
    result = run_job(popularity_job(tmp_path))
    # Positive counts: a=3, b=1, c=2 (unrated watch counts as positive). d has none.
    assert result.recommendations[1] == ["c", "b"]            # user 1 already saw a
    assert result.recommendations[5] == ["a", "c", "b"]       # unknown user: popularity order


def test_job_result_json_round_trip():
    original = JobResult({1: ["a"]}, {2: "boom"}, {"x": 1})
    assert JobResult.from_json(original.to_json()) == original


def test_finalize_drops_unknown_and_seen_and_truncates():
    cleaned, stats = finalize_recommendations(
        {1: ["a", "zzz", "b", "c", "d"], 2: ["a"]},
        seen={1: {"b"}}, catalog_ids={"a", "b", "c", "d"}, k=2,
    )
    assert cleaned == {1: ["a", "c"], 2: ["a"]}
    assert stats == {"unknown_ids_dropped": 1, "seen_dropped": 1}
