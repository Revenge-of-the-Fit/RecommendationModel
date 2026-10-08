import pytest

from model_comparison.adapters import get_adapter
from model_comparison.job import Job
from model_comparison.repos import resolve_repo
from model_comparison.runner import run_job
from conftest import DATA_DIR, EXTERNAL_DIR


def smoke_job(name, real_slice, params, tmp_path, k=5, user_ids=None):
    adapter = get_adapter(name)
    repo = resolve_repo(adapter.REPO, EXTERNAL_DIR)
    if repo is not None and not repo.is_dir():
        pytest.skip(f"external/{adapter.REPO} not cloned; run scripts/setup_external.sh")
    with_history = [int(u) for u in real_slice["interactions"]["user_id"].unique()]
    return Job(
        adapter=name, params=params,
        interactions=real_slice["interactions"], events=real_slice["events"],
        movies=real_slice["tables"].movies, users=real_slice["tables"].users,
        user_ids=user_ids if user_ids is not None else with_history[:3], k=k,
        repo_dir=str(repo) if repo else "", data_dir=str(DATA_DIR),
        cache_dir=str(tmp_path / "cache"),
    )


def assert_valid(result, job):
    catalog = set(job.movies["movie_id"])
    for user_id in job.user_ids:
        movies = result.recommendations[user_id]
        assert 0 < len(movies) <= job.k
        assert set(movies) <= catalog
        assert len(set(movies)) == len(movies)


def test_helixan_smoke(real_slice, tmp_path):
    job = smoke_job("helixan", real_slice, {"regularization": 100, "min_rating": 7}, tmp_path)
    assert_valid(run_job(job), job)


def test_muhammad_smoke(real_slice, tmp_path):
    job = smoke_job("muhammad", real_slice, {"n_neighbors": 20}, tmp_path)
    assert_valid(run_job(job), job)


def test_rec_zilla_smoke(real_slice, tmp_path):
    users = [int(u) for u in real_slice["interactions"]["user_id"].unique()[:2]]
    job = smoke_job("rec_zilla", real_slice, {"neighborhood_size": 10}, tmp_path, user_ids=users)
    result = run_job(job)
    assert_valid(result, job)
    assert not result.failed_users
