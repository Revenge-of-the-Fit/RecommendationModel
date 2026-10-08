import pandas as pd
import pytest

from model_comparison.adapters import d_urbonas
from model_comparison.job import Job
from conftest import make_interactions


class FakeProfile:
    def model_dump(self):
        return {"liked_genres": ["Drama"]}


class FakeModule:
    """Stands in for d-urbonas's model.py; no network."""

    def __init__(self, fail_users=()):
        self.calls = []
        self.fail_users = set(fail_users)

    def recommend_movies(self, user_id, top_k=10):
        self.calls.append((user_id, top_k))
        if user_id in self.fail_users:
            raise RuntimeError("The LLM found no positive preferences for this user.")
        movies = [
            {"title": "Alpha", "year": "1999", "genres": "Drama", "score": 0.9},
            {"title": "Ghost", "year": "2001", "genres": "Drama", "score": 0.8},  # not in catalog
            {"title": "Beta", "year": "2000", "genres": "Drama", "score": 0.7},
            {"title": "Beta", "year": "2000", "genres": "Drama", "score": 0.6},  # duplicate key below
        ][:top_k]
        return FakeProfile(), movies


def catalog():
    return pd.DataFrame(
        {
            "movie_id": ["alpha+1999", "beta+2000", "beta+2000b", "gamma+2002"],
            "title": ["Alpha", "Beta", "Beta", "Gamma"],
            "release_date": ["1999-01-01", "2000-05-05", "2000-06-06", "2002-02-02"],
        }
    )


def make_job(tmp_path, user_ids, interactions=None):
    return Job(
        adapter="d-urbonas", params={}, interactions=interactions if interactions is not None else make_interactions([]),
        events=pd.DataFrame(), movies=catalog(), users=pd.DataFrame(), user_ids=user_ids,
        k=2, repo_dir="", data_dir="", cache_dir=str(tmp_path / "cache"),
    )


def test_title_index_flags_ambiguous_title_year():
    index, ambiguous = d_urbonas.build_title_index(catalog())
    assert index[("Alpha", "1999")] == "alpha+1999"
    assert ("Beta", "2000") not in index  # two movies share title and year
    assert ambiguous == 1


def test_map_titles_counts_unmapped():
    index, _ = d_urbonas.build_title_index(catalog())
    entries = [{"title": "Alpha", "year": "1999"}, {"title": "Ghost", "year": "2001"},
               {"title": "Beta", "year": "2000"}]
    ids, unmapped = d_urbonas.map_titles(entries, index)
    assert ids == ["alpha+1999"] and unmapped == 2


def test_failed_user_is_recorded_not_dropped(tmp_path):
    module = FakeModule(fail_users={"2"})
    result = d_urbonas.run_with_module(make_job(tmp_path, [1, 2]), module, retriable=())
    assert result.recommendations[2] == []
    assert "no positive preferences" in result.failed_users[2]
    assert result.recommendations[1] == ["alpha+1999"]


def test_requests_enough_results_to_survive_seen_filtering(tmp_path):
    seen = make_interactions([(1, "alpha+1999", 8.0, 1), (1, "gamma+2002", 8.0, 1)])
    module = FakeModule()
    d_urbonas.run_with_module(make_job(tmp_path, [1], interactions=seen), module, retriable=())
    assert module.calls == [("1", 2 + 2)]  # k + number of the user's training items


def test_second_run_uses_cache_and_makes_no_calls(tmp_path):
    first = FakeModule()
    d_urbonas.run_with_module(make_job(tmp_path, [1]), first, retriable=())
    second = FakeModule()
    result = d_urbonas.run_with_module(make_job(tmp_path, [1]), second, retriable=())
    assert second.calls == []
    assert result.recommendations[1] == ["alpha+1999"]


def test_failed_users_are_not_cached(tmp_path):
    d_urbonas.run_with_module(make_job(tmp_path, [2]), FakeModule(fail_users={"2"}), retriable=())
    retry = FakeModule()
    d_urbonas.run_with_module(make_job(tmp_path, [2]), retry, retriable=())
    assert retry.calls  # tried again


class QuotaError(Exception):
    code = "insufficient_quota"


def test_quota_exhaustion_stops_the_run_instead_of_failing_every_user(tmp_path):
    class OutOfCredit(FakeModule):
        def recommend_movies(self, user_id, top_k=10):
            self.calls.append((user_id, top_k))
            raise QuotaError("You have no credits remaining")

    module = OutOfCredit()
    with pytest.raises(RuntimeError, match="credits"):
        d_urbonas.run_with_module(make_job(tmp_path, [1, 2, 3]), module, retriable=(),
                                is_fatal=d_urbonas.is_quota_exhausted)
    assert len(module.calls) == 1  # stopped at the first user


def test_is_quota_exhausted_matches_the_real_openai_error_shape():
    # Observed from the live API: type 'insufficient_quota', code 'credit_balance_exhausted'
    class LiveShape(Exception):
        type = "insufficient_quota"
        code = "credit_balance_exhausted"

    assert d_urbonas.is_quota_exhausted(LiveShape())


def test_is_quota_exhausted_only_matches_insufficient_quota():
    assert d_urbonas.is_quota_exhausted(QuotaError())
    assert not d_urbonas.is_quota_exhausted(RuntimeError("rate limited, try again"))


def test_missing_api_key_fails_before_any_work(tmp_path, monkeypatch):
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        d_urbonas.require_api_key()
