import json

import pytest

from model_comparison.adapters import d_urbonas
from model_comparison.runner import run_job
from test_adapters_smoke import smoke_job
from test_d_urbonas import FakeModule, make_job
from test_runner import popularity_job


def test_popularity_reports_fit_latency_and_size_costs(tmp_path):
    costs = run_job(popularity_job(tmp_path)).notes["costs"]
    assert costs["fit_seconds"] >= 0
    assert costs["requests"] == 2                      # one timed request per user
    assert costs["latency_p95_ms"] >= 0
    assert costs["model_size_bytes"] > 0
    assert costs["peak_rss_bytes"] > 0


def test_live_latency_is_stored_in_the_cache_and_reported_on_cached_reruns(tmp_path):
    first = d_urbonas.run_with_module(make_job(tmp_path, [1]), FakeModule(), retriable=())
    assert first.notes["costs"]["requests"] == 1
    stored = json.loads((tmp_path / "cache" / "d-urbonas" / "1.json").read_text())["latency_s"]
    assert stored >= 0

    again = d_urbonas.run_with_module(make_job(tmp_path, [1]), FakeModule(), retriable=())
    assert again.notes["costs"]["requests"] == 1       # the cached call still reports its real latency
    assert again.notes["costs"]["latency_max_ms"] == pytest.approx(stored * 1000)


def test_failed_calls_do_not_count_as_latency_samples(tmp_path):
    result = d_urbonas.run_with_module(make_job(tmp_path, [2]), FakeModule(fail_users={"2"}), retriable=())
    assert result.notes["costs"]["requests"] == 0


@pytest.mark.parametrize(
    "name, params",
    [("Helixan", {"regularization": 100, "min_rating": 7}),
     ("MuhammadDF", {"n_neighbors": 20}),
     ("MajorTomLanded", {"neighborhood_size": 10})],
)
def test_real_adapters_report_every_cost(name, params, real_slice, tmp_path):
    users = [int(u) for u in real_slice["interactions"]["user_id"].unique()[:2]]
    costs = run_job(smoke_job(name, real_slice, params, tmp_path, user_ids=users)).notes["costs"]
    assert costs["fit_seconds"] > 0
    assert costs["requests"] == 2
    assert costs["latency_p50_ms"] > 0
    assert costs["model_size_bytes"], costs["model_size_note"]
    assert costs["peak_rss_bytes"] > 0
