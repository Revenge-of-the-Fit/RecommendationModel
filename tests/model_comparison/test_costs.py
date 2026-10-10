import threading

import pytest

from model_comparison.costs import CostRecorder, machine_info, median_fit_seconds


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def recorder(clock, rss_values):
    values = iter(rss_values)
    return CostRecorder(clock=clock, rss=lambda: next(values), peak_rss=lambda: 999)


def test_fit_records_seconds_memory_growth_and_returns_result():
    clock = FakeClock()
    costs = recorder(clock, [100, 160])

    def fit():
        clock.advance(2.5)
        return "model"

    assert costs.fit(fit) == "model"
    summary = costs.summary()
    assert summary["fit_seconds"] == 2.5
    assert summary["fit_memory_growth_bytes"] == 60
    assert summary["peak_rss_bytes"] == 999


def test_requests_report_percentiles_in_milliseconds_and_throughput():
    clock = FakeClock()
    costs = recorder(clock, [])
    for seconds in [0.1] * 9 + [1.0]:
        costs.request(lambda s=seconds: clock.advance(s))
    summary = costs.summary()
    assert summary["requests"] == 10
    assert summary["latency_p50_ms"] == pytest.approx(100)
    assert summary["latency_max_ms"] == pytest.approx(1000)
    assert 100 < summary["latency_p95_ms"] <= 1000
    assert summary["throughput_per_s"] == pytest.approx(10 / 1.9)


def test_added_latencies_count_like_timed_requests():
    costs = recorder(FakeClock(), [])
    costs.add_request_latency(0.25)
    costs.add_request_latency(0.75)
    assert costs.summary()["latency_max_ms"] == pytest.approx(750)


def test_without_fit_or_requests_the_fields_are_none():
    summary = recorder(FakeClock(), []).summary()
    assert summary["fit_seconds"] is None
    assert summary["fit_memory_growth_bytes"] is None
    assert summary["requests"] == 0
    assert summary["latency_p95_ms"] is None
    assert summary["throughput_per_s"] is None


def test_measure_size_is_the_pickle_length():
    costs = recorder(FakeClock(), [])
    costs.measure_size({"a": list(range(1000))})
    assert costs.summary()["model_size_bytes"] > 1000


def test_unpicklable_model_is_reported_not_guessed():
    costs = recorder(FakeClock(), [])
    costs.measure_size(threading.Lock())
    summary = costs.summary()
    assert summary["model_size_bytes"] is None
    assert "pickle" in summary["model_size_note"].lower()


def test_set_size_bytes_overrides_with_an_explicit_value():
    costs = recorder(FakeClock(), [])
    costs.set_size_bytes(1234, "movie_embeddings.npy")
    summary = costs.summary()
    assert summary["model_size_bytes"] == 1234
    assert summary["model_size_note"] == "movie_embeddings.npy"


def test_median_fit_seconds_ignores_runs_without_a_fit():
    runs = [{"fit_seconds": 3.0}, {"fit_seconds": 1.0}, {"fit_seconds": 2.0}]
    assert median_fit_seconds(runs) == 2.0
    assert median_fit_seconds([{"fit_seconds": None}]) is None


def test_machine_info_describes_the_hardware():
    info = machine_info()
    assert info["cpu_count"] >= 1
    assert info["memory_bytes"] > 0
    assert info["platform"] and info["python"]
