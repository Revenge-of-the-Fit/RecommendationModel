"""Training cost, inference cost and model size, recorded inside the adapter subprocess."""
import os
import pickle
import platform
import resource
import statistics
import sys
import time

import numpy as np
import psutil


def current_rss() -> int:
    return psutil.Process().memory_info().rss


def peak_rss() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024  # macOS reports bytes, Linux KiB


class CostRecorder:
    """Times one fit and each single-user recommendation; the model is never modified."""

    def __init__(self, clock=time.perf_counter, rss=current_rss, peak_rss=peak_rss):
        self._clock, self._rss, self._peak_rss = clock, rss, peak_rss
        self._fit_seconds = None
        self._memory_growth = None
        self._latencies: list[float] = []
        self._size = None
        self._size_note = None

    def fit(self, function, *args, **kwargs):
        before, start = self._rss(), self._clock()
        result = function(*args, **kwargs)
        self._fit_seconds = self._clock() - start
        self._memory_growth = self._rss() - before
        return result

    def request(self, function, *args, **kwargs):
        start = self._clock()
        result = function(*args, **kwargs)
        self._latencies.append(self._clock() - start)
        return result

    def add_request_latency(self, seconds: float) -> None:
        self._latencies.append(seconds)

    def measure_size(self, model) -> None:
        try:
            self._size, self._size_note = len(pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)), "pickle of the trained model"
        except Exception as error:  # some objects (locks, open files) cannot be pickled
            self._size, self._size_note = None, f"not measurable: pickle failed ({type(error).__name__})"

    def set_size_bytes(self, size: int, note: str) -> None:
        self._size, self._size_note = size, note

    def summary(self) -> dict:
        latencies = np.array(self._latencies) * 1000 if self._latencies else None
        total = sum(self._latencies)
        return {
            "fit_seconds": self._fit_seconds,
            "fit_memory_growth_bytes": self._memory_growth,
            "peak_rss_bytes": self._peak_rss(),
            "model_size_bytes": self._size,
            "model_size_note": self._size_note,
            "requests": len(self._latencies),
            "latency_p50_ms": None if latencies is None else float(np.percentile(latencies, 50)),
            "latency_p95_ms": None if latencies is None else float(np.percentile(latencies, 95)),
            "latency_max_ms": None if latencies is None else float(latencies.max()),
            "throughput_per_s": len(self._latencies) / total if total > 0 else None,
        }


def median_fit_seconds(runs: list[dict]):
    times = [run["fit_seconds"] for run in runs if run.get("fit_seconds") is not None]
    return statistics.median(times) if times else None


def machine_info() -> dict:
    return {
        "platform": platform.platform(),
        "processor": platform.processor() or platform.machine(),
        "cpu_count": os.cpu_count() or 1,
        "memory_bytes": psutil.virtual_memory().total,
        "python": platform.python_version(),
    }
