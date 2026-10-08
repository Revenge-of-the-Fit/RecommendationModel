"""Subprocess entry point: run one adapter job and write its result as JSON."""
import importlib
import json
import pickle
import sys
from pathlib import Path


def main(argv: list[str]) -> None:
    job_path, out_path = argv
    with open(job_path, "rb") as handle:
        job = pickle.load(handle)  # written by our own runner moments ago
    adapter = importlib.import_module(f"model_comparison.adapters.{job.adapter}")
    result = adapter.recommend(job)
    Path(out_path).write_text(json.dumps(result.to_json()), encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1:])
