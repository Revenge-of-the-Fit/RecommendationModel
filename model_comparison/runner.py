"""Launch an adapter in its own interpreter so each repo's module names stay isolated."""
import json
import pickle
import subprocess
import sys
import tempfile
from pathlib import Path

from model_comparison.job import Job, JobResult

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class AdapterError(RuntimeError):
    pass


def run_job(job: Job) -> JobResult:
    with tempfile.TemporaryDirectory() as temporary:
        temporary = Path(temporary)
        job.work_dir = str(temporary / "work")
        Path(job.work_dir).mkdir()
        job_path, out_path = temporary / "job.pkl", temporary / "result.json"
        with job_path.open("wb") as handle:
            pickle.dump(job, handle)
        # stdout/stderr are inherited so long runs show progress
        completed = subprocess.run(
            [sys.executable, "-m", "model_comparison.worker", str(job_path), str(out_path)],
            cwd=PROJECT_ROOT,
        )
        if completed.returncode != 0:
            raise AdapterError(
                f"Adapter '{job.adapter}' failed with exit code {completed.returncode}; "
                "see the output above"
            )
        return JobResult.from_json(json.loads(out_path.read_text(encoding="utf-8")))
