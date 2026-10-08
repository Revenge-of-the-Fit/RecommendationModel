"""Muhammad's item-neighbor model. It trains from CSV files, so the training split is written out."""
import sys
from pathlib import Path

from model_comparison.job import Job, JobResult

NAME = "muhammad"
REPO = "muhammad"
FULL_POPULATION = True
PARAM_GRID = [{"n_neighbors": n} for n in (10, 20, 40)]


def recommend(job: Job) -> JobResult:
    sys.path.insert(0, job.repo_dir)
    import model as muhammad_model  # Muhammad's model.py

    data_dir = Path(job.work_dir) / "muhammad_data"
    data_dir.mkdir(parents=True, exist_ok=True)
    job.movies.to_csv(data_dir / "movies.csv", index=False)
    # Only events for training pairs, so held-out interactions never reach the model
    job.events.to_csv(data_dir / "events.csv", index=False)

    trained = muhammad_model.train_model(data_dir, n_neighbors=job.params["n_neighbors"])
    recommendations = {
        int(user_id): [m["movie_id"] for m in muhammad_model.recommend(trained, int(user_id), top_k=job.k)]
        for user_id in job.user_ids
    }
    return JobResult(recommendations)
