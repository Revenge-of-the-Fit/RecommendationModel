"""Provision the verified catalog and trained model used by the service."""

import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from dataset import MovieDataset
from download_data import DatasetDownloader, FILE_HASHES
from recommender import EaseRecommender


def checksum(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def main() -> None:
    data_directory = Path(os.environ["DATA_DIRECTORY"])
    model_path = Path(os.environ["MODEL_PATH"])
    manifest_path = Path(os.environ["ARTIFACT_MANIFEST_PATH"])
    retrain = os.environ.get("RETRAIN_MODEL", "0") == "1"

    DatasetDownloader(data_directory).download()

    if retrain or not model_path.is_file():
        dataset = MovieDataset(data_directory)
        model = EaseRecommender()
        model.fit(dataset.get_interactions(), dataset.movies)
        model.save(model_path)
        print(f"Trained model at {model_path}")
    else:
        model = EaseRecommender.load(model_path)
        print(f"Using existing model at {model_path}")

    manifest = {
        "code_revision": os.environ.get("APP_REVISION", "unknown"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model": {
            "path": str(model_path),
            "sha256": checksum(model_path),
            "regularization": model.regularization,
            "min_rating": model.min_rating,
        },
        "catalog": {
            name: {
                "sha256": checksum(data_directory / name),
                "expected_sha256": expected,
            }
            for name, expected in FILE_HASHES.items()
        },
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote artifact manifest to {manifest_path}")


if __name__ == "__main__":
    main()
