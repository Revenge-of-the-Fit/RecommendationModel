"""Provision the verified catalog and trained model used by the service."""

import hashlib
import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from dataset import MovieDataset
from download_data import DatasetDownloader, FILE_HASHES
from recommender import EaseRecommender
from deployment import initialize_slots
from provenance import TRAINING_FILES, publish_manifest
from routing import write_json
from services.versions import dataset_version


def checksum(path: Path) -> str:
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def main() -> None:
    data_directory = Path(os.environ["DATA_DIRECTORY"])
    model_path = Path(os.environ["MODEL_PATH"])
    manifest_path = Path(os.environ["ARTIFACT_MANIFEST_PATH"])
    retrain = os.environ.get("RETRAIN_MODEL", "0") == "1"

    DatasetDownloader(data_directory).download()

    trained = retrain or not model_path.is_file()
    if trained:
        dataset = MovieDataset(data_directory)
        model = EaseRecommender()
        model.fit(dataset.get_interactions(), dataset.movies)
        with tempfile.TemporaryDirectory(dir=model_path.parent) as staging:
            candidate = Path(staging) / "recommender.npz"
            model.save(candidate)
            os.replace(candidate, model_path)
        print(f"Trained model at {model_path}")
    else:
        model = EaseRecommender.load(model_path)
        print(f"Using existing model at {model_path}")

    manifest = {
        "code_revision": os.environ.get("APP_REVISION", "unknown") if trained else "unknown",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model": {
            "path": str(model_path),
            "sha256": checksum(model_path),
            "algorithm": "EASE",
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
    # A normal restart must not replace a trained model's provenance with provisioning metadata.
    existing = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    if trained or existing is None or existing.get("model", {}).get("sha256") != manifest["model"]["sha256"]:
        if trained:
            version = dataset_version(data_directory)
            snapshot = model_path.parent / "snapshots" / version.removeprefix("sha256:")
            snapshot.mkdir(parents=True, exist_ok=True)
            for name in TRAINING_FILES:
                target = snapshot / name
                if not target.exists():
                    temporary = target.with_suffix(".tmp")
                    shutil.copyfile(data_directory / name, temporary)
                    os.replace(temporary, target)
            if dataset_version(snapshot) != version:
                raise ValueError("Saved bootstrap snapshot differs from training data")
            manifest["training"] = {"data_directory": str(snapshot), "dataset_version": version,
                                    "files": {name: checksum(snapshot / name) for name in TRAINING_FILES}}
        write_json(manifest_path, manifest)
        print(f"Wrote artifact manifest to {manifest_path}")
    else:
        manifest = existing
    archives = model_path.parent / "artifacts"
    archives.mkdir(parents=True, exist_ok=True)
    digest = manifest["model"]["sha256"]
    archived_model = archives / (digest + ".npz")
    if not archived_model.exists():
        os.link(model_path, archived_model)
    publish_manifest(archives / (digest + ".json"), manifest)
    initialize_slots(model_path.parent)


if __name__ == "__main__":
    main()
