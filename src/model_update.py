"""Validate/install a trained candidate and verify its loaded production version."""
import argparse
import json
import os
import sqlite3
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

import numpy as np
import pandas as pd

from dataset import MovieDataset
from prepare_recent_data import sha256
from recommender import EaseRecommender
from services.versions import dataset_version
from storage.payloads import load_json


def install_candidate(candidate, data_directory, model_path, manifest_path, catalog_directory):
    candidate, model_path, manifest_path = map(Path, (candidate, model_path, manifest_path))
    source = json.loads((Path(data_directory) / "dataset-manifest.json").read_text())
    if sha256(model_path) != source["reference_model_sha256"]:
        raise ValueError("The running artifact changed during preparation/training; prepare again")
    dataset, model, previous = MovieDataset(Path(data_directory)), EaseRecommender.load(candidate), EaseRecommender.load(model_path)
    catalog = pd.read_csv(Path(catalog_directory) / "movies.csv.gz", dtype=str, keep_default_na=False)
    if set(model.movie_ids) != set(dataset.movies.movie_id) or not set(model.movie_ids) <= set(catalog.movie_id):
        raise ValueError("Candidate movies do not match the training/serving catalogs")
    if (model.regularization, model.min_rating) != (previous.regularization, previous.min_rating):
        raise ValueError("Candidate must retain the deployed hyperparameters")
    shape = (len(model.user_ids), len(model.movie_ids))
    if (model.weights.shape != (len(model.movie_ids), len(model.movie_ids))
            or model.user_profiles.shape != shape or model.seen_movies.shape != shape
            or model.popularity.shape != (len(model.movie_ids),)
            or not np.isfinite(model.weights).all() or not np.isfinite(model.popularity).all()
            or not (model.popularity > 0).any()):
        raise ValueError("Invalid candidate weights/popularity")
    unknown = next(user for user in range(1, len(model.user_ids) + 2) if user not in model.user_index)
    if model.recommend(unknown, 20).empty:
        raise ValueError("Candidate has no valid fallback recommendations")
    # Check exported file identity before promotion, not just its schema.
    for name, expected in source["files"].items():
        if sha256(Path(data_directory) / name) != expected:
            raise ValueError("Training data changed after preparation")
    old_manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else {}
    report = {
        "code_revision": os.environ.get("APP_REVISION", "unknown"),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model": {"path": str(model_path), "sha256": sha256(candidate),
                  "regularization": model.regularization, "min_rating": model.min_rating},
        "catalog": old_manifest.get("catalog", {}),
        "training": {"data_directory": str(data_directory), "dataset_version": dataset_version(Path(data_directory)), **source},
        "previous_model_sha256": source["reference_model_sha256"],
    }
    # Same-volume links keep the candidate/export and one previous artifact available.
    with tempfile.TemporaryDirectory(prefix=".model-install-", dir=model_path.parent) as temporary:
        stage = Path(temporary)
        prepared_manifest = stage / "manifest.json"
        with prepared_manifest.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps(report, indent=2) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.link(model_path, stage / "previous.npz")
        os.link(candidate, stage / "candidate.npz")
        os.replace(stage / "previous.npz", model_path.with_name("recommender.previous.npz"))
        os.replace(stage / "candidate.npz", model_path)
        os.replace(prepared_manifest, manifest_path)
    return report


def verify_deployment(manifest_path, storage_path, *, base_url="http://127.0.0.1:8082", user_id=1):
    manifest = json.loads(Path(manifest_path).read_text())
    started = time.perf_counter()
    with urlopen(f"{base_url}/recommend/{user_id}", timeout=2) as response:
        body = response.read().decode().strip()
        request_id = response.headers["x-request-id"]
    elapsed = (time.perf_counter() - started) * 1000
    ids = body.split(",")
    if not request_id or not 1 <= len(ids) <= 20 or len(ids) != len(set(ids)) or any(not item or any(c.isspace() for c in item) for item in ids):
        raise ValueError("The updated service returned an invalid recommendation response")
    expected = "sha256:" + manifest["model"]["sha256"]
    record = None
    deadline = time.monotonic() + 30
    while True:
        connection = sqlite3.connect(Path(storage_path).resolve().as_uri() + "?mode=ro", uri=True, timeout=1)
        try:
            row = connection.execute("SELECT record_json FROM recommendation_requests WHERE request_id=?", (request_id,)).fetchone()
            record = load_json(row[0]) if row else None
        finally:
            connection.close()
        if record or time.monotonic() >= deadline:
            break
        time.sleep(0.1)
    if not record:
        raise ValueError(f"Request log was not persisted within 30 seconds (request={request_id}); inspect /health/storage")
    if record.get("versions", {}).get("model") != expected or record.get("status") != 200:
        raise ValueError("No successful request record confirms the candidate was loaded")
    return {"status": "deployed", "request_id": request_id, "model_version": expected,
            "previous_model_sha256": manifest["previous_model_sha256"], "http_ms": round(elapsed, 3),
            "recommendations": len(ids), "data_directory": manifest["training"]["data_directory"],
            "training_start": manifest["training"]["actual_training_start"],
            "training_end": manifest["training"]["actual_training_end"]}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    install = subparsers.add_parser("install")
    install.add_argument("--candidate", type=Path, required=True)
    install.add_argument("--data-dir", type=Path, required=True)
    install.add_argument("--catalog-dir", type=Path, default="/app/data")
    verify = subparsers.add_parser("verify")
    for subparser in (install, verify):
        subparser.add_argument("--manifest", type=Path, default="/app/models/artifact-manifest.json")
    install.add_argument("--model-path", type=Path, default="/app/models/recommender.npz")
    verify.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    args = parser.parse_args()
    try:
        result = (install_candidate(args.candidate, args.data_dir, args.model_path, args.manifest, args.catalog_dir)
                  if args.command == "install" else verify_deployment(args.manifest, args.storage_path))
    except (ValueError, OSError, sqlite3.Error, KeyError) as error:
        parser.exit(1, f"Model update failed ({type(error).__name__}): {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
