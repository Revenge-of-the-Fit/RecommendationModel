"""Attach an immutable serving manifest to request records and trace it later."""
import argparse
import json
import os
import re
import sqlite3
import tempfile
from pathlib import Path

from services.versions import dataset_version, file_version
from storage.payloads import load_json
from storage.profiles import canonical_json, content_version


TRAINING_FILES = ("events.csv.gz", "movies.csv.gz", "users.csv.gz")


def manifest_name(version):
    if not isinstance(version, str) or not re.fullmatch(r"sha256:[0-9a-f]{64}", version):
        raise ValueError("Invalid version fingerprint")
    return version.removeprefix("sha256:") + ".json"


def load_manifest(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def matching_model(manifest, version):
    return "sha256:" + manifest.get("model", {}).get("sha256", "") == version


def training_source(model_manifest):
    training = model_manifest.get("training", {})
    return {
        "dataset_version": training.get("dataset_version"),
        "directory": training.get("data_directory"),
        "files": {name: training.get("files", {}).get(name) for name in TRAINING_FILES},
        "pipeline_revision": model_manifest.get("code_revision"),
    }


def artifact_candidates(manifest):
    training_dir = manifest.get("training", {}).get("data_directory")
    if training_dir:
        yield Path(training_dir).parent / "candidate.npz"
    if manifest.get("model", {}).get("path"):
        yield Path(manifest["model"]["path"])


def publish_manifest(target, record):
    """Write once; a reader never sees a partly written manifest."""
    if target.exists():
        return
    with tempfile.NamedTemporaryFile(mode="w", dir=target.parent, encoding="utf-8", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            handle.write(canonical_json(record) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
            try:
                os.link(temporary, target)
            except FileExistsError:
                pass
        finally:
            temporary.unlink(missing_ok=True)


def register_serving(directory, *, model_version, catalog_version, ranking, source_version,
                     source_revision, model_path, model):
    """Publish once at startup, with no database or file work on the request path."""
    slot_manifest = model_path.with_name("artifact-manifest.json")
    manifest = load_manifest(slot_manifest)
    if not matching_model(manifest, model_version):
        raise ValueError("Loaded weights do not match the slot manifest")
    # The model lineage may differ even if retraining produced identical bytes.
    training = training_source(manifest)
    parameters = {"algorithm": "EASE", "regularization": model.regularization,
                  "min_rating": model.min_rating}
    identity = {"model": model_version, "catalog": catalog_version, "ranking": ranking,
                "serving_code": source_version, "serving_revision": source_revision,
                "parameters": parameters, "training": training}
    version = content_version(identity)
    record = {
        "format_version": 1, "served_version": version, "identity": identity,
        "serving_revision": source_revision,
        "model": {**parameters, "version": model_version,
                  "artifact_candidates": [str(path) for path in artifact_candidates(manifest)]},
        "training": training,
    }
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / manifest_name(version)
    publish_manifest(target, record)
    if load_manifest(target).get("identity") != identity:
        raise ValueError("Serving manifest conflicts with its fingerprint")
    return version, training["dataset_version"]


def stored_request(storage, request_id):
    connection = sqlite3.connect(Path(storage).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)
    try:
        row = connection.execute("SELECT record_json FROM recommendation_requests WHERE request_id=?",
                                 (request_id,)).fetchone()
    finally:
        connection.close()
    if not row:
        raise ValueError("Request not found in the retained API logs")
    record = load_json(row[0])
    if record.get("status") != 200 or not record.get("recommendations"):
        raise ValueError("The request did not produce a successful recommendation")
    return record


def archived_manifest(models, version):
    # Prefer the per-run archive over mutable slot copies; older M2 runs may only
    # have the root/slot manifest. A bare dataset manifest is not serving evidence.
    matches = []
    for path in sorted(models.glob("updates/*/artifact-manifest.json")) + [
        models / "artifacts" / manifest_name(version), models / "artifact-manifest.json",
        *sorted(models.glob("slots/*/artifact-manifest.json")),
    ]:
        if path.is_file():
            manifest = load_manifest(path)
            if matching_model(manifest, version):
                matches.append((path, manifest))
    if not matches:
        raise ValueError("No retained artifact manifest matches this request's model hash")
    if len({canonical_json(training_source(manifest)) for _, manifest in matches}) > 1:
        raise ValueError("This legacy request cannot distinguish multiple training runs with the same artifact hash")
    return matches[0]


def trace_request(storage, registry, models, request_id):
    record = stored_request(storage, request_id)
    versions = record.get("versions") or {}
    model_version = versions.get("model")
    manifest_name(model_version)
    version = versions.get("serving")
    if version:
        path = Path(registry) / manifest_name(version)
        manifest = load_manifest(path)
        if (manifest.get("served_version") != version or content_version(manifest["identity"]) != version
                or manifest["identity"]["model"] != model_version
                or manifest["identity"]["training"] != manifest["training"]
                or manifest["model"]["version"] != model_version):
            raise ValueError("Serving receipt does not match the logged version")
        training = manifest["training"]
        candidates = [Path(item) for item in manifest["model"]["artifact_candidates"]]
        settings = manifest["identity"]["parameters"]
        ranking = manifest["identity"]["ranking"]
    else:
        path, manifest = archived_manifest(Path(models), model_version)
        training = training_source(manifest)
        candidates = list(artifact_candidates(manifest))
        settings = {"algorithm": manifest["model"].get("algorithm", "EASE"), **{
            k: manifest["model"].get(k) for k in ("regularization", "min_rating")}}
        ranking = None  # Legacy logs did not distinguish ranking configurations.
    candidates += [Path(models) / "artifacts" / manifest_name(model_version).replace(".json", ".npz")]
    artifact = next((str(p) for p in candidates if p.is_file() and file_version(p) == model_version), None)
    directory = Path(training["directory"]) if training["directory"] else None
    checks = {name: bool(directory and (directory / name).is_file() and expected
                         and file_version(directory / name) == "sha256:" + expected)
              for name, expected in training["files"].items()}
    dataset_verified = bool(all(checks.values()) and dataset_version(directory) == training["dataset_version"])
    revision = training["pipeline_revision"]
    git_revision_recorded = bool(re.fullmatch(r"[0-9a-f]{40}", revision or ""))
    return {
        "request_id": request_id, "started_at": record.get("started_at"),
        "serving_method": record.get("serving_method"), "fallback_reason": record.get("fallback_reason"),
        "profile_reference": record.get("profile_reference"), "recommendation_count": len(record["recommendations"]),
        "versions": versions, "manifest": str(path), "ranking": ranking,
        "model": {**settings, "version": model_version, "verified_artifact": artifact},
        "training": {**training, "file_checks": checks, "dataset_verified": dataset_verified,
                     "git_revision_recorded": git_revision_recorded},
        "training_lineage_verified": bool(artifact and dataset_verified and git_revision_recorded),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("request_id", help="Successful API x-request-id")
    parser.add_argument("--storage-path", type=Path, default=Path(os.getenv("STORAGE_PATH", "/app/state/events/events.sqlite3")))
    parser.add_argument("--registry", type=Path, default=Path(os.getenv("PROVENANCE_DIRECTORY", "/app/state/provenance")))
    parser.add_argument("--model-dir", type=Path, default=Path("/app/models"))
    args = parser.parse_args()
    try:
        trace = trace_request(args.storage_path, args.registry, args.model_dir, args.request_id)
    except (ValueError, KeyError, OSError, sqlite3.Error) as error:
        parser.exit(1, f"Trace unavailable: {error}\n")
    print(json.dumps(trace, indent=2))
    return 0 if trace["training_lineage_verified"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
