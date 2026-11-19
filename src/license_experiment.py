"""Prepare frozen EASE arms and manually register the eight-hour license experiment."""
import argparse
import csv
import json
import logging
import os
import re
import shutil
import sqlite3
import tempfile
from collections import Counter, deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np

from deployment import get_json, slot_paths
from evaluate_online import load_licensing_fees, sha256
from experiment import start_experiment
from monitoring import readonly
from routing import BACKENDS, read_routing, write_json
from recommender import EaseRecommender
from services.metadata import MetadataRateLimiter, split_rejected_batch
from services.metadata_http import MetadataHttpFetcher


PENALTY = 0.01
MAX_FEE_REQUESTS = 256
LOGGER = logging.getLogger(__name__)


def assets_path(directory, experiment_id):
    if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", experiment_id):
        raise ValueError("Use a new experiment ID of 1-64 letters, digits, hyphens or underscores")
    return Path(directory) / "license-policies" / experiment_id


def freeze_fees(catalog, storage, output, metadata_url):
    """Broaden catalog coverage from a bounded indexed tail before fixing prices."""
    fees, invalid = load_licensing_fees(catalog)
    connection, _ = readonly(storage)
    try:
        connection.execute("BEGIN")
        partitions = [row[0] for row in connection.execute(
            "SELECT partition FROM live_event_cursors WHERE source_id='cmu-movielog' AND topic='movielog2'"
        )]
        if not partitions:
            raise ValueError("No Kafka partitions available for fee preparation")
        movies = set(fees)
        counts = Counter()
        for partition in partitions:
            counts.update(row[0] for row in connection.execute("""
                SELECT movie_id FROM (
                    SELECT movie_id,event_type,parse_status FROM kafka_events
                    WHERE source_id='cmu-movielog' AND topic='movielog2' AND partition=?
                    ORDER BY offset DESC LIMIT 100000
                ) WHERE parse_status='parsed' AND event_type IN ('watch','rating')
            """, (partition,)) if row[0])
        movies.update(counts)
    finally:
        connection.close()
    missing = sorted((movie for movie in movies if fees.get(movie) is None),
                     key=lambda movie: (-counts[movie], movie))
    limiter = MetadataRateLimiter(1)
    fetched = 0
    requests, rejected = 0, Counter()
    batches = deque(missing[index:index + 25] for index in range(0, len(missing), 25))
    with MetadataHttpFetcher(metadata_url, timeout=10) as client:
        while batches and requests < MAX_FEE_REQUESTS:
            batch_ids = batches.popleft()
            limiter.wait()
            batch = client("movie", batch_ids)
            requests += 1
            if split := split_rejected_batch(batch, batch_ids):
                batches.extendleft(reversed(split))
                continue
            if len(batch_ids) == 1 and batch.http_status in (400, 404):
                rejected[str(batch.http_status)] += 1
                fees[batch_ids[0]] = None
                continue
            if batch.error_type or batch.http_status != 200:
                raise ValueError(f"Fee preparation failed: metadata HTTP {batch.http_status}, {batch.error_type}")
            # Reuse the canonical CSV fee validator for numeric and missing values.
            with tempfile.TemporaryDirectory() as temporary:
                batch_file = Path(temporary) / "fees.csv"
                with batch_file.open("w", newline="") as handle:
                    writer = csv.writer(handle)
                    writer.writerow(["movie_id", "license_cost"])
                    for movie in batch_ids:
                        record = batch.records.get(movie, {})
                        raw = record.get("license_cost")
                        writer.writerow([movie, record.get("cost") if raw is None else raw])
                resolved, _ = load_licensing_fees(batch_file)
            fees.update(resolved)
            fetched += sum(value is not None for value in resolved.values())
    unresolved = sum(fees.get(movie) is None for movie in movies)
    if rejected or batches or unresolved:
        LOGGER.warning(
            "Fee preparation: unknown=%s, rejected_movie_requests=%s, deferred_movies=%s, requests=%s/%s",
            unresolved, dict(rejected), sum(map(len, batches)), requests, MAX_FEE_REQUESTS,
        )
    with Path(output).open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["movie_id", "license_cost"])
        writer.writerows((movie, fees.get(movie)) for movie in sorted(movies))
    return {
        "catalog_source": str(catalog), "catalog_sha256": sha256(catalog),
        "metadata_url": metadata_url, "tail_rows_per_partition": 100000,
        "partitions": partitions, "resolved_additional_fees": fetched,
        "movies": len(movies), "unknown_fees": sum(fees.get(movie) is None for movie in movies),
        "invalid_original_fees": len(invalid),
        "metadata_requests": requests, "max_metadata_requests": MAX_FEE_REQUESTS,
        "rejected_movie_requests": dict(rejected),
        "deferred_movies": sum(map(len, batches)),
        "limitation": "Recent movie coverage does not guarantee coverage of future watches; unknowns remain unknown.",
    }


def prepare(directory, experiment_id, fees_path, storage_path, metadata_url, *, refresh=True):
    directory = Path(directory)
    archive = assets_path(directory, experiment_id)
    routing = read_routing(directory / "routing.json")
    if routing.get("experiment") or routing["secondary_percent"]:
        raise ValueError("Finish the current experiment or split before preparation")
    source, manifest = slot_paths(directory, routing["primary"])
    health = get_json(BACKENDS[routing["primary"]] + "/health/model")
    digest = sha256(source)
    if (health["versions"]["model"] != "sha256:" + digest
            or json.loads(manifest.read_text())["model"]["sha256"] != digest
            or health.get("ranking", {}).get("licensing")):
        raise ValueError("Active control must be unpenalized EASE matching its saved artifact and manifest")
    archive.parent.mkdir(parents=True, exist_ok=True)
    if archive.exists():
        raise ValueError("Keep previous assets immutable; choose a new experiment ID")
    with tempfile.TemporaryDirectory(prefix=".license-", dir=archive.parent) as temporary:
        stage = Path(temporary)
        shutil.copyfile(source, stage / "recommender.npz")
        shutil.copyfile(manifest, stage / "artifact-manifest.json")
        if refresh:
            reference = freeze_fees(fees_path, storage_path, stage / "fees.csv", metadata_url)
        else:
            fees, invalid = load_licensing_fees(fees_path)
            with (stage / "fees.csv").open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["movie_id", "license_cost"])
                writer.writerows(fees.items())
            reference = {"source": str(fees_path), "sha256": sha256(fees_path),
                         "unknown_fees": sum(value is None for value in fees.values()),
                         "invalid_original_fees": len(invalid)}
        fees, _ = load_licensing_fees(stage / "fees.csv")
        if not any(value is not None for value in fees.values()):
            raise ValueError("At least one validated fee is required")
        if sha256(stage / "recommender.npz") != digest or sha256(source) != digest:
            raise ValueError("Active model changed during preparation")
        specification = {
            "id": experiment_id, "prepared_at": datetime.now(timezone.utc).isoformat(),
            "model_sha256": digest, "fees_sha256": sha256(stage / "fees.csv"),
            "penalty_per_usd": PENALTY, "fee_reference": reference,
            "enrollment_hours": 7, "follow_up_hours": 1, "control_slot": "blue",
            "control_description": "Current EASE, no licensing penalty",
            "treatment_description": (
                "Same EASE weights; eligible personalized score minus 0.01 * fee_USD before top 20; "
                "unknown fee uses maximum validated snapshot fee for ranking only; "
                "unchanged cold-start/popularity policies"
            ),
        }
        write_json(stage / "specification.json", specification)
        if read_routing(directory / "routing.json") != routing:
            raise ValueError("Routing changed during preparation")
        os.rename(stage, archive)
    return specification


def specification(directory, experiment_id):
    archive = assets_path(directory, experiment_id)
    spec = json.loads((archive / "specification.json").read_text())
    if (spec["id"] != experiment_id or spec["penalty_per_usd"] != PENALTY
            or sha256(archive / "fees.csv") != spec["fees_sha256"]
            or sha256(archive / "recommender.npz") != spec["model_sha256"]):
        raise ValueError("Frozen experiment assets changed")
    return archive, spec


def stage_slot(directory, experiment_id, slot):
    archive, spec = specification(directory, experiment_id)
    routing = read_routing(Path(directory) / "routing.json")
    if routing.get("experiment") or routing["secondary_percent"] or routing["primary"] == slot:
        raise ValueError("Only stage an inactive slot outside an experiment or split")
    model, manifest = slot_paths(directory, slot)
    backup = archive / ("previous-" + slot)
    backup.mkdir(exist_ok=False)
    shutil.copyfile(model, backup / model.name)
    shutil.copyfile(manifest, backup / manifest.name)
    temporary = model.with_name(".license-candidate.npz")
    try:
        shutil.copyfile(archive / "recommender.npz", temporary)
        os.replace(temporary, model)
    finally:
        temporary.unlink(missing_ok=True)
    write_json(manifest, json.loads((archive / "artifact-manifest.json").read_text()))
    return {"slot": slot, "model_sha256": spec["model_sha256"], "backup": str(backup)}


def start(directory, capture_directory, storage_path, experiment_id, *, clock=None):
    archive, spec = specification(directory, experiment_id)
    routing = read_routing(Path(directory) / "routing.json")
    if routing["primary"] != "blue" or routing["secondary_percent"] or routing.get("experiment"):
        raise ValueError("Keep all traffic on blue/control with no registered experiment before launch")
    health = {slot: get_json(url + "/health/model") for slot, url in BACKENDS.items()}
    if any(row["versions"]["model"] != "sha256:" + spec["model_sha256"] for row in health.values()):
        raise ValueError("Both arms must load the same frozen EASE artifact")
    if (not health["blue"]["versions"].get("dataset")
            or health["blue"]["versions"]["dataset"] != health["green"]["versions"].get("dataset")):
        raise ValueError("Both arms must use the same serving catalog")
    control, treatment = (health[slot]["ranking"] for slot in ("blue", "green"))
    licensing = treatment.get("licensing", {})
    if (control.get("licensing") or licensing.get("penalty_per_usd") != PENALTY
            or licensing.get("fees_sha256") != spec["fees_sha256"]
            or {key: value for key, value in treatment.items() if key != "licensing"} != control):
        raise ValueError("Only the pre-specified licensing policy may differ between arms")
    if any(not row["versions"].get("serving") or row.get("provenance_error") for row in health.values()):
        raise ValueError("Both arms require healthy serving provenance")
    clock = clock or datetime.now(timezone.utc)
    plan = start_experiment(
        directory, capture_directory, storage_path, experiment_id=experiment_id,
        start=clock + timedelta(minutes=3), enrollment_hours=7, follow_up_hours=1,
        control_slot="blue", control_description=spec["control_description"],
        treatment_description=spec["treatment_description"], fees_path=archive / "fees.csv",
        fee_column="license_cost", clock=clock,
        expected_versions={slot: row["versions"]["serving"] for slot, row in health.items()},
        policy_reference=spec,
    )
    return plan


def replay(model_path, fees_path):
    """A relevance/cost diagnostic, not an estimate of held-out ratings or watches."""
    model = EaseRecommender.load(model_path)
    fees, _ = load_licensing_fees(fees_path)
    eligible = [int(user) for user in model.user_ids if model.has_positive_history(int(user))]
    if not eligible:
        raise ValueError("Replay requires users with positive training history")
    users = np.random.default_rng(42).choice(eligible, min(500, len(eligible)), replace=False)
    control = {int(user): model.recommend(int(user), 20)["movie_id"].tolist() for user in users}
    model.configure_licensing_penalty(fees_path, PENALTY)
    costs, relevance, overlaps, unknown = [[], []], [[], []], [], [0, 0]
    for user in users:
        slates = (control[int(user)], model.recommend(int(user), 20)["movie_id"].tolist())
        scores = model.get_scores(int(user))
        overlaps.append(len(set(slates[0]) & set(slates[1])) / len(slates[0]) if slates[0] else 0)
        for arm, movies in enumerate(slates):
            unknown[arm] += sum(fees.get(movie) is None for movie in movies)
            if movies:
                costs[arm].append(float(np.mean([float(fees[movie]) for movie in movies])) if all(
                    fees.get(movie) is not None for movie in movies) else None)
                relevance[arm].append(float(np.mean([scores[model.movie_index[movie]] for movie in movies])))
    return {
        "kind": "Read-only training-profile replay; NOT held-out quality, watched costs, or live ratings",
        "model_sha256": sha256(model_path), "fees_sha256": sha256(fees_path),
        "seed": 42, "users": len(users), "penalty_per_usd": PENALTY,
        "mean_top20_overlap": float(np.mean(overlaps)), "unknown_recommended_fees": unknown,
        "mean_recommended_fee_usd": [
            float(np.mean(values)) if values and all(value is not None for value in values) else None
            for values in costs
        ],
        "mean_original_ease_score": [float(np.mean(values)) if values else None for values in relevance],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default="/app/models")
    parser.add_argument("--capture-directory", type=Path, default="/app/state/experiments")
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare")
    preparation.add_argument("--id", required=True)
    preparation.add_argument("--fees", type=Path, default="/app/data/movies.csv.gz")
    preparation.add_argument("--metadata-url", default=os.environ.get("METADATA_BASE_URL", "http://128.2.24.239:8080"))
    preparation.add_argument("--no-refresh", action="store_true", help="Use an already broadened validated fee snapshot")
    staging = commands.add_parser("stage")
    staging.add_argument("--id", required=True)
    staging.add_argument("--slot", choices=("blue", "green"), required=True)
    launch = commands.add_parser("start")
    launch.add_argument("--id", required=True)
    diagnostic = commands.add_parser("replay")
    diagnostic.add_argument("--model", type=Path, required=True)
    diagnostic.add_argument("--fees", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "prepare":
            result = prepare(args.directory, args.id, args.fees, args.storage_path, args.metadata_url, refresh=not args.no_refresh)
        elif args.command == "stage":
            result = stage_slot(args.directory, args.id, args.slot)
        elif args.command == "start":
            result = start(args.directory, args.capture_directory, args.storage_path, args.id)
        else:
            result = replay(args.model, args.fees)
        print(json.dumps(result, indent=2, allow_nan=False))
    except (ValueError, OSError, KeyError, sqlite3.Error) as error:
        parser.exit(1, f"License experiment stopped ({type(error).__name__}): {error}\n")


if __name__ == "__main__":
    main()
