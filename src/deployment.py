"""Stage, verify, and activate one of two independently loaded model versions."""
import argparse
import json
import os
import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

from routing import BACKENDS, MODEL_DIRECTORY, SLOTS, other_slot, read_routing, selected_slot, write_json


def now():
    return datetime.now(timezone.utc).isoformat()


def slot_paths(directory, slot):
    other_slot(slot)
    root = Path(directory) / "slots" / slot
    return root / "recommender.npz", root / "artifact-manifest.json"


def initialize_slots(directory, primary="blue"):
    """Import the M2 model once; provisioning must never undo a later update."""
    directory = Path(directory)
    other_slot(primary)
    for slot in SLOTS:
        model, manifest = slot_paths(directory, slot)
        model.parent.mkdir(parents=True, exist_ok=True)
        if not model.exists() and not manifest.exists():
            shutil.copyfile(directory / "recommender.npz", model)
            shutil.copyfile(directory / "artifact-manifest.json", manifest)
        elif not model.exists() or not manifest.exists():
            raise ValueError(f"Incomplete {slot} slot; restore its artifact and manifest before starting")
    routing = directory / "routing.json"
    if not routing.exists():
        write_json(routing, {"primary": primary, "secondary_percent": 0,
                             "salt": "m3-users-v1", "generation": uuid.uuid4().hex})
    return read_routing(routing)


def get_json(url):
    with urlopen(url, timeout=2) as response:
        return json.load(response)


def expected_version(directory, slot):
    _, manifest = slot_paths(directory, slot)
    return "sha256:" + json.loads(manifest.read_text())["model"]["sha256"]


def wait_ready(directory, slot, *, timeout=600):
    expected = expected_version(directory, slot)
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = get_json(BACKENDS[slot] + "/health/model")
        except (OSError, ValueError):
            result = None
        if result is not None:
            if result.get("versions", {}).get("model") != expected:
                raise ValueError(f"{slot} loaded a different model than its manifest")
            return result
        if time.monotonic() >= deadline:
            raise ValueError(f"{slot} did not become ready; existing traffic configuration was retained")
        time.sleep(min(5, max(0, deadline - time.monotonic())))


def stage_candidate(directory, candidate, data_directory, catalog_directory):
    from model_update import validate_candidate
    from prepare_recent_data import utc

    directory, candidate = Path(directory), Path(candidate)
    config = read_routing(directory / "routing.json")
    if config["secondary_percent"] != 0:
        raise ValueError("End the traffic split before updating either experiment model")
    active, target = config["primary"], other_slot(config["primary"])
    current, current_manifest = slot_paths(directory, active)
    model, manifest = slot_paths(directory, target)
    report = validate_candidate(candidate, data_directory, current, current_manifest, catalog_directory)
    previous = json.loads(current_manifest.read_text())
    prior_end = previous.get("training", {}).get("actual_training_end")
    if prior_end and utc(report["training"]["actual_training_end"]) <= utc(prior_end):
        raise ValueError("Candidate training data must include events newer than the active model's data")
    if report["model"]["sha256"] == report["previous_model_sha256"]:
        raise ValueError("Candidate must be a distinct model artifact")
    report["model"]["path"] = str(candidate)
    report["slot"] = target
    # Preserve this run's manifest alongside its original candidate and exact training export.
    archived_manifest = candidate.parent / "artifact-manifest.json"
    if archived_manifest.exists():
        raise ValueError("This update run has already been staged; use a new run directory")
    if read_routing(directory / "routing.json") != config:
        raise ValueError("Traffic configuration changed during staging")
    write_json(archived_manifest, report)
    temporary = model.with_name(".candidate-" + uuid.uuid4().hex)
    try:
        os.link(candidate, temporary)
        os.replace(temporary, model)
    finally:
        temporary.unlink(missing_ok=True)
    write_json(manifest, report)
    return {"slot": target, "model_version": "sha256:" + report["model"]["sha256"],
            "manifest": str(archived_manifest)}


def switch_traffic(directory, slot, percentage, storage_path, *, gateway_url="http://gateway:8082",
                   record_path=None):
    from model_update import verify_deployment

    directory = Path(directory)
    other_slot(slot)
    if type(percentage) is not int or not 0 <= percentage <= 100:
        raise ValueError("Secondary traffic percentage must be an integer from 0 to 100")
    path = directory / "routing.json"
    previous = read_routing(path)
    if get_json(gateway_url + "/health/routing") != previous:
        raise ValueError("Gateway is not using the current routing file")
    # Keep a proven, ready fallback, even when its configured share is zero.
    for backend in SLOTS:
        wait_ready(directory, backend, timeout=0)
    _, manifest = slot_paths(directory, slot)
    candidate_probe = verify_deployment(manifest, storage_path, base_url=BACKENDS[slot])
    config = {**previous, "primary": slot, "secondary_percent": percentage, "generation": uuid.uuid4().hex}
    record_path = Path(record_path or directory / "switches" / (config["generation"] + ".json"))
    if record_path.exists():
        raise ValueError("Use a new evidence path for each switch")
    report = {"status": "switching", "started_at": now(), "previous_routing": previous,
              "routing": config, "versions": {key: expected_version(directory, key) for key in SLOTS},
              "candidate_probe": candidate_probe, "evidence_path": str(record_path)}
    write_json(record_path, report)
    if read_routing(path) != previous:
        raise ValueError("Traffic configuration changed before activation")
    try:
        report["switched_at"] = now()
        write_json(path, config)
        if get_json(gateway_url + "/health/routing") != config:
            raise ValueError("Gateway did not observe the new routing configuration")
        # Verify a public request for every configured arm, including the persisted model identity.
        needed = {slot} if percentage == 0 else {other_slot(slot)} if percentage == 100 else set(SLOTS)
        probes = {}
        for user_id in range(1, 100001):
            selected = selected_slot(config, user_id)
            if selected in needed and selected not in probes:
                _, served_manifest = slot_paths(directory, selected)
                probes[selected] = verify_deployment(served_manifest, storage_path,
                                                     base_url=gateway_url, user_id=user_id)
            if set(probes) == needed:
                break
        if set(probes) != needed:
            raise ValueError("Could not verify both traffic arms")
        report.update(status="deployed", public_probes=probes, finished_at=now())
    except Exception as error:
        write_json(path, previous)
        report.update(status="rolled_back", finished_at=now(), error_type=type(error).__name__)
        write_json(record_path, report)
        raise
    write_json(record_path, report)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default=MODEL_DIRECTORY)
    commands = parser.add_subparsers(dest="command", required=True)
    initialize = commands.add_parser("initialize")
    initialize.add_argument("--primary", choices=SLOTS, default="blue")
    status = commands.add_parser("status")
    status.add_argument("--shell", action="store_true")
    stage = commands.add_parser("stage")
    stage.add_argument("--candidate", type=Path, required=True)
    stage.add_argument("--data-dir", type=Path, required=True)
    stage.add_argument("--catalog-dir", type=Path, default="/app/data")
    wait = commands.add_parser("wait")
    wait.add_argument("slot", choices=SLOTS)
    wait.add_argument("--timeout", type=float, default=600)
    switch = commands.add_parser("switch")
    switch.add_argument("slot", choices=SLOTS)
    switch.add_argument("--secondary-percent", type=int, default=0)
    switch.add_argument("--record", type=Path)
    switch.add_argument("--gateway-url", default="http://gateway:8082")
    switch.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    args = parser.parse_args()
    try:
        if args.command == "initialize":
            result = initialize_slots(args.directory, args.primary)
        elif args.command == "status":
            result = read_routing(args.directory / "routing.json")
            if args.shell:
                if result["secondary_percent"]:
                    raise ValueError("End the traffic split before updating models")
                wait_ready(args.directory, result["primary"], timeout=0)
                print(result["primary"], other_slot(result["primary"]))
                return
        elif args.command == "stage":
            result = stage_candidate(args.directory, args.candidate, args.data_dir, args.catalog_dir)
        elif args.command == "wait":
            result = wait_ready(args.directory, args.slot, timeout=args.timeout)
        else:
            result = switch_traffic(args.directory, args.slot, args.secondary_percent, args.storage_path,
                                    gateway_url=args.gateway_url, record_path=args.record)
    except (ValueError, OSError, KeyError) as error:
        parser.exit(1, f"Deployment stopped ({type(error).__name__}): {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
