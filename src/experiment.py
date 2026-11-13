"""Register a fixed production experiment, export its observations, and compare arms."""
import argparse
import gzip
import json
import os
import shutil
import sqlite3
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from deployment import get_json
from evaluate_online import load_licensing_fees, sha256, utc
from experiments.analysis import analyze_snapshot, markdown_report
from experiments.config import experiment_routing, validate_plan
from routing import BACKENDS, read_routing, write_json
from storage.payloads import load_json


def readonly(path):
    return sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=5)


def start_experiment(directory, capture_directory, storage_path, *, experiment_id, start,
                     enrollment_hours, follow_up_hours, control_slot, control_description,
                     treatment_description, fees_path, fee_column, gateway_url="http://gateway:8082", clock=None):
    clock = clock or datetime.now(timezone.utc)
    if start < clock + timedelta(minutes=2):
        raise ValueError("Start at least two minutes in the future so capture begins before enrollment")
    directory, capture_directory, fees_path = Path(directory), Path(capture_directory), Path(fees_path)
    routing_path = directory / "routing.json"
    previous = read_routing(routing_path)
    if previous.get("experiment") or previous["secondary_percent"]:
        raise ValueError("Finish the existing experiment or traffic split first")
    health = get_json(gateway_url + "/health/experiment")
    if not health.get("healthy"):
        raise ValueError("Gateway experiment logging is not healthy")
    heartbeat = json.loads((capture_directory / "capture-status.json").read_text())
    if not timedelta(0) <= clock - utc(heartbeat["updated_at"]) < timedelta(seconds=30):
        raise ValueError("Kafka experiment capture is not current; inspect ingestion before starting")
    versions = {slot: get_json(url + "/health/model")["versions"]["model"] for slot, url in BACKENDS.items()}
    connection = readonly(storage_path)
    try:
        partitions = [row[0] for row in connection.execute(
            "SELECT partition FROM live_event_cursors WHERE source_id='cmu-movielog' AND topic='movielog2' ORDER BY partition"
        )]
    finally:
        connection.close()
    fees, invalid = load_licensing_fees(fees_path, fee_column=fee_column)
    if not any(value is not None for value in fees.values()):
        raise ValueError("Fee snapshot has no valid fees")
    enrollment_end = start + timedelta(hours=enrollment_hours)
    plan = validate_plan({
        "id": experiment_id, "registered_at": clock.isoformat(), "start": start.isoformat(),
        "enrollment_end": enrollment_end.isoformat(), "follow_up_seconds": follow_up_hours * 3600,
        "observation_end": (enrollment_end + timedelta(hours=follow_up_hours)).isoformat(),
        "control_slot": control_slot, "treatment_percent": 50, "salt": uuid.uuid4().hex,
        "versions": versions, "control_description": control_description, "treatment_description": treatment_description,
        "source_id": "cmu-movielog", "topic": "movielog2", "partitions": partitions,
        "fees_source": str(fees_path), "fees_file": "fees.csv.gz" if fees_path.suffix == ".gz" else "fees.csv",
        "fee_column": fee_column, "fees_sha256": sha256(fees_path), "invalid_fee_movies": len(invalid),
        "code_revision": os.environ.get("APP_REVISION", "unknown"),
        "exposure": "First gateway request in enrollment window; include failed requests by assigned arm",
        "viewing_unit": "Distinct user/movie watched strictly after first assignment and before assignment + follow-up",
        "ratings": "Latest submitted 1-10 rating per watched movie after its first qualifying watch, within user follow-up",
    })
    archive = directory / "experiments" / plan["id"]
    archive.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(fees_path, archive / plan["fees_file"])
    write_json(archive / "plan.json", plan)
    config = {**previous, **experiment_routing(plan), "generation": uuid.uuid4().hex, "experiment": plan}
    if read_routing(routing_path) != previous:
        raise ValueError("Routing changed during registration")
    write_json(routing_path, config)
    try:
        if get_json(gateway_url + "/health/routing") != config:
            raise ValueError("Gateway did not load the experiment plan")
    except Exception:
        write_json(routing_path, previous)
        raise
    return plan


def export_experiment(directory, capture_directory, experiment_id, output, *, gateway_url="http://gateway:8082", clock=None):
    directory, capture_directory, output = Path(directory), Path(capture_directory), Path(output)
    archive = directory / "experiments" / experiment_id
    plan = validate_plan(json.loads((archive / "plan.json").read_text()))
    clock = clock or datetime.now(timezone.utc)
    end = utc(plan["observation_end"])
    if clock < end + timedelta(minutes=1):
        raise ValueError("Wait until all users have full follow-up plus one minute for ingestion")
    if read_routing(directory / "routing.json").get("experiment") != plan:
        raise ValueError("Export while this experiment's fixed routing is still registered")
    health = get_json(gateway_url + "/health/experiment")
    if not health.get("healthy") or health.get("queued") or health.get("experiment") != plan:
        raise ValueError("Gateway logging must be healthy, drained and using the fixed plan")
    if utc(health["started_at"]) > utc(plan["start"]):
        raise ValueError("Gateway restarted during/after the experiment; queued assignments may have been lost")
    if output.exists():
        raise ValueError("Snapshot output must be a new directory; keep previous exports")
    capture = capture_directory / experiment_id
    if json.loads((capture / "capture-plan.json").read_text()) != plan:
        raise ValueError("Kafka captured a different experiment plan")
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".experiment-", dir=output.parent) as temporary:
        stage = Path(temporary)
        write_json(stage / "plan.json", plan)
        shutil.copyfile(archive / plan["fees_file"], stage / plan["fees_file"])
        if sha256(stage / plan["fees_file"]) != plan["fees_sha256"]:
            raise ValueError("The registered fee snapshot changed")
        connection = readonly(capture / "outcomes.sqlite3")
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("BEGIN")
            progress = {str(row["partition"]): dict(row) for row in connection.execute("SELECT * FROM progress")}
            for partition in plan["partitions"]:
                row = progress.get(str(partition))
                if (not row or not row["first_time"] or not row["latest_time"]
                        or utc(row["first_time"]) > utc(plan["start"])
                        or utc(row["latest_time"]) < end + timedelta(minutes=1)):
                    raise ValueError(f"Partition {partition} does not bracket the experiment and completed follow-up")
            with gzip.open(stage / "outcomes.jsonl.gz", "wt", encoding="utf-8") as target:
                for row in connection.execute("SELECT * FROM observations ORDER BY partition, offset"):
                    target.write(json.dumps(dict(row)) + "\n")
        finally:
            connection.close()
        connection = readonly(capture_directory / "requests.sqlite3")
        try:
            with gzip.open(stage / "requests.jsonl.gz", "wt", encoding="utf-8") as target:
                for payload, in connection.execute("SELECT record_json FROM recommendation_requests"):
                    row = load_json(payload)
                    if row.get("experiment_id") == experiment_id:
                        target.write(json.dumps(row) + "\n")
        finally:
            connection.close()
        write_json(stage / "snapshot.json", {
            "exported_at": clock.isoformat(), "code_revision": os.environ.get("APP_REVISION", "unknown"),
            "capture": {"partitions": progress, "gateway_logging": health,
                        "note": "Partition bounds establish captured time coverage, not proof that every source event was valid or every client received a response."},
            "files": {name: sha256(stage / name) for name in ("plan.json", plan["fees_file"], "requests.jsonl.gz", "outcomes.jsonl.gz")},
        })
        result = analyze_snapshot(stage)
        write_json(stage / "results.json", result)
        (stage / "results.md").write_text(markdown_report(result), encoding="utf-8")
        os.replace(stage, output)
    return {"snapshot": str(output), "results": result}


def finish_experiment(directory, *, abort_reason=None, clock=None):
    directory = Path(directory)
    config = read_routing(directory / "routing.json")
    plan = config.get("experiment")
    if not plan:
        raise ValueError("No experiment is registered")
    clock = clock or datetime.now(timezone.utc)
    if not abort_reason and clock < utc(plan["observation_end"]) + timedelta(minutes=1):
        raise ValueError("Keep configurations fixed through follow-up, or explicitly abort with a reason")
    if not abort_reason and not (directory / "experiments" / plan["id"] / "snapshot" / "snapshot.json").is_file():
        raise ValueError("Export the completed experiment to its default snapshot directory before finishing")
    report = {"experiment_id": plan["id"], "finished_at": clock.isoformat(), "aborted": bool(abort_reason), "reason": abort_reason}
    write_json(directory / "experiments" / plan["id"] / "completion.json", report)
    config.pop("experiment")
    config.update(primary=plan["control_slot"], secondary_percent=0, generation=uuid.uuid4().hex)
    write_json(directory / "routing.json", config)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", type=Path, default="/app/models")
    parser.add_argument("--capture-directory", type=Path, default=os.environ.get("EXPERIMENT_DIRECTORY", "/app/state/experiments"))
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("start")
    start.add_argument("--id", required=True)
    start.add_argument("--start", type=utc, required=True)
    start.add_argument("--enrollment-hours", type=float, required=True)
    start.add_argument("--follow-up-hours", type=float, required=True)
    start.add_argument("--control-slot", choices=("blue", "green"), required=True)
    start.add_argument("--control-description", required=True)
    start.add_argument("--treatment-description", required=True)
    start.add_argument("--fees", type=Path, default="/app/data/movies.csv.gz")
    start.add_argument("--fee-column", default="license_cost")
    start.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", "/app/state/events/events.sqlite3"))
    export = commands.add_parser("export")
    export.add_argument("--id", required=True)
    finish = commands.add_parser("finish")
    finish.add_argument("--abort-reason")
    analysis = commands.add_parser("analyze")
    analysis.add_argument("--snapshot", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "start":
            result = start_experiment(args.directory, args.capture_directory, args.storage_path,
                experiment_id=args.id, start=args.start, enrollment_hours=args.enrollment_hours,
                follow_up_hours=args.follow_up_hours, control_slot=args.control_slot,
                control_description=args.control_description, treatment_description=args.treatment_description,
                fees_path=args.fees, fee_column=args.fee_column)
        elif args.command == "export":
            result = export_experiment(args.directory, args.capture_directory, args.id,
                                      args.directory / "experiments" / args.id / "snapshot")
        elif args.command == "finish":
            result = finish_experiment(args.directory, abort_reason=args.abort_reason)
        else:
            result = analyze_snapshot(args.snapshot)
        print(json.dumps(result, indent=2, allow_nan=False))
    except (ValueError, OSError, KeyError, sqlite3.Error, OverflowError) as error:
        parser.exit(1, f"Experiment stopped ({type(error).__name__}): {error}\n")


if __name__ == "__main__":
    main()
