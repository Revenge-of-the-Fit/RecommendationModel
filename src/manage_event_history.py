"""Explicit controls for the opt-in event history maintenance worker."""
import argparse
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path

from storage.database import DEFAULT_STORAGE_PATH
from storage.event_history import GIB, activate, configure, control, history_status, protected_windows
from storage.operations import capacity_status


def report(path, models):
    with closing(sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True, timeout=2)) as connection:
        current = control(connection)
        pins = protected_windows(json.loads(current["policy_json"]), models) if current else []
        history = history_status(connection, pins)
    return {"history": history, "capacity": capacity_status(path, min_free_bytes=16 * GIB)}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--models", type=Path, default=os.environ.get("MODEL_DIRECTORY", "/app/models"))
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "activate", "pause", "resume", "enable-pruning", "disable-pruning"):
        commands.add_parser(name)
    settings = commands.add_parser("configure")
    settings.add_argument("--history-gib", type=int)
    settings.add_argument("--min-days", type=int)
    settings.add_argument("--submission", help="Actual submission time with UTC offset; protects -72/+96 hours")
    pin = commands.add_parser("pin")
    pin.add_argument("--id", required=True)
    pin.add_argument("--start", required=True)
    pin.add_argument("--end", required=True)
    pin.add_argument("--reason", required=True)
    unpin = commands.add_parser("unpin")
    unpin.add_argument("--id", required=True)
    args = parser.parse_args(argv)
    if args.command == "activate":
        # Require the production model volume before creating any state.
        json.loads((args.models / "routing.json").read_text())
        activate(args.storage_path)
    elif args.command in ("pause", "resume"):
        configure(args.storage_path, paused=args.command == "pause")
    elif args.command in ("enable-pruning", "disable-pruning"):
        # Validate the metadata needed to honor automatic pins before enabling.
        report(args.storage_path, args.models)
        configure(args.storage_path, pruning=args.command == "enable-pruning")
    elif args.command in ("configure", "pin", "unpin"):
        with closing(sqlite3.connect(args.storage_path)) as connection:
            current = control(connection)
        if current is None:
            raise ValueError("Activate event history first")
        policy = json.loads(current["policy_json"])
        if args.command == "configure":
            if args.history_gib is not None:
                policy["history_bytes"] = args.history_gib * GIB
            if args.min_days is not None:
                policy["min_days"] = args.min_days
            if args.submission is not None:
                policy["submission"] = args.submission
        else:
            policy["pins"] = [pin for pin in policy["pins"] if pin["id"] != args.id]
            if args.command == "pin":
                policy["pins"].append({"id": args.id, "start": args.start, "end": args.end, "reason": args.reason})
        configure(args.storage_path, policy=policy)
    print(json.dumps(report(args.storage_path, args.models), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
