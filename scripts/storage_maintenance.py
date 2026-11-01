import argparse
import json
import math
import os
import signal
import sys
import threading
import time
from pathlib import Path

from storage.database import DEFAULT_STORAGE_PATH
from storage.operations import backup_database, capacity_status, checkpoint_database


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("The value must be positive")
    return number


def nonnegative_int(value):
    number = int(value)
    if number < 0:
        raise argparse.ArgumentTypeError("The value cannot be negative")
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("The value must be positive and finite")
    return number


def run_maintenance(
    path, archive_directory, *, retain=2, min_free_bytes=0,
    max_database_bytes=None, max_archive_bytes=None, interval_hours=24, once=False, stop_event=None,
    backups_enabled=True,
):
    stopped = stop_event if stop_event is not None else threading.Event()
    completed = 0
    next_backup = 0
    while not stopped.is_set():
        wal = Path(str(path) + "-wal")
        checkpoint = checkpoint_database(path) if wal.exists() and wal.stat().st_size > 64 * 1024**2 else None
        status = capacity_status(
            path, min_free_bytes=min_free_bytes, max_database_bytes=max_database_bytes,
        )
        print(json.dumps({"operation": "storage_check", **status, "checkpoint": checkpoint,
                          "backups_enabled": backups_enabled}, sort_keys=True), flush=True)
        if not status["healthy"]:
            raise RuntimeError("Storage health or capacity check failed")
        if backups_enabled and time.monotonic() >= next_backup:
            try:
                manifest = backup_database(
                    path, archive_directory, retain=retain, min_free_bytes=min_free_bytes,
                    max_database_bytes=max_database_bytes, max_archive_bytes=max_archive_bytes,
                )
            except Exception as error:
                if once:
                    raise
                print(json.dumps({"operation": "storage_backup", "status": "failed",
                                  "error_type": type(error).__name__}), file=sys.stderr, flush=True)
                next_backup = time.monotonic() + min(3600, interval_hours * 3600)
            else:
                print(json.dumps({"operation": "storage_backup", **manifest}, sort_keys=True), flush=True)
                completed += 1
                next_backup = time.monotonic() + interval_hours * 3600
        if once:
            break
        stopped.wait(min(60, interval_hours * 3600))
    return completed


def main(argv=None):
    parser = argparse.ArgumentParser(description="Check persistent storage and rotate verified compressed backups.")
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--archive-directory", type=Path, default=os.environ.get("ARCHIVE_DIRECTORY", "/app/state/archives"))
    parser.add_argument("--interval-hours", type=positive_float, default=os.environ.get("STORAGE_BACKUP_INTERVAL_HOURS", "24"))
    parser.add_argument("--retain", type=positive_int, default=os.environ.get("STORAGE_BACKUP_RETAIN", "2"))
    parser.add_argument("--min-free-bytes", type=nonnegative_int, default=os.environ.get("STORAGE_MIN_FREE_BYTES", "8589934592"))
    parser.add_argument("--max-database-bytes", type=positive_int, default=os.environ.get("STORAGE_MAX_DATABASE_BYTES", "17179869184") or None)
    parser.add_argument("--max-archive-bytes", type=positive_int, default=os.environ.get("STORAGE_MAX_ARCHIVE_BYTES", "25769803776") or None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--backups", action=argparse.BooleanOptionalAction, default=True,
                        help="Create backups as well as checking storage. Use --no-backups to disable.")
    arguments = parser.parse_args(argv)
    stopped = threading.Event()
    previous_handlers = {}
    if threading.current_thread() is threading.main_thread():
        for name in ("SIGINT", "SIGTERM"):
            signum = getattr(signal, name, None)
            if signum is not None:
                previous_handlers[signum] = signal.signal(signum, lambda *_: stopped.set())
    try:
        run_maintenance(
            arguments.storage_path, arguments.archive_directory,
            retain=arguments.retain, min_free_bytes=arguments.min_free_bytes,
            max_database_bytes=arguments.max_database_bytes,
            max_archive_bytes=arguments.max_archive_bytes,
            interval_hours=arguments.interval_hours, once=arguments.once,
            stop_event=stopped, backups_enabled=arguments.backups,
        )
        return 0
    except Exception as error:
        print(json.dumps({
            "operation": "storage_maintenance", "status": "failed",
            "error_type": type(error).__name__,
        }, sort_keys=True), file=sys.stderr, flush=True)
        return 1
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)


if __name__ == "__main__":
    raise SystemExit(main())
