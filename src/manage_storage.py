import argparse
import json
import os
import sys
from pathlib import Path

from storage.database import DEFAULT_STORAGE_PATH, StorageError
from storage.operations import TABLES, backup_database, export_records, restore_backup, storage_status


def build_parser():
    parser = argparse.ArgumentParser(
        description="Inspect, archive, recover, or export the append-only observation database.",
        epilog="Retention rotates archived gzip backups only; live requests, Kafka events, metadata, and profiles are never purged. Restore requires a new path and the matching archive manifest. Exports retain stored JSON and encode binary columns as explicit base64 objects.",
    )
    commands = parser.add_subparsers(dest="command", required=True)
    status = commands.add_parser("status", help="Report integrity, row counts, and configured capacity guards")
    backup = commands.add_parser("backup", help="Create a consistent verified SQLite gzip backup, then rotate old archives")
    restore = commands.add_parser("restore", help="Integrity-check and restore a backup into a new database path")
    export = commands.add_parser("export", help="Stream stored records to JSONL for recovery or later analysis")
    for command in (status, backup, export):
        command.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    for command in (status, backup, restore):
        command.add_argument("--min-free-bytes", type=int, default=0)
    for command in (status, backup):
        command.add_argument("--max-database-bytes", type=int, help="Guard database plus WAL/SHM bytes; exceeding the guard requires increasing capacity or exporting/moving data before backup")
    backup.add_argument("--archive-dir", type=Path, required=True)
    backup.add_argument("--retain", type=int, default=7, help="Maximum archived backups for this database; at least one (default: 7)")
    backup.add_argument("--max-archive-bytes", type=int, help="Maximum combined managed gzip and manifest bytes; newest oversized backup fails without removing prior archives")
    restore.add_argument("--archive", type=Path, required=True)
    restore.add_argument("--target", type=Path, required=True, help="New path; existing files are never overwritten")
    export.add_argument("--output", type=Path, help="New JSONL path; omit to write records to stdout")
    export.add_argument("--table", choices=TABLES, action="append")
    add_export_filters(export)
    export.epilog = "Start is inclusive and end exclusive. As-of restricts when stored data became available, independently of event time. Source filters include tables with a source namespace; topic filters include Kafka only. Current derived live state is excluded from dated exports. Filtered exports may omit foreign-key dependencies; use an unfiltered backup for complete restoration."
    return parser


def add_export_filters(parser):
    parser.add_argument("--source-id")
    parser.add_argument("--topic")
    parser.add_argument("--start", help="Inclusive ISO event timestamp with timezone")
    parser.add_argument("--end", help="Exclusive ISO event timestamp with timezone")
    parser.add_argument("--as-of", help="Exclude records unavailable at this ISO timestamp with timezone")


def main(argv=None):
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        if arguments.command == "status":
            result = storage_status(arguments.storage_path, min_free_bytes=arguments.min_free_bytes, max_database_bytes=arguments.max_database_bytes)
        elif arguments.command == "backup":
            result = backup_database(
                arguments.storage_path, arguments.archive_dir, retain=arguments.retain,
                min_free_bytes=arguments.min_free_bytes, max_database_bytes=arguments.max_database_bytes,
                max_archive_bytes=arguments.max_archive_bytes,
            )
        elif arguments.command == "restore":
            result = restore_backup(arguments.archive, arguments.target, min_free_bytes=arguments.min_free_bytes)
        else:
            result = export_records(
                arguments.storage_path, arguments.output or sys.stdout, tables=arguments.table,
                source_id=arguments.source_id, topic=arguments.topic, start=arguments.start,
                end=arguments.end, as_of=arguments.as_of,
            )
        destination = sys.stderr if arguments.command == "export" and arguments.output is None else sys.stdout
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False), file=destination)
        return 1 if arguments.command == "status" and not result["healthy"] else 0
    except ValueError as error:
        parser.error(str(error))
    except (StorageError, OSError):
        print("Storage operation failed; inspect integrity, capacity, and destination paths.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
