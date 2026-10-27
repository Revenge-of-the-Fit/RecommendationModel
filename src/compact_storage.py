"""Build a compact, verified database copy while preserving the original."""

import argparse
import json
import os
import shutil
import sqlite3
import tempfile
from contextlib import closing
from pathlib import Path

from storage.database import DATABASE_SCHEMA_VERSION, DEFAULT_STORAGE_PATH, StorageError
from storage.operations import _report, _sync_directory
from storage.payloads import pack_json, unpack_json


COMPRESSED_TABLES = {"recommendation_requests", "metadata_fetches"}


def quoted(name):
    return '"' + name.replace('"', '""') + '"'


def compact_database(path, output, *, min_free_bytes=8 * 1024**3, progress=None):
    path, output = Path(path).resolve(), Path(output).resolve()
    if not path.is_file() or output.exists() or output in {
        path, Path(str(path) + "-wal"), Path(str(path) + "-shm"),
    }:
        raise ValueError("Use an existing source and a new, separate output path")
    if min_free_bytes < 0:
        raise ValueError("Minimum free space cannot be negative")
    emit = progress or (lambda _: None)
    temporary = None
    try:
        with closing(sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=5)) as source:
            # This also prevents an accidentally running writer from changing the copy.
            source.execute("BEGIN IMMEDIATE")
            version = source.execute("PRAGMA user_version").fetchone()[0]
            if version not in (6, DATABASE_SCHEMA_VERSION):
                raise StorageError("Unsupported source schema for compaction")
            logical = source.execute("PRAGMA page_size").fetchone()[0] * source.execute("PRAGMA page_count").fetchone()[0]
            if shutil.disk_usage(output.parent).free < logical * 2 + min_free_bytes:
                raise StorageError("Insufficient space for the copy and index construction")
            objects = source.execute("SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%'").fetchall()
            tables = [(name, sql) for kind, name, sql in objects if kind == "table"]
            if any("CREATE VIRTUAL TABLE" in sql.upper() for _, sql in tables):
                raise StorageError("Virtual tables require a separate migration")
            descriptor, name = tempfile.mkstemp(prefix="." + output.name + "-", suffix=".partial", dir=output.parent)
            os.close(descriptor)
            temporary = Path(name)
            counts = {}
            original_bytes = packed_bytes = compressed_rows = 0
            with closing(sqlite3.connect(temporary)) as target:
                target.execute("PRAGMA journal_mode=DELETE")
                for _, sql in tables:
                    target.execute(sql)
                for table, _ in tables:
                    columns = [row[1] for row in source.execute(f"PRAGMA table_info({quoted(table)})")]
                    position = columns.index("record_json") if table in COMPRESSED_TABLES else None
                    insert = f"INSERT INTO {quoted(table)} VALUES ({','.join('?' for _ in columns)})"
                    cursor = source.execute(f"SELECT * FROM {quoted(table)}")
                    count = 0
                    while rows := cursor.fetchmany(500):
                        if position is not None:
                            converted = []
                            for row in rows:
                                values = list(row)
                                plain = unpack_json(values[position])
                                packed = pack_json(plain)
                                original_bytes += len(plain.encode("utf-8"))
                                packed_bytes += len(packed.encode("utf-8") if isinstance(packed, str) else packed)
                                compressed_rows += isinstance(packed, bytes)
                                values[position] = packed
                                converted.append(values)
                            rows = converted
                        with target:
                            target.executemany(insert, rows)
                        count += len(rows)
                        if count % 10000 == 0:
                            emit({"table": table, "rows_copied": count})
                    counts[table] = count
                    emit({"table": table, "rows_copied": count, "complete": True})
                emit({"operation": "build_indexes"})
                for kind, _, sql in objects:
                    if kind != "table":
                        target.execute(sql)
                target.execute(f"PRAGMA user_version={DATABASE_SCHEMA_VERSION}")
                target.commit()
                emit({"operation": "verify_copy"})
                report = _report(target)
                if not report["integrity_ok"] or report["foreign_key_violations"] or any(
                    (report["table_counts"][table] if table in report["table_counts"] else
                     target.execute(f"SELECT count(*) FROM {quoted(table)}").fetchone()[0]) != count
                    for table, count in counts.items()
                ):
                    raise StorageError("Compact copy failed integrity, foreign-key, or row-count verification")
            attributes = path.stat()
            os.chmod(temporary, attributes.st_mode & 0o777)
            if hasattr(os, "chown"):
                os.chown(temporary, attributes.st_uid, attributes.st_gid)
            with temporary.open("r+b") as handle:
                os.fsync(handle.fileno())
            os.link(temporary, output)  # Never replace an existing destination.
            _sync_directory(output.parent)
            return {
                "source_schema": version, "schema_version": DATABASE_SCHEMA_VERSION,
                "source_bytes": attributes.st_size, "output_bytes": output.stat().st_size,
                "table_counts": counts, "compressed_rows": compressed_rows,
                "original_payload_bytes": original_bytes, "stored_payload_bytes": packed_bytes,
                "integrity_ok": True, "foreign_key_violations": 0,
                "output": str(output), "source_preserved": True,
            }
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--min-free-bytes", type=int, default=8 * 1024**3)
    parser.add_argument("--offline", action="store_true", required=True, help="Confirm API, Kafka, and maintenance writers are stopped")
    args = parser.parse_args(argv)
    try:
        result = compact_database(args.storage_path, args.output, min_free_bytes=args.min_free_bytes,
                                  progress=lambda item: print(json.dumps(item), flush=True))
        print(json.dumps(result, sort_keys=True), flush=True)
        return 0
    except (OSError, sqlite3.Error, StorageError, ValueError) as error:
        print(json.dumps({"status": "failed", "error_type": type(error).__name__, "source_preserved": True}), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
