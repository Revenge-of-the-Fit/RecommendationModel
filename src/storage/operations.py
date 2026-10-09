import base64
import gzip
import hashlib
import json
import math
import os
import re
import shutil
import sqlite3
import tempfile
import uuid
import zlib
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from storage.database import StorageError


TABLES = (
    "recommendation_requests", "kafka_events", "metadata_fetches", "metadata_snapshots",
    "llm_attempts", "preference_profiles", "profile_uses",
)
ARCHIVE_PATTERN = re.compile(r"observations-backup-[0-9]{8}T[0-9]{12}Z-[0-9a-f]{32}\.sqlite3\.gz")
CHUNK_BYTES = 1024 * 1024


def _limits(min_free_bytes, max_database_bytes):
    if type(min_free_bytes) is not int or min_free_bytes < 0:
        raise ValueError("Minimum free bytes must be a nonnegative integer")
    if max_database_bytes is not None and (
        type(max_database_bytes) is not int or max_database_bytes <= 0
    ):
        raise ValueError("Maximum database bytes must be a positive integer")


def _timestamp(value, required=False):
    try:
        stamp = datetime.fromisoformat(value)
        if stamp.utcoffset() is None:
            raise ValueError
        return stamp.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        if required:
            raise ValueError("Timestamps must be valid ISO timestamps with a timezone") from None
        return None


def _filters(source_id, topic, start, end, as_of):
    for value in (source_id, topic):
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError("Source and topic filters must be nonempty strings")
    start = _timestamp(start, True) if start is not None else None
    end = _timestamp(end, True) if end is not None else None
    as_of = _timestamp(as_of, True) if as_of is not None else None
    if start is not None and end is not None and start >= end:
        raise ValueError("Start must be earlier than end")
    return start, end, as_of


def _in_range(stamp, start, end):
    return not (start is not None and (stamp is None or stamp < start)) and not (
        end is not None and (stamp is None or stamp >= end)
    )


def _available(stamp, as_of):
    return as_of is None or stamp is not None and stamp <= as_of


def _sizes(path):
    sizes = []
    for candidate in (path, Path(str(path) + "-wal"), Path(str(path) + "-shm")):
        try:
            sizes.append(candidate.stat().st_size)
        except FileNotFoundError:
            sizes.append(0)
    return sizes


def _free_space(path):
    parent = path if path.is_dir() else path.parent
    while not parent.exists() and parent != parent.parent:
        parent = parent.parent
    return shutil.disk_usage(parent).free


@contextmanager
def _read_database(path):
    path = Path(path).resolve()
    if not path.is_file():
        raise StorageError("Storage database is missing")
    connection = None
    try:
        connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5.0)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA cache_size=-2048")
        yield connection
    except sqlite3.Error:
        raise StorageError("Storage database cannot be read") from None
    finally:
        if connection is not None:
            connection.close()


def _tables(connection):
    present = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    return [name for name in TABLES if name in present]


def _report(connection):
    return {
        "schema_version": connection.execute("PRAGMA user_version").fetchone()[0],
        "integrity_ok": all(row[0] == "ok" for row in connection.execute("PRAGMA integrity_check")),
        "foreign_key_violations": sum(1 for _ in connection.execute("PRAGMA foreign_key_check")),
        "table_counts": {
            name: connection.execute(f'SELECT count(*) FROM "{name}"').fetchone()[0]
            for name in _tables(connection)
        },
    }


def capacity_status(path, *, min_free_bytes=0, max_database_bytes=None):
    _limits(min_free_bytes, max_database_bytes)
    path = Path(path)
    result = {
        "healthy": False, "exists": False,
        "database_bytes": 0, "wal_bytes": 0, "shm_bytes": 0, "total_bytes": 0,
        "free_bytes": None, "min_free_bytes": min_free_bytes,
        "max_database_bytes": max_database_bytes, "capacity_ok": False,
        "capacity_reasons": [], "last_error": None,
    }
    try:
        result["exists"] = path.is_file()
        result["database_bytes"], result["wal_bytes"], result["shm_bytes"] = _sizes(path)
        result["total_bytes"] = sum(result[key] for key in ("database_bytes", "wal_bytes", "shm_bytes"))
        result["free_bytes"] = _free_space(path)
        if result["free_bytes"] < min_free_bytes:
            result["capacity_reasons"].append("minimum_free_space")
        if max_database_bytes is not None and result["total_bytes"] > max_database_bytes:
            result["capacity_reasons"].append("maximum_database_size")
        result["capacity_ok"] = not result["capacity_reasons"]
        result["healthy"] = result["exists"] and result["capacity_ok"]
    except OSError as error:
        result["last_error"] = type(error).__name__
    return result


def storage_status(path, *, min_free_bytes=0, max_database_bytes=None):
    result = {
        **capacity_status(path, min_free_bytes=min_free_bytes, max_database_bytes=max_database_bytes),
        "schema_version": None, "integrity_ok": False,
        "foreign_key_violations": None, "table_counts": {},
    }
    result["healthy"] = False
    try:
        with _read_database(path) as connection:
            connection.execute("BEGIN")
            result.update(_report(connection))
        result["healthy"] = result["integrity_ok"] and not result["foreign_key_violations"] and result["capacity_ok"]
    except (StorageError, OSError) as error:
        result["last_error"] = type(error).__name__
    return result


def _hash_file(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        while chunk := handle.read(CHUNK_BYTES):
            digest.update(chunk)
    return digest.hexdigest()


def _temporary_file(directory, suffix):
    descriptor, name = tempfile.mkstemp(prefix=".observations-", suffix=suffix, dir=directory)
    os.close(descriptor)
    return Path(name)


def _publish(temporary, target):
    os.link(temporary, target)
    temporary.unlink()


def _sync_directory(directory):
    if os.name == "nt":
        return
    descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _manifest_path(archive):
    return Path(str(archive) + ".manifest.json")


def _acquire_archive_lock(directory):
    handle = None
    try:
        handle = (directory / ".observations-backup.lock").open("a+b")
        if handle.seek(0, os.SEEK_END) == 0:
            handle.write(b"\0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        return handle
    except OSError:
        if handle is not None:
            handle.close()
        raise StorageError("Backup archive directory is unavailable or already in use") from None


def _managed_archives(directory, identifier):
    result = []
    for path in directory.glob("observations-backup-*.sqlite3.gz.manifest.json"):
        try:
            with path.open("r", encoding="utf-8") as handle:
                manifest = json.load(handle)
            name = manifest["archive_name"]
            if (
                manifest["manifest_version"] == 1
                and manifest["source_identifier"] == identifier
                and isinstance(name, str) and ARCHIVE_PATTERN.fullmatch(name)
                and path.name == name + ".manifest.json"
                and (directory / name).is_file()
            ):
                result.append((name, path, directory / name))
        except (OSError, ValueError, KeyError, TypeError):
            pass
    return sorted(result, reverse=True)


def backup_database(path, archive_directory, *, retain=7, min_free_bytes=0, max_database_bytes=None, max_archive_bytes=None):
    _limits(min_free_bytes, max_database_bytes)
    _limits(0, max_archive_bytes)
    if type(retain) is not int or retain < 1:
        raise ValueError("At least one archived backup must be retained")
    path = Path(path).resolve()
    directory = Path(archive_directory).resolve()
    status = storage_status(path, min_free_bytes=min_free_bytes, max_database_bytes=max_database_bytes)
    if not status["healthy"]:
        raise StorageError("Storage integrity or capacity check failed")
    temporary_paths = []
    published_paths = []
    complete = False
    archive_lock = None
    try:
        directory.mkdir(parents=True, exist_ok=True)
        archive_lock = _acquire_archive_lock(directory)
        with _read_database(path) as source:
            source.execute("BEGIN")
            size = source.execute("PRAGMA page_count").fetchone()[0] * source.execute("PRAGMA page_size").fetchone()[0]
            if _free_space(directory) < min_free_bytes + size * 2 + CHUNK_BYTES:
                raise StorageError("Insufficient free space for a verified backup")
            snapshot = _temporary_file(directory, ".sqlite3")
            temporary_paths.append(snapshot)
            with closing(sqlite3.connect(snapshot)) as target:
                source.backup(target, pages=128, sleep=0.01)
                target.execute("PRAGMA journal_mode=DELETE")
                report = _report(target)
                if not report["integrity_ok"] or report["foreign_key_violations"]:
                    raise StorageError("Backup integrity check failed")
        created = datetime.now(timezone.utc)
        name = f"observations-backup-{created.strftime('%Y%m%dT%H%M%S%fZ')}-{uuid.uuid4().hex}.sqlite3.gz"
        archive = directory / name
        compressed = _temporary_file(directory, ".gz")
        temporary_paths.append(compressed)
        with snapshot.open("rb") as original, compressed.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as output:
                shutil.copyfileobj(original, output, CHUNK_BYTES)
            raw.flush()
            os.fsync(raw.fileno())
        verification = hashlib.sha256()
        with gzip.open(compressed, "rb") as handle:
            while chunk := handle.read(CHUNK_BYTES):
                verification.update(chunk)
        original_hash = _hash_file(snapshot)
        if verification.hexdigest() != original_hash:
            raise StorageError("Compressed backup verification failed")
        identifier = hashlib.sha256(os.path.normcase(str(path)).encode("utf-8")).hexdigest()
        manifest = {
            "manifest_version": 1, "archive_name": name,
            "created_at": created.isoformat(timespec="microseconds"),
            "source_identifier": identifier,
            "schema_version": report["schema_version"],
            "database_bytes": snapshot.stat().st_size, "database_sha256": original_hash,
            "archive_bytes": compressed.stat().st_size, "archive_sha256": _hash_file(compressed),
            "table_counts": report["table_counts"],
        }
        manifest_temporary = _temporary_file(directory, ".json")
        temporary_paths.append(manifest_temporary)
        with manifest_temporary.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, ensure_ascii=False, sort_keys=True, allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if max_archive_bytes is not None and compressed.stat().st_size + manifest_temporary.stat().st_size > max_archive_bytes:
            raise StorageError("A verified backup exceeds the archive capacity limit")
        _publish(compressed, archive)
        published_paths.append(archive)
        manifest_path = _manifest_path(archive)
        _publish(manifest_temporary, manifest_path)
        published_paths.append(manifest_path)
        _sync_directory(directory)
        complete = True
        previous = [item for item in _managed_archives(directory, identifier) if item[0] != name]
        for _, old_manifest, old_archive in previous[retain - 1:]:
            old_archive.unlink()
            old_manifest.unlink()
        remaining = _managed_archives(directory, identifier)
        archive_total = sum(item[1].stat().st_size + item[2].stat().st_size for item in remaining)
        for _, old_manifest, old_archive in reversed(remaining):
            if max_archive_bytes is None or archive_total <= max_archive_bytes:
                break
            if old_archive == archive:
                continue
            archive_total -= old_manifest.stat().st_size + old_archive.stat().st_size
            old_archive.unlink()
            old_manifest.unlink()
        _sync_directory(directory)
        return {
            **manifest, "retained_archives": len(_managed_archives(directory, identifier)),
            "manifest_path": str(manifest_path), "archive_path": str(archive),
            "archive_total_bytes": archive_total, "max_archive_bytes": max_archive_bytes,
        }
    except (OSError, sqlite3.Error, ValueError, EOFError, zlib.error):
        raise StorageError("Storage backup failed") from None
    finally:
        try:
            for temporary in temporary_paths:
                temporary.unlink(missing_ok=True)
            if not complete:
                for published in published_paths:
                    published.unlink(missing_ok=True)
        finally:
            if archive_lock is not None:
                archive_lock.close()


def restore_backup(archive, target, *, min_free_bytes=0):
    _limits(min_free_bytes, None)
    archive = Path(archive).resolve()
    target = Path(target).absolute()
    if os.path.lexists(target):
        raise StorageError("Restore requires a new destination path")
    temporary = None
    try:
        with _manifest_path(archive).open("r", encoding="utf-8") as handle:
            manifest = json.load(handle)
        if (
            not isinstance(manifest, dict)
            or type(manifest.get("manifest_version")) is not int or manifest["manifest_version"] != 1
            or manifest.get("archive_name") != archive.name
            or type(manifest.get("database_bytes")) is not int or manifest["database_bytes"] <= 0
            or type(manifest.get("archive_bytes")) is not int
            or not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("database_sha256", "")))
            or not re.fullmatch(r"[0-9a-f]{64}", str(manifest.get("archive_sha256", "")))
        ):
            raise StorageError("Backup manifest is invalid")
        if archive.stat().st_size != manifest["archive_bytes"] or _hash_file(archive) != manifest["archive_sha256"]:
            raise StorageError("Backup archive checksum failed")
        target.parent.mkdir(parents=True, exist_ok=True)
        if _free_space(target) < min_free_bytes + manifest["database_bytes"]:
            raise StorageError("Insufficient free space for restoration")
        temporary = _temporary_file(target.parent, ".sqlite3")
        digest = hashlib.sha256()
        written = 0
        with gzip.open(archive, "rb") as original, temporary.open("wb") as output:
            while chunk := original.read(CHUNK_BYTES):
                written += len(chunk)
                if written > manifest["database_bytes"]:
                    raise StorageError("Restored database exceeds its declared size")
                if _free_space(target) < min_free_bytes + len(chunk):
                    raise StorageError("Insufficient free space for restoration")
                digest.update(chunk)
                output.write(chunk)
            output.flush()
            os.fsync(output.fileno())
        if written != manifest["database_bytes"] or digest.hexdigest() != manifest["database_sha256"]:
            raise StorageError("Restored database checksum failed")
        with _read_database(temporary) as connection:
            report = _report(connection)
        if (
            not report["integrity_ok"] or report["foreign_key_violations"]
            or report["schema_version"] != manifest.get("schema_version")
            or report["table_counts"] != manifest.get("table_counts")
        ):
            raise StorageError("Restored database integrity check failed")
        _publish(temporary, target)
        _sync_directory(target.parent)
        return {**report, "restored_path": str(target), "database_bytes": written, "database_sha256": digest.hexdigest()}
    except (OSError, sqlite3.Error, ValueError, TypeError, EOFError, zlib.error):
        raise StorageError("Storage restoration failed") from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _json(value):
    try:
        result = json.loads(value)
        return result if isinstance(result, dict) else {}
    except (TypeError, ValueError, RecursionError):
        return {}


def _row_times(table, row):
    if table == "recommendation_requests":
        record = _json(row["record_json"])
        return _timestamp(row["started_at"]), _timestamp(record.get("finished_at"))
    if table == "kafka_events":
        ingested = _timestamp(row["ingested_at"])
        return _timestamp(row["event_timestamp"]) or ingested, ingested
    if table == "metadata_fetches":
        return _timestamp(row["started_at"]), _timestamp(row["finished_at"])
    if table == "metadata_snapshots":
        stamp = _timestamp(row["fetched_at"])
        return stamp, stamp
    if table == "llm_attempts":
        record = _json(row["record_json"])
        available = row["finished_at"] or record.get("response_received_at") or record.get("updated_at")
        if row["status"] == "pending":
            available = row["started_at"]
        return _timestamp(row["started_at"]), _timestamp(available)
    field = "created_at" if table == "preference_profiles" else "used_at"
    stamp = _timestamp(row[field])
    return stamp, stamp


def _encoded_row(row):
    return {
        key: {"$type": "bytes", "base64": base64.b64encode(value).decode("ascii")}
        if isinstance(value, bytes) else value
        for key, value in dict(row).items()
    }


@contextmanager
def _output_handle(output, database_path):
    if hasattr(output, "write"):
        yield output
        return
    output = Path(output)
    protected = {
        Path(database_path).resolve(), Path(str(database_path) + "-wal").resolve(),
        Path(str(database_path) + "-shm").resolve(),
    }
    if output.resolve() in protected:
        raise ValueError("Export output must differ from the storage database")
    with output.open("x", encoding="utf-8", newline="\n") as handle:
        yield handle


def _write_line(output, record):
    output.write(json.dumps(record, ensure_ascii=False, allow_nan=False, separators=(",", ":")) + "\n")


def export_records(path, output, *, tables=None, source_id=None, topic=None, start=None, end=None, as_of=None):
    start, end, as_of = _filters(source_id, topic, start, end, as_of)
    if tables is not None and (not tables or any(name not in TABLES for name in tables)):
        raise ValueError("Export tables must be selected from the supported storage tables")
    counts = {}
    try:
        with _read_database(path) as connection, _output_handle(output, path) as handle:
            connection.execute("BEGIN")
            for table in _tables(connection):
                if tables is not None and table not in tables:
                    continue
                counts[table] = 0
                columns = list(connection.execute(f'PRAGMA table_info("{table}")'))
                names = {row[1] for row in columns}
                if source_id is not None and "source_id" not in names:
                    continue
                if topic is not None and table != "kafka_events":
                    continue
                predicates, parameters = [], []
                if source_id is not None:
                    predicates.append('"source_id"=?')
                    parameters.append(source_id)
                if topic is not None:
                    predicates.append('"topic"=?')
                    parameters.append(topic)
                primary = [row[1] for row in sorted(columns, key=lambda item: item[5]) if row[5]]
                query = f'SELECT * FROM "{table}"'
                if predicates:
                    query += " WHERE " + " AND ".join(predicates)
                if primary:
                    query += " ORDER BY " + ",".join('"' + name + '"' for name in primary)
                for row in connection.execute(query, parameters):
                    stamp, available = _row_times(table, row)
                    if _in_range(stamp, start, end) and _available(available, as_of):
                        _write_line(handle, {"table": table, "row": _encoded_row(row)})
                        counts[table] += 1
    except (OSError, sqlite3.Error):
        raise StorageError("Storage export failed") from None
    return counts


def _event_identity(row):
    return {name: row[name] for name in ("source_id", "topic", "partition", "offset")}


def _impression(table, row):
    if table == "recommendation_requests":
        record = _json(row["record_json"])
        if record.get("status") != 200 or not record.get("response_complete"):
            return None
        stamp = _timestamp(record.get("finished_at"))
        available = stamp
        recommendations = record.get("recommendations")
        identity = {"request_id": row["request_id"]}
        provenance = {key: record.get(key) for key in (
            "serving_method", "versions", "profile_reference", "llm_model", "fallback_reason", "cached",
        )}
        basis = "response_finished_at"
    else:
        if row["event_type"] != "recommendation" or row["parse_status"] != "parsed":
            return None
        record = _json(row["parsed_json"]).get("fields", {})
        if record.get("status") != 200:
            return None
        stamp = _timestamp(row["event_timestamp"])
        available = _timestamp(row["ingested_at"])
        recommendations = record.get("recommendations")
        identity = _event_identity(row)
        provenance = {"server": record.get("server")}
        basis = "recommendation_event_timestamp"
    if (
        stamp is None or available is None or row["user_id"] is None
        or not isinstance(recommendations, list)
        or not all(isinstance(item, dict) and isinstance(item.get("movie_id"), str) for item in recommendations)
    ):
        return None
    return {
        "record_type": "impression", "origin_table": table, "identity": identity,
        "user_id": row["user_id"], "event_timestamp": stamp.isoformat(timespec="microseconds"),
        "available_at": available.isoformat(timespec="microseconds"), "timestamp_basis": basis,
        "recommendations": recommendations, "provenance": provenance,
    }


def export_observations(path, output, *, source_id=None, topic=None, start=None, end=None, as_of=None, match_window_seconds=86400):
    start, end, as_of = _filters(source_id, topic, start, end, as_of)
    if isinstance(match_window_seconds, bool) or not isinstance(match_window_seconds, (int, float)) or not math.isfinite(match_window_seconds) or match_window_seconds <= 0:
        raise ValueError("The matching window must be a finite positive number of seconds")
    counts = {"impressions": 0, "observed_events": 0, "candidate_links": 0, "ambiguous_events": 0}
    try:
        with _read_database(path) as connection, _output_handle(output, path) as handle:
            connection.execute("PRAGMA temp_store=FILE")
            # Large exports keep candidate lookups on disk instead of collecting them in memory.
            connection.execute("CREATE TEMP TABLE candidate_impressions (user_id INTEGER, movie_id TEXT, served_at TEXT, impression_json TEXT, recommendations_json TEXT)")
            connection.execute("CREATE INDEX temp.candidates_by_user_movie_time ON candidate_impressions(user_id,movie_id,served_at)")
            connection.execute("BEGIN")
            available_tables = _tables(connection)
            for table in ("recommendation_requests", "kafka_events"):
                if table not in available_tables:
                    continue
                query = f'SELECT * FROM "{table}"'
                predicates, parameters = [], []
                if table == "kafka_events":
                    predicates.append("event_type='recommendation'")
                    if source_id is not None:
                        predicates.append("source_id=?")
                        parameters.append(source_id)
                    if topic is not None:
                        predicates.append("topic=?")
                        parameters.append(topic)
                    query += " WHERE " + " AND ".join(predicates)
                for row in connection.execute(query, parameters):
                    impression = _impression(table, row)
                    if impression is None:
                        continue
                    stamp = _timestamp(impression["event_timestamp"])
                    if not _in_range(stamp, start, end) or not _available(_timestamp(impression["available_at"]), as_of):
                        continue
                    _write_line(handle, impression)
                    counts["impressions"] += 1
                    by_movie = {}
                    for item in impression["recommendations"]:
                        by_movie.setdefault(item["movie_id"], []).append(item)
                    encoded = json.dumps(impression, ensure_ascii=False, allow_nan=False)
                    for movie, recommendations in by_movie.items():
                        connection.execute("INSERT INTO candidate_impressions VALUES (?,?,?,?,?)", (
                            impression["user_id"], movie, impression["event_timestamp"], encoded,
                            json.dumps(recommendations, ensure_ascii=False, allow_nan=False),
                        ))
            if "kafka_events" not in available_tables:
                return counts
            predicates = ["event_type IN ('watch','rating')", "parse_status='parsed'"]
            parameters = []
            for name, value in (("source_id", source_id), ("topic", topic)):
                if value is not None:
                    predicates.append(name + "=?")
                    parameters.append(value)
            query = "SELECT * FROM kafka_events WHERE " + " AND ".join(predicates) + " ORDER BY event_timestamp,source_id,topic,partition,offset"
            for row in connection.execute(query, parameters):
                stamp = _timestamp(row["event_timestamp"])
                if stamp is None or row["user_id"] is None or row["movie_id"] is None:
                    continue
                if not _in_range(stamp, start, end) or not _available(_timestamp(row["ingested_at"]), as_of):
                    continue
                lower = (stamp - timedelta(seconds=match_window_seconds)).isoformat(timespec="microseconds")
                upper = stamp.isoformat(timespec="microseconds")
                candidate_parameters = (row["user_id"], row["movie_id"], lower, upper)
                selection = " FROM candidate_impressions WHERE user_id=? AND movie_id=? AND served_at>=? AND served_at<?"
                candidate_count = connection.execute("SELECT count(*)" + selection, candidate_parameters).fetchone()[0]
                fields = _json(row["parsed_json"]).get("fields", {})
                observed = {
                    "record_type": "observed_event", "identity": _event_identity(row),
                    "user_id": row["user_id"], "movie_id": row["movie_id"],
                    "event_type": row["event_type"], "event_timestamp": upper,
                    "available_at": row["ingested_at"],
                    "candidate_count": candidate_count, "ambiguous": candidate_count > 1,
                    "attribution": "candidate_only", "fields": fields,
                }
                if row["event_type"] == "watch":
                    observed["observation_unit"] = "movie_minute"
                    observed["observed_minute"] = fields.get("minute")
                else:
                    observed["rating"] = fields.get("rating")
                _write_line(handle, observed)
                counts["observed_events"] += 1
                counts["ambiguous_events"] += int(candidate_count > 1)
                for candidate in connection.execute("SELECT impression_json,recommendations_json" + selection + " ORDER BY served_at,impression_json", candidate_parameters):
                    impression = _json(candidate["impression_json"])
                    _write_line(handle, {
                        "record_type": "candidate_link", "observed_identity": observed["identity"],
                        "impression_origin_table": impression["origin_table"],
                        "impression_identity": impression["identity"],
                        "user_id": row["user_id"], "movie_id": row["movie_id"],
                        "recommendations": json.loads(candidate["recommendations_json"]),
                        "seconds_after_impression": (stamp - _timestamp(impression["event_timestamp"])).total_seconds(),
                        "candidate_count": candidate_count, "ambiguous": candidate_count > 1,
                        "attribution": "candidate_only", "matching_basis": "same_user_movie_and_later_timestamp",
                    })
                    counts["candidate_links"] += 1
    except (OSError, sqlite3.Error, OverflowError):
        raise StorageError("Observation export failed") from None
    return counts
