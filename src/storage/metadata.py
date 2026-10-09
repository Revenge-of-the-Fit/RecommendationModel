import hashlib
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path

from storage.database import StorageError, open_database
from storage.source_redaction import redact_headers, redact_source_fields


def normalize_entity_id(entity_type: str, value) -> str:
    if entity_type == "movie":
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Movie IDs must be nonempty strings")
        return value
    if entity_type != "user":
        raise ValueError("Metadata entity types must be user or movie")
    if type(value) is int:
        user_id = value
    elif isinstance(value, str) and value.isascii() and value.isdecimal():
        digits = value.lstrip("0") or "0"
        if len(digits) > 19:
            raise ValueError("User IDs must fit positive 64-bit integers")
        user_id = int(digits)
    else:
        raise ValueError("User IDs must be positive 64-bit integers")
    if not 0 < user_id <= 2**63 - 1:
        raise ValueError("User IDs must fit positive 64-bit integers")
    return str(user_id)


def _identifier(value, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"A metadata {label} is required")
    return value


def _timestamp(value) -> str:
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Metadata timestamps must include a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _json(value) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def _redact_metadata(value):
    if isinstance(value, dict):
        saved = {}
        for name, item in value.items():
            if isinstance(name, str) and name.lower().replace("_", "-").endswith("headers") and isinstance(item, (list, tuple)):
                if all(
                    isinstance(header, (list, tuple)) and len(header) == 2 and isinstance(header[0], str)
                    and (header[1] is None or isinstance(header[1], str)) for header in item
                ):
                    headers, _ = redact_headers([
                        (header[0], header[1].encode("utf-8") if header[1] is not None else None)
                        for header in item
                    ])
                    saved[name] = [[name, content.decode("utf-8") if content is not None else None] for name, content in headers]
                    continue
            saved[name] = _redact_metadata(item)
        return redact_source_fields(saved)
    if isinstance(value, (list, tuple)):
        return [_redact_metadata(item) for item in value]
    return redact_source_fields(value)


def prepare_metadata_record(record: dict) -> dict:
    if not isinstance(record, dict):
        raise ValueError("Metadata entity records must be objects")
    saved = _redact_metadata(record)
    _json(saved)
    return saved


def _prepare_fetch(record: dict, snapshots: list[dict]) -> tuple[dict, list[dict], str]:
    if not isinstance(record, dict):
        raise ValueError("A metadata fetch record is required")
    fetch_id = _identifier(record.get("fetch_id"), "fetch ID")
    source_id = _identifier(record.get("source_id"), "source ID")
    entity_type = record.get("entity_type")
    if entity_type not in ("user", "movie"):
        raise ValueError("Metadata entity types must be user or movie")
    requested_ids = record.get("requested_ids")
    if not isinstance(requested_ids, list):
        raise ValueError("Requested metadata IDs must be a list")
    requested_ids = [normalize_entity_id(entity_type, item) for item in requested_ids]
    started_at = _timestamp(record.get("started_at"))
    finished_at = _timestamp(record.get("finished_at"))
    if finished_at < started_at:
        raise ValueError("Metadata fetch completion precedes its start")
    status = record.get("status")
    if status not in ("success", "partial", "failed"):
        raise ValueError("Invalid metadata fetch status")
    http_status = record.get("http_status")
    if http_status is not None and (type(http_status) is not int or not 100 <= http_status <= 599):
        raise ValueError("Invalid metadata HTTP status")
    error_type = record.get("error_type")
    if error_type is not None and (
        not isinstance(error_type, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", error_type) is None
    ):
        raise ValueError("Metadata errors require a safe category")
    if not isinstance(snapshots, list) or any(not isinstance(item, dict) for item in snapshots):
        raise ValueError("Metadata snapshots must be a list of records")
    if status == "failed" and snapshots:
        raise ValueError("Failed metadata fetches cannot contain snapshots")
    saved = {
        **_redact_metadata(record),
        "fetch_id": fetch_id, "source_id": source_id, "entity_type": entity_type,
        "requested_ids": requested_ids, "started_at": started_at, "finished_at": finished_at,
        "status": status, "http_status": http_status, "error_type": error_type,
    }
    prepared = []
    snapshot_ids = set()
    for item in snapshots:
        snapshot_id = _identifier(item.get("snapshot_id"), "snapshot ID")
        if snapshot_id in snapshot_ids:
            raise ValueError("Snapshot IDs must be unique within a metadata fetch")
        snapshot_ids.add(snapshot_id)
        if item.get("source_id") != source_id or item.get("entity_type") != entity_type:
            raise ValueError("Metadata snapshots must match their fetch source and kind")
        if "fetch_id" in item and item["fetch_id"] != fetch_id:
            raise ValueError("Metadata snapshots must belong to their fetch")
        entity_id = normalize_entity_id(entity_type, item.get("entity_id"))
        if entity_id not in requested_ids:
            raise ValueError("Metadata snapshots must refer to requested IDs")
        fetched_at = _timestamp(item.get("fetched_at"))
        if not started_at <= fetched_at <= finished_at:
            raise ValueError("Snapshot availability must fall within its fetch timestamps")
        safe_record = prepare_metadata_record(item.get("record"))
        content_version = "sha256:" + hashlib.sha256(_json(safe_record).encode("utf-8")).hexdigest()
        if "content_version" in item and item["content_version"] != content_version:
            raise ValueError("Metadata content versions must match the entity record")
        prepared.append({
            **_redact_metadata(item),
            "snapshot_id": snapshot_id, "fetch_id": fetch_id, "source_id": source_id,
            "entity_type": entity_type, "entity_id": entity_id, "fetched_at": fetched_at,
            "record": safe_record, "content_version": content_version,
        })
    prepared.sort(key=lambda item: item["snapshot_id"])
    fingerprint = hashlib.sha256(_json([saved, prepared]).encode("utf-8")).hexdigest()
    return saved, prepared, fingerprint


class MetadataStore:
    def __init__(self, path: Path, busy_timeout: float = 1.0):
        self.connection = open_database(path, busy_timeout)

    def save_fetch(self, fetch_record: dict, snapshots: list[dict] | None = None) -> bool:
        fetch, snapshots, fingerprint = _prepare_fetch(fetch_record, snapshots if snapshots is not None else [])
        with self.connection:
            cursor = self.connection.execute("""
                INSERT INTO metadata_fetches (
                    fetch_id, source_id, entity_type, started_at, finished_at, status, fingerprint, record_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(fetch_id) DO NOTHING
            """, (
                fetch["fetch_id"], fetch["source_id"], fetch["entity_type"],
                fetch["started_at"], fetch["finished_at"], fetch["status"], fingerprint, _json(fetch),
            ))
            if cursor.rowcount == 0:
                existing = self.connection.execute(
                    "SELECT fingerprint FROM metadata_fetches WHERE fetch_id=?", (fetch["fetch_id"],),
                ).fetchone()[0]
                if existing != fingerprint:
                    raise StorageError("A metadata fetch ID already contains different records")
                return False
            for item in snapshots:
                cursor = self.connection.execute("""
                    INSERT INTO metadata_snapshots (
                        snapshot_id, fetch_id, source_id, entity_type, entity_id, fetched_at, content_version, record_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(snapshot_id) DO NOTHING
                """, (
                    item["snapshot_id"], item["fetch_id"], item["source_id"], item["entity_type"],
                    item["entity_id"], item["fetched_at"], item["content_version"], _json(item),
                ))
                if cursor.rowcount == 0:
                    existing = self.connection.execute(
                        "SELECT record_json FROM metadata_snapshots WHERE snapshot_id=?", (item["snapshot_id"],),
                    ).fetchone()[0]
                    if existing != _json(item):
                        raise StorageError("A metadata snapshot ID already contains a different record")
        return True

    def get_fetch(self, fetch_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT record_json FROM metadata_fetches WHERE fetch_id=?", (fetch_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def get_snapshot(self, snapshot_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT record_json FROM metadata_snapshots WHERE snapshot_id=?", (snapshot_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def latest_snapshot(self, source_id: str, entity_type: str, entity_id, as_of=None) -> dict | None:
        _identifier(source_id, "source ID")
        entity_id = normalize_entity_id(entity_type, entity_id)
        parameters = [source_id, entity_type, entity_id]
        time_filter = ""
        if as_of is not None:
            time_filter = "AND fetched_at <= ?"
            parameters.append(_timestamp(as_of))
        row = self.connection.execute(f"""
            SELECT record_json FROM metadata_snapshots
            WHERE source_id=? AND entity_type=? AND entity_id=? {time_filter}
            ORDER BY fetched_at DESC, snapshot_id DESC LIMIT 1
        """, parameters).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def is_fresh(snapshot: dict | None, max_age_seconds: float, now=None) -> bool:
        if isinstance(max_age_seconds, bool) or not math.isfinite(max_age_seconds) or max_age_seconds <= 0:
            raise ValueError("Metadata cache age must be positive and finite")
        if snapshot is None:
            return False
        current = datetime.fromisoformat(_timestamp(now if now is not None else datetime.now(timezone.utc)))
        fetched_at = datetime.fromisoformat(_timestamp(snapshot["fetched_at"]))
        age = (current - fetched_at).total_seconds()
        return 0 <= age < max_age_seconds

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
