import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from storage.database import StorageError, open_database
from storage.metadata import normalize_entity_id, prepare_metadata_record


def utc_timestamp(value=None):
    value = datetime.now(timezone.utc) if value is None else value
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("Profile timestamps require a timezone")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def canonical_json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":"))


def content_version(profile):
    return "sha256:" + hashlib.sha256(canonical_json(profile).encode("utf-8")).hexdigest()


def _identifier(value):
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Profile record identifiers must be nonempty strings")
    return value


class ProfileStore:
    def __init__(self, path: Path, busy_timeout: float = 1.0):
        self.connection = open_database(path, busy_timeout)

    def _context(self, record):
        user_id = record.get("user_id")
        if user_id is not None:
            user_id = int(normalize_entity_id("user", user_id))
        snapshot_id = record.get("source_snapshot_id")
        if snapshot_id is not None:
            snapshot_id = _identifier(snapshot_id)
            snapshot = self.connection.execute(
                "SELECT entity_type, entity_id FROM metadata_snapshots WHERE snapshot_id=?", (snapshot_id,),
            ).fetchone()
            if snapshot is None or snapshot[0] != "user" or user_id is None or snapshot[1] != str(user_id):
                raise ValueError("Profile inputs must reference metadata for the same user")
        return user_id, snapshot_id

    def start_attempt(self, record):
        saved = prepare_metadata_record(record)
        saved["attempt_id"] = _identifier(saved.get("attempt_id"))
        saved["cache_key"] = _identifier(saved.get("cache_key"))
        saved["started_at"] = utc_timestamp(saved.get("started_at"))
        saved["finished_at"] = None
        saved["status"] = "pending"
        saved["user_id"], saved["source_snapshot_id"] = self._context(saved)
        payload = canonical_json(saved)
        with self.connection:
            cursor = self.connection.execute("""
                INSERT INTO llm_attempts VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(attempt_id) DO NOTHING
            """, (
                saved["attempt_id"], saved["cache_key"], saved["started_at"], None, "pending",
                saved["user_id"], saved["source_snapshot_id"], payload,
            ))
            if not cursor.rowcount and self.get_attempt(saved["attempt_id"]) != saved:
                raise StorageError("An LLM attempt already has different data")
        return saved

    def update_attempt(self, record, profile=None):
        saved = prepare_metadata_record(record)
        attempt_id = _identifier(saved.get("attempt_id"))
        previous = self.get_attempt(attempt_id)
        if previous is None:
            raise StorageError("An LLM attempt must be started before it is updated")
        for field in ("attempt_id", "cache_key", "started_at", "user_id", "source_snapshot_id", "request", "versions"):
            if saved.get(field) != previous.get(field):
                raise StorageError("An LLM attempt's inputs cannot change")
        status = saved.get("status")
        if status not in ("responded", "success", "failed"):
            raise ValueError("Invalid LLM processing outcome")
        if status == "responded":
            if saved.get("response") is None:
                raise ValueError("A responded attempt requires a response")
            saved["finished_at"] = None
        else:
            saved["finished_at"] = utc_timestamp(saved.get("finished_at"))
            if saved["finished_at"] < previous["started_at"]:
                raise ValueError("LLM completion precedes its start")
        if previous["status"] in ("success", "failed"):
            if previous != saved:
                raise StorageError("A completed LLM attempt cannot change")
            return False
        if previous["status"] == "responded" and saved.get("response") != previous.get("response"):
            raise StorageError("A retained LLM response cannot change")
        if profile is not None and (status != "success" or profile.get("attempt_id") != attempt_id):
            raise ValueError("Generated profiles must belong to successful LLM attempts")
        with self.connection:
            cursor = self.connection.execute("""
                UPDATE llm_attempts SET finished_at=?, status=?, record_json=?
                WHERE attempt_id=? AND record_json=?
            """, (saved["finished_at"], status, canonical_json(saved), attempt_id, canonical_json(previous)))
            if cursor.rowcount != 1:
                raise StorageError("The LLM attempt changed concurrently")
            if profile is not None:
                self._save_profile(profile)
        return True

    def _save_profile(self, record):
        saved = prepare_metadata_record(record)
        profile_id = _identifier(saved.get("profile_id"))
        cache_key = _identifier(saved.get("cache_key"))
        saved["created_at"] = utc_timestamp(saved.get("created_at"))
        if not isinstance(saved.get("profile"), dict):
            raise ValueError("Preference profiles must be objects")
        version = content_version(saved["profile"])
        if saved.get("content_version") != version:
            raise ValueError("Profile versions must match their retained contents")
        if saved.get("origin") not in ("llm", "legacy_cache"):
            raise ValueError("Profile origin must identify generated or legacy data")
        attempt_id = saved.get("attempt_id")
        if saved["origin"] == "llm":
            attempt = self.get_attempt(attempt_id)
            if attempt is None or attempt["status"] != "success" or attempt["cache_key"] != cache_key:
                raise ValueError("Generated profiles require a successful matching attempt")
        elif attempt_id is not None:
            raise ValueError("Legacy cache imports cannot claim an audited attempt")
        payload = canonical_json(saved)
        cursor = self.connection.execute("""
            INSERT INTO preference_profiles VALUES(?,?,?,?,?,?) ON CONFLICT(profile_id) DO NOTHING
        """, (profile_id, cache_key, saved["created_at"], attempt_id, version, payload))
        if not cursor.rowcount and self.get_profile(profile_id) != saved:
            raise StorageError("A preference profile already has different data")
        return saved

    def save_profile(self, record):
        with self.connection:
            return self._save_profile(record)

    def record_use(self, record):
        saved = prepare_metadata_record(record)
        saved["use_id"] = _identifier(saved.get("use_id"))
        saved["profile_id"] = _identifier(saved.get("profile_id"))
        saved["used_at"] = utc_timestamp(saved.get("used_at"))
        saved["user_id"], saved["source_snapshot_id"] = self._context(saved)
        profile = self.get_profile(saved["profile_id"])
        if profile is None or saved["used_at"] < profile["created_at"]:
            raise ValueError("Profile use must follow its availability")
        payload = canonical_json(saved)
        with self.connection:
            cursor = self.connection.execute("""
                INSERT INTO profile_uses VALUES(?,?,?,?,?,?) ON CONFLICT(use_id) DO NOTHING
            """, (
                saved["use_id"], saved["profile_id"], saved["used_at"],
                saved["user_id"], saved["source_snapshot_id"], payload,
            ))
            if not cursor.rowcount:
                existing = self.connection.execute("SELECT record_json FROM profile_uses WHERE use_id=?", (saved["use_id"],)).fetchone()
                if existing[0] != payload:
                    raise StorageError("A profile use already has different data")
        return saved

    def get_attempt(self, attempt_id):
        row = self.connection.execute("SELECT record_json FROM llm_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def get_profile(self, profile_id):
        row = self.connection.execute("SELECT record_json FROM preference_profiles WHERE profile_id=?", (profile_id,)).fetchone()
        return json.loads(row[0]) if row else None

    def list_attempts(self, limit=100):
        if not 1 <= limit <= 10000:
            raise ValueError("The attempt limit must be between 1 and 10000")
        rows = self.connection.execute("SELECT record_json FROM llm_attempts ORDER BY started_at, attempt_id LIMIT ?", (limit,))
        return [json.loads(row[0]) for row in rows]

    def list_profiles(self, cache_key, limit=100):
        if not 1 <= limit <= 10000:
            raise ValueError("The profile limit must be between 1 and 10000")
        rows = self.connection.execute("SELECT record_json FROM preference_profiles WHERE cache_key=? ORDER BY created_at, profile_id LIMIT ?", (cache_key, limit))
        return [json.loads(row[0]) for row in rows]

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
