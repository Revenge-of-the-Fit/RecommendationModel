import json
import logging
import queue
import re
import threading
from datetime import datetime, timezone
from pathlib import Path

from storage.database import StorageError, open_database


LOGGER = logging.getLogger(__name__)
SCHEMA_VERSION = 1
REDACTED = "[redacted]"


def redact_sensitive_fields(value):
    if isinstance(value, dict):
        result = {}
        for key, item in value.items():
            normalized = re.sub(r"[^a-z0-9]", "", str(key).lower())
            sensitive = any(name in normalized for name in (
                "authorization", "password", "passwd", "secret", "apikey",
                "credential", "cookie", "sessionid", "sessionkey", "privatekey",
            )) or normalized.endswith("token") or normalized in ("auth", "session")
            result[key] = REDACTED if sensitive else redact_sensitive_fields(item)
        return result
    if isinstance(value, (list, tuple)):
        return [redact_sensitive_fields(item) for item in value]
    return value


def prepare_request(record: dict) -> tuple[str, str, int | None, str]:
    request_id = record.get("request_id")
    started_at = record.get("started_at")
    user_id = record.get("user_id")
    if not isinstance(request_id, str) or not request_id.strip():
        raise ValueError("A request ID is required")
    if not isinstance(started_at, str) or not started_at.strip():
        raise ValueError("A request start timestamp is required")
    timestamp = datetime.fromisoformat(started_at)
    if timestamp.utcoffset() is None:
        raise ValueError("Request timestamps must include a timezone")
    started_at = timestamp.astimezone(timezone.utc).isoformat()
    if user_id is not None and (
        type(user_id) is not int or not 0 < user_id <= 2**63 - 1
    ):
        raise ValueError("User IDs must be positive 64-bit integers or null")
    saved = redact_sensitive_fields({
        **record, "started_at": started_at, "schema_version": SCHEMA_VERSION,
    })
    payload = json.dumps(
        saved, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
    )
    return request_id, started_at, user_id, payload


class RequestStore:
    def __init__(self, path: Path, busy_timeout: float = 1.0):
        self.path = Path(path)
        self.connection = open_database(self.path, busy_timeout)

    def save_request(self, record: dict) -> bool:
        return self.save_prepared(prepare_request(record))

    def save_prepared(self, record: tuple[str, str, int | None, str]) -> bool:
        with self.connection:
            cursor = self.connection.execute("""
                INSERT INTO recommendation_requests(request_id, started_at, user_id, record_json)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(request_id) DO NOTHING
            """, record)
            if cursor.rowcount == 0:
                existing = self.connection.execute(
                    "SELECT record_json FROM recommendation_requests WHERE request_id = ?",
                    (record[0],),
                ).fetchone()[0]
                if existing != record[3]:
                    raise StorageError("A request ID already exists with a different record")
        return cursor.rowcount == 1

    def get_request(self, request_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT record_json FROM recommendation_requests WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        return json.loads(row[0]) if row else None

    def list_requests(self, user_id: int, limit: int = 100) -> list[dict]:
        if not 1 <= limit <= 10000:
            raise ValueError("The request limit must be between 1 and 10000")
        rows = self.connection.execute("""
            SELECT record_json FROM recommendation_requests WHERE user_id = ?
            ORDER BY started_at, request_id LIMIT ?
        """, (user_id, limit))
        return [json.loads(row[0]) for row in rows]

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class RequestLog:
    def __init__(self, path: Path, queue_size: int = 1024, busy_timeout: float = 1.0):
        if queue_size <= 0 or busy_timeout <= 0:
            raise ValueError("Queue size and lock timeout must be positive")
        self.path = Path(path)
        self.busy_timeout = busy_timeout
        self.queue = queue.Queue(maxsize=queue_size)
        self.lock = threading.Lock()
        self.closing = threading.Event()
        self.ready = threading.Event()
        self.startup_error = None
        self.counts = {name: 0 for name in ("accepted", "written", "duplicates", "failed", "rejected")}
        self.last_error = None
        self.last_write_at = None
        self.worker = threading.Thread(target=self._run, name="request-storage", daemon=True)
        self.worker.start()
        self.ready.wait()
        if self.startup_error is not None:
            raise StorageError(f"Request storage initialization failed ({self.startup_error})")

    def submit(self, record: dict) -> bool:
        try:
            prepared = prepare_request(record)
        except (ValueError, TypeError, AttributeError, OverflowError):
            self._reject("invalid_record")
            return False
        with self.lock:
            if self.closing.is_set() or not self.worker.is_alive():
                reason = "writer_stopped"
            else:
                try:
                    self.queue.put_nowait(prepared)
                except queue.Full:
                    reason = "queue_full"
                else:
                    self.counts["accepted"] += 1
                    return True
            self.counts["rejected"] += 1
            self.last_error = reason
        LOGGER.error("Request logging rejected a record (%s)", reason)
        return False

    def _reject(self, reason: str) -> None:
        with self.lock:
            self.counts["rejected"] += 1
            self.last_error = reason
        LOGGER.error("Request logging rejected a record (%s)", reason)

    def _run(self) -> None:
        try:
            store = RequestStore(self.path, self.busy_timeout)
        except Exception as error:
            self.startup_error = type(error).__name__
            LOGGER.error("Request storage initialization failed (%s)", self.startup_error)
            self.ready.set()
            return
        self.ready.set()
        try:
            while not self.closing.is_set() or not self.queue.empty():
                try:
                    record = self.queue.get(timeout=0.1)
                except queue.Empty:
                    continue
                try:
                    inserted = store.save_prepared(record)
                except Exception as error:
                    category = type(error).__name__
                    with self.lock:
                        self.counts["failed"] += 1
                        self.last_error = category
                    LOGGER.error("Request storage write failed (%s)", category)
                else:
                    with self.lock:
                        self.counts["written" if inserted else "duplicates"] += 1
                        self.last_write_at = datetime.now(timezone.utc).isoformat()
                finally:
                    self.queue.task_done()
        finally:
            store.close()

    def status(self) -> dict:
        with self.lock:
            return {
                **self.counts,
                "queued": self.queue.qsize(),
                "healthy": self.worker.is_alive() and not self.closing.is_set()
                and self.counts["failed"] == 0 and self.counts["rejected"] == 0,
                "last_error": self.last_error,
                "last_write_at": self.last_write_at,
            }

    def close(self, timeout: float = 5.0) -> bool:
        with self.lock:
            self.closing.set()
        self.worker.join(timeout=timeout)
        with self.lock:
            drained = not self.worker.is_alive() and self.counts["failed"] == 0
        if not drained:
            LOGGER.error("Request storage shutdown did not persist all accepted records")
        return drained
