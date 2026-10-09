import logging
import math
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable
from uuid import uuid4

from storage.metadata import MetadataStore, normalize_entity_id, prepare_metadata_record


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class MetadataBatch:
    http_status: int | None
    records: dict = field(default_factory=dict)
    response: object = None
    error_type: str | None = None


@dataclass
class MetadataCollection:
    snapshots: dict = field(default_factory=dict)
    unresolved: list = field(default_factory=list)
    fetch_ids: list = field(default_factory=list)
    cached: int = 0
    fetched: int = 0


class MetadataRateLimiter:
    def __init__(self, min_interval: float = 1.0, *, monotonic=time.monotonic, sleep=time.sleep):
        if isinstance(min_interval, bool) or not math.isfinite(min_interval) or min_interval < 0:
            raise ValueError("Metadata call interval must be nonnegative and finite")
        self.min_interval = min_interval
        self.monotonic = monotonic
        self.sleep = sleep
        self.last_call = None
        self.lock = threading.Lock()

    def wait(self):
        with self.lock:
            if self.last_call is not None:
                while True:
                    remaining = self.last_call + self.min_interval - self.monotonic()
                    if remaining <= 0:
                        break
                    self.sleep(remaining)
            self.last_call = self.monotonic()


class MetadataCollector:
    def __init__(
        self, store: MetadataStore, source_id: str,
        fetcher: Callable[[str, list[str]], MetadataBatch], *,
        max_age_seconds: float = 86400, batch_size: int = 200,
        limiter: MetadataRateLimiter | None = None,
        clock: Callable[[], datetime] | None = None,
    ):
        if not isinstance(source_id, str) or not source_id.strip():
            raise ValueError("A metadata source ID is required")
        if isinstance(max_age_seconds, bool) or not math.isfinite(max_age_seconds) or max_age_seconds <= 0:
            raise ValueError("Metadata cache age must be positive and finite")
        if type(batch_size) is not int or not 1 <= batch_size <= 200:
            raise ValueError("Metadata batches must contain between 1 and 200 IDs")
        if not callable(fetcher):
            raise ValueError("A metadata fetcher is required")
        self.store = store
        self.source_id = source_id
        self.fetcher = fetcher
        self.max_age_seconds = max_age_seconds
        self.batch_size = batch_size
        self.limiter = limiter if limiter is not None else MetadataRateLimiter()
        self.clock = clock if clock is not None else lambda: datetime.now(timezone.utc)

    def _now(self):
        current = self.clock()
        if not isinstance(current, datetime) or current.utcoffset() is None:
            raise ValueError("The metadata clock must return a timezone-aware datetime")
        return current.astimezone(timezone.utc)

    @staticmethod
    def _records(entity_type, requested, batch):
        if not isinstance(batch.records, dict):
            raise ValueError("Metadata batch records must be objects")
        records = {}
        for entity_id, record in batch.records.items():
            entity_id = normalize_entity_id(entity_type, entity_id)
            if entity_id not in requested or entity_id in records or not isinstance(record, dict):
                raise ValueError("Metadata response IDs do not match the requested batch")
            records[entity_id] = prepare_metadata_record(record)
        return records

    def collect(self, entity_type: str, entity_ids, *, force: bool = False, offline: bool = False) -> MetadataCollection:
        if entity_type not in ("user", "movie"):
            raise ValueError("Metadata entity types must be user or movie")
        entity_ids = list(dict.fromkeys(normalize_entity_id(entity_type, value) for value in entity_ids))
        result = MetadataCollection()
        now = self._now()
        pending = []
        for entity_id in entity_ids:
            snapshot = self.store.latest_snapshot(self.source_id, entity_type, entity_id, as_of=now)
            if not force and self.store.is_fresh(snapshot, self.max_age_seconds, now):
                result.snapshots[entity_id] = snapshot
                result.cached += 1
            else:
                pending.append(entity_id)
        if offline or not pending:
            result.unresolved = pending
            return result
        for start in range(0, len(pending), self.batch_size):
            requested = pending[start:start + self.batch_size]
            self.limiter.wait()
            started_at = self._now()
            response = None
            http_status = None
            error_type = None
            records = {}
            try:
                batch = self.fetcher(entity_type, list(requested))
                if not isinstance(batch, MetadataBatch):
                    raise ValueError("The metadata fetcher returned an invalid batch")
                response = batch.response
                http_status = batch.http_status
                if http_status is not None and (type(http_status) is not int or not 100 <= http_status <= 599):
                    http_status = None
                    raise ValueError("Invalid metadata HTTP status")
                error_type = batch.error_type
                if error_type is not None and (
                    not isinstance(error_type, str) or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", error_type) is None
                ):
                    raise ValueError("Metadata errors require a safe category")
                if error_type is None:
                    if http_status is None or not 200 <= http_status < 300:
                        error_type = "HttpError" if http_status is not None else "MissingHttpStatus"
                    else:
                        records = self._records(entity_type, requested, batch)
            except Exception as error:
                error_type = type(error).__name__
                records = {}
            finished_at = self._now()
            missing = [entity_id for entity_id in requested if entity_id not in records]
            if not records and error_type is None:
                error_type = "MissingMetadata"
            status = "failed" if not records else ("partial" if missing else "success")
            fetch_id = str(uuid4())
            fetch = {
                "fetch_id": fetch_id, "source_id": self.source_id, "entity_type": entity_type,
                "requested_ids": requested, "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(), "status": status,
                "http_status": http_status, "error_type": error_type, "response": response,
                "missing_ids": missing,
            }
            snapshots = [{
                "snapshot_id": str(uuid4()), "source_id": self.source_id,
                "entity_type": entity_type, "entity_id": entity_id,
                "fetched_at": finished_at.isoformat(), "record": record,
            } for entity_id, record in records.items()]
            self.store.save_fetch(fetch, snapshots)
            result.fetch_ids.append(fetch_id)
            for snapshot in snapshots:
                saved = self.store.get_snapshot(snapshot["snapshot_id"])
                result.snapshots[snapshot["entity_id"]] = saved
                result.fetched += 1
            result.unresolved.extend(missing)
            if status != "success":
                LOGGER.warning("Metadata fetch outcome: %s (error=%s, missing=%s)", status, error_type, len(missing))
        return result
