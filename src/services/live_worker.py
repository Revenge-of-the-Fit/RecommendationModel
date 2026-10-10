import logging
import json
import threading
from contextlib import nullcontext
from datetime import datetime, timezone

import preferences
from services.metadata import MetadataCollector, MetadataRateLimiter
from services.metadata_http import MetadataHttpFetcher
from storage.live import LiveStore, merge_history, read_live_user
from storage.metadata import MetadataStore
from storage.profiles import content_version


LOGGER = logging.getLogger(__name__)
METADATA_SOURCE_ID = "cmu-metadata-api"
TRANSIENT_ERRORS = {
    "TransportError", "RequestTimeout", "TimeoutError", "ConnectionError",
    "ConnectTimeout", "ReadTimeout", "WriteTimeout", "PoolTimeout",
    "APIConnectionError", "APITimeoutError", "MissingMetadata",
}


def transient_failure(error_type, http_status=None):
    return error_type in TRANSIENT_ERRORS or (
        type(http_status) is int and (
            http_status in (404, 408, 409, 425, 429) or 500 <= http_status < 600
        )
    )


class LiveProfileWorker:
    def __init__(self, settings, cold_start, *, clock=None, poll_seconds=1.0, fetcher=None):
        self.settings = settings
        self.interpreter = cold_start.interpreter
        self.model = cold_start.model
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.poll_seconds = poll_seconds
        self.fetcher = fetcher
        self.limiter = MetadataRateLimiter()
        self.interpretation_version = content_version({
            "model": preferences.PROFILE_CACHE_NAMESPACE, "instructions": preferences.INSTRUCTIONS,
            "schema": self.interpreter._response_schema(),
        })
        self.stop_event = threading.Event()
        self.thread = None
        self.version_refreshed = False

    def _now(self):
        value = self.clock()
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError("The live profile clock requires a timezone")
        return value.astimezone(timezone.utc)

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._run, name="live-profiles", daemon=True)
            self.thread.start()
        return self

    def close(self, timeout=5.0):
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join(timeout)
        return self.thread is None or not self.thread.is_alive()

    def _run(self):
        while not self.stop_event.is_set():
            try:
                self.run_once()
            except Exception as error:
                LOGGER.error("Live profile worker iteration failed (%s)", type(error).__name__)
            self.stop_event.wait(self.poll_seconds)

    def _fail(self, store, user_id, category, transient):
        store.fail(
            user_id, category, transient, self.settings.profile_refresh_seconds,
            now=self._now().timestamp(),
        )
        LOGGER.warning("Live profile preparation failed (%s)", category)

    def run_once(self, fetcher=None):
        with LiveStore(self.settings.storage_path) as live:
            live.ingest(self.settings.live_source_id, self.settings.live_topic, limit=1000)
            if not self.version_refreshed:
                live.refresh_version(self.interpretation_version)
                self.version_refreshed = True
            pending = live.due_users(
                self._now().timestamp(), limit=200, interpretation_version=self.interpretation_version,
            )
            if not pending or self.stop_event.is_set():
                return
            try:
                fetcher = fetcher if fetcher is not None else self.fetcher
                client = nullcontext(fetcher) if fetcher is not None else MetadataHttpFetcher(
                    self.settings.metadata_base_url,
                )
            except (ValueError, TypeError) as error:
                for job in pending:
                    self._fail(live, job["user_id"], type(error).__name__, False)
                return
            with client as fetch, MetadataStore(
                self.settings.storage_path, self.settings.storage_busy_timeout,
            ) as metadata:
                collector = MetadataCollector(
                    metadata, METADATA_SOURCE_ID, fetch,
                    max_age_seconds=self.settings.profile_refresh_seconds,
                    batch_size=200, limiter=self.limiter, clock=self._now,
                )
                collection = collector.collect("user", [job["user_id"] for job in pending])
                failures = {}
                for fetch_id in collection.fetch_ids:
                    attempt = metadata.get_fetch(fetch_id)
                    category = attempt["error_type"] or "MissingMetadata"
                    for user_id in attempt["missing_ids"]:
                        failures[user_id] = (
                            category, transient_failure(category, attempt["http_status"]),
                        )
                profile_calls = 0
                for job in pending:
                    if self.stop_event.is_set() or profile_calls >= 5:
                        break
                    user_id = job["user_id"]
                    snapshot = collection.snapshots.get(str(user_id))
                    if snapshot is None:
                        category, transient = failures.get(str(user_id), ("MissingMetadata", True))
                        self._fail(live, user_id, category, transient)
                        continue
                    try:
                        fields = snapshot["record"]
                        likes = fields.get("self_description_likes")
                        dislikes = fields.get("self_description_dislikes")
                        likes = "" if likes is None else likes
                        dislikes = "" if dislikes is None else dislikes
                        if not isinstance(likes, str) or not isinstance(dislikes, str):
                            raise ValueError("User descriptions must be strings or null")
                        profile = None
                        if likes.strip() or dislikes.strip():
                            previous = json.loads(job["record_json"]) if job["record_json"] else {}
                            history = read_live_user(
                                self.settings.storage_path, user_id,
                                self.settings.live_source_id, self.settings.live_topic,
                            )
                            positive, _ = merge_history(self.model, user_id, history) if self.model is not None else (set(), set())
                            if not positive or previous.get("profile") is not None:
                                profile_calls += 1
                                profile, _ = self.interpreter.get_profile_record(
                                    likes, dislikes, offline=False,
                                    context={"user_id": user_id, "source_snapshot_id": snapshot["snapshot_id"]},
                                )
                        live.complete(user_id, {
                            "snapshot_id": snapshot["snapshot_id"],
                            "metadata_version": snapshot["content_version"],
                            "likes": likes, "dislikes": dislikes, "profile": profile,
                        }, self.interpretation_version, self.settings.profile_refresh_seconds,
                            now=self._now().timestamp())
                    except Exception as error:
                        details = getattr(error, "provenance", {}) or {}
                        category = type(error).__name__
                        transient = transient_failure(
                            details.get("provider_error_type", category), details.get("http_status"),
                        )
                        self._fail(live, user_id, category, transient)
