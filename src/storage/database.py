import os
import sqlite3
import time
from pathlib import Path


DATABASE_SCHEMA_VERSION = 4
DEFAULT_STORAGE_PATH = Path(__file__).resolve().parents[2] / "data" / "live" / "events.sqlite3"


class StorageError(RuntimeError):
    pass


def open_database(path: Path, busy_timeout: float = 1.0) -> sqlite3.Connection:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(path, timeout=busy_timeout)
    try:
        connection.execute("PRAGMA busy_timeout=0")
        deadline = time.monotonic() + busy_timeout
        while True:
            try:
                mode = connection.execute("PRAGMA journal_mode=WAL").fetchone()[0]
                break
            except sqlite3.OperationalError as error:
                code = getattr(error, "sqlite_errorcode", None)
                busy = code is not None and code & 255 in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED)
                remaining = deadline - time.monotonic()
                if not busy or remaining <= 0:
                    raise
                time.sleep(min(0.01, remaining))
        connection.execute(f"PRAGMA busy_timeout={int(busy_timeout * 1000)}")
        connection.execute("PRAGMA foreign_keys=ON")
        if mode != "wal":
            raise StorageError("Storage requires a file-backed WAL database")
        connection.execute("PRAGMA synchronous=FULL")
        maximum = int(os.environ.get("STORAGE_MAX_BYTES", 16 * 1024**3))
        if maximum <= 0:
            raise ValueError("STORAGE_MAX_BYTES must be positive")
        page_size = connection.execute("PRAGMA page_size").fetchone()[0]
        connection.execute(f"PRAGMA max_page_count={max(1, maximum // page_size)}")
        connection.execute("PRAGMA journal_size_limit=67108864")
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if not 0 <= version <= DATABASE_SCHEMA_VERSION:
            raise StorageError("Unsupported storage schema version")
        if version < DATABASE_SCHEMA_VERSION:
            connection.execute("BEGIN IMMEDIATE")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if not 0 <= version <= DATABASE_SCHEMA_VERSION:
                raise StorageError("Unsupported storage schema version")
            if version == 0:
                connection.execute("""
                    CREATE TABLE recommendation_requests (
                        request_id TEXT PRIMARY KEY NOT NULL,
                        started_at TEXT NOT NULL,
                        user_id INTEGER,
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE INDEX requests_by_user_time
                    ON recommendation_requests(user_id, started_at, request_id)
                """)
            if version < 2:
                connection.execute("""
                    CREATE TABLE kafka_events (
                        source_id TEXT NOT NULL,
                        topic TEXT NOT NULL,
                        partition INTEGER NOT NULL,
                        offset INTEGER NOT NULL,
                        source_fingerprint TEXT NOT NULL,
                        raw_key BLOB,
                        raw_value BLOB,
                        headers_json TEXT NOT NULL,
                        raw_redacted INTEGER NOT NULL,
                        broker_timestamp_ms INTEGER,
                        broker_timestamp_type INTEGER NOT NULL,
                        leader_epoch INTEGER,
                        ingested_at TEXT NOT NULL,
                        event_timestamp TEXT,
                        user_id INTEGER,
                        movie_id TEXT,
                        event_type TEXT,
                        parse_status TEXT NOT NULL,
                        parser_version INTEGER NOT NULL,
                        parsed_json TEXT NOT NULL,
                        PRIMARY KEY(source_id, topic, partition, offset)
                    ) WITHOUT ROWID
                """)
                connection.execute("""
                    CREATE INDEX events_by_user_time
                    ON kafka_events(user_id, event_timestamp, event_type)
                """)
            if version < 3:
                connection.execute("""
                    CREATE TABLE metadata_fetches (
                        fetch_id TEXT PRIMARY KEY NOT NULL,
                        source_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL CHECK(entity_type IN ('user', 'movie')),
                        started_at TEXT NOT NULL,
                        finished_at TEXT NOT NULL,
                        status TEXT NOT NULL CHECK(status IN ('success', 'partial', 'failed')),
                        fingerprint TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE TABLE metadata_snapshots (
                        snapshot_id TEXT PRIMARY KEY NOT NULL,
                        fetch_id TEXT NOT NULL REFERENCES metadata_fetches(fetch_id),
                        source_id TEXT NOT NULL,
                        entity_type TEXT NOT NULL CHECK(entity_type IN ('user', 'movie')),
                        entity_id TEXT NOT NULL,
                        fetched_at TEXT NOT NULL,
                        content_version TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE INDEX metadata_by_entity_time
                    ON metadata_snapshots(source_id, entity_type, entity_id, fetched_at, snapshot_id)
                """)
            if version < 4:
                connection.execute("""
                    CREATE TABLE llm_attempts (
                        attempt_id TEXT PRIMARY KEY NOT NULL,
                        cache_key TEXT NOT NULL,
                        started_at TEXT NOT NULL,
                        finished_at TEXT,
                        status TEXT NOT NULL CHECK(status IN ('pending', 'responded', 'success', 'failed')),
                        user_id INTEGER,
                        source_snapshot_id TEXT REFERENCES metadata_snapshots(snapshot_id),
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE TABLE preference_profiles (
                        profile_id TEXT PRIMARY KEY NOT NULL,
                        cache_key TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        attempt_id TEXT REFERENCES llm_attempts(attempt_id),
                        content_version TEXT NOT NULL,
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE INDEX profiles_by_cache_time
                    ON preference_profiles(cache_key, created_at, profile_id)
                """)
                connection.execute("""
                    CREATE TABLE profile_uses (
                        use_id TEXT PRIMARY KEY NOT NULL,
                        profile_id TEXT NOT NULL REFERENCES preference_profiles(profile_id),
                        used_at TEXT NOT NULL,
                        user_id INTEGER,
                        source_snapshot_id TEXT REFERENCES metadata_snapshots(snapshot_id),
                        record_json TEXT NOT NULL
                    )
                """)
                connection.execute("""
                    CREATE INDEX profile_uses_by_user_time
                    ON profile_uses(user_id, used_at, use_id)
                """)
            connection.execute(f"PRAGMA user_version={DATABASE_SCHEMA_VERSION}")
            connection.commit()
        return connection
    except Exception:
        connection.rollback()
        connection.close()
        raise
