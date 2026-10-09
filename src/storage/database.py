import sqlite3
import time
from pathlib import Path


DATABASE_SCHEMA_VERSION = 2
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
        if mode != "wal":
            raise StorageError("Storage requires a file-backed WAL database")
        connection.execute("PRAGMA synchronous=FULL")
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
            connection.execute(f"PRAGMA user_version={DATABASE_SCHEMA_VERSION}")
            connection.commit()
        return connection
    except Exception:
        connection.rollback()
        connection.close()
        raise
