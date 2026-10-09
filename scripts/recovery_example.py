import argparse
import json
import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from events.parser import parse_event
from storage.database import StorageError
from storage.events import EventStore, KafkaEnvelope
from storage.operations import backup_database, export_observations, export_records, restore_backup
from storage.requests import RequestStore


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Run a local recovery/export example using synthetic impressions and watch/rating observations.",
        epilog="Example: python scripts/recovery_example.py --directory data/recovery-example. The directory must be new. The workflow creates synthetic records, verifies a SQLite gzip backup with its matching manifest, restores into a new recovered.sqlite3, and replays the same Kafka source/topic/partition/offset keys with zero inserted rows. It then exports raw-records.jsonl and observations.jsonl. Keep the archive and manifest together for restoration; retention rotates archived backups only. This creates no Kafka connection. Two impressions contain the same movie, so every later observation has two candidate matches. Even one match would not prove attribution without a shared request ID. Two movie-minute requests remain two viewing observations, never two movie plays.",
    )
    parser.add_argument("--directory", type=Path, required=True)
    arguments = parser.parse_args(argv)
    directory = arguments.directory
    try:
        directory.mkdir(parents=True, exist_ok=False)
        original = directory / "original.sqlite3"
        restored = directory / "recovered.sqlite3"
        with RequestStore(original) as store:
            for number, minute in enumerate(("00", "30"), 1):
                store.save_request({
                    "request_id": f"example-request-{number}", "user_id": 42,
                    "started_at": f"2026-10-09T12:00:{minute}+00:00",
                    "finished_at": f"2026-10-09T12:00:{minute}.100000+00:00",
                    "status": 200, "response_complete": True,
                    "recommendations": [{"movie_id": "example+movie", "rank": 1, "score": 0.8}],
                    "response_body": "example+movie", "serving_method": "popularity",
                    "versions": {"code": "example-code", "model": "example-model"},
                })
        events = [
            KafkaEnvelope(
                source_id="recovery-example", topic="movielog2", partition=0, offset=number,
                value=f"2026-10-09T12:0{number}:00+00:00,42,{body}".encode(),
                ingested_at=f"2026-10-09T12:0{number}:01+00:00",
            )
            for number, body in enumerate((
                "GET /data/m/example+movie/17.mpg",
                "GET /data/m/example+movie/18.mpg", "GET /rate/example+movie=9",
            ), 1)
        ]
        with EventStore(original) as store:
            for event in events:
                store.save_event(event, parse_event(event.value))
        archive = backup_database(original, directory / "archives")
        restore_backup(archive["archive_path"], restored)
        with EventStore(restored) as store:
            if any(store.save_event(event, parse_event(event.value)) for event in events):
                raise StorageError("Recovered offsets must remain deduplicated")
        raw_path = directory / "raw-records.jsonl"
        matched_path = directory / "observations.jsonl"
        exported = export_records(restored, raw_path)
        matched = export_observations(restored, matched_path)
        print(json.dumps({
            "status": "passed", "replay_added_rows": 0, "exported_rows": exported,
            **matched, "matching": "candidate_only", "shared_request_id": False,
            "watch_observation_unit": "movie_minute", "watch_observations": 2,
            "raw_export": str(raw_path), "observation_export": str(matched_path),
        }, sort_keys=True))
        return 0
    except (OSError, ValueError, StorageError) as error:
        print(json.dumps({"status": "failed", "error_type": type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
