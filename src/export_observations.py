import argparse
import json
import os
import sys
from pathlib import Path

from manage_storage import add_export_filters
from storage.database import DEFAULT_STORAGE_PATH, StorageError
from storage.operations import export_observations


def build_parser():
    parser = argparse.ArgumentParser(
        description="Stream recommendation impressions, latest watch positions, ratings, and possible later matches as JSONL.",
        epilog="Matches are candidates based only on the same user/movie and a later timestamp within the window. Multiple impressions remain ambiguous; even one candidate does not prove attribution. Watch positions are never play counts or proof that all earlier minutes were watched. Only the latest watch per user/movie is retained; earlier watch history cannot be reconstructed. Start is inclusive and end exclusive for impressions and observed events. As-of filters ingestion/availability to prevent later data leaking into an earlier view. Source/topic filters select Kafka data; local API impressions remain available as candidates. Raw unknown or malformed records remain recoverable through manage_storage export.",
    )
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--output", type=Path, help="New JSONL path; omit to stream to stdout")
    parser.add_argument("--match-window-seconds", type=float, default=86400, help="Maximum elapsed time after an impression (default: 86400)")
    add_export_filters(parser)
    return parser


def main(argv=None):
    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        result = export_observations(
            arguments.storage_path, arguments.output or sys.stdout, source_id=arguments.source_id,
            topic=arguments.topic, start=arguments.start, end=arguments.end, as_of=arguments.as_of,
            match_window_seconds=arguments.match_window_seconds,
        )
        print(json.dumps(result, sort_keys=True), file=sys.stderr if arguments.output is None else sys.stdout)
        return 0
    except ValueError as error:
        parser.error(str(error))
    except (StorageError, OSError):
        print("Observation export failed; inspect storage and destination paths.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
