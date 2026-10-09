import argparse
import json
import logging
import math
import os
from contextlib import nullcontext
from pathlib import Path

from services.metadata import MetadataCollector, MetadataRateLimiter
from services.metadata_http import MetadataHttpFetcher
from storage.database import DEFAULT_STORAGE_PATH
from storage.metadata import MetadataStore


LOGGER = logging.getLogger(__name__)


def positive_int(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError("The value must be positive")
    return number


def positive_float(value):
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise argparse.ArgumentTypeError("The value must be positive and finite")
    return number


def nonnegative_float(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError("The value must be nonnegative and finite")
    return number


def _offline_fetch(*_):
    raise RuntimeError("Offline metadata collection cannot fetch records")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Cache timestamped user and movie records from the course API.")
    parser.add_argument("--base-url", default=os.environ.get("METADATA_API_BASE_URL"))
    parser.add_argument("--source-id", default=os.environ.get("METADATA_SOURCE_ID", "cmu-metadata-api"))
    parser.add_argument("--entity-type", choices=("user", "movie"), required=True)
    parser.add_argument("--ids", nargs="+", required=True)
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--busy-timeout", type=positive_float, default=os.environ.get("STORAGE_BUSY_TIMEOUT", "1"))
    parser.add_argument("--batch-size", type=positive_int, default=200)
    parser.add_argument("--min-call-interval", type=nonnegative_float, default=1.0)
    parser.add_argument("--timeout", type=positive_float, default=10.0)
    parser.add_argument("--max-response-bytes", type=positive_int, default=16 * 1024 * 1024)
    parser.add_argument("--max-age-seconds", type=positive_float, default=86400.0)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--force", action="store_true")
    mode.add_argument("--offline", action="store_true")
    arguments = parser.parse_args(argv)
    if not arguments.offline and not arguments.base_url:
        parser.error("Set --base-url or METADATA_API_BASE_URL for online collection")
    if arguments.batch_size > 200:
        parser.error("Metadata batches cannot exceed 200 IDs")
    logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
    try:
        client = nullcontext(_offline_fetch) if arguments.offline else MetadataHttpFetcher(
            arguments.base_url, timeout=arguments.timeout,
            max_response_bytes=arguments.max_response_bytes,
        )
        with client as fetcher, MetadataStore(arguments.storage_path, arguments.busy_timeout) as store:
            collector = MetadataCollector(
                store, arguments.source_id, fetcher,
                batch_size=arguments.batch_size, max_age_seconds=arguments.max_age_seconds,
                limiter=MetadataRateLimiter(arguments.min_call_interval),
            )
            result = collector.collect(
                arguments.entity_type, arguments.ids,
                force=arguments.force, offline=arguments.offline,
            )
    except Exception as error:
        LOGGER.error("Metadata collection stopped (%s)", type(error).__name__)
        return 1
    print(json.dumps({
        "entity_type": arguments.entity_type,
        "cached": result.cached, "fetched": result.fetched,
        "unresolved": result.unresolved, "fetch_ids": result.fetch_ids,
        "snapshots": {
            entity_id: {name: snapshot[name] for name in ("snapshot_id", "content_version")}
            for entity_id, snapshot in result.snapshots.items()
        },
    }, sort_keys=True))
    return 1 if result.unresolved else 0


if __name__ == "__main__":
    raise SystemExit(main())
