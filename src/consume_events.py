import argparse
import json
import logging
import math
import os
import signal
import threading
from pathlib import Path

from confluent_kafka import Consumer

from events.consumer import SafeKafkaLogger, load_kafka_config, run_consumer
from storage.database import DEFAULT_STORAGE_PATH
from storage.events import EventStore


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


def main() -> int:
    config_home = Path(os.environ.get("XDG_CONFIG_HOME", Path.home() / ".config"))
    parser = argparse.ArgumentParser(description="Persist the course Kafka stream before acknowledging offsets.")
    parser.add_argument("--config", type=Path, default=os.environ.get("KAFKA_CONFIG", config_home / "mlip-kafka.conf"))
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--source-id", required=True, help="Stable identifier for the source cluster, independent of tunnel address.")
    parser.add_argument("--topic", default="movielog2")
    parser.add_argument("--storage-path", type=Path, default=os.environ.get("STORAGE_PATH", DEFAULT_STORAGE_PATH))
    parser.add_argument("--busy-timeout", type=positive_float, default=os.environ.get("STORAGE_BUSY_TIMEOUT", "1"))
    parser.add_argument("--offset-reset", choices=("earliest", "latest", "error"), default="earliest")
    parser.add_argument("--event-timezone", help="Timezone for timestamps without an offset; unset preserves them as timezone_missing.")
    parser.add_argument("--max-messages", type=positive_int)
    parser.add_argument("--idle-timeout", type=positive_float)
    parser.add_argument("--replay-from-start", action="store_true", help="Re-read retained records; use a separate group for validation.")
    arguments = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    stopped = threading.Event()
    previous_handlers = {}
    for name in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, name, None)
        if signum is not None:
            previous_handlers[signum] = signal.signal(signum, lambda *_: stopped.set())
    try:
        config = load_kafka_config(arguments.config, arguments.group_id, arguments.offset_reset)
        with EventStore(arguments.storage_path, arguments.busy_timeout) as store:
            consumer = Consumer(config, logger=SafeKafkaLogger())
            stats = run_consumer(
                consumer, store, arguments.source_id, arguments.topic,
                event_timezone=arguments.event_timezone, max_messages=arguments.max_messages,
                idle_timeout=arguments.idle_timeout, replay_from_start=arguments.replay_from_start,
                stop_event=stopped,
            )
        print(json.dumps(stats, sort_keys=True))
        return 0 if stats["received"] or arguments.max_messages is None or stopped.is_set() else 1
    except Exception as error:
        LOGGER.error("Kafka ingestion stopped (%s)", type(error).__name__)
        return 1
    finally:
        for signum, previous in previous_handlers.items():
            signal.signal(signum, previous)


if __name__ == "__main__":
    raise SystemExit(main())
