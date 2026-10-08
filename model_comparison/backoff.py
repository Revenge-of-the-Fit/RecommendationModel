"""Retry transient API errors (rate limits, timeouts) with exponential backoff."""
import functools
import time


def with_backoff(
    fn, retriable, max_attempts=6, base_delay=2.0, max_delay=60.0, sleep=time.sleep,
    give_up=lambda error: False,
):
    """`give_up(error)` marks errors that can never succeed on retry (e.g. exhausted credit)."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        for attempt in range(1, max_attempts + 1):
            try:
                return fn(*args, **kwargs)
            except retriable as error:
                if attempt == max_attempts or give_up(error):
                    raise
                sleep(min(base_delay * 2 ** (attempt - 1), max_delay))

    return wrapper
