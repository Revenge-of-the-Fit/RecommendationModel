"""Coordinate profile preparation across API processes sharing the event volume."""
import errno
import os
from contextlib import contextmanager
from pathlib import Path


@contextmanager
def preparation_lock(database_path):
    path = Path(str(database_path) + ".profiles.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as handle:
        if os.name == "nt":
            import msvcrt
            if path.stat().st_size == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            acquire = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            release = lambda: msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            acquire = lambda: fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            release = lambda: fcntl.flock(handle, fcntl.LOCK_UN)
        try:
            acquire()
        except OSError as error:
            if error.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            yield False
        else:
            try:
                yield True
            finally:
                handle.seek(0)
                release()
