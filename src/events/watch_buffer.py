import math
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime

from storage.database import watch_order


@dataclass
class _Session:
    envelope: object
    parsed: dict
    order: str
    last_seen: float
    event_ms: float | None
    dirty: bool = False
    blocked: dict = field(default_factory=dict)


class WatchBuffer:
    def __init__(self, idle_seconds=300, max_sessions=50000):
        if not math.isfinite(idle_seconds) or idle_seconds <= 0:
            raise ValueError("Watch inactivity must be positive and finite")
        if not isinstance(max_sessions, int) or isinstance(max_sessions, bool) or max_sessions <= 0:
            raise ValueError("The watch session limit must be a positive integer")
        self.idle_seconds = idle_seconds
        self.max_sessions = max_sessions
        self.sessions = OrderedDict()
        self.coalesced = 0

    def __len__(self):
        return len(self.sessions)

    @property
    def active_count(self):
        return len(self.sessions)

    @property
    def buffered_count(self):
        return sum(session.dirty for session in self.sessions.values())

    @staticmethod
    def _event_ms(envelope, parsed):
        stamp = parsed.get("event_timestamp")
        return datetime.fromisoformat(stamp).timestamp() * 1000 if stamp else envelope.broker_timestamp_ms

    def _take(self, key):
        session = self.sessions.pop(key)
        return [(session.envelope, session.parsed)] if session.dirty else []

    def process(self, envelope, parsed, now, sequence):
        if (parsed.get("event_type") != "watch" or parsed.get("parse_status") != "parsed"
                or parsed.get("user_id") is None or parsed.get("movie_id") is None):
            return [(envelope, parsed)]
        key = (envelope.source_id, envelope.topic, parsed["user_id"], parsed["movie_id"])
        event_ms = self._event_ms(envelope, parsed)
        order = watch_order(parsed.get("event_timestamp"), envelope.broker_timestamp_ms,
                            envelope.partition, envelope.offset)
        ready = []
        session = self.sessions.get(key)
        if session is not None and (now - session.last_seen >= self.idle_seconds or (
                event_ms is not None and session.event_ms is not None
                and event_ms - session.event_ms >= self.idle_seconds * 1000)):
            ready.extend(self._take(key))
            session = None
        if session is None:
            if len(self.sessions) >= self.max_sessions:
                ready.extend(self._take(next(iter(self.sessions))))
            self.sessions[key] = _Session(envelope, parsed, order, now, event_ms)
            ready.append((envelope, parsed))
            return ready
        self.coalesced += int(session.dirty)
        session.dirty = True
        session.last_seen = now
        if event_ms is not None:
            session.event_ms = event_ms if session.event_ms is None else max(session.event_ms, event_ms)
        if order > session.order:
            session.envelope, session.parsed, session.order = envelope, parsed, order
        position = (envelope.topic, envelope.partition)
        previous = session.blocked.get(position)
        if previous is None or envelope.offset < previous[0]:
            session.blocked[position] = (envelope.offset, sequence)
        self.sessions.move_to_end(key)
        return ready

    def blocked_positions(self):
        positions = {}
        for session in self.sessions.values():
            for partition, blocked in session.blocked.items():
                if partition not in positions or blocked[0] < positions[partition][0]:
                    positions[partition] = blocked
        return positions

    def expire(self, now, event_watermark_ms=None):
        keys = [key for key, session in self.sessions.items()
                if now - session.last_seen >= self.idle_seconds or (
                    event_watermark_ms is not None and session.event_ms is not None
                    and event_watermark_ms - session.event_ms >= self.idle_seconds * 1000)]
        return [record for key in keys for record in self._take(key)]

    def flush(self):
        return [record for key in list(self.sessions) for record in self._take(key)]
