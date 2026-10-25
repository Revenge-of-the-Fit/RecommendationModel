"""Lossless compression for large audit JSON; legacy TEXT remains readable."""

import json
import zlib


PREFIX = b"MLIPZ1\0"


def pack_json(text):
    raw = text.encode("utf-8")
    if len(raw) >= 1024:
        compressed = PREFIX + zlib.compress(raw, 3)
        if len(compressed) + 64 < len(raw):
            return compressed
    return text


def unpack_json(value):
    if isinstance(value, bytes) and value.startswith(PREFIX):
        try:
            return zlib.decompress(value[len(PREFIX):]).decode("utf-8")
        except (zlib.error, UnicodeError):
            raise ValueError("Invalid compressed audit JSON") from None
    return value


def load_json(value):
    return json.loads(unpack_json(value))
