"""Lossless joint encoding of Kafka raw bytes and the exact retained parser JSON.

The JSON-column sentinel distinguishes compressed records from arbitrary source
bytes. All indexed columns and source fingerprints keep their original values.
"""
import json
import struct
import zlib

SENTINEL = "@MLIP:event:1"
MAGIC = b"MLIPE1\0"
NULL_RAW = 0xFFFFFFFF
MAX_DECODED_BYTES = 64 * 1024**2


def pack_event(raw, parsed_json):
    if raw is not None and not isinstance(raw, bytes):
        raise TypeError("Kafka source payloads must be bytes or null")
    parsed = parsed_json.encode("utf-8")
    raw_bytes = raw or b""
    if len(raw_bytes) + len(parsed) > MAX_DECODED_BYTES:
        raise ValueError("Kafka audit payload exceeds the supported 64 MiB limit")
    content = struct.pack(">I", NULL_RAW if raw is None else len(raw)) + raw_bytes + parsed
    packed = MAGIC + zlib.compress(content, 3)
    # Conversion is deliberately checked before either representation is persisted.
    if unpack_event(packed, SENTINEL) != (raw, parsed_json):
        raise ValueError("Kafka payload round trip failed")
    return packed, SENTINEL


def unpack_event(raw, parsed_json):
    if parsed_json != SENTINEL:
        if not isinstance(parsed_json, str) or not parsed_json.lstrip().startswith("{"):
            raise ValueError("Unrecognized Kafka parser payload format")
        return raw, parsed_json
    if not isinstance(raw, bytes) or not raw.startswith(MAGIC):
        raise ValueError("Invalid joint Kafka payload header")
    try:
        decoder = zlib.decompressobj()
        content = decoder.decompress(raw[len(MAGIC):], MAX_DECODED_BYTES + 5)
        if (len(content) < 4 or len(content) > MAX_DECODED_BYTES + 4
                or not decoder.eof or decoder.unused_data or decoder.unconsumed_tail):
            raise ValueError("Invalid joint Kafka payload length")
        length = struct.unpack(">I", content[:4])[0]
        end = 4 if length == NULL_RAW else 4 + length
        if end > len(content):
            raise ValueError("Truncated joint Kafka source payload")
        original = None if length == NULL_RAW else content[4:end]
        parsed = content[end:].decode("utf-8")
        if not parsed.lstrip().startswith("{"):
            raise ValueError("Invalid retained parser JSON")
        return original, parsed
    except (zlib.error, UnicodeError, struct.error) as error:
        raise ValueError("Corrupt joint Kafka payload") from error


def parsed_event(row):
    return json.loads(unpack_event(row["raw_value"], row["parsed_json"])[1])


def decoded_event_row(row):
    decoded = dict(row)
    decoded["raw_value"], decoded["parsed_json"] = unpack_event(row["raw_value"], row["parsed_json"])
    return decoded
