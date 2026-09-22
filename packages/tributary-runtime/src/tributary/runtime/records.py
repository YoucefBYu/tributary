"""Kafka record <-> SDK `Message`, and the reject path for a record that cannot be decoded.

Gate requirement 13 lives here in practice: a record arriving from the network can have a header that
is not UTF-8, a `tb-attempt` of `"1_0"`, or no `tb-msg-id` at all. Every one of those is a **reject**
that produces a DLQ record and a lifecycle event, never an exception that kills the consumer loop.
`decode_record` therefore raises exactly one typed error, and the loop knows what to do with it.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderError, TributaryError

RawHeaders = Sequence[tuple[str, bytes | None]] | None


class RecordDecodeError(TributaryError):
    """A consumed record is not a usable Tributary message. Carries a stable code for the DLQ."""

    def __init__(self, error_code: str, reason: str) -> None:
        super().__init__(f"{error_code}: {reason}")
        self.error_code = error_code
        self.reason = reason


@dataclass(frozen=True, slots=True)
class ConsumedRecord:
    """One Kafka record with its coordinates.

    The coordinates are kept beside the `Message` rather than inside it: a step must be testable with
    no Kafka (invariant 7), and an offset is not part of a message's identity.
    """

    topic: str
    partition: int
    offset: int
    message: Message

    @property
    def msg_id(self) -> str | None:
        """The message id, or `None` if the record has none. Safe to log; the key is not."""
        return self.message.get(h.TB_MSG_ID)


def decode_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    headers: RawHeaders,
) -> ConsumedRecord:
    """Build a `ConsumedRecord`, or raise `RecordDecodeError` with a DLQ-ready code."""
    try:
        decoded = h.decode_headers(headers or [])
    except HeaderError as exc:
        raise RecordDecodeError(
            "HEADER_NOT_TEXT", f"header {exc.name if hasattr(exc, 'name') else ''} is not UTF-8 text"
        ) from exc

    message = Message(headers=decoded, payload=value, key=key)
    _require_usable(message)
    return ConsumedRecord(topic=topic, partition=partition, offset=offset, message=message)


def _require_usable(message: Message) -> None:
    """The minimum a worker needs to route, retry and report a record.

    Deliberately small: a consumer must not reject a record for an unknown header (envelope contract
    1.2), so only what the engine itself reads is checked.
    """
    try:
        _ = (message.msg_id, message.channel, message.channel_version, message.attempt)
    except HeaderError as exc:
        raise RecordDecodeError("ENVELOPE_INVALID", str(exc)) from exc


QUARANTINE_MAGIC = b"TBQ1"


def quarantine_blob(*, key: bytes | None, value: bytes | None, headers: Iterable[tuple[str, bytes | None]]) -> bytes:
    """Frame an undecodable record's own bytes into one opaque blob.

    Envelope contract 5.2 puts the original headers and payload in the quarantine wrapper's **payload**,
    because that is the one part of the envelope that is opaque and already treated as PHI-bearing. It
    does not say how to frame them, so this is a runtime choice, recorded here and reported to the
    architect: a magic, then length-prefixed slots, so a human with a hex viewer can separate the parts
    without the runtime ever decoding them.

    `TBQ1 | u32 key_len | key | u32 header_count | (u32 name_len | name | u32 value_len | value)* | payload`

    A null key or header value is `0xFFFFFFFF`. The original **key** is included even though the
    contract's table names only headers and payload: without it the quarantined record is not the
    record, and the key is already on a topic whose ACLs and retention assume PHI. Flagged for the
    architect rather than decided quietly.
    """
    import struct

    def slot(raw: bytes | None) -> bytes:
        if raw is None:
            return struct.pack(">I", 0xFFFFFFFF)
        return struct.pack(">I", len(raw)) + raw

    collected = list(headers)
    parts = [QUARANTINE_MAGIC, slot(key), struct.pack(">I", len(collected))]
    for name, header_value in collected:
        parts.append(slot(name.encode("utf-8", errors="surrogateescape") if isinstance(name, str) else bytes(name)))
        parts.append(slot(header_value))
    parts.append(value if value is not None else b"")
    return b"".join(parts)


def to_kafka_headers(message: Message) -> list[tuple[str, bytes]]:
    """Encode headers for `confluent_kafka.Producer.produce`."""
    return [(name, value.encode("utf-8")) for name, value in message.headers.items()]


def apply_usr_headers(message: Message, usr: Iterable[tuple[str, str]]) -> Message:
    """Apply the `tb-usr-*` values a step set through `ctx`, validating each one."""
    result = message
    for name, value in usr:
        result = result.with_usr(name, value)
    return result
