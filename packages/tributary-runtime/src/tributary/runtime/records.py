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


#: The contract code for the quarantine path (envelope 5.2). Deliberately not `HEADER_PARSE_ERROR`:
#: that one means a single malformed header on a record that could still be identified, which keeps its
#: own `tb-msg-id` and reading stage. `RECORD_UNDECODABLE` means nothing about the record is
#: trustworthy. A rise in the first points at one producer's field; a rise in the second at framing,
#: corruption, or someone else writing to the topic. Collapsing them hides the second behind the first.
RECORD_UNDECODABLE = "RECORD_UNDECODABLE"


class RecordDecodeError(TributaryError):
    """A consumed record is not a usable Tributary message.

    `error_code` is what reaches the wrapper header and the event: always `RECORD_UNDECODABLE`, because
    the envelope schema keys its quarantine rules on it. `reason_code` is a finer code from this
    module's own vocabulary, for the log line only - a fixed token, never anything from the record.
    """

    def __init__(self, reason_code: str, reason: str) -> None:
        super().__init__(f"{RECORD_UNDECODABLE}/{reason_code}: {reason}")
        self.error_code = RECORD_UNDECODABLE
        self.reason_code = reason_code
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

    Lengths are unsigned 32-bit **big-endian** and `0xFFFFFFFF` means null, so a null key or value is
    distinguishable from a zero-length one. The magic carries the format version: a future framing is
    `TBQ2` and tells itself apart on sight.

    The original **key** lives in the blob and nowhere else. It must not become the wrapper's key,
    which is the minted `tb-msg-id`, and must not be lifted into a header, an `attrs` entry or a log
    line (envelope 5.2).

    Nothing in the message flow parses this. Workers and loaders write it and never read it; the
    archive, the browser and the CLI parse it on an explicit operator action. A component that parsed
    it to auto-retry a quarantined record would feed attacker-controlled bytes back into the pipeline.

    **Fidelity limit:** `confluent_kafka` hands header names to Python as `str`, so a name that was not
    valid UTF-8 on the wire cannot be preserved byte for byte; it is re-encoded with `surrogateescape`,
    which round-trips whatever the client decoded. Values are untouched bytes.
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
