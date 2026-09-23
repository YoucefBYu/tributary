"""Kafka record <-> SDK `Message`, and the reject path for a record that cannot be decoded.

Gate requirement 13 lives here in practice: a record arriving from the network can have a header that
is not UTF-8, a `tb-attempt` of `"1_0"`, or no `tb-msg-id` at all. Every one of those is a **reject**
that produces a DLQ record and a lifecycle event, never an exception that kills the consumer loop.
`decode_record` therefore raises exactly one typed error, and the loop knows what to do with it.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime

from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderError, TributaryError
from tributary.sdk.ids import new_traceparent

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


#: The contract code for the ordinary reject path (envelope 2.3.1): one malformed header on a record
#: whose identity is intact. Distinct from `RECORD_UNDECODABLE` on purpose — see that constant.
HEADER_PARSE_ERROR = "HEADER_PARSE_ERROR"


@dataclass(frozen=True, slots=True)
class DecodeContext:
    """What the consumer knows independently of the record, and therefore what a reject normalises to.

    Every value here comes from the subscription, the applied spec or this component — never from the
    record, whose headers are the thing under suspicion (envelope 2.3.1).
    """

    channel: str
    channel_version: int
    source: str


class RecordRejectError(TributaryError):
    """One or more registered headers were malformed on a record whose identity is intact.

    Carries the **normalised** record: the offending headers replaced by the contract's placeholders,
    or dropped when optional. The bad values are not preserved anywhere in the envelope — the original
    record is still on the topic it was read from at a known offset, so a reject needs the record to
    stay findable, not copied (envelope 2.3.1). `bad_headers` names them for the event, which is the
    only place the silence is broken; header names are ours, values are the sender's.
    """

    def __init__(self, record: ConsumedRecord, bad_headers: tuple[str, ...]) -> None:
        super().__init__(f"{HEADER_PARSE_ERROR}: {', '.join(bad_headers)}")
        self.record = record
        self.bad_headers = bad_headers
        self.error_code = HEADER_PARSE_ERROR


def decode_record(
    *,
    topic: str,
    partition: int,
    offset: int,
    key: bytes | None,
    value: bytes | None,
    headers: RawHeaders,
    context: DecodeContext | None = None,
) -> ConsumedRecord:
    """Build a `ConsumedRecord`, or raise the right kind of reject (envelope 2.3.1).

    * `RecordDecodeError` (`RECORD_UNDECODABLE`) — nothing about the record is trustworthy: the headers
      are not text, or `tb-msg-id` is absent or not a valid UUIDv7, or `tb-claim-check` is malformed.
      The caller quarantines it.
    * `RecordRejectError` (`HEADER_PARSE_ERROR`) — identity is intact and some other registered header
      is malformed. The caller dead-letters the normalised record it carries.

    Without a `context` the normalising branch cannot run (it has no channel or version to normalise to),
    so every malformed header is treated as undecodable. Callers in the data plane always pass one.
    """
    try:
        decoded = h.decode_headers(headers or [])
    except HeaderError as exc:
        raise RecordDecodeError(
            "HEADER_NOT_TEXT", f"header {exc.name if hasattr(exc, 'name') else ''} is not UTF-8 text"
        ) from exc

    message = Message(headers=decoded, payload=value, key=key)

    # Identity first: it is the discriminator between the two shapes.
    if not h.is_well_formed(h.TB_MSG_ID, decoded.get(h.TB_MSG_ID, "")):
        raise RecordDecodeError("ENVELOPE_INVALID", "tb-msg-id is absent or not a valid UUIDv7")

    # Then the claim check, which quarantines whatever the identity says: the payload lives in the
    # object store and the pointer to it is the broken part, so a normalised record would be perfectly
    # conformant and its bytes unfindable — silent loss that passes every other check.
    claim_check = decoded.get(h.TB_CLAIM_CHECK)
    if claim_check is not None and not h.is_well_formed(h.TB_CLAIM_CHECK, claim_check):
        raise RecordDecodeError("CLAIM_CHECK_INVALID", "tb-claim-check is malformed; the payload is unfindable")

    bad = _malformed_headers(decoded)
    if not bad:
        return ConsumedRecord(topic=topic, partition=partition, offset=offset, message=message)

    if context is None:
        raise RecordDecodeError("ENVELOPE_INVALID", "a registered header is malformed and no decode context was given")

    normalised = _normalise(message, bad, context)
    raise RecordRejectError(ConsumedRecord(topic=topic, partition=partition, offset=offset, message=normalised), bad)


#: Required headers and the placeholder a reject substitutes, per the table in envelope 2.3.1. The
#: placeholder is a function of the decode context, never of the record.
_PLACEHOLDERS: tuple[tuple[str, Callable[[DecodeContext], str]], ...] = (
    (h.TB_CHANNEL, lambda ctx: ctx.channel),
    (h.TB_CHANNEL_VERSION, lambda ctx: str(ctx.channel_version)),
    (h.TB_SOURCE, lambda ctx: ctx.source),
    (h.TB_RECEIVED_AT, lambda _ctx: h.format_timestamp(datetime.now(UTC))),
    (h.TB_CONTENT_TYPE, lambda _ctx: "application/octet-stream"),
    (h.TB_ATTEMPT, lambda _ctx: "0"),
    (h.TRACEPARENT, lambda _ctx: new_traceparent()),
)

#: Optional headers a reject drops rather than replaces: there is no honest placeholder for a
#: correlation id or a destination, and a wrong one is worse than an absent one.
_DROPPED: tuple[str, ...] = (
    h.TB_CORRELATION_ID,
    h.TB_PARENT_ID,
    h.TB_DESTINATION,
    h.TB_RETRY_NOT_BEFORE,
    h.TB_STAGE,
    h.TB_ERROR_CODE,
)


def _malformed_headers(decoded: Mapping[str, str]) -> tuple[str, ...]:
    """Registered headers that are absent-but-required or present-and-malformed.

    Unregistered names are never checked: a consumer must ignore what it does not recognise (1.2), which
    is also why only the closed set from section 3 can trigger a reject — so naming them in an event is
    safe where quoting a value would not be.
    """
    bad: list[str] = []
    for name, _placeholder in _PLACEHOLDERS:
        value = decoded.get(name)
        if value is None or not h.is_well_formed(name, value):
            bad.append(name)
    bad.extend(name for name in _DROPPED if name in decoded and not h.is_well_formed(name, decoded[name]))
    return tuple(bad)


def _normalise(message: Message, bad: tuple[str, ...], context: DecodeContext) -> Message:
    """Replace each malformed required header with its placeholder and drop each malformed optional one."""
    result = message
    for name, placeholder in _PLACEHOLDERS:
        if name in bad:
            result = result.with_header(name, placeholder(context))
    for name in _DROPPED:
        if name in bad:
            result = result.without_header(name)
    return result


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
