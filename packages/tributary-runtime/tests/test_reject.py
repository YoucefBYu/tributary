"""Envelope 2.3.1: the two reject shapes, and a test per placeholder row.

The architect's note is the reason this file exists: **the rule is not schema-enforceable.** A
normalised reject record is structurally identical to an ordinary `dlq` record — that is exactly what
normalising is for — so the conformance suite cannot catch a wrong placeholder. Every row of the
placeholder table therefore gets a unit test, and every branch of the discriminator gets one.

The three-way split under test:

* `tb-msg-id` present, decodable, valid UUIDv7 -> **ordinary reject**, identity kept, `HEADER_PARSE_ERROR`
* `tb-msg-id` absent, undecodable or not UUIDv7 -> **quarantine**, `RECORD_UNDECODABLE`
* `tb-claim-check` malformed, whatever `tb-msg-id` says -> **quarantine**: the payload is elsewhere and
  the pointer is the broken part, so a normalised record would be conformant and its bytes unfindable
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.records import (
    HEADER_PARSE_ERROR,
    RECORD_UNDECODABLE,
    ConsumedRecord,
    DecodeContext,
    RecordDecodeError,
    RecordRejectError,
    decode_record,
    to_kafka_headers,
)
from tributary.runtime.spec import RetryPolicy
from tributary.runtime.worker import dlq_message
from tributary.sdk import headers as h
from tributary.sdk.projection import record_projection
from tributary.sdk.testing import make_message

from .fakes import FakeConsumer, FakeRecord

ENVELOPE_SCHEMA = Path(__file__).resolve().parents[3] / "contracts" / "envelope" / "v0" / "envelope.schema.json"
TOPIC = "dev.adt-passthrough.raw"
CONTEXT = DecodeContext(channel="adt-passthrough", channel_version=7, source="worker:pipeline-1@worker-4")

#: A value that is malformed for every header in the table: not a UUID, not an integer, not a
#: timestamp, not a traceparent, not a topic-safe name, not a media type.
MALFORMED = "!! not a valid value !!"


def raw_headers(**overrides: str) -> list[tuple[str, bytes]]:
    """Contract-shaped headers from the real builder, with specific ones corrupted."""
    message = make_message(b"synthetic", key=b"SYNTH-KEY-1", channel="adt-passthrough")
    headers = dict(message.headers)
    headers.update(overrides)
    return [(name, value.encode("utf-8")) for name, value in headers.items()]


def decode(headers: list[tuple[str, bytes]], *, context: DecodeContext | None = CONTEXT) -> ConsumedRecord:
    return decode_record(
        topic=TOPIC,
        partition=2,
        offset=17,
        key=b"SYNTH-KEY-1",
        value=b"synthetic",
        headers=headers,
        context=context,
    )


# -- the discriminator ----------------------------------------------------------------------------


def test_a_well_formed_record_is_neither_rejected_nor_quarantined() -> None:
    """Mutation control for the whole file: the checks must let a good record through."""
    record = decode(raw_headers())
    assert record.message.channel == "adt-passthrough"


@pytest.mark.parametrize("bad_id", [MALFORMED, "", "018f6b2d-1c40-4a55-8e77-5b9d0e3f7a21"])
def test_an_unusable_message_id_quarantines(bad_id: str) -> None:
    """Identity is the discriminator, and a v4 id is not identity we trust (the third value is UUIDv4)."""
    with pytest.raises(RecordDecodeError) as caught:
        decode(raw_headers(**{h.TB_MSG_ID: bad_id}))
    assert caught.value.error_code == RECORD_UNDECODABLE


def test_an_absent_message_id_quarantines() -> None:
    headers = [(name, value) for name, value in raw_headers() if name != h.TB_MSG_ID]
    with pytest.raises(RecordDecodeError) as caught:
        decode(headers)
    assert caught.value.error_code == RECORD_UNDECODABLE


def test_a_malformed_claim_check_quarantines_even_with_a_perfect_identity() -> None:
    """The third trigger. Normalising would produce a conformant record whose bytes cannot be found:
    silent data loss that passes every other check we have."""
    with pytest.raises(RecordDecodeError) as caught:
        decode(raw_headers(**{h.TB_CLAIM_CHECK: "s3://bucket/obj?X-Amz-Signature=deadbeef"}))
    assert caught.value.error_code == RECORD_UNDECODABLE
    assert caught.value.reason_code == "CLAIM_CHECK_INVALID"


def test_a_well_formed_claim_check_is_not_a_reject() -> None:
    record = decode(raw_headers(**{h.TB_CLAIM_CHECK: "s3://tb-claims/dev/adt-passthrough/2026/09/22/01920000"}))
    assert record.message.claim_check is not None


def test_a_presigned_url_is_what_the_claim_check_check_is_for() -> None:
    """A query string here would become a stored, replayable credential in ClickHouse and the browser."""
    for locator in (
        "https://bucket.example/obj?X-Amz-Signature=abc",
        "https://user:pass@bucket.example/obj",
        "s3://bucket/obj#fragment",
    ):
        with pytest.raises(RecordDecodeError):
            decode(raw_headers(**{h.TB_CLAIM_CHECK: locator}))


def test_a_malformed_non_identity_header_is_an_ordinary_reject() -> None:
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{h.TB_ATTEMPT: "1_0"}))
    assert caught.value.error_code == HEADER_PARSE_ERROR
    assert caught.value.bad_headers == (h.TB_ATTEMPT,)
    assert caught.value.record.message.msg_id, "the record keeps its own identity"


def test_a_reject_needs_a_decode_context_or_it_becomes_a_quarantine() -> None:
    """Without a channel and version there is nothing honest to normalise to, so the safe shape wins."""
    with pytest.raises(RecordDecodeError):
        decode(raw_headers(**{h.TB_ATTEMPT: "1_0"}), context=None)


# -- one test per placeholder row (envelope 2.3.1) -------------------------------------------------


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (h.TB_CHANNEL, CONTEXT.channel),
        (h.TB_CHANNEL_VERSION, str(CONTEXT.channel_version)),
        (h.TB_SOURCE, CONTEXT.source),
        (h.TB_CONTENT_TYPE, "application/octet-stream"),
        (h.TB_ATTEMPT, "0"),
    ],
)
def test_a_malformed_required_header_is_replaced_by_its_pinned_placeholder(header: str, expected: str) -> None:
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{header: MALFORMED}))

    rejected = caught.value.record.message
    assert rejected.headers[header] == expected
    assert caught.value.bad_headers == (header,)
    assert MALFORMED not in " ".join(rejected.headers.values()), "the bad value is not preserved anywhere"


def test_a_malformed_received_at_becomes_the_reject_time() -> None:
    """The placeholder is the reject time, which is why end-to-end latency for this record is meaningless."""
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{h.TB_RECEIVED_AT: MALFORMED}))

    rejected = caught.value.record.message
    assert rejected.received_at.endswith("Z")
    assert rejected.received_at_time.year >= 2026
    assert MALFORMED not in rejected.received_at


def test_a_malformed_traceparent_becomes_a_new_trace_not_a_repaired_one() -> None:
    """An unparseable trace context cannot be continued, and inventing a parent id corrupts an
    unrelated trace."""
    original = make_message(b"synthetic", key=b"K")
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{h.TRACEPARENT: "00-not-a-trace"}))

    traceparent = caught.value.record.message.traceparent
    assert traceparent is not None and traceparent.startswith("00-")
    assert len(traceparent.split("-")[1]) == 32
    assert traceparent != original.traceparent
    assert "not-a-trace" not in traceparent


@pytest.mark.parametrize(
    "header",
    [h.TB_CORRELATION_ID, h.TB_PARENT_ID, h.TB_DESTINATION, h.TB_RETRY_NOT_BEFORE, h.TB_STAGE, h.TB_ERROR_CODE],
)
def test_a_malformed_optional_header_is_dropped_rather_than_invented(header: str) -> None:
    """There is no honest placeholder for a correlation id or a destination; a wrong one is worse than none."""
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{header: MALFORMED}))

    rejected = caught.value.record.message
    assert header not in rejected.headers
    assert caught.value.bad_headers == (header,)


def test_several_malformed_headers_are_all_named_and_all_normalised() -> None:
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{h.TB_ATTEMPT: "1_0", h.TB_CONTENT_TYPE: MALFORMED, h.TB_DESTINATION: MALFORMED}))

    assert set(caught.value.bad_headers) == {h.TB_ATTEMPT, h.TB_CONTENT_TYPE, h.TB_DESTINATION}
    rejected = caught.value.record.message
    assert rejected.attempt == 0
    assert rejected.content_type == "application/octet-stream"
    assert h.TB_DESTINATION not in rejected.headers


def test_an_unregistered_header_never_triggers_a_reject() -> None:
    """Envelope 1.2: rejecting an unknown header turns an additive contract change into an outage. It is
    also why naming headers in an event is safe — only the closed set from section 3 can be named."""
    record = decode(raw_headers(**{"tb-usr-batch-id": "b-4471", "x-forwarded-for": "10.0.0.1"}))
    assert record.message.headers["tb-usr-batch-id"] == "b-4471"


def test_the_normalised_record_is_a_conformant_dlq_record() -> None:
    """The point of normalising: what lands on the DLQ passes the envelope schema like any dead letter."""
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{h.TB_ATTEMPT: "1_0", h.TB_RECEIVED_AT: MALFORMED}))

    dead = dlq_message(caught.value.record.message, error_code=HEADER_PARSE_ERROR, stage="queued")
    validator = Draft202012Validator(json.loads(ENVELOPE_SCHEMA.read_text(encoding="utf-8")))
    assert list(validator.iter_errors(record_projection(dead, topic_class="dlq"))) == []
    assert dead.key == dead.msg_id.encode("utf-8")


# -- the loop side --------------------------------------------------------------------------------


class RecordingDlq:
    def __init__(self) -> None:
        self.dead: list[tuple[str, str, str]] = []

    def dead_letter(self, record: ConsumedRecord, *, error_code: str, stage: str) -> None:
        self.dead.append((error_code, stage, record.message.headers[h.TB_ATTEMPT]))

    def quarantine(self, raw: Any, *, channel: str, channel_version: int) -> str:
        raise AssertionError("an identifiable record must not be quarantined")


def loop_with(consumer: FakeConsumer, dlq: RecordingDlq, stream: io.StringIO) -> tuple[ConsumerLoop, RecordingEmitter]:
    emitter = RecordingEmitter()
    return (
        ConsumerLoop(
            consumer,
            handler=_NeverCalled(),
            topics=[TOPIC],
            retry=RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
            settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
            emitter=emitter,
            log=JsonLogger("test", stream=stream, level="debug"),
            channel="adt-passthrough",
            channel_version=7,
            instance="worker:pipeline-1@worker-4",
            dlq=dlq,
            read_stage="queued",
        ),
        emitter,
    )


class _NeverCalled:
    def handle(self, record: ConsumedRecord) -> None:
        raise AssertionError("a rejected record must never reach the handler")


def test_the_loop_dead_letters_a_reject_and_breaks_the_silence_with_an_event() -> None:
    """Normalising is silent: a reject with `tb-attempt: "0"` looks like a genuine first attempt, so the
    event has to name the offending header."""
    consumer = FakeConsumer()
    consumer.append(
        FakeRecord(
            _topic=TOPIC,
            _partition=0,
            _offset=0,
            _key=b"SYNTH-KEY-1",
            _value=b"synthetic",
            _headers=raw_headers(**{h.TB_ATTEMPT: "1_0"}),
        )
    )
    dlq = RecordingDlq()
    stream = io.StringIO()
    loop, emitter = loop_with(consumer, dlq, stream)

    stats = loop.run(max_iterations=2)

    assert dlq.dead == [(HEADER_PARSE_ERROR, "queued", "0")], "normalised, with the reading stage"
    assert stats.rejected == 1
    assert consumer.committed[-1] == [(TOPIC, 0, 1)], "the partition moves on; a reject is not a retry"

    event = next(item for item in emitter.events if item.error_code == HEADER_PARSE_ERROR)
    assert event.stage == "error" and event.status == "error"
    assert event.attrs == {"bad_header": h.TB_ATTEMPT}
    assert event.error_message is not None and h.TB_ATTEMPT in event.error_message


def test_a_reject_log_line_names_headers_and_never_their_values() -> None:
    consumer = FakeConsumer()
    consumer.append(
        FakeRecord(
            _topic=TOPIC,
            _partition=0,
            _offset=0,
            _key=b"SYNTH-KEY-MARKER",
            _value=b"PAYLOAD-MARKER",
            _headers=raw_headers(**{h.TB_SOURCE: "SYNTH-VALUE-MARKER not a valid source!"}),
        )
    )
    stream = io.StringIO()
    loop, _ = loop_with(consumer, RecordingDlq(), stream)
    loop.run(max_iterations=2)

    written = stream.getvalue()
    assert "record_rejected" in written
    assert h.TB_SOURCE in written, "the header NAME is ours and is safe to record"
    assert "SYNTH-VALUE-MARKER" not in written, "the value is the sender's and is not"
    assert "SYNTH-KEY-MARKER" not in written
    assert "PAYLOAD-MARKER" not in written


def test_to_kafka_headers_round_trips_a_normalised_record() -> None:
    with pytest.raises(RecordRejectError) as caught:
        decode(raw_headers(**{h.TB_ATTEMPT: MALFORMED}))
    encoded = dict(to_kafka_headers(caught.value.record.message))
    assert encoded[h.TB_ATTEMPT] == b"0"
