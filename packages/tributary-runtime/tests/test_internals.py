"""Logging, `ctx`, record decoding and the pipeline executor — all with no Kafka.

The record-decode tests are gate requirement 13 at the record level: a header that arrives malformed
from the network must become a typed reject with a DLQ-ready code, never an exception that kills the
consumer loop.
"""

from __future__ import annotations

import io

import orjson
import pytest
from tributary.runtime.context import EnvSecretStore, UnavailableKeyValueStore, WorkerContext
from tributary.runtime.executor import PipelineExecutor, StepFailure
from tributary.runtime.logs import JsonLogger
from tributary.runtime.records import (
    RECORD_UNDECODABLE,
    ConsumedRecord,
    DecodeContext,
    RecordDecodeError,
    apply_usr_headers,
    decode_record,
    to_kafka_headers,
)
from tributary.sdk import Drop, Message, step
from tributary.sdk import headers as h
from tributary.sdk.errors import AttrError, LogFieldError, SecretNotFoundError, UsrHeaderError
from tributary.sdk.ids import new_traceparent, uuid7
from tributary.sdk.testing import make_message


def logger(stream: io.StringIO) -> JsonLogger:
    return JsonLogger("worker", stream=stream, level="debug", base={"channel": "adt-passthrough"})


def test_log_lines_are_single_line_json_with_the_base_fields() -> None:
    stream = io.StringIO()
    logger(stream).info("delivered", message_id="0192", attempt=1, duration_ms=3.5)
    lines = stream.getvalue().splitlines()
    assert len(lines) == 1
    record = orjson.loads(lines[0])
    assert record["event"] == "delivered"
    assert record["level"] == "info"
    assert record["component"] == "worker"
    assert record["channel"] == "adt-passthrough"
    assert record["ts"].endswith("Z")


def test_a_phi_shaped_log_field_raises_rather_than_being_written() -> None:
    """A guard that only warns is a guard the caller learns to ignore."""
    stream = io.StringIO()
    log = logger(stream)
    with pytest.raises(LogFieldError):
        log.info("delivered", key=b"PID-SYNTH-1")
    with pytest.raises(LogFieldError):
        log.info("delivered", mrn="123456")
    with pytest.raises(LogFieldError):
        log.info("delivered", note="forged\nline")
    assert stream.getvalue() == ""


def test_bind_carries_fields_without_repeating_them_at_every_call() -> None:
    stream = io.StringIO()
    logger(stream).bind(partition=2).info("paused", attempt=3)
    record = orjson.loads(stream.getvalue())
    assert record["partition"] == 2
    assert record["channel"] == "adt-passthrough"


def test_level_filtering() -> None:
    stream = io.StringIO()
    JsonLogger("worker", stream=stream, level="warning").info("ignored")
    assert stream.getvalue() == ""


def context() -> WorkerContext:
    return WorkerContext(
        channel="adt-passthrough",
        channel_version=2,
        instance="worker:pipeline-1@localhost",
        log=JsonLogger("worker", stream=io.StringIO()),
        secrets=EnvSecretStore({"fhir-oauth": "token"}),
        traceparent=new_traceparent(),
    )


def test_the_worker_context_enforces_the_same_guards_as_the_fake() -> None:
    ctx = context()
    ctx.set_attr("trigger", "a01")
    ctx.set_usr("batch-id", "b-4471")
    assert ctx.attrs() == {"trigger": "a01"}
    assert ctx.usr_headers() == {"batch-id": "b-4471"}

    with pytest.raises(AttrError):
        ctx.set_attr("mrn", "123456")
    with pytest.raises(AttrError):
        ctx.set_attr("mrn_hmac", "9f" * 32)
    with pytest.raises(UsrHeaderError):
        ctx.set_usr("patient-id", "123456")


def test_secrets_resolve_without_the_control_plane_and_never_echo_the_value() -> None:
    """Invariant 3: a worker starts and runs with the API down (channel-spec contract 6 rule 2)."""
    ctx = context()
    assert ctx.secrets.get("fhir-oauth") == "token"
    with pytest.raises(SecretNotFoundError) as caught:
        ctx.secrets.get("absent-ref")
    assert "absent-ref" in str(caught.value)


def test_kv_is_honest_about_not_existing_yet() -> None:
    with pytest.raises(NotImplementedError):
        UnavailableKeyValueStore().get("cursor")


def raw_headers(message: Message) -> list[tuple[str, bytes]]:
    return to_kafka_headers(message)


def test_a_well_formed_record_decodes() -> None:
    message = make_message(b"synthetic", key=b"SYNTH-KEY-1")
    record = decode_record(
        topic="dev.adt-passthrough.raw",
        partition=2,
        offset=17,
        key=b"SYNTH-KEY-1",
        value=b"synthetic",
        headers=raw_headers(message),
    )
    assert isinstance(record, ConsumedRecord)
    assert record.message.msg_id == message.msg_id
    assert record.msg_id == message.msg_id
    assert record.offset == 17


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda hs: [(name, value) for name, value in hs if name != h.TB_MSG_ID], "ENVELOPE_INVALID"),
        (lambda hs: [*hs, (h.TB_MSG_ID, b"not-a-uuid7")], "ENVELOPE_INVALID"),
        (lambda hs: [*hs, (h.TB_CHANNEL, bytes([0xFF, 0xFE]))], "HEADER_NOT_TEXT"),
    ],
)
def test_an_unidentifiable_record_quarantines_with_one_contract_code(mutate: object, reason: str) -> None:
    """One contract code out (`RECORD_UNDECODABLE`), a finer reason code for the log line.

    Only records whose **identity** is unusable land here; a malformed `tb-attempt` on an identifiable
    record is an ordinary reject and is covered in `test_reject.py` — a decode context is what separates
    the two, so this test passes one deliberately rather than relying on the no-context fallback.
    """
    message = make_message(b"synthetic", key=b"K")
    headers = mutate(raw_headers(message))  # type: ignore[operator]
    with pytest.raises(RecordDecodeError) as caught:
        decode_record(
            topic="dev.adt-passthrough.raw",
            partition=0,
            offset=1,
            key=b"K",
            value=b"synthetic",
            headers=headers,
            context=DecodeContext(channel="adt-passthrough", channel_version=1, source="worker:test@localhost"),
        )
    assert caught.value.error_code == RECORD_UNDECODABLE
    assert caught.value.reason_code == reason


def test_an_unknown_header_does_not_make_a_record_undeliverable() -> None:
    """Envelope contract 1.2: rejecting an unknown header turns an additive change into an outage."""
    message = make_message(b"synthetic", key=b"K")
    record = decode_record(
        topic="dev.adt-passthrough.raw",
        partition=0,
        offset=1,
        key=b"K",
        value=b"synthetic",
        headers=[*raw_headers(message), ("tb-something-newer", b"1")],
    )
    assert record.message.get("tb-something-newer") == "1"


@step
def passthrough(msg: Message) -> None:
    return None


@step
def add_attr(msg: Message, ctx: WorkerContext) -> Message:
    ctx.set_attr("step", "add_attr")
    return msg.with_header("tb-destination", "http-stub")


@step
def drop_it(msg: Message) -> Drop:
    return Drop(reason="unsupported trigger event")


class Counter:
    def __init__(self) -> None:
        self.calls = 0


def test_the_executor_runs_steps_in_order() -> None:
    ctx = context()
    result = PipelineExecutor([passthrough, add_attr]).run(make_message(), ctx)
    assert result.dropped is False
    assert result.message is not None and result.message.destination == "http-stub"
    assert result.steps_run == 2
    assert ctx.attrs() == {"step": "add_attr"}


def test_a_drop_stops_the_pipeline_where_it_happened() -> None:
    result = PipelineExecutor([drop_it, add_attr]).run(make_message(), context())
    assert result.dropped is True
    assert result.drop is not None and result.drop.reason == "unsupported trigger event"
    assert result.steps_run == 1


def test_a_raising_step_is_retried_in_place_then_becomes_a_typed_failure() -> None:
    counter = Counter()

    @step
    def always_raises(msg: Message) -> Message:
        counter.calls += 1
        raise ValueError(f"MSH|^~\\&|TRIBUTARY-SYNTH patient SYNTH^JANE mrn {msg.msg_id}")

    executor = PipelineExecutor([always_raises], step_retries=2)
    with pytest.raises(StepFailure) as caught:
        executor.run(make_message(), context())

    assert counter.calls == 3, "step_retries=2 means three attempts in total (ADR-003 D4)"
    assert caught.value.error_code == "TRANSFORM_ERROR"
    assert caught.value.exception_type == "ValueError"
    # The exception text is dropped on purpose: parser and validator messages quote the field value.
    assert "SYNTH" not in str(caught.value)
    assert "MSH" not in str(caught.value)


def test_a_step_that_recovers_on_a_later_attempt_succeeds() -> None:
    counter = Counter()

    @step
    def flaky(msg: Message) -> Message:
        counter.calls += 1
        if counter.calls < 3:
            raise RuntimeError("transient")
        return msg

    result = PipelineExecutor([flaky], step_retries=3).run(make_message(), context())
    assert result.dropped is False and counter.calls == 3


def test_usr_headers_from_ctx_are_applied_to_the_outgoing_message() -> None:
    message = apply_usr_headers(make_message(), [("batch-id", "b-4471")])
    assert message.headers["tb-usr-batch-id"] == "b-4471"
    assert to_kafka_headers(message)[-1] == ("tb-usr-batch-id", b"b-4471")


def test_uuid7_ids_are_version_and_variant_correct() -> None:
    """Gate section 6 G1: S3 shipped a v4 binary msg-id. The schema checks the nibbles."""
    for _ in range(50):
        value = uuid7()
        assert value[14] == "7"
        assert value[19] in "89ab"
