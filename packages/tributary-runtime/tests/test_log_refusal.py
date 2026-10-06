"""A refused log line never takes down the caller (lead ruling on the set 1.8 code review, HIGH 1 and 2).

Default (production): the refused line is replaced by one fixed `log_refused` line carrying the checked
event name and a closed reason code, nothing from the refused call; `stats.refused` counts it; nothing
is raised. Strict (`strict=True` or `TB_LOG_STRICT=1`, set for test runs by the SDK's pytest plugin):
`LogFieldError`. Each production-path test has a strict control showing the refusal really fired.

Validation runs before the level filter, so a bad `debug` call is caught at `info`.

Synthetic values only; the marker `SYNTHVALUE` must never reach a stream.
"""

from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest
from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import MAX_FIELDS, REFUSED_EVENT, RESERVED_FIELDS, STRICT_ENV, JsonLogger, LogStats
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import to_kafka_headers
from tributary.runtime.spec import ChannelSpec, RetryPolicy
from tributary.runtime.worker import KafkaDlqWriter, PipelineHandler
from tributary.sdk import Context, Message, step
from tributary.sdk.errors import LogFieldError
from tributary.sdk.testing import make_message

from .fakes import FakeConsumer, FakeError, FakeProducer, FakeRecord
from .log_line_schema import assert_lines_conform

MARKER = "SYNTHVALUE"
CHANNEL = "adt-passthrough"
RAW = f"dev.{CHANNEL}.raw"
EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
FILLER = {f"f{index}": index for index in range(MAX_FIELDS - len(RESERVED_FIELDS) - 1)}  # one short of the cap


def lenient(stream: io.StringIO, **kwargs: Any) -> JsonLogger:
    return JsonLogger("worker", stream=stream, strict=False, **kwargs)


def refused_lines(stream: io.StringIO) -> list[dict[str, Any]]:
    return [
        line for line in assert_lines_conform(stream.getvalue(), expect_at_least=0) if line["event"] == REFUSED_EVENT
    ]


# -- the fallback line -------------------------------------------------------------------------------

CASES: list[tuple[str, dict[str, Any], str]] = [
    ("field_refused", {"payload": MARKER}, "a content name (L4)"),
    ("field_refused", {"mrn": MARKER}, "an identifier name (L3)"),
    ("field_refused", {"note": MARKER + "\nforged"}, "CR/LF"),
    ("field_refused", {"note": MARKER.encode()}, "bytes"),
    ("reserved_field", {"event": MARKER}, "overriding the event name"),
    ("too_many_fields", {**FILLER, "one": 1, "two": 2}, "33 fields"),
    ("unserializable", {"count": 2**70}, "an int orjson cannot hold"),
]


@pytest.mark.parametrize(("reason", "fields", "_why"), CASES, ids=[why for _r, _f, why in CASES])
def test_a_refused_call_writes_one_fixed_line_counts_it_and_returns(
    reason: str, fields: dict[str, Any], _why: str
) -> None:
    stream = io.StringIO()
    log = lenient(stream, base={"channel": CHANNEL})
    log.info("delivered", **fields)  # does not raise

    [line] = assert_lines_conform(stream.getvalue())
    assert set(line) == {"ts", "level", "event", "component", "refused_event", "reason", "channel"}
    assert (line["event"], line["refused_event"], line["reason"], line["level"]) == (
        REFUSED_EVENT,
        "delivered",
        reason,
        "error",
    )
    assert MARKER not in stream.getvalue()
    assert not set(fields) & set(line) - RESERVED_FIELDS, "no field name from the refused call"
    assert log.stats.refused == 1


@pytest.mark.parametrize(("_reason", "fields", "_why"), CASES, ids=[why for _r, _f, why in CASES])
def test_strict_mode_raises_for_the_same_calls(_reason: str, fields: dict[str, Any], _why: str) -> None:
    stream = io.StringIO()
    log = JsonLogger("worker", stream=stream, strict=True, base={"channel": CHANNEL})
    with pytest.raises(LogFieldError) as caught:
        log.info("delivered", **fields)
    assert MARKER not in str(caught.value)
    assert stream.getvalue() == "" and log.stats.refused == 0


def test_an_unchecked_event_name_is_not_echoed() -> None:
    stream = io.StringIO()
    lenient(stream).info(f"patient {MARKER} admitted")
    [line] = refused_lines(stream)
    assert line["reason"] == "event_refused" and "refused_event" not in line
    assert MARKER not in stream.getvalue()


def test_a_refused_bind_keeps_the_parent_fields_and_drops_all_of_its_own() -> None:
    stream = io.StringIO()
    parent = lenient(stream, base={"channel": CHANNEL})
    child = parent.bind(partition=2, payload=MARKER)
    child.info("paused", attempt=1)
    refused, written = assert_lines_conform(stream.getvalue())
    assert (refused["event"], refused["reason"]) == (REFUSED_EVENT, "bound_field_refused")
    assert written["event"] == "paused" and written["channel"] == CHANNEL and "partition" not in written
    assert MARKER not in stream.getvalue()


def test_a_refused_base_starts_the_logger_with_no_base_fields() -> None:
    stream = io.StringIO()
    log = lenient(stream, base={"channel": CHANNEL, "body": MARKER})
    log.info("started")
    refused, written = assert_lines_conform(stream.getvalue())
    assert refused["reason"] == "base_field_refused" and "channel" not in written
    assert MARKER not in stream.getvalue()


def test_one_counter_covers_a_logger_and_everything_bound_from_it() -> None:
    stats = LogStats()
    stream = io.StringIO()
    parent = lenient(stream, stats=stats)
    parent.bind(partition=1).info("x", payload=MARKER)
    parent.bind(partition=2).bind(attempt=3).warning("y", raw=MARKER)
    parent.error("z", sql=MARKER)
    assert stats.refused == 3 and parent.stats is stats


def test_the_default_comes_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(STRICT_ENV, "1")
    assert JsonLogger("worker", stream=io.StringIO()).strict is True
    monkeypatch.setenv(STRICT_ENV, "0")
    assert JsonLogger("worker", stream=io.StringIO()).strict is False
    monkeypatch.delenv(STRICT_ENV)
    assert JsonLogger("worker", stream=io.StringIO()).strict is False, "production default: never raise"
    assert JsonLogger("worker", stream=io.StringIO(), strict=True).strict is True, "the flag wins"


def test_the_test_suite_runs_strict() -> None:
    """The SDK's pytest plugin sets `TB_LOG_STRICT=1`, so a refused line anywhere in the suite fails a test."""
    assert JsonLogger("worker", stream=io.StringIO()).strict is True


def test_a_stream_that_is_gone_does_not_raise() -> None:
    stream = io.StringIO()
    log = lenient(stream)
    stream.close()
    log.info("delivered", attempt=1)
    assert log.stats.write_failed == 1


# -- HIGH 2: validation before the level filter -------------------------------------------------------


def test_a_bad_debug_call_is_caught_at_info_level() -> None:
    stream = io.StringIO()
    log = JsonLogger("worker", stream=stream, level="info", strict=False)
    log.debug("trace_detail", attempt=1)
    assert stream.getvalue() == "", "a good debug call is still filtered"
    log.debug("trace_detail", payload=MARKER)
    [line] = refused_lines(stream)
    assert (line["refused_event"], line["reason"]) == ("trace_detail", "field_refused")
    with pytest.raises(LogFieldError):
        JsonLogger("worker", stream=io.StringIO(), level="info", strict=True).debug("trace_detail", payload=MARKER)


# -- inside a worker loop ----------------------------------------------------------------------------


@step
def noisy(message: Message, ctx: Context) -> Message:
    """A step with a logging bug: it logs the payload."""
    ctx.log.info("step_note", payload=message.payload.decode())
    return message


def loopback_spec() -> ChannelSpec:
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"]["version"] = 1
    document["spec"]["pipeline"]["steps"] = [{"name": "noisy", "uses": "t.noisy"}]
    document["spec"]["pipeline"]["stepRetries"] = 0
    return ChannelSpec.from_document(document)


def run_loop(log: JsonLogger, records: int) -> tuple[ConsumerLoop, FakeConsumer, FakeProducer]:
    spec = loopback_spec()
    consumer, producer = FakeConsumer(), FakeProducer()
    for offset in range(records):
        message = make_message(MARKER.encode(), channel=CHANNEL)
        consumer.append(FakeRecord(RAW, 0, offset, message.key, message.payload, to_kafka_headers(message)))
    produce = RecordProducer(producer, log=log)
    pipeline = PipelineHandler(
        env="dev",
        spec_provider=lambda: spec,
        producer=produce,
        emitter=RecordingEmitter(),
        log=log,
        instance="worker:test@localhost",
        resolve=lambda reference: noisy.bind(reference.params),
    )
    pipeline.prepare(spec)
    loop = ConsumerLoop(
        consumer,
        pipeline,
        topics=[RAW],
        retry=RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
        settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
        emitter=RecordingEmitter(),
        log=log,
        channel=CHANNEL,
        channel_version=1,
        instance="worker:test@localhost",
        dlq=KafkaDlqWriter(produce, f"dev.{CHANNEL}.dlq", log=log, source="worker:test@localhost"),
    )
    loop.run(max_iterations=records + 1)
    return loop, consumer, producer


def test_a_refused_field_in_a_step_does_not_stop_the_worker_loop() -> None:
    stream = io.StringIO()
    log = lenient(stream, base={"channel": CHANNEL})
    loop, consumer, producer = run_loop(log, records=3)

    assert (loop.stats.handled, loop.stats.dead_lettered) == (3, 0), "every record went through"
    assert consumer.committed[-1] == [(RAW, 0, 3)], "and was committed"
    assert len([p for p in producer.produced if ".out." in p[0]]) == 3
    assert [line["refused_event"] for line in refused_lines(stream)] == ["step_note"] * 3
    assert log.stats.refused == 3
    assert MARKER not in stream.getvalue()


def test_control_the_same_step_under_strict_logging_fails_its_records() -> None:
    """Proves the refusal fired inside the step: strict turns it into a step failure and a dead letter."""
    loop, _consumer, producer = run_loop(JsonLogger("worker", stream=io.StringIO(), strict=True), records=3)
    assert (loop.stats.handled, loop.stats.dead_lettered) == (0, 3)
    assert not [p for p in producer.produced if ".out." in p[0]]


# -- on an error path --------------------------------------------------------------------------------


def fatal_loop(log: JsonLogger) -> ConsumerLoop:
    consumer = FakeConsumer()
    consumer.append(FakeRecord(RAW, 0, 0, None, None, None, FakeError(-1, True)))  # type: ignore[arg-type]
    return ConsumerLoop(
        consumer,
        object(),  # never reached: the first poll is a fatal error
        topics=[RAW],
        retry=RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
        settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
        emitter=RecordingEmitter(),
        log=log,
        channel=CHANNEL,
        channel_version=1,
        instance="worker:test@localhost",
    )


def test_the_original_exception_still_propagates_when_its_log_line_is_refused() -> None:
    """A logger already one field short of the cap refuses `consumer_error` (two fields): the loop's own
    `RuntimeError` must still be what the caller sees, not a `LogFieldError` raised from the log call."""
    stream = io.StringIO()
    crowded = lenient(stream).bind(**FILLER)
    with pytest.raises(RuntimeError, match="fatal consumer error"):
        fatal_loop(crowded).run(max_iterations=2)
    assert "consumer_error" in [line["refused_event"] for line in refused_lines(stream)]


def test_control_strict_logging_would_have_replaced_the_original_exception() -> None:
    crowded = JsonLogger("worker", stream=io.StringIO(), strict=True).bind(**FILLER)
    with pytest.raises(LogFieldError):
        fatal_loop(crowded).run(max_iterations=2)
