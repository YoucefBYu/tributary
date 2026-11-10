"""`sent` (events/v0 2.1) from every loader: after the destination acknowledged, never on the delivery path.

The wrapper is shared by all eight loaders (`tributary.loaders.sent_events`); each loader's `main.build_loop`
passes its handler through it, which the parametrized wiring test pins. Synthetic ids only."""

from __future__ import annotations

import importlib
import inspect
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import pytest
from jsonschema import Draft202012Validator, FormatChecker
from tributary.loaders.sent_events import with_sent_events
from tributary.runtime.errors import DeliveryError
from tributary.runtime.events import RecordingEmitter, serialize
from tributary.runtime.records import ConsumedRecord
from tributary.sdk import Message, ids
from tributary.sdk import headers as h

SCHEMA = json.loads(
    (Path(__file__).resolve().parents[3] / "contracts" / "events" / "v0" / "event.schema.json").read_text("utf-8")
)
LOADERS = ["http", "sftp", "mllp", "fhir", "kafka", "clickhouse", "s3", "postgresql"]


def record(offset: int = 7, *, partition: int = 0, claim_check: str | None = None) -> ConsumedRecord:
    headers = {
        h.TB_MSG_ID: ids.uuid7(),
        h.TB_CHANNEL: "adt",
        h.TB_CHANNEL_VERSION: "3",
        h.TB_SOURCE: "mllp:i@p",
        h.TB_RECEIVED_AT: h.format_timestamp(datetime.now(UTC)),
        h.TB_CONTENT_TYPE: "application/hl7-v2+er7",
        h.TB_ATTEMPT: "0",
        h.TRACEPARENT: ids.new_traceparent(),
    }
    if claim_check:
        headers[h.TB_CLAIM_CHECK] = claim_check
    return ConsumedRecord(
        "dev.adt.out.dest", partition, offset, Message(headers=headers, payload=b"MSH|TRIBUTARY-SYNTH")
    )


class Handler:
    """Stands in for a loader's record handler: `outcomes` is consumed one per call."""

    def __init__(self, *outcomes: BaseException | None) -> None:
        self.outcomes = list(outcomes)
        self.calls = 0
        self.closed = False

    def handle(self, rec: ConsumedRecord) -> None:
        self.calls += 1
        outcome = self.outcomes.pop(0) if self.outcomes else None
        if outcome is not None:
            raise outcome

    def metrics_snapshot(self) -> dict[str, int]:
        return {"delivered": self.calls}

    def close(self) -> None:
        self.closed = True


def wrap(handler: Any, emitter: Any, **kw: Any) -> Any:
    return with_sent_events(
        handler, emitter=emitter, channel="adt", channel_version=3, instance="loader:x@y", destination="dest", **kw
    )


def test_a_confirmed_delivery_emits_one_sent_event_that_validates_against_the_contract() -> None:
    emitter = RecordingEmitter()
    rec = record()
    wrap(Handler(), emitter).handle(rec)
    [event] = emitter.events
    assert event.stage == "sent" and event.status == "ok"
    document = orjson.loads(serialize(event))
    assert document["message_id"] == rec.msg_id and document["destination"] == "dest" and document["attempt"] == 1
    assert document["offset"] == 7 and document["size_bytes"] == len(b"MSH|TRIBUTARY-SYNTH")
    assert "MSH" not in json.dumps({k: v for k, v in document.items() if k != "size_bytes"})
    errors = list(Draft202012Validator(SCHEMA, format_checker=FormatChecker()).iter_errors(document))
    assert errors == [], [e.message for e in errors]


def test_a_failed_attempt_emits_nothing_and_the_error_reaches_the_loop_unchanged() -> None:
    emitter = RecordingEmitter()
    failure = DeliveryError("TIMEOUT", retryable=True)
    with pytest.raises(DeliveryError) as raised:
        wrap(Handler(failure), emitter).handle(record())
    assert raised.value is failure and emitter.events == []


def test_attempt_counts_the_try_that_succeeded_and_resets_for_the_next_record() -> None:
    emitter = RecordingEmitter()
    wrapped = wrap(Handler(DeliveryError("TIMEOUT", retryable=True), DeliveryError("TIMEOUT", retryable=True)), emitter)
    rec = record(offset=5)
    for _ in range(2):
        with pytest.raises(DeliveryError):
            wrapped.handle(rec)
    wrapped.handle(rec)
    wrapped.handle(record(offset=6))
    assert [e.attempt for e in emitter.events] == [3, 1]


def test_a_redelivery_of_the_same_try_repeats_the_same_dedup_key() -> None:
    emitter = RecordingEmitter()
    wrapped = wrap(Handler(), emitter)
    rec = record()
    wrapped.handle(rec)
    wrapped.handle(rec)  # replayed after a crash or a window abort
    keys = {(e.message_id, e.stage, e.destination, e.attempt) for e in emitter.events}
    assert len(emitter.events) == 2 and len(keys) == 1


def test_a_claim_checked_payload_reports_zero_inline_bytes() -> None:
    emitter = RecordingEmitter()
    wrap(Handler(), emitter).handle(record(claim_check="s3://bucket/obj"))
    assert emitter.events[0].size_bytes == 0 and emitter.events[0].claim_check == "s3://bucket/obj"


class Exploding:
    def emit(self, event: object) -> None:
        raise RuntimeError("SYNTH-EMIT-FAILURE-TEXT")

    def close(self) -> None:
        pass


class Logs:
    def __init__(self) -> None:
        self.lines: list[tuple[str, dict[str, object]]] = []

    def warning(self, event: str, /, **fields: object) -> None:
        self.lines.append((event, fields))

    debug = info = error = warning


def test_an_emitter_that_fails_never_fails_the_delivery_and_leaks_no_text() -> None:
    log = Logs()
    wrapped = wrap(Handler(), Exploding(), log=log)
    wrapped.handle(record())  # does not raise
    wrapped.handle(record(offset=8))
    assert log.lines == [("sent_event_failed", {"error_type": "RuntimeError"})]  # once, class name only


def test_the_event_is_emitted_after_the_handler_returned_not_before() -> None:
    order: list[str] = []

    class Ordered(Handler):
        def handle(self, rec: ConsumedRecord) -> None:
            order.append("delivered")

    class Emitter(RecordingEmitter):
        def emit(self, event: Any) -> None:
            order.append("sent")

    wrap(Ordered(), Emitter()).handle(record())
    assert order == ["delivered", "sent"]


def test_other_handler_members_pass_through() -> None:
    inner = Handler()
    wrapped = wrap(inner, RecordingEmitter())
    wrapped.handle(record())
    assert wrapped.metrics_snapshot() == {"delivered": 1}
    wrapped.close()
    assert inner.closed


@pytest.mark.parametrize("loader", LOADERS)
def test_every_loader_passes_its_handler_through_the_sent_wrapper(loader: str) -> None:
    source = inspect.getsource(importlib.import_module(f"tributary.loaders.{loader}.main"))
    call = source.split("loop = ConsumerLoop(", 1)[1].split("topics=", 1)[0]
    assert "with_sent_events(" in call and "emitter=emitter" in call and "destination=a.destination" in call
