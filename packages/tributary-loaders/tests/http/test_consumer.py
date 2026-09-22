"""`OutConsumer.handle`: the retry/DLQ/halt state machine, against a fake `confluent_kafka`
consumer (no broker) and a fake `HttpLoader` (no network). `test_integration_http_loader.py` is the
guarded, real-Kafka version."""

from __future__ import annotations

import asyncio

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.http.config import HttpDestinationConfig
from tributary.loaders.http.consumer import OutConsumer
from tributary.loaders.http.dlq import build_dlq_envelope
from tributary.loaders.http.loader import DeliveryOutcome
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def make_config(**overrides: object) -> HttpDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "dest",
        "url": "https://example.org/ingest",
        "method": "POST",
        "timeout_s": 2.0,
        "headers": {},
        "auth": None,
        "circuit_breaker": None,
        "idempotency_technique": "none",
        "idempotency_header": None,
        "backoff": "fixed",
        "initial_delay_ms": 1,  # ms: keep the retry tests fast
        "max_delay_ms": 5,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return HttpDestinationConfig(**base)  # type: ignore[arg-type]


class FakeKafkaMessage:
    def __init__(self, topic: str = "dev.c.out.dest", partition: int = 0, offset: int = 42) -> None:
        self._topic, self._partition, self._offset = topic, partition, offset
        self._msg_id = ids.uuid7()

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return None

    def value(self) -> bytes | None:
        return b"MSH|...\r"

    def headers(self) -> list[tuple[str, bytes]]:
        return [
            (h.TB_MSG_ID, self._msg_id.encode()),
            (h.TB_CHANNEL, b"c"),
            (h.TB_CHANNEL_VERSION, b"0"),
            (h.TB_SOURCE, b"mllp:i@p"),
            (h.TB_RECEIVED_AT, b"2026-01-01T00:00:00.000Z"),
            (h.TB_CONTENT_TYPE, b"application/hl7-v2+er7"),
            (h.TB_ATTEMPT, b"0"),
            (h.TRACEPARENT, ids.new_traceparent().encode()),
            (h.TB_DESTINATION, b"dest"),
        ]

    def error(self) -> None:
        return None


class FakeConsumer:
    def __init__(self) -> None:
        self.paused: list[object] = []
        self.resumed: list[object] = []
        self.sought: list[object] = []
        self.committed: list[object] = []

    def pause(self, partitions: object) -> None:
        self.paused.append(partitions)

    def resume(self, partitions: object) -> None:
        self.resumed.append(partitions)

    def seek(self, tp: object) -> None:
        self.sought.append(tp)

    def commit(self, *, message: object, asynchronous: bool) -> None:
        self.committed.append(message)


class ScriptedLoader:
    """`deliver` returns each outcome in `outcomes` in order, then repeats the last one."""

    def __init__(self, outcomes: list[DeliveryOutcome]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    async def deliver(self, message: Message) -> DeliveryOutcome:
        outcome = self.outcomes[min(self.calls, len(self.outcomes) - 1)]
        self.calls += 1
        return outcome


class FakeDlqPublisher:
    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.published: list[Message] = []

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.published.append(message)
        return self.ok


def test_success_commits_and_resets_attempts() -> None:
    consumer = FakeConsumer()
    loader = ScriptedLoader([DeliveryOutcome(success=True, reason="success", status_code=200)])
    out = OutConsumer(make_config(), loader, consumer=consumer, topic="dev.c.out.dest")
    msg = FakeKafkaMessage()
    asyncio.run(out.handle(msg))
    assert consumer.committed == [msg]
    assert out._attempts == {}


def test_retryable_failure_pauses_seeks_back_and_resumes() -> None:
    consumer = FakeConsumer()
    loader = ScriptedLoader([DeliveryOutcome(success=False, reason="http-5xx", status_code=500)])
    out = OutConsumer(make_config(max_attempts=5), loader, consumer=consumer, topic="dev.c.out.dest")
    msg = FakeKafkaMessage(offset=7)
    asyncio.run(out.handle(msg))
    assert consumer.committed == []
    assert len(consumer.paused) == 1
    assert len(consumer.resumed) == 1
    assert consumer.sought[0].offset == 7
    assert out._attempts[("dev.c.out.dest", 0)] == 1


def test_retries_exhausted_with_halt_partition_pauses_forever() -> None:
    consumer = FakeConsumer()
    outcome = DeliveryOutcome(success=False, reason="http-5xx", status_code=500)
    loader = ScriptedLoader([outcome])
    out = OutConsumer(make_config(max_attempts=2, on_exhaust="halt-partition"), loader, consumer=consumer, topic="t")
    msg = FakeKafkaMessage()

    asyncio.run(out.handle(msg))  # attempt 1: retry
    asyncio.run(out.handle(msg))  # attempt 2: exhausted -> halt
    assert consumer.committed == []
    assert ("dev.c.out.dest", 0) in out._halted

    # a halted partition is never touched again
    resumed_before, paused_before = len(consumer.resumed), len(consumer.paused)
    asyncio.run(out.handle(msg))
    assert len(consumer.resumed) == resumed_before
    assert len(consumer.paused) == paused_before


def test_non_retryable_failure_goes_straight_to_on_exhaust() -> None:
    """A terminal 4xx does not wait for max_attempts: retrying it is pointless."""
    consumer = FakeConsumer()
    loader = ScriptedLoader([DeliveryOutcome(success=False, reason="http-4xx", status_code=422)])
    out = OutConsumer(make_config(max_attempts=10, on_exhaust="halt-partition"), loader, consumer=consumer, topic="t")
    asyncio.run(out.handle(FakeKafkaMessage()))
    assert ("dev.c.out.dest", 0) in out._halted
    assert len(consumer.paused) == 1  # no retry pause/resume cycle happened first


def test_retries_exhausted_with_dlq_and_continue_publishes_and_commits() -> None:
    consumer = FakeConsumer()
    outcome = DeliveryOutcome(success=False, reason="http-5xx", status_code=500)
    loader = ScriptedLoader([outcome])
    dlq = FakeDlqPublisher(ok=True)
    out = OutConsumer(
        make_config(max_attempts=1, on_exhaust="dlq-and-continue"),
        loader,
        consumer=consumer,
        topic="t",
        dlq_publisher=dlq,
    )
    msg = FakeKafkaMessage()
    asyncio.run(out.handle(msg))
    assert len(dlq.published) == 1
    assert dlq.published[0].error_code == "HTTP_5XX"
    assert dlq.published[0].stage == "dlq"
    assert consumer.committed == [msg]
    assert out._attempts == {}


def test_dlq_publish_failure_does_not_commit_and_retries_the_write() -> None:
    consumer = FakeConsumer()
    outcome = DeliveryOutcome(success=False, reason="http-5xx", status_code=500)
    loader = ScriptedLoader([outcome])
    dlq = FakeDlqPublisher(ok=False)
    out = OutConsumer(
        make_config(max_attempts=1, on_exhaust="dlq-and-continue"),
        loader,
        consumer=consumer,
        topic="t",
        dlq_publisher=dlq,
    )
    asyncio.run(out.handle(FakeKafkaMessage()))
    assert consumer.committed == []  # never drop the record
    assert len(consumer.paused) == 1
    assert len(consumer.resumed) == 1


def test_dlq_and_continue_without_a_publisher_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="dlq_publisher"):
        OutConsumer(make_config(on_exhaust="dlq-and-continue"), ScriptedLoader([]))


def test_build_dlq_envelope_preserves_msg_id_and_sets_key() -> None:
    original = Message(
        headers={
            h.TB_MSG_ID: (msg_id := ids.uuid7()),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/hl7-v2+er7",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "dest",
        },
        payload=b"x",
    )
    dlq_message = build_dlq_envelope(original, error_code="HTTP_5XX", final_attempt=3)
    assert dlq_message.msg_id == msg_id
    assert dlq_message.key == msg_id.encode("utf-8")
    assert dlq_message.stage == "dlq"
    assert dlq_message.error_code == "HTTP_5XX"
    assert dlq_message.attempt == 3
    assert dlq_message.retry_not_before is None
