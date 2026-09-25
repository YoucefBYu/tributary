"""`KafkaLoader.deliver`: the header the idempotency technique adds, classification of delivery-report
and global errors, the circuit breaker, and the idempotency-key-header duplicate-delivery test
(invariant 4: every loader documents and tests its idempotency technique keyed by `tb-msg-id`).
`FakeProducer` stands in for `confluent_kafka.Producer`; `tests/kafka/test_integration_kafka_loader.py`
proves the same behaviour against a real broker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from tributary.loaders.kafka.config import (
    CircuitBreakerSpec,
    KafkaConnectionSpec,
    KafkaDestinationConfig,
    KafkaSecuritySpec,
)
from tributary.loaders.kafka.loader import KafkaLoader, LastErrorSink
from tributary.sdk import Message, ids
from tributary.sdk import headers as h


def make_message(*, msg_id: str | None = None, key: bytes | None = None) -> Message:
    headers = {
        h.TB_MSG_ID: msg_id or ids.uuid7(),
        h.TB_CHANNEL: "c",
        h.TB_CHANNEL_VERSION: "0",
        h.TB_SOURCE: "mllp:i@p",
        h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
        h.TB_CONTENT_TYPE: "application/json",
        h.TB_ATTEMPT: "0",
        h.TRACEPARENT: ids.new_traceparent(),
        h.TB_DESTINATION: "partner-feed",
    }
    return Message(headers=headers, payload=b'{"value": 1}', key=key)


def make_config(**overrides: object) -> KafkaDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "partner-feed",
        "connection": KafkaConnectionSpec(("kafka.example.org:9093",), KafkaSecuritySpec("SSL")),
        "topic": "partner.events",
        "key_mode": "preserve",
        "timeout_s": 2.0,
        "idempotency_technique": "idempotency-key-header",
        "idempotency_header": "tb-msg-id",
        "circuit_breaker": None,
        "backoff": "fixed",
        "initial_delay_ms": 100,
        "max_delay_ms": 1000,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return KafkaDestinationConfig(**base)  # type: ignore[arg-type]


@dataclass
class FakeKafkaError:
    _code: int
    _name: str
    _retriable: bool

    def code(self) -> int:
        return self._code

    def name(self) -> str:
        return self._name

    def retriable(self) -> bool:
        return self._retriable


class FakeProducer:
    """Simulates just enough of `confluent_kafka.Producer` to drive the loader's classification:
    `respond` decides the delivery-report error (or `None` for success), `queue_full` simulates
    `produce()` itself raising `BufferError`, and every call is recorded."""

    def __init__(self, respond: Any = lambda args: None, *, queue_full: bool = False, flush_remaining: int = 0) -> None:
        self._respond = respond
        self.queue_full = queue_full
        self.flush_remaining = flush_remaining
        self.produced: list[dict[str, Any]] = []
        self.flush_calls: list[float] = []

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Any = None,
        on_delivery: Any = None,
    ) -> None:
        if self.queue_full:
            raise BufferError("local queue full")
        record = {"topic": topic, "value": value, "key": key, "headers": headers}
        self.produced.append(record)
        record["on_delivery"] = on_delivery
        record["error"] = self._respond(record)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_calls.append(timeout)
        if self.flush_remaining > 0:
            return self.flush_remaining
        for record in self.produced:
            if record.get("on_delivery") is not None:
                record["on_delivery"](record.get("error"), None)
        return 0


# -- headers and key -------------------------------------------------------------------------------


def test_idempotency_key_header_is_sent_on_every_attempt() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    message = make_message()
    loader.deliver(message)
    loader.deliver(message)
    assert producer.produced[0]["headers"] == [("tb-msg-id", message.msg_id.encode("utf-8"))]
    assert producer.produced[1]["headers"] == producer.produced[0]["headers"]


def test_none_technique_sends_no_header() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(idempotency_technique="none", idempotency_header=None), producer)
    loader.deliver(make_message())
    assert producer.produced[0]["headers"] == []


def test_key_preserve_sends_the_messages_key() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(key_mode="preserve"), producer)
    loader.deliver(make_message(key=b"SYNTH-1"))
    assert producer.produced[0]["key"] == b"SYNTH-1"


def test_key_none_sends_a_null_key_even_if_the_message_has_one() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(key_mode="none"), producer)
    loader.deliver(make_message(key=b"SYNTH-1"))
    assert producer.produced[0]["key"] is None


def test_delivers_to_the_configured_topic() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(topic="partner.events"), producer)
    loader.deliver(make_message())
    assert producer.produced[0]["topic"] == "partner.events"


# -- classification ----------------------------------------------------------------------------------


def test_success_is_reported() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.success is True
    assert outcome.reason == "success"


def test_queue_full_is_retryable() -> None:
    producer = FakeProducer(queue_full=True)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "QUEUE_FULL"
    assert outcome.retryable is True


def test_flush_timeout_with_no_delivery_report_is_timeout_and_retryable() -> None:
    producer = FakeProducer(flush_remaining=1)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "TIMEOUT"
    assert outcome.retryable is True


def test_retriable_broker_delivery_error_is_reported_as_retryable() -> None:
    err = FakeKafkaError(14, "COORDINATOR_LOAD_IN_PROGRESS", True)
    producer = FakeProducer(respond=lambda args: err)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "KAFKA_COORDINATOR_LOAD_IN_PROGRESS"
    assert outcome.retryable is True


def test_msg_timed_out_delivery_error_is_timeout_and_retryable_even_though_not_marked_retriable() -> None:
    """Found by testing against a real broker: `KafkaError.retriable()` reports `False` for
    `_MSG_TIMED_OUT` (loader.py's module docstring), which would otherwise halt the partition on an
    ordinary, recoverable produce timeout — exactly the fault
    `tests/kafka/test_integration_kafka_loader.py` forces for real."""
    err = FakeKafkaError(-191, "_MSG_TIMED_OUT", False)
    producer = FakeProducer(respond=lambda args: err)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "TIMEOUT"
    assert outcome.retryable is True


def test_local_transport_delivery_error_is_retryable_even_if_marked_not_retriable() -> None:
    err = FakeKafkaError(-195, "_TRANSPORT", False)
    producer = FakeProducer(respond=lambda args: err)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.reason == "KAFKA__TRANSPORT"
    assert outcome.retryable is True


def test_non_retriable_delivery_error_is_reported_as_non_retryable() -> None:
    err = FakeKafkaError(10, "MSG_SIZE_TOO_LARGE", False)
    producer = FakeProducer(respond=lambda args: err)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.success is False
    assert outcome.reason == "KAFKA_MSG_SIZE_TOO_LARGE"
    assert outcome.retryable is False


def test_sasl_authentication_failure_is_never_retryable_even_if_marked_retriable() -> None:
    err = FakeKafkaError(96, "SASL_AUTHENTICATION_FAILED", True)
    producer = FakeProducer(respond=lambda args: err)
    loader = KafkaLoader(make_config(), producer)
    outcome = loader.deliver(make_message())
    assert outcome.reason == "KAFKA_SASL_AUTHENTICATION_FAILED"
    assert outcome.retryable is False


def test_a_timeout_explained_by_a_global_ssl_error_is_tls_verify_failed_and_not_retryable() -> None:
    sink = LastErrorSink()
    sink.error = FakeKafkaError(-181, "_SSL", False)
    producer = FakeProducer(flush_remaining=1)
    loader = KafkaLoader(make_config(), producer, error_sink=sink)
    outcome = loader.deliver(make_message())
    assert outcome.reason == "TLS_VERIFY_FAILED"
    assert outcome.retryable is False
    assert sink.error is None  # consumed, so it does not keep explaining a later, unrelated timeout


def test_a_timeout_explained_by_a_global_sasl_error_is_not_retryable() -> None:
    sink = LastErrorSink()
    sink.error = FakeKafkaError(96, "SASL_AUTHENTICATION_FAILED", False)
    producer = FakeProducer(flush_remaining=1)
    loader = KafkaLoader(make_config(), producer, error_sink=sink)
    outcome = loader.deliver(make_message())
    assert outcome.reason == "KAFKA_SASL_AUTHENTICATION_FAILED"
    assert outcome.retryable is False


def test_a_timeout_with_an_unrelated_global_error_stays_a_plain_timeout() -> None:
    sink = LastErrorSink()
    sink.error = FakeKafkaError(-195, "_TRANSPORT", True)
    producer = FakeProducer(flush_remaining=1)
    loader = KafkaLoader(make_config(), producer, error_sink=sink)
    outcome = loader.deliver(make_message())
    assert outcome.reason == "TIMEOUT"
    assert outcome.retryable is True


def test_circuit_breaker_opens_after_repeated_failures_and_skips_the_producer() -> None:
    producer = FakeProducer(queue_full=True)
    loader = KafkaLoader(make_config(circuit_breaker=CircuitBreakerSpec(2, 60.0)), producer)
    loader.deliver(make_message())
    loader.deliver(make_message())
    outcome = loader.deliver(make_message())
    assert outcome.reason == "circuit-open"
    assert len(producer.produced) == 0  # queue_full never actually appends; length always 0 here


# -- idempotency -------------------------------------------------------------------------------------


def test_duplicate_delivery_carries_the_same_header() -> None:
    """The `idempotency-key-header` technique: this loader's own half of invariant 4 is sending the
    identical header value on every attempt at the same message — the only thing README 5.1 says the
    engine can promise for a foreign sink."""
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    message = make_message()  # one message, one tb-msg-id, delivered twice (a blocking retry)

    first = loader.deliver(message)
    second = loader.deliver(message)  # simulates a redelivery of the identical record

    assert first.success is True
    assert second.success is True
    assert len(producer.produced) == 2
    first_header = dict(producer.produced[0]["headers"])
    second_header = dict(producer.produced[1]["headers"])
    assert first_header["tb-msg-id"] == second_header["tb-msg-id"] == message.msg_id.encode("utf-8")


def test_two_different_messages_carry_different_headers() -> None:
    producer = FakeProducer()
    loader = KafkaLoader(make_config(), producer)
    loader.deliver(make_message())
    loader.deliver(make_message())
    first_header = dict(producer.produced[0]["headers"])
    second_header = dict(producer.produced[1]["headers"])
    assert first_header["tb-msg-id"] != second_header["tb-msg-id"]


# -- metrics -------------------------------------------------------------------------------------


def test_metrics_snapshot_counts_by_reason() -> None:
    err = FakeKafkaError(10, "MSG_SIZE_TOO_LARGE", False)
    responses = iter([None, err])

    producer = FakeProducer(respond=lambda args: next(responses))
    loader = KafkaLoader(make_config(), producer)
    loader.deliver(make_message())
    outcome = loader.deliver(make_message())
    assert outcome.reason == "KAFKA_MSG_SIZE_TOO_LARGE"
    snapshot = loader.metrics.snapshot()
    assert snapshot["delivery_success"] == 1
    assert snapshot["delivery_KAFKA_MSG_SIZE_TOO_LARGE"] == 1
