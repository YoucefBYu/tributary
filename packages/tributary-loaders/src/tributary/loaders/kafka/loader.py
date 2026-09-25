"""`KafkaLoader`: one delivery attempt of one `Message`, produced as one record on a destination's own
foreign topic (`contracts/channel-spec/v0/README.md` 3.4.7, ADR-007 D11).

## The record

The payload is sent as-is — `README 3.4.7` gives this loader no payload-shaping rule the way
`postgresql`/`clickhouse` have a row shape, because a `kafka` destination's own consumers define what
they expect on that topic, not this contract. **The key**: `key: preserve` sends the channel's own
`message.key` (usually a patient identifier — README 3.4.7 calls this a disclosure decision, stated by
the spec author, not defaulted); `key: none` sends a null key. **Headers**: only `tb-msg-id`, and only
under the *destination-chosen* header name (`idempotency.header`), when `idempotency.technique` is
`idempotency-key-header` — "no other `tb-*` header is propagated in set 1.1" (README 3.4.7). No
envelope validation runs against this record: `tributary.runtime.producer.RecordProducer` (used for the
engine's own `raw`/`out`/`retry`/`dlq`/`reply` hops) does not apply here, because this topic is not one
of this engine's topic classes at all.

## Idempotency (README 5.1)

Keyed by `tb-msg-id`, never configurable. `idempotency-key-header` is this loader's half of the
contract: the same message produces the same header value on every attempt (blocking retry redelivers
the identical `Message`; a sender's own resend mints a *new* `tb-msg-id`, a different logical delivery
on purpose) — proven by `tests/kafka/test_loader.py::test_duplicate_delivery_carries_the_same_header`.
**What this loader cannot prove, and says so rather than implying it:** whether the foreign topic's own
consumer actually deduplicates on that header is out of this engine's control (the sink is "someone
else's", README 5.1) — the confluent-kafka idempotent producer (`client.py`) only protects one
`produce()` call's own transport retries, not two separate `deliver()` calls for the same `tb-msg-id`.
`none` is the honest alternative when no such consumer exists, and needs the schema's `note`.

## Timeout, backoff, circuit breaker

One `deliver()` call is one attempt: `produce()` queues, then `flush(timeout_s)` blocks until either
every queued record's delivery report has arrived or the deadline passes. `retry.policy: blocking` (the
only value the schema allows) means the *caller* — the runtime's `ConsumerLoop`, through
`KafkaRecordHandler` — holds the partition, waits out a backoff, and calls `deliver` again with the same
message; this loader does not retry internally beyond what `client.py`'s `message.timeout.ms` already
bounds for one `flush()` call. `config.circuit_breaker`, when configured, is consulted first, same as
every other loader in this package.

## Error codes (README 3.4 table: `KAFKA_<librdkafka error name>`, plus the generic family)

* `TIMEOUT` — `flush()` returned with records still queued: no delivery report arrived in time.
  Retryable, **unless** the producer's `error_cb` most recently reported a fatal, non-transient error
  (an SSL handshake or SASL authentication failure) that explains *why* nothing will ever arrive no
  matter how long `flush()` waits — in that case the fatal error's own classification wins (see below).
  This is the same "a driver's silent internal state explains an otherwise-generic timeout" concern
  wave 2's risk section 6.3 flags for library defaults; `_ERR_SSL`/`SASL_AUTHENTICATION_FAILED` are the
  two names checked.
* `TLS_VERIFY_FAILED` — the `error_cb` reported `KafkaError._SSL` (librdkafka's certificate/hostname
  verification failure). Not retryable.
* `KAFKA_SASL_AUTHENTICATION_FAILED` — the `error_cb` reported that name. Not retryable: a bad
  credential fails identically on every attempt.
* `TIMEOUT` (again) — the delivery report itself carried `KafkaError._MSG_TIMED_OUT` or `_TIMED_OUT`.
  Retryable. **Found by testing against a real broker, not assumed**: librdkafka's own
  `KafkaError.retriable()` reports `False` for `_MSG_TIMED_OUT` — a message that timed out locally is
  not something librdkafka itself will retry internally, because from its perspective the caller (this
  loader) owns the next attempt. Trusting `.retriable()` blindly here would classify an ordinary,
  recoverable produce timeout (the exact fault this package's own duplicate-delivery integration test
  forces, `tests/kafka/test_integration_kafka_loader.py`) as non-retryable and halt the partition
  instead of retrying it — precisely the "driver default silently breaks a guarantee" shape wave 2's
  risk section 6.3 warns about, just from a judgement call instead of a mutable default.
* `KAFKA_<name>` — every other delivery-report `KafkaError`. A **local** error (librdkafka's name
  starts with `_`, e.g. `_TRANSPORT`, `_ALL_BROKERS_DOWN`) is treated as retryable regardless of
  `.retriable()`, for the same reason as `_MSG_TIMED_OUT` above: these describe *this client's* inability
  to reach a broker, not a broker's considered answer, and are exactly the kind of failure blocking
  retry exists to ride out. A **broker-returned** error (no leading `_`, e.g. `MSG_SIZE_TOO_LARGE`,
  `TOPIC_AUTHORIZATION_FAILED`) uses `KafkaError.retriable()` as intended — it is the broker's own
  judgement of its own response.
* `QUEUE_FULL` — `produce()` raised `BufferError`: the local queue is full. Retryable — back-pressure,
  not a lost message, the same reading `tributary.runtime.producer.RecordProducer` gives it.
"""

from __future__ import annotations

from typing import Any, Protocol

from tributary.sdk import Message
from tributary.sdk.context import Logger

from ..common import CircuitBreaker, DeliveryOutcome, LoaderMetrics, NullLogger, State
from .config import KafkaDestinationConfig

#: A wedged producer (every attempt timing out) is a narrower failure mode than an arbitrary HTTP
#: destination; these apply whenever `config.circuit_breaker` is not set — same reasoning as
#: `postgresql/loader.py`'s defaults.
DEFAULT_FAILURE_THRESHOLD = 5
DEFAULT_OPEN_SECONDS = 30.0

#: librdkafka error names an `error_cb` can report that explain an otherwise-generic `flush()` timeout
#: (module docstring). Checked by name, not by the numeric `KafkaError._SSL` constant, so a unit test
#: can construct the fake without importing `confluent_kafka` for the exact int value.
_SSL_ERROR_NAME = "_SSL"
_SASL_AUTH_ERROR_NAME = "SASL_AUTHENTICATION_FAILED"
#: `_MSG_TIMED_OUT`/`_TIMED_OUT` fold into the generic `TIMEOUT` reason (module docstring: found by
#: testing against a real broker — `KafkaError.retriable()` reports `False` for these, which would
#: otherwise halt the partition on an ordinary, recoverable produce timeout).
_LOCAL_TIMEOUT_NAMES = frozenset({"_MSG_TIMED_OUT", "_TIMED_OUT"})


class KafkaErrorLike(Protocol):
    def code(self) -> int: ...

    def name(self) -> str: ...

    def retriable(self) -> bool: ...


class KafkaProducerLike(Protocol):
    """The slice of `confluent_kafka.Producer` this loader depends on. Small on purpose — a unit test
    supplies a fake with no real broker; `tests/kafka/test_integration_kafka_loader.py` (`testcontainers`)
    proves the same behaviour against a real one."""

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        headers: Any = None,
        on_delivery: Any = None,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


class LastErrorSink:
    """Holds the most recent error `confluent_kafka.Producer`'s `error_cb` reported — a **global**
    error, not tied to any one record, which is the only channel librdkafka has for "every broker is
    unreachable" or "the SASL handshake failed" style failures that a delivery report alone would just
    render as an endless string of per-record timeouts. `main.py` wires one instance into both the
    `Producer` config (`error_cb=sink`) and this loader (`error_sink=`), so `deliver()` can consult it
    when a `flush()` call times out with no delivery report at all. Deliberately holds only the last
    error, not a log of every one: this loader only ever asks "does something explain the timeout I
    just saw", never "what has failed since startup"."""

    def __init__(self) -> None:
        self.error: KafkaErrorLike | None = None

    def __call__(self, err: KafkaErrorLike) -> None:
        self.error = err

    def clear(self) -> None:
        self.error = None


def _classify_delivery_error(err: KafkaErrorLike) -> tuple[str, bool]:
    name = err.name()
    if name == _SASL_AUTH_ERROR_NAME:
        return f"KAFKA_{name}", False
    if name in _LOCAL_TIMEOUT_NAMES:
        return "TIMEOUT", True
    if name.startswith("_"):
        # A local/transport-level error (this client's inability to reach a broker), not a broker's
        # own considered response: always retryable, regardless of `.retriable()` (module docstring).
        return f"KAFKA_{name}", True
    return f"KAFKA_{name}", bool(err.retriable())


def _classify_global_error(err: KafkaErrorLike) -> tuple[str, bool] | None:
    """`None` means the global error does not explain a timeout any better than a plain `TIMEOUT`
    would — e.g. a transient, already-retriable transport blip that a later attempt may still clear."""
    name = err.name()
    if name == _SSL_ERROR_NAME:
        return "TLS_VERIFY_FAILED", False
    if name == _SASL_AUTH_ERROR_NAME:
        return f"KAFKA_{name}", False
    return None


class KafkaLoader:
    def __init__(
        self,
        config: KafkaDestinationConfig,
        producer: KafkaProducerLike,
        *,
        error_sink: LastErrorSink | None = None,
        log: Logger | None = None,
    ) -> None:
        self.config = config
        self._producer = producer
        self._error_sink = error_sink if error_sink is not None else LastErrorSink()
        self._log = log if log is not None else NullLogger()
        self.metrics = LoaderMetrics()
        breaker_spec = config.circuit_breaker
        self.breaker = CircuitBreaker(
            breaker_spec.failure_threshold if breaker_spec else DEFAULT_FAILURE_THRESHOLD,
            breaker_spec.open_seconds if breaker_spec else DEFAULT_OPEN_SECONDS,
        )

    def _headers(self, message: Message) -> list[tuple[str, bytes]]:
        if self.config.idempotency_technique == "idempotency-key-header" and self.config.idempotency_header:
            return [(self.config.idempotency_header, message.msg_id.encode("utf-8"))]
        return []

    def deliver(self, message: Message) -> DeliveryOutcome:
        msg_id = message.msg_id
        if not self.breaker.allow():
            self.metrics.record("circuit-open")
            return DeliveryOutcome(success=False, reason="circuit-open", retryable=True)

        key = message.key if self.config.key_mode == "preserve" else None
        delivery: dict[str, KafkaErrorLike | None] = {"err": None}

        def _on_delivery(err: KafkaErrorLike | None, _msg: Any) -> None:
            delivery["err"] = err

        try:
            self._producer.produce(
                self.config.topic,
                value=message.payload,
                key=key,
                headers=self._headers(message),
                on_delivery=_on_delivery,
            )
        except BufferError:
            return self._record_failure(msg_id, "QUEUE_FULL", retryable=True)
        except Exception as exc:  # a malformed record confluent-kafka rejects synchronously
            return self._record_failure(msg_id, f"KAFKA_{type(exc).__name__.upper()}", retryable=False)

        remaining = self._producer.flush(self.config.timeout_s)

        err = delivery["err"]
        if err is not None:
            reason, retryable = _classify_delivery_error(err)
            return self._record_failure(msg_id, reason, retryable=retryable)

        if remaining > 0:
            global_error = self._error_sink.error
            if global_error is not None:
                classified = _classify_global_error(global_error)
                self._error_sink.clear()
                if classified is not None:
                    reason, retryable = classified
                    return self._record_failure(msg_id, reason, retryable=retryable)
            return self._record_failure(msg_id, "TIMEOUT", retryable=True)

        was_half_open = self.breaker.state is State.HALF_OPEN
        self.breaker.record_success()
        if was_half_open:
            self.metrics.circuit_closes += 1
            self._log.info("circuit_closed", destination=self.config.destination)
        self.metrics.record("success")
        return DeliveryOutcome(success=True, reason="success", retryable=False)

    def _record_failure(self, msg_id: str, reason: str, *, retryable: bool) -> DeliveryOutcome:
        was_open = self.breaker.state is State.OPEN
        self.breaker.record_failure()
        if not was_open and self.breaker.state is State.OPEN:
            self.metrics.circuit_opens += 1
            self._log.warning("circuit_open", destination=self.config.destination, reason=reason)
        self.metrics.record(reason)
        self._log.warning("delivery_failed", destination=self.config.destination, message_id=msg_id, reason=reason)
        return DeliveryOutcome(success=False, reason=reason, retryable=retryable)

    def close(self) -> None:
        self._producer.flush(5.0)
