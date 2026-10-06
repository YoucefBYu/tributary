"""How the consumer loop makes progress durable: plain offset commits, or Kafka transactions.

`ConsumerLoop` does not care which. It calls `before_record()` before handing a record to the handler,
`commit(offsets)` when a window is complete, and `abort()` when something inside the window failed.

**`OffsetCommitter`** is the at-least-once default: the handler has already waited for every
`out.<dest>` delivery, so a synchronous `consumer.commit` afterwards is commit-after-delivery.

**`TransactionalCommitter`** is ADR-003 D6's opt-in exactly-once for `raw` -> `out.<dest>`
(`pipeline.transactional: true`). Everything produced in a window - `out` records and DLQ records -
and the input offsets commit atomically, or none of it does. The rules, each learned in S5:

* **One `transactional.id` per worker, stable across restarts** - the contract's template
  `tb.<env>.<channel>.pipeline.<instance>`. `init_transactions` then fences the previous incarnation
  and aborts whatever it left open. Since KIP-447 the group metadata passed to
  `send_offsets_to_transaction` fences zombies, so the id must *not* follow the assignment: under
  cooperative-sticky the assignment changes while the worker lives.
* **Offsets go in the transaction, never through `consumer.commit`.** An auto or manual commit
  alongside the transaction races it and breaks atomicity.
* **A transaction must not stay open.** A downstream `read_committed` consumer cannot read past an open
  transaction (the last stable offset), so a quiet channel with a half-full window would stall every
  loader behind it. The loop therefore also commits on age and on an idle poll (`should_commit_now`).
* **`message.timeout.ms` must not exceed `transaction.timeout.ms`**: librdkafka refuses to construct
  the producer otherwise. The message timeout is clamped down rather than the transaction timeout
  raised, because the transaction timeout is how long readers stall behind a producer that died with a
  transaction open.
* **An abort discards the whole window**, including records from partitions that did nothing wrong, so
  the loop rewinds every partition in the window to where the window began, not only the failed one.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from confluent_kafka import KafkaException, TopicPartition
from tributary.sdk.context import Logger

from .errors import DeliveryError

#: Oldest an open transaction may get before the loop commits it regardless of `commit_every`.
#: JUDGEMENT, unmeasured: well under a human-visible delay, and S5 measured commit p50 < 1 ms, p99 78 ms.
DEFAULT_MAX_TRANSACTION_AGE_S = 0.1

#: Bounded retries of a retriable `commit_transaction` error before treating it as abortable.
COMMIT_RETRIES = 3


class TransactionalProducerLike(Protocol):
    def init_transactions(self, timeout: float) -> None: ...

    def begin_transaction(self) -> None: ...

    def send_offsets_to_transaction(
        self, positions: list[TopicPartition], group_metadata: Any, timeout: float
    ) -> None: ...

    def commit_transaction(self, timeout: float) -> None: ...

    def abort_transaction(self, timeout: float) -> None: ...


class GroupMetadataSource(Protocol):
    def consumer_group_metadata(self) -> Any: ...


class Committer(Protocol):
    """What the consumer loop needs to make a window of progress durable."""

    @property
    def transactional(self) -> bool: ...

    def before_record(self) -> None: ...

    def commit(self, offsets: list[TopicPartition]) -> None: ...

    def abort(self) -> None: ...

    def should_commit_now(self, *, idle: bool) -> bool: ...


class OffsetCommitter:
    """At-least-once: a synchronous offset commit after the handler has seen every delivery."""

    transactional = False

    def __init__(self, consumer: Any) -> None:
        self._consumer = consumer

    def before_record(self) -> None:
        return None

    def commit(self, offsets: list[TopicPartition]) -> None:
        self._consumer.commit(offsets=offsets, asynchronous=False)

    def abort(self) -> None:
        return None

    def should_commit_now(self, *, idle: bool) -> bool:
        return False


class TransactionError(DeliveryError):
    """A transaction could not be committed and was aborted: the window must be re-consumed."""


class TransactionalCommitter:
    """Read-process-write in one transaction per window."""

    transactional = True

    def __init__(
        self,
        producer: TransactionalProducerLike,
        consumer: GroupMetadataSource,
        *,
        log: Logger,
        timeout_s: float = 30.0,
        max_age_s: float = DEFAULT_MAX_TRANSACTION_AGE_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._producer = producer
        self._consumer = consumer
        self._log = log
        self._timeout_s = timeout_s
        self._max_age_s = max_age_s
        self._clock = clock
        self._opened_at: float | None = None
        self.committed = 0
        self.aborted = 0

    def initialise(self) -> None:
        """Fence any previous incarnation with this `transactional.id` and abort what it left open."""
        self._producer.init_transactions(self._timeout_s)

    @property
    def open(self) -> bool:
        return self._opened_at is not None

    def before_record(self) -> None:
        if self._opened_at is None:
            self._producer.begin_transaction()
            self._opened_at = self._clock()

    def should_commit_now(self, *, idle: bool) -> bool:
        if self._opened_at is None:
            return False
        return idle or self._clock() - self._opened_at >= self._max_age_s

    def commit(self, offsets: list[TopicPartition]) -> None:
        """Put the offsets in the transaction and commit it. Raises `TransactionError` after an abort."""
        if self._opened_at is None:
            if not offsets:
                return
            self.before_record()
        try:
            if offsets:
                self._producer.send_offsets_to_transaction(
                    offsets, self._consumer.consumer_group_metadata(), self._timeout_s
                )
            self._commit_with_retries()
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if error is not None and getattr(error, "fatal", lambda: False)():
                # Fenced by a newer incarnation, or the producer is unusable: nothing here can recover.
                self._opened_at = None
                raise
            self.abort()
            raise TransactionError("TRANSACTION_ABORTED", retryable=True, detail=_code(error)) from exc
        self._opened_at = None
        self.committed += 1

    def _commit_with_retries(self) -> None:
        for attempt in range(COMMIT_RETRIES + 1):
            try:
                self._producer.commit_transaction(self._timeout_s)
                return
            except KafkaException as exc:
                error = exc.args[0] if exc.args else None
                retriable = error is not None and getattr(error, "retriable", lambda: False)()
                if not retriable or attempt == COMMIT_RETRIES:
                    raise

    def abort(self) -> None:
        if self._opened_at is None:
            return
        self._opened_at = None
        try:
            self._producer.abort_transaction(self._timeout_s)
        finally:
            self.aborted += 1
            self._log.warning("transaction_aborted", aborted_total=self.aborted)


def _code(error: Any) -> str:
    """A librdkafka error *name* (`_TIMED_OUT`), never its message string."""
    try:
        return str(error.name())
    except Exception:  # diagnostics only: any failure reads as "unknown"
        return "unknown"


def transactional_id(template: str, *, env: str, channel: str, instance: str) -> str:
    """Fill the contract's `transactional.id` template (`topic-defaults.json`, never restated here)."""
    return template.replace("<env>", env).replace("<channel>", channel).replace("<instance>", instance)


def transactional_producer_config(
    base: Mapping[str, Any], *, transactional_id_value: str, transaction_timeout_ms: int = 60_000
) -> dict[str, Any]:
    """The durable producer config plus transactions, with `message.timeout.ms` clamped (see above)."""
    config = dict(base)
    config["transactional.id"] = transactional_id_value
    config["transaction.timeout.ms"] = transaction_timeout_ms
    delivery = int(config.pop("delivery.timeout.ms", transaction_timeout_ms))
    config["message.timeout.ms"] = min(delivery, transaction_timeout_ms)
    return config
