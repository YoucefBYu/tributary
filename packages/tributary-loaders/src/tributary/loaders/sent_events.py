"""The `sent` lifecycle event (`contracts/events/v0` 2.1), emitted by a loader after its destination acknowledged.

`with_sent_events(handler, ...)` wraps a loader's `RecordHandler` (the object `ConsumerLoop` calls once per
record). When `handle` returns normally, the destination has confirmed the delivery, so one
`MessageEvent(stage="sent", status="ok")` goes to the existing `EventEmitter`. Nothing else changes:

* **Fire-and-forget (invariants 1 and 9).** The event is produced after the handler returned and before the loop
  marks the offset; `KafkaEventEmitter.emit` only enqueues (`on_delivery=None`) and swallows its own failures, and
  this wrapper additionally swallows anything the emitter or the event constructor raises. A slow, full or absent
  events topic can neither fail, delay nor reorder a delivery or a commit. The handler's exceptions propagate
  untouched, so retry, DLQ and halt behave exactly as before, and no `sent` is emitted for a failed attempt.
* **Re-delivery.** A record delivered again (a replay after a crash, a window abort) emits `sent` again with the
  same `(message_id, stage, destination, attempt)` when it succeeds on the same try, which the archive collapses
  (events README 2.2 and 7 rule 1).
* **`attempt`.** The 1-based number of the delivery try that succeeded, counted the way `retrying` and
  `partition_paused` count failures (`ConsumerLoop` numbers the first failure `1`): a first-try success is `1`, a
  success after two failed tries is `3`. The loop does not hand its retry state to a handler, so the wrapper counts
  consecutive failures of the same `(topic, partition, offset)` itself, one entry per partition (bounded). After a
  restart the count restarts, which can only repeat a dedup key, never invent a gap.
* **PHI (invariant 6).** The event carries ids, the destination name, coordinates, sizes, the content type and the
  trace parent. No payload byte, header value other than those, or exception text.
"""

from __future__ import annotations

import time
from typing import Any

from tributary.runtime.consumer import RecordHandler
from tributary.runtime.errors import DeliveryError
from tributary.runtime.events import EventEmitter, MessageEvent
from tributary.runtime.records import ConsumedRecord
from tributary.sdk import headers as h
from tributary.sdk.context import Logger

__all__ = ["SentEventHandler", "with_sent_events"]


class SentEventHandler:
    def __init__(
        self,
        inner: RecordHandler,
        *,
        emitter: EventEmitter,
        channel: str,
        channel_version: int,
        instance: str,
        destination: str,
        log: Logger | None = None,
    ) -> None:
        self._inner = inner
        self._emitter = emitter
        self._channel = channel
        self._channel_version = channel_version
        self._instance = instance
        self._destination = destination
        self._log = log
        self._failures: dict[tuple[str, int], tuple[int, int]] = {}  # (topic, partition) -> (offset, failed tries)
        self._emit_failing = False

    def handle(self, record: ConsumedRecord) -> None:
        started = time.monotonic()
        key = (record.topic, record.partition)
        try:
            self._inner.handle(record)
        except DeliveryError:
            offset, failed = self._failures.get(key, (record.offset, 0))
            self._failures[key] = (record.offset, (failed if offset == record.offset else 0) + 1)
            raise
        except BaseException:
            self._failures.pop(key, None)  # not a delivery verdict (a step failure, a stop): count afresh
            raise
        offset, failed = self._failures.pop(key, (record.offset, 0))
        self._emit_sent(record, attempt=(failed if offset == record.offset else 0) + 1, started=started)

    def _emit_sent(self, record: ConsumedRecord, *, attempt: int, started: float) -> None:
        message_id = record.msg_id
        if not message_id:
            return  # an event needs an id; the loop would have rejected a record without one before dispatch
        try:
            claim_check = record.message.get(h.TB_CLAIM_CHECK)
            payload = record.message.payload
            self._emitter.emit(
                MessageEvent(
                    stage="sent",
                    status="ok",
                    channel=self._channel,
                    channel_version=self._channel_version,
                    instance=self._instance,
                    message_id=message_id,
                    destination=self._destination,
                    attempt=attempt,
                    content_type=record.message.get(h.TB_CONTENT_TYPE),
                    size_bytes=0 if claim_check else (len(payload) if payload is not None else None),
                    claim_check=claim_check,
                    topic=record.topic,
                    partition=record.partition,
                    offset=record.offset,
                    duration_ms=(time.monotonic() - started) * 1000,
                    traceparent=record.message.get(h.TRACEPARENT),
                    parent_id=record.message.get(h.TB_PARENT_ID),
                    correlation_id=record.message.get(h.TB_CORRELATION_ID),
                )
            )
        except Exception as exc:  # observability must never fail a delivery that already succeeded
            if self._log is not None and not self._emit_failing:
                self._emit_failing = True
                self._log.warning("sent_event_failed", error_type=type(exc).__name__)

    def __getattr__(self, name: str) -> Any:
        """Everything else (`metrics_snapshot`, `close`, ...) is the wrapped handler's."""
        return getattr(self._inner, name)


def with_sent_events(
    handler: RecordHandler,
    *,
    emitter: EventEmitter,
    channel: str,
    channel_version: int,
    instance: str,
    destination: str,
    log: Logger | None = None,
) -> SentEventHandler:
    return SentEventHandler(
        handler,
        emitter=emitter,
        channel=channel,
        channel_version=channel_version,
        instance=instance,
        destination=destination,
        log=log,
    )
