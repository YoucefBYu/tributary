"""Lifecycle events on `_tb.events`, one per stage transition.

Invariant 9: the archive is off the critical path, so emission is
fire-and-forget. A failed event must never fail or delay message flow.
Invariant 6: the event carries ids and sizes only, never payload bytes.
"""

from __future__ import annotations

import logging
import time
from enum import StrEnum

import orjson
from confluent_kafka import Producer
from tbcommon.envelope import Envelope

log = logging.getLogger("s5.events")


class Stage(StrEnum):
    RECEIVED = "received"
    QUEUED = "queued"
    TRANSFORMED = "transformed"
    ROUTED = "routed"
    PENDING = "pending"
    SENT = "sent"
    RETRYING = "retrying"
    PARTITION_PAUSED = "partition_paused"
    PARTITION_RESUMED = "partition_resumed"
    PARTITION_HALTED = "partition_halted"
    ERROR = "error"
    DLQ = "dlq"
    FILTERED = "filtered"


class EventEmitter:
    """Non-blocking emitter. Drops events rather than blocking the data plane."""

    def __init__(self, producer: Producer, topic: str, worker: str) -> None:
        self._producer = producer
        self._topic = topic
        self._worker = worker
        self.emitted = 0
        self.dropped = 0

    def emit(
        self,
        stage: Stage,
        env: Envelope,
        *,
        status: str = "ok",
        destination: str | None = None,
        error_code: str | None = None,
        size_bytes: int | None = None,
        extra: dict[str, object] | None = None,
    ) -> None:
        event = {
            "ts": time.time(),
            "message_id": env.msg_id,
            "channel": env.channel,
            "channel_version": env.channel_version,
            "stage": str(stage),
            "status": status,
            "destination": destination,
            "attempt": env.attempt,
            "content_type": env.content_type,
            "size_bytes": size_bytes,
            "error_code": error_code,
            "worker": self._worker,
            "traceparent": env.traceparent,
            **(extra or {}),
        }
        try:
            self._producer.produce(
                self._topic,
                key=env.msg_id.encode(),
                value=orjson.dumps(event),
                headers=[("traceparent", env.traceparent.encode())],
            )
            self.emitted += 1
        except BufferError:
            # Queue full: shed the event, never the message.
            self.dropped += 1
        except Exception:  # noqa: BLE001 - observability must not break delivery
            self.dropped += 1
            log.debug("event emit failed", exc_info=True)
        finally:
            self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)
