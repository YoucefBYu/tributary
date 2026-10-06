"""Publishing to the compacted `_tb.<env>.channels` topic (channel-spec/v0 section 6).

Record: key = UTF-8 channel name, value = the stored document's published bytes (metadata.version included),
or `None` (a tombstone) for undeploy. Reuses the runtime's engine-kafka loader and `producer_config`
(`acks=all`, idempotent producer, invariant 1); `published` means the broker acknowledged the record.

This replaces the Compose-only stand-in `deploy/compose/channel-publish/publish_channel.py` for the control
plane; the record shape is the same. Nothing in the data plane calls this (invariant 3).
"""

from __future__ import annotations

import asyncio
import threading
from typing import Any, Protocol

from tributary.runtime.topics import channels_topic


class PublishError(Exception):
    """The record was not acknowledged. `code` is the contract's `failure_code`."""

    code = "PUBLISH_FAILED"


class PublishTimeoutError(PublishError):
    """No acknowledgement in time. The outcome is unknown; re-deploying the version is the repair (README 3.5)."""

    code = "PUBLISH_TIMEOUT"


class ChannelsPublisher(Protocol):
    async def publish(self, env: str, channel: str, value: bytes | None) -> None: ...

    @property
    def degraded(self) -> bool: ...


class KafkaChannelsPublisher:
    def __init__(self, producer: Any, *, timeout_s: float) -> None:
        self._producer = producer
        self._timeout_s = timeout_s
        self._degraded = False

    @property
    def degraded(self) -> bool:
        return self._degraded

    async def publish(self, env: str, channel: str, value: bytes | None) -> None:
        try:
            await asyncio.to_thread(self._produce_and_wait, channels_topic(env), channel.encode("utf-8"), value)
        except PublishError:
            self._degraded = True
            raise
        self._degraded = False

    def _produce_and_wait(self, topic: str, key: bytes, value: bytes | None) -> None:
        from confluent_kafka import KafkaError

        done = threading.Event()
        errors: list[object] = []

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                errors.append(err)
            done.set()

        try:
            self._producer.produce(topic, key=key, value=value, on_delivery=on_delivery)
        except Exception:
            raise PublishError from None
        # The producer's delivery.timeout.ms equals this timeout, so librdkafka reports the timeout itself;
        # the wait below is only a backstop.
        deadline_hit = True
        for _ in range(int((self._timeout_s + 2.0) / 0.05)):
            self._producer.poll(0.05)
            if done.is_set():
                deadline_hit = False
                break
        if deadline_hit:
            raise PublishTimeoutError
        if errors:
            err = errors[0]
            if getattr(err, "code", lambda: None)() == KafkaError._MSG_TIMED_OUT:
                raise PublishTimeoutError
            raise PublishError

    def close(self) -> None:
        self._producer.flush(5)
