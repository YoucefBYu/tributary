"""The Kafka source as a lifecycle component: a consume loop task, a pause for a tombstone, a bounded drain."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher
from .config import KafkaSourceSettings
from .receiver import KafkaSourceReceiver, SourceConsumer
from .reload import restart_required_fields


class KafkaSourceService:
    def __init__(
        self, cfg: KafkaSourceSettings, publisher: RawPublisher, consumer: SourceConsumer, *, metrics: Metrics
    ) -> None:
        self._receiver = KafkaSourceReceiver(cfg, publisher, consumer, metrics=metrics)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    @property
    def cfg(self) -> KafkaSourceSettings:
        return self._receiver.cfg

    @property
    def receiver(self) -> KafkaSourceReceiver:
        return self._receiver

    @property
    def open_connections(self) -> int:
        """1 while the consume loop runs, else 0."""
        return 1 if self._task is not None and not self._task.done() else 0

    async def start(self) -> Any:
        self._task = asyncio.create_task(self._receiver.run(self._stop))
        return None

    async def apply_settings(self, new: KafkaSourceSettings) -> None:
        """Everything that can fail runs before `receiver.apply_settings`, which is one assignment."""
        if restart_required_fields(self.cfg, new):
            raise ValueError("settings changed a field that needs a restart")
        self._receiver.apply_settings(new)

    def stop_accepting(self, reason: str) -> None:
        """Pause: finish the record in flight, publish nothing more, then commit what was acked and close.
        `reason` is a fixed string."""
        self._receiver.pause()
        self._stop.set()

    async def drain(self, timeout_s: float) -> bool:
        """Wait, bounded, for the loop to commit and close the consumer. A loop still running after
        `timeout_s` is cancelled; `run`'s `finally` still commits only what was acked. True when it finished."""
        self.stop_accepting("draining")
        task = self._task
        if task is None:
            return True
        done, _ = await asyncio.wait({task}, timeout=timeout_s)
        if not done:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
            return False
        with contextlib.suppress(asyncio.CancelledError, Exception):
            task.result()
        return True
