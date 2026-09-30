"""The SFTP poller as a lifecycle component: a poll loop task, a pause for a tombstone, a bounded drain."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher
from .config import SftpSettings
from .poller import SftpPoller
from .reload import restart_required_fields
from .session import SftpSession


class SftpService:
    def __init__(
        self,
        cfg: SftpSettings,
        publisher: RawPublisher,
        session_factory: Callable[[], SftpSession],
        *,
        metrics: Metrics,
    ) -> None:
        self._poller = SftpPoller(cfg, publisher, session_factory, metrics=metrics)
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._on_failure: Callable[[BaseException], None] | None = None

    def set_failure_handler(self, handler: Callable[[BaseException], None]) -> None:
        self._on_failure = handler

    def _task_done(self, task: asyncio.Task[None]) -> None:
        """A loop that ends with an error (anything but a cancel) is reported, never swallowed."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None and self._on_failure is not None:
            self._on_failure(exc)

    @property
    def cfg(self) -> SftpSettings:
        return self._poller.cfg

    @property
    def poller(self) -> SftpPoller:
        return self._poller

    @property
    def open_connections(self) -> int:
        """1 while the poll loop runs (a cycle or the wait between two), else 0."""
        return 1 if self._task is not None and not self._task.done() else 0

    async def start(self) -> Any:
        self._task = asyncio.create_task(self._poller.run(self._stop))
        self._task.add_done_callback(self._task_done)
        return None

    async def apply_settings(self, new: SftpSettings) -> None:
        """Everything that can fail runs before `poller.apply_settings`, which is one assignment."""
        if restart_required_fields(self.cfg, new):
            raise ValueError("settings changed a field that needs a restart")
        self._poller.apply_settings(new)

    def stop_accepting(self, reason: str) -> None:
        """Pause: finish the file in flight, claim and publish nothing more. `reason` is a fixed string."""
        self._poller.pause()
        self._stop.set()

    async def drain(self, timeout_s: float) -> bool:
        """Wait, bounded, for the file in flight; a loop still running after `timeout_s` is cancelled (the file
        stays under its claim and is recovered by the next poller of this instance). True when it finished."""
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
