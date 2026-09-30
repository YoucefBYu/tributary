"""The MLLP receiver's lifecycle around the live channel spec (invariant 3).

The generic part (spec feed, not-ready gating, the live-vs-restart decision, tombstone and redeploy, the
last good spec kept through a channels-topic outage) is `tributary.receivers.lifecycle`. This module adds
what is MLLP's: the listener, the raw publisher and the retired servers a tombstone leaves draining.
`reload.py` is the live-versus-restart table.

* **undeployed** (tombstone): `stop_accepting` closes the listener; frames already committed still get
  their ACK; a frame arriving later on an open connection is NACKed with a fixed text and the connection
  closes. A later spec for the channel opens a fresh listener.
* **stopping**: stop accepting, wait bounded for every admitted frame's ACK to be written, close the
  connections, flush the publisher.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.context import Logger

from ..lifecycle import (
    POLLS_PER_TICK,
    LiveSpecFeed,
    ReceiverLifecycle,
    RunnerOptions,
    SpecFeed,
    print_event,
)
from ..metrics import Metrics
from .config import MllpSettings, mllp_settings_from_spec
from .kafka_publisher import RawPublisher
from .reload import live_fields_changed, restart_required_fields
from .server import MllpServer

__all__ = ["POLLS_PER_TICK", "LiveSpecFeed", "MllpRunner", "RunnerOptions", "SpecFeed"]


class MllpRunner(ReceiverLifecycle[MllpSettings]):
    EVENT_START_RETRY = "listener_bind_failed"
    EVENT_START_REFUSED = "listener_refused"

    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[MllpSettings], RawPublisher],
        make_server: Callable[[MllpSettings, RawPublisher, Metrics], MllpServer],
        options: RunnerOptions,
        log: Logger,
        metrics: Metrics | None = None,
        metrics_file: str | None = None,
        emit: Callable[[dict[str, Any]], None] = print_event,
    ) -> None:
        super().__init__(
            options=options, log=log, open_feed=open_feed, metrics=metrics, metrics_file=metrics_file, emit=emit
        )
        self._make_publisher = make_publisher
        self._make_server = make_server
        self._server: MllpServer | None = None
        self._retired: list[MllpServer] = []
        self._publisher: RawPublisher | None = None
        self._publisher_ordering = "none"

    @property
    def server(self) -> MllpServer | None:
        return self._server

    # -- ReceiverLifecycle ------------------------------------------------------------------------

    def build_settings(self, spec: ChannelSpec) -> MllpSettings:
        return mllp_settings_from_spec(spec, env=self._opts.env, instance=self._opts.instance)

    def running_settings(self) -> MllpSettings | None:
        return self._server.cfg if self._server is not None else None

    def version_of(self, cfg: MllpSettings) -> int:
        return cfg.channel_version

    def restart_fields(self, old: MllpSettings, new: MllpSettings) -> list[str]:
        return restart_required_fields(old, new)

    def live_fields(self, old: MllpSettings, new: MllpSettings) -> list[str]:
        return live_fields_changed(old, new)

    def on_tick(self) -> None:
        self._retired = [s for s in self._retired if s.open_connections]

    async def apply_component(self, cfg: MllpSettings) -> None:
        server = self._server
        if server is None:
            raise RuntimeError("no running server to apply settings to")
        await server.apply_settings(cfg)

    def pause_component(self) -> None:
        server = self._server
        if server is not None:
            server.stop_accepting("channel-undeployed")
            self._retired.append(server)
            self._server = None

    async def start_component(self, cfg: MllpSettings) -> dict[str, Any] | None:
        if self._publisher is not None and self._publisher_ordering != cfg.ordering:
            # Redeployed after a tombstone with a different ordering: the producer's key rule is fixed
            # at construction, so it is rebuilt. The retired servers still hold frames whose commit is
            # in flight on the old producer: they drain first, then the old producer flushes.
            await self._drain_retired()
            old, self._publisher = self._publisher, None
            await asyncio.to_thread(old.close)
        if self._publisher is None:
            self._publisher = self._make_publisher(cfg)
            self._publisher_ordering = cfg.ordering
        server = self._make_server(cfg, self._publisher, self.metrics)
        listener = await server.start()
        self._server = server
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": listener.sockets[0].getsockname()[1],
            "tls": cfg.tls is not None,
        }

    async def _drain_retired(self) -> None:
        retired, self._retired = self._retired, []
        if retired:
            await asyncio.gather(*(s.drain(s.cfg.ack_timeout_s + s.cfg.write_timeout_s) for s in retired))

    async def stop_component(self) -> None:
        servers = ([self._server] if self._server is not None else []) + self._retired
        for server in servers:
            server.stop_accepting("shutting-down")
        if servers:
            await asyncio.gather(*(s.drain(s.cfg.ack_timeout_s + s.cfg.write_timeout_s) for s in servers))
        self._server = None
        self._retired = []

    async def close_resources(self) -> None:
        if self._publisher is not None:
            await asyncio.to_thread(self._publisher.close)
