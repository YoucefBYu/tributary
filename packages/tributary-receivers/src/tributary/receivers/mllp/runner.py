"""The MLLP receiver's lifecycle around the live channel spec (invariant 3).

The generic part (spec feed, not-ready gating, the live-versus-restart decision, tombstone and redeploy, the
last good spec kept through a channels-topic outage) is `tributary.receivers.lifecycle`; the listener and
publisher handling shared with the HTTP receiver is `tributary.receivers.listener`. This module adds what is
MLLP's: how a spec becomes `MllpSettings`, `reload.py`'s table, and the `listening` event.

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
    RunnerOptions,
    SpecFeed,
    print_event,
)
from ..listener import ListenerLifecycle, ListenerServer
from ..metrics import Metrics
from .config import MllpSettings, mllp_settings_from_spec
from .kafka_publisher import RawPublisher
from .reload import live_fields_changed, restart_required_fields
from .server import MllpServer

__all__ = ["POLLS_PER_TICK", "LiveSpecFeed", "MllpRunner", "RunnerOptions", "SpecFeed"]


class MllpRunner(ListenerLifecycle[MllpSettings]):
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
        self._init_listener_state()

    @property
    def server(self) -> MllpServer | None:
        server = self._server
        return server if isinstance(server, MllpServer) else None

    def build_settings(self, spec: ChannelSpec) -> MllpSettings:
        return mllp_settings_from_spec(spec, env=self._opts.env, instance=self._opts.instance)

    def version_of(self, cfg: MllpSettings) -> int:
        return cfg.channel_version

    def restart_fields(self, old: MllpSettings, new: MllpSettings) -> list[str]:
        return restart_required_fields(old, new)

    def live_fields(self, old: MllpSettings, new: MllpSettings) -> list[str]:
        return live_fields_changed(old, new)

    def ordering_of(self, cfg: MllpSettings) -> str:
        return cfg.ordering

    def drain_budget_s(self, cfg: MllpSettings) -> float:
        return cfg.ack_timeout_s + cfg.write_timeout_s

    def build_publisher(self, cfg: MllpSettings) -> RawPublisher:
        return self._make_publisher(cfg)

    def build_server(self, cfg: MllpSettings, publisher: RawPublisher) -> ListenerServer[MllpSettings]:
        return self._make_server(cfg, publisher, self.metrics)

    def started_event(
        self, cfg: MllpSettings, server: ListenerServer[MllpSettings], listener: asyncio.base_events.Server
    ) -> dict[str, Any]:
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": listener.sockets[0].getsockname()[1],
            "tls": cfg.tls is not None,
        }
