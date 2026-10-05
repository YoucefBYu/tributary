"""The MLLP receiver's lifecycle around the live channel spec (invariant 3).

The generic part (spec feed, not-ready gating, the live-versus-restart decision, tombstone and redeploy, the
last good spec kept through a channels-topic outage) is `tributary.receivers.lifecycle`; the listener and
publisher handling shared with the HTTP receiver is `tributary.receivers.publishing`. This module adds what is
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

from ..errors import ConfigError
from ..lifecycle import (
    POLLS_PER_TICK,
    LiveSpecFeed,
    RunnerOptions,
    SpecFeed,
    print_event,
)
from ..metrics import Metrics
from ..publishing import PublishingComponent, PublishingLifecycle
from ..reply.feed import ReplyFeed
from .config import MllpSettings, mllp_settings_from_spec
from .kafka_publisher import RawPublisher
from .reload import live_fields_changed, restart_required_fields
from .server import MllpServer

__all__ = ["POLLS_PER_TICK", "LiveSpecFeed", "MllpRunner", "RunnerOptions", "SpecFeed"]


class MllpRunner(PublishingLifecycle[MllpSettings]):
    EVENT_START_RETRY = "listener_bind_failed"
    EVENT_START_REFUSED = "listener_refused"

    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[MllpSettings], RawPublisher],
        make_server: Callable[[MllpSettings, RawPublisher, Metrics], MllpServer],
        options: RunnerOptions,
        make_reply_feed: Callable[[Metrics], ReplyFeed] | None = None,
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
        self._make_reply_feed = make_reply_feed
        self._reply_feed: ReplyFeed | None = None
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
        reply_wait = cfg.reply.timeout_s if cfg.reply is not None else 0.0
        return cfg.ack_timeout_s + reply_wait + cfg.write_timeout_s

    def build_publisher(self, cfg: MllpSettings) -> RawPublisher:
        return self._make_publisher(cfg)

    def build_server(self, cfg: MllpSettings, publisher: RawPublisher) -> PublishingComponent[MllpSettings]:
        server = self._make_server(cfg, publisher, self.metrics)
        if cfg.reply is not None:
            server.use_reply_feed(self._reply_feed_or_start())
        return server

    def _reply_feed_or_start(self) -> ReplyFeed:
        """One reply consumer for the life of the process, built when the first synchronous spec starts. A
        tombstone and redeploy reuse it: a second consumer on the same `group.instance.id` would fence it."""
        if self._reply_feed is None:
            if self._make_reply_feed is None:
                raise ConfigError("source.reply is set but this process has no reply consumer")
            feed = self._make_reply_feed(self.metrics)
            feed.start(asyncio.get_running_loop())
            self._reply_feed = feed
        return self._reply_feed

    async def close_resources(self) -> None:
        if self._reply_feed is not None:
            await asyncio.to_thread(self._reply_feed.close)
        await super().close_resources()

    def started_event(
        self, cfg: MllpSettings, server: PublishingComponent[MllpSettings], started: Any
    ) -> dict[str, Any]:
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": started.sockets[0].getsockname()[1],
            "tls": cfg.tls is not None,
        }
