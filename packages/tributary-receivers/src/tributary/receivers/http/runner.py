"""The HTTP receiver's lifecycle around the live channel spec (invariant 3).

The generic part is `tributary.receivers.lifecycle`, the listener and publisher handling shared with MLLP is
`tributary.receivers.publishing`; `reload.py` is the live-versus-restart table. This module adds how a spec
becomes `HttpSettings` and the `listening` event.

* **undeployed** (tombstone): the listener closes; a request already handed to Kafka still gets its answer;
  one that arrives later on an open connection gets `503` with `Retry-After`, nothing published. A later spec
  for the channel opens a fresh listener.
* **stopping**: stop accepting, wait bounded for every committed request's response to be written, close the
  connections, flush the publisher.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import Any

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.context import Logger

from ..errors import ConfigError
from ..lifecycle import LiveSpecFeed, RunnerOptions, SpecFeed
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher
from ..publishing import PublishingComponent, PublishingLifecycle
from ..reply.feed import ReplyFeed
from .config import HttpSettings, http_settings_from_spec
from .reload import live_fields_changed, restart_required_fields
from .server import HttpServer

__all__ = ["HttpRunner", "LiveSpecFeed", "RunnerOptions", "SpecFeed"]


class HttpRunner(PublishingLifecycle[HttpSettings]):
    RECEIVER_TYPE = "http"
    EVENT_START_RETRY = "listener_bind_failed"
    EVENT_START_REFUSED = "listener_refused"

    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[HttpSettings], RawPublisher],
        make_server: Callable[[HttpSettings, RawPublisher, Metrics], HttpServer],
        options: RunnerOptions,
        make_reply_feed: Callable[[Metrics], ReplyFeed] | None = None,
        log: Logger,
        metrics: Metrics | None = None,
        metrics_file: str | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
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
    def server(self) -> HttpServer | None:
        server = self._server
        return server if isinstance(server, HttpServer) else None

    def build_settings(self, spec: ChannelSpec) -> HttpSettings:
        return http_settings_from_spec(spec, env=self._opts.env, instance=self._opts.instance)

    def version_of(self, cfg: HttpSettings) -> int:
        return cfg.channel_version

    def restart_fields(self, old: HttpSettings, new: HttpSettings) -> list[str]:
        return restart_required_fields(old, new)

    def live_fields(self, old: HttpSettings, new: HttpSettings) -> list[str]:
        return live_fields_changed(old, new)

    def ordering_of(self, cfg: HttpSettings) -> str:
        return cfg.ordering

    def drain_budget_s(self, cfg: HttpSettings) -> float:
        reply_wait = cfg.reply.timeout_s if cfg.reply is not None else 0.0
        return cfg.ack_timeout_s + reply_wait + cfg.write_timeout_s

    def build_publisher(self, cfg: HttpSettings) -> RawPublisher:
        return self._make_publisher(cfg)

    def build_server(self, cfg: HttpSettings, publisher: RawPublisher) -> PublishingComponent[HttpSettings]:
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
            feed.fatal_handler = lambda _exc: self.report_fatal()
            feed.start(asyncio.get_running_loop())
            self._reply_feed = feed
        return self._reply_feed

    def sync_reply_required(self, cfg: HttpSettings) -> bool:
        return cfg.reply is not None

    def reply_consumer_reason(self, cfg: HttpSettings) -> str | None:
        feed = self._reply_feed
        return "no_partition" if feed is None else feed.readiness_reason()

    async def close_resources(self) -> None:
        if self._reply_feed is not None:
            await asyncio.to_thread(self._reply_feed.close)
        await super().close_resources()

    def started_event(
        self, cfg: HttpSettings, server: PublishingComponent[HttpSettings], started: Any
    ) -> dict[str, Any]:
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": started.sockets[0].getsockname()[1],
            "tls": cfg.tls is not None,
        }
