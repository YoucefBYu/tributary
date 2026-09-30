"""The HTTP receiver's lifecycle around the live channel spec (invariant 3).

The generic part is `tributary.receivers.lifecycle`, the listener and publisher handling shared with MLLP is
`tributary.receivers.listener`; `reload.py` is the live-versus-restart table. This module adds how a spec
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

from ..lifecycle import LiveSpecFeed, RunnerOptions, SpecFeed, print_event
from ..listener import ListenerLifecycle, ListenerServer
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher
from .config import HttpSettings, http_settings_from_spec
from .reload import live_fields_changed, restart_required_fields
from .server import HttpServer

__all__ = ["HttpRunner", "LiveSpecFeed", "RunnerOptions", "SpecFeed"]


class HttpRunner(ListenerLifecycle[HttpSettings]):
    EVENT_START_RETRY = "listener_bind_failed"
    EVENT_START_REFUSED = "listener_refused"

    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[HttpSettings], RawPublisher],
        make_server: Callable[[HttpSettings, RawPublisher, Metrics], HttpServer],
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
        return cfg.ack_timeout_s + cfg.write_timeout_s

    def build_publisher(self, cfg: HttpSettings) -> RawPublisher:
        return self._make_publisher(cfg)

    def build_server(self, cfg: HttpSettings, publisher: RawPublisher) -> ListenerServer[HttpSettings]:
        return self._make_server(cfg, publisher, self.metrics)

    def started_event(
        self, cfg: HttpSettings, server: ListenerServer[HttpSettings], listener: asyncio.base_events.Server
    ) -> dict[str, Any]:
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": listener.sockets[0].getsockname()[1],
            "tls": cfg.tls is not None,
        }
