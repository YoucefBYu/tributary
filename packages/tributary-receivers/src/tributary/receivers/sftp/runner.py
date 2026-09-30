"""The SFTP receiver's lifecycle around the live channel spec (invariant 3).

The generic part is `tributary.receivers.lifecycle`, the publisher and component handling shared with the other
publishing receivers is `tributary.receivers.publishing`; `reload.py` is the live-versus-restart table.

* **undeployed** (tombstone): polling pauses after the file in flight, which is committed and archived or
  handed back. Nothing new is claimed or published. A later spec starts a new poller once the paused one is done.
* **stopping**: the same pause, bounded by the ACK timeout plus the operation timeout, then the publisher
  flushes. A claim left behind is recovered by the next poller of this instance (README 3.2.2).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.context import Logger

from ..lifecycle import LiveSpecFeed, RunnerOptions, SpecFeed, print_event
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher
from ..publishing import PublishingComponent, PublishingLifecycle
from .config import SftpSettings, sftp_settings_from_spec
from .reload import live_fields_changed, restart_required_fields
from .service import SftpService

__all__ = ["LiveSpecFeed", "RunnerOptions", "SftpRunner", "SpecFeed"]


class SftpRunner(PublishingLifecycle[SftpSettings]):
    EVENT_START_RETRY = "poller_start_failed"
    EVENT_START_REFUSED = "poller_refused"
    DRAIN_RETIRED_BEFORE_START = True

    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[SftpSettings], RawPublisher],
        make_service: Callable[[SftpSettings, RawPublisher, Metrics], SftpService],
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
        self._make_service = make_service
        self._init_listener_state()

    @property
    def service(self) -> SftpService | None:
        server = self._server
        return server if isinstance(server, SftpService) else None

    def build_settings(self, spec: ChannelSpec) -> SftpSettings:
        return sftp_settings_from_spec(spec, env=self._opts.env, instance=self._opts.instance)

    def version_of(self, cfg: SftpSettings) -> int:
        return cfg.channel_version

    def restart_fields(self, old: SftpSettings, new: SftpSettings) -> list[str]:
        return restart_required_fields(old, new)

    def live_fields(self, old: SftpSettings, new: SftpSettings) -> list[str]:
        return live_fields_changed(old, new)

    def ordering_of(self, cfg: SftpSettings) -> str:
        return cfg.ordering

    def drain_budget_s(self, cfg: SftpSettings) -> float:
        return cfg.ack_timeout_s + cfg.op_timeout_s + cfg.connect_timeout_s

    def build_publisher(self, cfg: SftpSettings) -> RawPublisher:
        return self._make_publisher(cfg)

    def build_server(self, cfg: SftpSettings, publisher: RawPublisher) -> PublishingComponent[SftpSettings]:
        return self._make_service(cfg, publisher, self.metrics)

    def started_event(
        self, cfg: SftpSettings, server: PublishingComponent[SftpSettings], started: Any
    ) -> dict[str, Any]:
        # `listening` and `port: 0` keep the readiness line the spawn helpers wait for; a poller has no socket.
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": 0,
            "tls": False,
        }
