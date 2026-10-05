"""The Kafka-source receiver's lifecycle around the live channel spec (invariant 3).

The generic part is `tributary.receivers.lifecycle`, the publisher and component handling shared with the other
publishing receivers is `tributary.receivers.publishing`; `reload.py` is the live-versus-restart table.

* **undeployed** (tombstone): consumption pauses after the record in flight; the consumer commits **only the
  offsets that were acked** and closes. A record fetched but not published is re-read after a redeploy. A later
  spec starts a new consumer once the old one has committed and closed (two members must not share a static id).
* **stopping**: the same pause, bounded by the ACK timeout plus a fixed allowance for the commit, then the
  publisher flushes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.context import Logger

from ..lifecycle import LiveSpecFeed, RunnerOptions, SpecFeed
from ..metrics import Metrics
from ..mllp.kafka_publisher import RawPublisher
from ..publishing import PublishingComponent, PublishingLifecycle
from .config import KafkaSourceSettings, kafka_source_settings_from_spec
from .reload import live_fields_changed, restart_required_fields
from .service import KafkaSourceService

__all__ = ["KafkaSourceRunner", "LiveSpecFeed", "RunnerOptions", "SpecFeed"]

#: Allowance, beyond the ACK timeout, for the poll in progress and the final synchronous commit and close.
DRAIN_ALLOWANCE_S = 15.0


class KafkaSourceRunner(PublishingLifecycle[KafkaSourceSettings]):
    RECEIVER_TYPE = "kafka"
    EVENT_START_RETRY = "consumer_start_failed"
    EVENT_START_REFUSED = "consumer_refused"
    DRAIN_RETIRED_BEFORE_START = True

    def __init__(
        self,
        *,
        open_feed: Callable[[], SpecFeed],
        make_publisher: Callable[[KafkaSourceSettings], RawPublisher],
        make_service: Callable[[KafkaSourceSettings, RawPublisher, Metrics], KafkaSourceService],
        options: RunnerOptions,
        log: Logger,
        commit_every: int | None = None,
        metrics: Metrics | None = None,
        metrics_file: str | None = None,
        emit: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        super().__init__(
            options=options, log=log, open_feed=open_feed, metrics=metrics, metrics_file=metrics_file, emit=emit
        )
        self._make_publisher = make_publisher
        self._make_service = make_service
        self._commit_every = commit_every
        self._init_listener_state()

    @property
    def service(self) -> KafkaSourceService | None:
        server = self._server
        return server if isinstance(server, KafkaSourceService) else None

    def build_settings(self, spec: ChannelSpec) -> KafkaSourceSettings:
        return kafka_source_settings_from_spec(
            spec, env=self._opts.env, instance=self._opts.instance, commit_every=self._commit_every
        )

    def version_of(self, cfg: KafkaSourceSettings) -> int:
        return cfg.channel_version

    def restart_fields(self, old: KafkaSourceSettings, new: KafkaSourceSettings) -> list[str]:
        return restart_required_fields(old, new)

    def live_fields(self, old: KafkaSourceSettings, new: KafkaSourceSettings) -> list[str]:
        return live_fields_changed(old, new)

    def ordering_of(self, cfg: KafkaSourceSettings) -> str:
        return cfg.ordering

    def drain_budget_s(self, cfg: KafkaSourceSettings) -> float:
        return cfg.ack_timeout_s + cfg.poll_timeout_s + DRAIN_ALLOWANCE_S

    def build_publisher(self, cfg: KafkaSourceSettings) -> RawPublisher:
        return self._make_publisher(cfg)

    def build_server(
        self, cfg: KafkaSourceSettings, publisher: RawPublisher
    ) -> PublishingComponent[KafkaSourceSettings]:
        return self._make_service(cfg, publisher, self.metrics)

    def started_event(
        self, cfg: KafkaSourceSettings, server: PublishingComponent[KafkaSourceSettings], started: Any
    ) -> dict[str, Any]:
        # `listening` and `port: 0` keep the readiness line the spawn helpers wait for; a consumer has no socket.
        return {
            "event": "listening",
            "channel": cfg.channel,
            "channel_version": cfg.channel_version,
            "topic": self._opts.topic,
            "port": 0,
            "tls": False,
        }
