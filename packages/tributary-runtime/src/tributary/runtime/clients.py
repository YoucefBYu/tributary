"""Kafka client configuration, built from the contract rather than from opinion.

`contracts/topics/v0` section 3.3 lists the consumer settings invariants 1, 4 and 5 depend on, and
`topic-defaults.json` carries the measured numbers. Nothing here invents one: `session.timeout.ms`,
`heartbeat.interval.ms` and the assignment strategy come from `TopicDefaults`.

Two settings deserve their reason in the code:

* `group.instance.id` (static membership) makes a planned restart cost only the downtime (S5: 3.21 s
  for 2.0 s down) instead of a rebalance — and makes an *unplanned* death cost the full
  `session.timeout.ms`, which is why that value is explicit and short.
* `max.poll.interval.ms` must exceed the slowest destination timeout plus its backoff, or a slow sink
  looks like a dead consumer and the group rebalances in a loop.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .config import TopicDefaults
from .spec import ChannelSpec

#: Floor for `max.poll.interval.ms`. librdkafka's default is 300 s; a shorter value would make a long
#: blocking-retry pause look like a dead consumer (ADR-003 follow-up 7).
MIN_MAX_POLL_INTERVAL_MS = 300_000


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """Connection and security settings for every client this worker builds.

    `security` is passed through verbatim so a deployment can set `security.protocol`, `sasl.*` and
    `ssl.*` without this module knowing about them. `contracts/topics/v0` 3.4 requires SASL_SSL or
    mTLS on any listener a non-local client can reach; PLAINTEXT is a loopback-only lite affordance,
    and nothing here defaults it on.

    `security` is excluded from `repr`: it holds `sasl.password` (SCRAM) and key material, and a
    dataclass repr reaches tracebacks, test output and any log line that formats the settings.
    """

    bootstrap_servers: str
    security: Mapping[str, Any] = field(default_factory=dict, repr=False)


def max_poll_interval_ms(spec: ChannelSpec) -> int:
    """The slowest destination timeout plus its backoff cap, floored at the librdkafka default."""
    worst = 0
    for destination in spec.destinations:
        timeout = destination.params.get("timeoutMs")
        timeout_ms = timeout if isinstance(timeout, int) and not isinstance(timeout, bool) else 0
        worst = max(worst, timeout_ms + destination.retry.max_delay_ms)
    return max(MIN_MAX_POLL_INTERVAL_MS, worst * 2)


def consumer_config(
    settings: KafkaSettings,
    defaults: TopicDefaults,
    *,
    group_id: str,
    group_instance_id: str | None = None,
    max_poll_interval: int = MIN_MAX_POLL_INTERVAL_MS,
    auto_offset_reset: str = "earliest",
    enable_partition_eof: bool = False,
) -> dict[str, Any]:
    """A worker or loader consumer. Manual commit, cooperative-sticky, static membership."""
    config: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "partition.assignment.strategy": defaults.consumer.partition_assignment_strategy or "cooperative-sticky",
        "session.timeout.ms": defaults.consumer.session_timeout_ms,
        "heartbeat.interval.ms": defaults.consumer.heartbeat_interval_ms,
        "max.poll.interval.ms": max_poll_interval,
        "auto.offset.reset": auto_offset_reset,
        "enable.partition.eof": enable_partition_eof,
        **dict(settings.security),
    }
    if group_instance_id is not None:
        config["group.instance.id"] = group_instance_id
    return config


def producer_config(settings: KafkaSettings, defaults: TopicDefaults, *, client_id: str) -> dict[str, Any]:
    """A durable producer: `acks=all` plus idempotence, per the invariant-1 client profile."""
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        **dict(defaults.producer),
        **dict(settings.security),
    }


def events_producer_config(settings: KafkaSettings, *, client_id: str) -> dict[str, Any]:
    """The lifecycle-event producer.

    Deliberately different from the data-plane producer: a small bounded queue and a short delivery
    timeout, because invariant 9 says nothing in message flow waits on the archive. A large queue here
    would turn an events-topic outage into worker memory growth.
    """
    return {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "acks": 1,
        "linger.ms": 50,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 50_000,
        "delivery.timeout.ms": 30_000,
        **dict(settings.security),
    }
