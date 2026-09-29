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

from collections.abc import Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

from .config import TopicDefaults
from .errors import KafkaSecurityError
from .kafka_security import check_link, split_bootstrap
from .spec import ChannelSpec

#: Floor for `max.poll.interval.ms`. librdkafka's default is 300 s; a shorter value would make a long
#: blocking-retry pause look like a dead consumer (ADR-003 follow-up 7).
MIN_MAX_POLL_INTERVAL_MS = 300_000


class SecurityProperties(Mapping[str, Any]):
    """An immutable security map whose `repr` shows key names only.

    It holds `sasl.password`, so a `repr` that printed values would put a credential into any traceback,
    test failure or log line that formats the settings, including `dataclasses.asdict` output.
    """

    __slots__ = ("_items",)

    def __init__(self, items: Mapping[str, Any]) -> None:
        self._items: dict[str, Any] = dict(items)

    def __getitem__(self, key: str) -> Any:
        return self._items[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._items)

    def __len__(self) -> int:
        return len(self._items)

    def __repr__(self) -> str:
        return f"SecurityProperties(keys={sorted(self._items)!r})"

    __str__ = __repr__


@dataclass(frozen=True, slots=True)
class KafkaSettings:
    """Connection and security settings for every Kafka client a data-plane process builds.

    **Validated at construction** (`tributary.runtime.kafka_security.check_link`,
    `contracts/engine-kafka/v0` section 4): `security` may carry only the allow-listed security keys,
    `PLAINTEXT` is accepted only when every bootstrap server is loopback, TLS must verify with the AEAD
    pin, and SASL needs SCRAM-SHA-512 credentials. A violation raises `KafkaSecurityError`. There is no
    flag that relaxes any of it.

    Engine processes do not build this by hand: `tributary.runtime.engine_kafka.load_engine_kafka()`
    reads `TB_ENGINE_KAFKA_FILE` and returns one. Direct construction remains for the kafka source's
    foreign connection and for tests, and gets the same checks.

    `security` is copied into a `SecurityProperties` **before** the check, and the check runs on that
    copy, so neither a later mutation nor a mapping that answers `get` differently from its contents can
    change what was checked. Its `repr` never shows a value. The `*_config` builders below apply it **first** and the contract's
    client profile last, so it could not override `acks` or `enable.idempotence` even if the allow-list
    were widened (ADR-008 D4).
    """

    bootstrap_servers: str
    security: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        # Copy first, then check the copy, then keep the copy: a `Mapping` (or `dict` subclass) whose
        # `get` answers differently from its contents cannot pass the check with other values stored.
        if type(self.bootstrap_servers) is not str:
            raise KafkaSecurityError("bootstrap servers must be a plain str")
        if not isinstance(self.security, Mapping):
            raise KafkaSecurityError("the security map is not a mapping")
        frozen = SecurityProperties(self.security)
        check_link(self.bootstrap_servers, frozen)
        object.__setattr__(self, "security", frozen)

    @property
    def protocol(self) -> str:
        return str(self.security.get("security.protocol", "PLAINTEXT"))

    def describe(self) -> dict[str, Any]:
        """What a startup log may say about the link: protocol, mechanism and broker count, nothing else
        (`engine-kafka/v0` section 4 rule 5). No host, no principal name, no path."""
        return {
            "protocol": self.protocol,
            "sasl_mechanism": self.security.get("sasl.mechanism"),
            "client_certificate": "ssl.certificate.location" in self.security,
            "brokers": len(split_bootstrap(self.bootstrap_servers)),
        }


def max_poll_interval_ms(spec: ChannelSpec) -> int:
    """The slowest destination timeout plus its backoff cap, floored at the librdkafka default."""
    worst = 0
    for destination in spec.destinations:
        timeout = destination.params.get("timeoutMs")
        timeout_ms = timeout if isinstance(timeout, int) and not isinstance(timeout, bool) else 0
        worst = max(worst, timeout_ms + destination.retry.max_delay_ms)
    return max(MIN_MAX_POLL_INTERVAL_MS, worst * 2)


def _merge(settings: KafkaSettings, contract: Mapping[str, Any]) -> dict[str, Any]:
    """Security first, the contract's values last: a contract value always wins (ADR-008 D4)."""
    return {**dict(settings.security), **dict(contract)}


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
    contract: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "partition.assignment.strategy": defaults.consumer.partition_assignment_strategy or "cooperative-sticky",
        "session.timeout.ms": defaults.consumer.session_timeout_ms,
        "heartbeat.interval.ms": defaults.consumer.heartbeat_interval_ms,
        "max.poll.interval.ms": max_poll_interval,
        "auto.offset.reset": auto_offset_reset,
        "enable.partition.eof": enable_partition_eof,
    }
    if group_instance_id is not None:
        contract["group.instance.id"] = group_instance_id
    return _merge(settings, contract)


def producer_config(settings: KafkaSettings, defaults: TopicDefaults, *, client_id: str) -> dict[str, Any]:
    """A durable producer: `acks=all` plus idempotence, per the invariant-1 client profile.

    Until set 1.3 this merged `settings.security` last, so a security map carrying `acks` or
    `enable.idempotence` silently overrode invariant 1 (ADR-008 D4). Now the map cannot carry them and
    is merged first.
    """
    return _merge(
        settings,
        {"bootstrap.servers": settings.bootstrap_servers, "client.id": client_id, **dict(defaults.producer)},
    )


def events_producer_config(settings: KafkaSettings, *, client_id: str) -> dict[str, Any]:
    """The lifecycle-event producer.

    Deliberately different from the data-plane producer: a small bounded queue and a short delivery
    timeout, because invariant 9 says nothing in message flow waits on the archive. A large queue here
    would turn an events-topic outage into worker memory growth.
    """
    return _merge(
        settings,
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "client.id": client_id,
            "acks": 1,
            "linger.ms": 50,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 50_000,
            "delivery.timeout.ms": 30_000,
        },
    )
