"""Turning a `contracts/channel-spec/v0` document (`spec.source.type: kafka`) into `KafkaSourceSettings`.

A Kafka source reads a topic on a cluster that is **always configured explicitly with its own
credentials**, even when that is the same physical cluster the engine runs on (ADR-007 D11): nothing
here reads the engine's bootstrap servers or principal for the source side. The engine-side producer
to `<env>.<channel>.raw` is a separate connection (`TB_ENGINE_KAFKA_FILE`, the receiver principal).

Refused at startup, because the schema allows it but this receiver cannot honour it or it would be
unsafe: a `tls.minVersion` other than the default (librdkafka has no property that enforces a TLS
minimum version, so accepting `1.3` would be a promise nothing keeps; the same refusal
`tributary.loaders.kafka` makes), a `PLAINTEXT` connection to a non-loopback broker, a topic that is
`_tb.*` or channel-shaped, and any `ack.nackOn` value other than `parse-error` (a Kafka source has no
NACK: a refused record halts its partition, D16, so `queue-full` and `spec-invalid` cannot fire).

The spec arrives as a parsed `tributary.runtime.spec.ChannelSpec` from the live `_tb.<env>.channels` reader
(`kafka_source_settings_from_spec`), and `kafka_source/reload.py` says which settings apply live: every value
applied live is checked here, so a bad update is a `ConfigError` before anything is applied.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.keys import KeyExpression

from ..errors import ConfigError
from ..keys import key_expression_from_spec
from ..net import is_loopback_host_port

DEFAULT_ACK_TIMEOUT_MS = 10_000
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000  # envelope inline limit, invariant 8
DEFAULT_MIN_TLS_VERSION = "1.2"
SUPPORTED_NACK_ON = frozenset({"parse-error"})

_HOST_PORT = re.compile(r"^(\[::1\]|[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?):[0-9]{2,5}$")
_EXTERNAL_TOPIC = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_CHANNEL_SHAPED = re.compile(r"^[a-z][a-z0-9-]{0,15}\.[a-z][a-z0-9-]{0,63}\.(raw|retry|dlq|reply|out\..+)$")
_INSTANCE = re.compile(r"^[a-z0-9][a-z0-9-]{0,31}$")


@dataclass(frozen=True, slots=True)
class SourceTls:
    secret_ref: str | None = field(default=None, repr=False)
    min_version: str = DEFAULT_MIN_TLS_VERSION


@dataclass(frozen=True, slots=True)
class SourceSecurity:
    protocol: Literal["PLAINTEXT", "SSL", "SASL_SSL"]
    sasl_mechanism: str | None = None
    secret_ref: str | None = field(default=None, repr=False)
    tls: SourceTls | None = None


@dataclass(frozen=True, slots=True)
class KafkaSourceSettings:
    channel: str
    channel_version: int
    content_type: str
    source_bootstrap: tuple[str, ...]
    security: SourceSecurity
    topic: str
    start_from: Literal["earliest", "latest"]
    nack_on: frozenset[str]
    key_expression: KeyExpression | None
    max_message_bytes: int
    ack_timeout_s: float
    env: str
    instance: str = "kafka-0"
    commit_every: int = 50
    poll_timeout_s: float = 1.0
    commit_flush_interval_s: float = 1.0
    unavailable_backoff_initial_s: float = 1.0
    unavailable_backoff_max_s: float = 30.0
    #: `spec.ordering` and the `spec.partitionKey` text. Fixed for the life of the process (`kafka_source/reload.py`).
    ordering: str = "none"
    partition_key: str | None = None

    @property
    def group_id(self) -> str:
        """`tb.<env>.<channel>.receiver` (`topics/v0` 3.6): on the source cluster, under the source's credentials."""
        return f"tb.{self.env}.{self.channel}.receiver"

    def __post_init__(self) -> None:
        if (self.ordering == "per-key") != (self.key_expression is not None):
            raise ConfigError(f"channel {self.channel!r}: ordering per-key needs a key expression, none forbids one")
        if isinstance(self.max_message_bytes, bool) or not isinstance(self.max_message_bytes, int):
            raise ConfigError(f"channel {self.channel!r}: maxMessageBytes must be a positive integer")
        if self.max_message_bytes < 1:
            raise ConfigError(f"channel {self.channel!r}: maxMessageBytes must be a positive integer")
        if not isinstance(self.ack_timeout_s, int | float) or isinstance(self.ack_timeout_s, bool):
            raise ConfigError(f"channel {self.channel!r}: source.ack.timeoutMs must be a positive number")
        if not 0 < self.ack_timeout_s < float("inf"):
            raise ConfigError(f"channel {self.channel!r}: source.ack.timeoutMs must be a positive number")
        if self.start_from not in ("earliest", "latest"):
            raise ConfigError(f"channel {self.channel!r}: kafka.startFrom must be 'earliest' or 'latest'")
        if not _INSTANCE.match(self.instance):
            raise ConfigError(f"instance {self.instance!r} must match {_INSTANCE.pattern}")
        if self.max_message_bytes > DEFAULT_MAX_MESSAGE_BYTES:
            raise ConfigError(f"channel {self.channel!r}: maxMessageBytes exceeds the inline limit (invariant 8)")
        if self.commit_every < 1:
            raise ConfigError("commit_every must be at least 1")
        if not self.source_bootstrap or len(self.source_bootstrap) > 16:
            raise ConfigError(f"channel {self.channel!r}: kafka.connection.bootstrapServers must list 1-16 servers")
        if not all(_HOST_PORT.match(b) for b in self.source_bootstrap):
            raise ConfigError(f"channel {self.channel!r}: kafka.connection.bootstrapServers entry is not host:port")
        if self.security.protocol == "PLAINTEXT" and not all(is_loopback_host_port(b) for b in self.source_bootstrap):
            raise ConfigError(
                f"channel {self.channel!r}: PLAINTEXT is only allowed when every bootstrap server is loopback"
            )
        if (
            not _EXTERNAL_TOPIC.match(self.topic)
            or self.topic.startswith("_tb.")
            or _CHANNEL_SHAPED.match(self.topic)
            or self.topic in (".", "..")
        ):
            raise ConfigError(
                f"channel {self.channel!r}: kafka.topic is a platform or channel-shaped topic; a kafka source "
                "may only read a topic outside the engine's own layout (ADR-007 D11)"
            )


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def parse_security(obj: Mapping[str, Any], channel: str) -> SourceSecurity:
    protocol = obj.get("protocol")
    if protocol not in ("PLAINTEXT", "SSL", "SASL_SSL"):
        raise ConfigError(f"channel {channel!r}: kafka.connection.security.protocol is not supported")
    tls_obj = obj.get("tls")
    # `is not None`, never truthiness: `tls: {}` is "use the platform trust store", still explicit TLS
    # settings, and must not be read as `tls` being absent.
    tls = (
        SourceTls(tls_obj.get("secretRef"), tls_obj.get("minVersion", DEFAULT_MIN_TLS_VERSION))
        if tls_obj is not None
        else None
    )
    if tls is not None and tls.min_version != DEFAULT_MIN_TLS_VERSION:
        raise ConfigError(
            f"channel {channel!r}: kafka.connection.security.tls.minVersion {tls.min_version!r} cannot be "
            "enforced (librdkafka has no TLS minimum-version property); declare the default or use another type"
        )
    if protocol == "SASL_SSL":
        if obj.get("saslMechanism") != "SCRAM-SHA-512":
            raise ConfigError(f"channel {channel!r}: kafka.connection.security.saslMechanism must be SCRAM-SHA-512")
        return SourceSecurity(protocol, "SCRAM-SHA-512", _require(obj, "secretRef", "kafka.connection.security"), tls)
    if protocol == "PLAINTEXT":
        if obj.get("secretRef") is not None or tls is not None:
            raise ConfigError(f"channel {channel!r}: PLAINTEXT cannot carry a secretRef or tls")
        return SourceSecurity(protocol)
    return SourceSecurity(protocol, None, obj.get("secretRef"), tls)


def kafka_source_settings_from_channel_spec(
    spec: Mapping[str, Any],
    *,
    env: str,
    instance: str = "kafka-0",
    commit_every: int | None = None,
) -> KafkaSourceSettings:
    """Build settings from a parsed channel spec. `commit_every` defaults to the topics contract's
    consumer value (`load_topic_defaults`), which is also this source's replay window."""
    metadata = _require(spec, "metadata", "channel spec")
    body = _require(spec, "spec", "channel spec")
    return _settings(
        _require(metadata, "name", "metadata"),
        int(metadata.get("version", 0)),
        _require(body, "ordering", "spec"),
        _require(body, "source", "spec"),
        partition_key=body.get("partitionKey"),
        env=env,
        instance=instance,
        commit_every=commit_every,
    )


def kafka_source_settings_from_spec(
    spec: ChannelSpec, *, env: str, instance: str = "kafka-0", commit_every: int | None = None
) -> KafkaSourceSettings:
    """Build settings from the runtime's parsed spec (the `_tb.<env>.channels` record)."""
    if not spec.source:
        raise ConfigError(f"channel {spec.name!r}: spec.source is absent")
    return _settings(
        spec.name,
        spec.version,
        spec.ordering,
        spec.source,
        partition_key=spec.partition_key,
        env=env,
        instance=instance,
        commit_every=commit_every,
    )


def _settings(
    channel: str,
    channel_version: int,
    ordering: str,
    source: Mapping[str, Any],
    *,
    partition_key: str | None,
    env: str,
    instance: str,
    commit_every: int | None,
) -> KafkaSourceSettings:
    if source.get("type") != "kafka":
        raise ConfigError(f"channel {channel!r}: source.type is {source.get('type')!r}, not 'kafka'")
    ack = _require(source, "ack", "source")
    if ack.get("mode") != "after-commit":
        raise ConfigError("source.ack.mode must be 'after-commit' (invariant 1; the schema has no other value)")
    nack_on = frozenset(ack.get("nackOn", ()))
    if nack_on - SUPPORTED_NACK_ON:
        raise ConfigError(
            f"channel {channel!r}: source.ack.nackOn declares {sorted(nack_on - SUPPORTED_NACK_ON)}; a kafka "
            "source has no NACK (a refused record halts its partition, ADR-007 D16), so only 'parse-error' "
            "(which selects the JSON well-formedness check) is meaningful"
        )
    content_type = _require(source, "contentType", "source")
    kafka = _require(source, "kafka", "source")
    connection = _require(kafka, "connection", "source.kafka")
    key_expression = key_expression_from_spec(
        {"ordering": ordering, "partitionKey": partition_key},
        channel=channel,
        source_type="kafka",
        content_types=frozenset({content_type}),
    )
    if commit_every is None:
        from tributary.runtime.config import load_topic_defaults

        commit_every = load_topic_defaults().consumer.commit_every
    return KafkaSourceSettings(
        channel=channel,
        channel_version=channel_version,
        content_type=content_type,
        source_bootstrap=tuple(_require(connection, "bootstrapServers", "source.kafka.connection")),
        security=parse_security(_require(connection, "security", "source.kafka.connection"), channel),
        topic=_require(kafka, "topic", "source.kafka"),
        start_from=_require(kafka, "startFrom", "source.kafka"),
        nack_on=nack_on,
        key_expression=key_expression,
        max_message_bytes=source.get("maxMessageBytes", DEFAULT_MAX_MESSAGE_BYTES),
        ack_timeout_s=ack.get("timeoutMs", DEFAULT_ACK_TIMEOUT_MS) / 1000,
        env=env,
        instance=instance,
        commit_every=commit_every,
        ordering=ordering,
        partition_key=partition_key,
    )
