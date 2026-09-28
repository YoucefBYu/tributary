"""Turning one `contracts/channel-spec/v0` destination (`spec.destinations[]`, `type: kafka`) into
`KafkaDestinationConfig`. Shape: `channel.schema.json` `$defs/kafkaParams`/`$defs/kafkaConnection`/
`$defs/kafkaSecurity`, README section 3.4.7.

## ADR-007 D11: always an explicit, own connection

A `kafka` destination **never** borrows the engine's own Kafka principal or bootstrap servers, even
when it happens to point at the same physical cluster: `with.connection` is required and carries its
own `bootstrapServers` and `security` (protocol, SASL mechanism, `secretRef`, TLS), exactly like the
(not yet built) `kafka` *source*'s connection object. This keeps `contracts/topics/v0` 3.4's ACL model
(prefix grants on `<env>.<channel>.*`) free of a foreign-topic exception, at the cost of a second
credential for a same-cluster bridge (ADR-007 D11's own cost line). `externalTopic` also refuses
`_tb.*` and anything channel-shaped, for the same reason (`channel.schema.json` `$defs/externalTopic`).

## Idempotency (README 5.1)

Only `idempotency-key-header` and `none` are permitted for `kafka` — there is no `sink-native`
mechanism this loader can prove the way `postgresql`/`clickhouse` do (a foreign topic's own consumers
are not this engine's to inspect), and `conditional-update`/`deterministic-name` have no Kafka-record
analogue. `idempotency-key-header` means every produced record carries `tb-msg-id` as a **Kafka
header** named by `idempotency.header` (README 3.4.7: "No other `tb-*` header is propagated in set
1.1" — the header name is whatever the spec's `idempotency.header` says, `tb-msg-id` in the published
example, not a literal `tb-*` name this loader invents).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, Destination

from ..errors import ConfigError

#: `idempotency.technique` values this loader accepts (README 5.1's `kafka` row).
SUPPORTED_IDEMPOTENCY_TECHNIQUES = frozenset({"idempotency-key-header", "none"})

#: `$defs/kafkaSecurity.protocol`.
SUPPORTED_SECURITY_PROTOCOLS = frozenset({"PLAINTEXT", "SSL", "SASL_SSL"})

#: `$defs/loopbackHostPort`, duplicated from the schema (defence in depth, same situation
#: `postgresql/config.py`'s `_is_loopback` and `mllp/config.py`'s `_is_loopback` document): this
#: runtime-side parser (`tributary.runtime.spec.ChannelSpec.from_document`) never runs the full JSON
#: Schema, only its own hand-written field checks — the schema's "PLAINTEXT only when every bootstrap
#: server is this host" rule (`$defs/kafkaConnection.allOf`) is otherwise enforced *only* at deploy
#: time, by the control plane, which invariant 3 says the data plane must not depend on being up, let
#: alone on having actually run against this exact document. Without this check here, a `kafka`
#: destination's `bootstrapServers` pointed at a real, non-loopback broker with `protocol: PLAINTEXT`
#: builds a config this loader would happily send PHI through in the clear (security review, High).
_LOOPBACK_HOST_PORT_RE = re.compile(r"^(localhost|127\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}|\[::1\]):[0-9]{2,5}$")

#: `$defs/externalTopic`: not `_tb.*`, not channel-shaped. Duplicated from the schema pattern rather
#: than imported (there is no shared home for JSON-Schema-derived constants yet — same situation
#: `postgresql/config.py`'s `_TABLE_RE` documents), so this is defence in depth: the control plane
#: already refuses a bad `topic` at deploy time; a hand-built `Destination` in a test would not be.
_EXTERNAL_TOPIC_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")
_TB_PREFIXED_RE = re.compile(r"^_tb\.")
_CHANNEL_SHAPED_RE = re.compile(r"^[a-z][a-z0-9-]{0,15}\.[a-z][a-z0-9-]{0,63}\.(raw|retry|dlq|reply|out\..+)$")

DEFAULT_MIN_TLS_VERSION = "1.2"


@dataclass(frozen=True, slots=True)
class KafkaTlsSpec:
    secret_ref: str | None = None
    min_version: str = DEFAULT_MIN_TLS_VERSION


@dataclass(frozen=True, slots=True)
class KafkaSecuritySpec:
    protocol: Literal["PLAINTEXT", "SSL", "SASL_SSL"]
    sasl_mechanism: str | None = None
    secret_ref: str | None = None  # SASL credentials; None for PLAINTEXT/SSL-without-mTLS-cert
    tls: KafkaTlsSpec | None = None


@dataclass(frozen=True, slots=True)
class KafkaConnectionSpec:
    bootstrap_servers: tuple[str, ...]
    security: KafkaSecuritySpec


@dataclass(frozen=True, slots=True)
class CircuitBreakerSpec:
    failure_threshold: int
    open_seconds: float


@dataclass(frozen=True, slots=True)
class KafkaDestinationConfig:
    channel: str
    destination: str
    connection: KafkaConnectionSpec
    topic: str
    key_mode: Literal["preserve", "none"]
    timeout_s: float
    idempotency_technique: str
    idempotency_header: str | None
    circuit_breaker: CircuitBreakerSpec | None
    backoff: str
    initial_delay_ms: int
    max_delay_ms: int
    max_attempts: int
    on_exhaust: str
    extra_producer_config: Mapping[str, Any] = field(default_factory=dict)


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def _security_from_params(security: Mapping[str, Any], destination: str) -> KafkaSecuritySpec:
    protocol = security.get("protocol")
    if protocol not in SUPPORTED_SECURITY_PROTOCOLS:
        raise ConfigError(
            f"destination {destination!r}: with.connection.security.protocol {protocol!r} is not "
            f"supported (supported: {sorted(SUPPORTED_SECURITY_PROTOCOLS)})"
        )
    tls_obj = security.get("tls")
    # `tls_obj is not None`, never a truthiness check (security review, High, the same class of bug as
    # `7ed8343`'s TLS-failure classification and the `mllp` empty-`tls`-block finding): `tls: {}` is a
    # present-but-empty object -- "use the platform trust store" ($defs/clientTls's own comment) -- and
    # `if tls_obj` treats it exactly like `tls` being absent, silently dropping the explicit
    # `ssl.endpoint.identification.algorithm=https` this loader's `client.py` only sets when
    # `security.tls is not None`.
    tls = (
        KafkaTlsSpec(tls_obj.get("secretRef"), tls_obj.get("minVersion", DEFAULT_MIN_TLS_VERSION))
        if tls_obj is not None
        else None
    )
    if protocol == "SASL_SSL":
        mechanism = security.get("saslMechanism")
        if mechanism != "SCRAM-SHA-512":
            raise ConfigError(
                f"destination {destination!r}: with.connection.security.saslMechanism {mechanism!r} is "
                "not supported (supported: ['SCRAM-SHA-512'])"
            )
        secret_ref = _require(security, "secretRef", "with.connection.security")
        return KafkaSecuritySpec(protocol, mechanism, secret_ref, tls)
    if protocol == "PLAINTEXT":
        if security.get("secretRef") is not None or tls is not None:
            raise ConfigError(
                f"destination {destination!r}: with.connection.security.protocol PLAINTEXT cannot carry "
                "a secretRef or tls (README 3.4.7 / $defs/kafkaSecurity)"
            )
        return KafkaSecuritySpec(protocol, None, None, None)
    return KafkaSecuritySpec(protocol, None, security.get("secretRef"), tls)  # SSL, optional mTLS cert


def _connection_from_params(params: Mapping[str, Any], destination: str) -> KafkaConnectionSpec:
    connection = _require(params, "connection", "with")
    bootstrap = _require(connection, "bootstrapServers", "with.connection")
    if not isinstance(bootstrap, list) or not bootstrap or not all(isinstance(b, str) for b in bootstrap):
        raise ConfigError(f"destination {destination!r}: with.connection.bootstrapServers must be a non-empty list")
    security = _require(connection, "security", "with.connection")
    security_spec = _security_from_params(security, destination)
    if security_spec.protocol == "PLAINTEXT" and not all(_LOOPBACK_HOST_PORT_RE.match(b) for b in bootstrap):
        # Defence in depth for `$defs/kafkaConnection.allOf`'s "PLAINTEXT only when every bootstrap
        # server is this host" (module constant's docstring, security review, High): this runtime-side
        # parser must refuse the same shape the schema refuses at deploy time, not trust that every
        # document it is ever handed already passed that check.
        raise ConfigError(
            f"destination {destination!r}: with.connection.security.protocol PLAINTEXT is only allowed "
            "when every bootstrapServers entry is loopback (README 3.4.7 / $defs/kafkaConnection.allOf)"
        )
    return KafkaConnectionSpec(tuple(bootstrap), security_spec)


def _validate_topic(topic: Any, destination: str) -> str:
    if not isinstance(topic, str) or not _EXTERNAL_TOPIC_RE.match(topic):
        raise ConfigError(f"destination {destination!r}: with.topic {topic!r} is not a valid external topic name")
    if _TB_PREFIXED_RE.match(topic) or _CHANNEL_SHAPED_RE.match(topic) or topic in (".", ".."):
        raise ConfigError(
            f"destination {destination!r}: with.topic {topic!r} names a platform or channel-shaped topic "
            "(README 3.4.7 / $defs/externalTopic); a kafka destination may only target a foreign topic"
        )
    return topic


def kafka_destination_config_from_destination(channel: str, destination: Destination) -> KafkaDestinationConfig:
    """Build `KafkaDestinationConfig` from an already-parsed `tributary.runtime.spec.Destination`.
    Raises `ConfigError` for anything this loader cannot honour."""
    if destination.type != "kafka":
        raise ConfigError(f"destination {destination.name!r}: type is {destination.type!r}, not 'kafka'")

    if destination.idempotency.technique not in SUPPORTED_IDEMPOTENCY_TECHNIQUES:
        raise ConfigError(
            f"destination {destination.name!r}: idempotency.technique "
            f"{destination.idempotency.technique!r} is not supported by this loader "
            f"(supported: {sorted(SUPPORTED_IDEMPOTENCY_TECHNIQUES)}; README 5.1: a kafka destination "
            "has no sink-native, conditional-update or deterministic-name mechanism this loader can prove)"
        )

    params = destination.params
    name = destination.name
    connection = _connection_from_params(params, name)
    topic = _validate_topic(_require(params, "topic", "with"), name)

    key_mode = _require(params, "key", "with")
    if key_mode not in ("preserve", "none"):
        raise ConfigError(f"destination {name!r}: with.key {key_mode!r} must be 'preserve' or 'none'")

    idempotency_header = None
    if destination.idempotency.technique == "idempotency-key-header":
        idempotency_header = destination.idempotency.header
        if not idempotency_header:
            raise ConfigError(f"destination {name!r}: idempotency.technique idempotency-key-header needs a header")

    breaker_obj = params.get("circuitBreaker")
    breaker = (
        CircuitBreakerSpec(breaker_obj["failureThreshold"], float(breaker_obj["openSeconds"])) if breaker_obj else None
    )

    retry = destination.retry
    return KafkaDestinationConfig(
        channel=channel,
        destination=name,
        connection=connection,
        topic=topic,
        key_mode=key_mode,
        timeout_s=_require(params, "timeoutMs", "with") / 1000,
        idempotency_technique=destination.idempotency.technique,
        idempotency_header=idempotency_header,
        circuit_breaker=breaker,
        backoff=retry.backoff,
        initial_delay_ms=retry.initial_delay_ms,
        max_delay_ms=retry.max_delay_ms,
        max_attempts=retry.max_attempts,
        on_exhaust=retry.on_exhaust,
    )


def kafka_destination_config_from_channel_spec(
    spec: Mapping[str, Any], destination_name: str
) -> KafkaDestinationConfig:
    """Parse the whole document with `ChannelSpec.from_document` and build the one destination."""
    try:
        channel_spec = ChannelSpec.from_document(spec)
        destination = channel_spec.destination(destination_name)
    except ChannelSpecError as exc:
        raise ConfigError(str(exc)) from exc
    return kafka_destination_config_from_destination(channel_spec.name, destination)
