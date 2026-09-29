"""Turning a `contracts/channel-spec/v0` document (`spec.source.type: mllp`) into `MllpSettings`.

The channel spec is produced strictly and validated against the schema before it is published to the
compacted `_tb.<env>.channels` topic (invariant 3; `contracts/channel-spec/v0/channel.schema.json`
`$comment`: "Producers ... validate strictly; the runtime must ignore unknown fields"). This module is
the *runtime* side of that split: it trusts the shape for fields the schema already constrains, and
never re-validates format details a producer already checked (that would just be a second copy of the
schema to keep in sync); what it does check is the handful of things that are legal per the schema but
this wave's receiver cannot yet honour, and it refuses to start rather than silently drop them — the
same "specified and quietly not applied" mistake the gate report flagged twice in Phase 0 spike code
(gate report 6, findings 1 and 2).

The spec arrives as a parsed `tributary.runtime.spec.ChannelSpec` from the live `_tb.<env>.channels`
reader (`mllp_settings_from_spec`), not from a file: invariant 3 and `mllp/reload.py` (which settings
apply live and which need a restart). `mllp_settings_from_channel_spec` keeps the published-document
shape for tests and tools; both share `_settings`.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from tributary.runtime.spec import ChannelSpec

from ..backpressure import DEFAULT_MAX_BYTES
from ..errors import ConfigError
from ..tls import TlsSpec

#: `ack.nackOn` conditions this v0 receiver can actually produce. `spec-invalid` is in the contract's
#: vocabulary (a spec can legally list it) but nothing in this wave detects it — there is no live
#: `_tb.<env>.channels` consumption to notice a spec going bad after the receiver already started —
#: so a spec that declares it is refused at startup rather than silently never firing it.
SUPPORTED_NACK_ON = frozenset({"parse-error", "queue-full"})

DEFAULT_ACK_TIMEOUT_MS = 10_000
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000  # envelope inline limit, invariant 8


@dataclass(frozen=True, slots=True)
class MllpSettings:
    """Everything one MLLP receiver instance needs, split into what the channel spec fixes
    (bind, port, tls, ack timing, message and queue limits, nackOn) and what deployment fixes
    (env, bootstrap servers, and the operational tunables the schema has no opinion on)."""

    channel: str
    channel_version: int
    bind: str
    port: int
    tls: TlsSpec | None
    ack_timeout_s: float
    nack_on: frozenset[str]
    max_message_bytes: int
    queue_max_bytes: int
    queue_max_messages: int | None
    env: str
    instance: str = "mllp-0"
    idle_timeout_s: float = 60.0
    write_timeout_s: float = 10.0
    max_conns: int = 512
    tls_handshake_timeout_s: float = 10.0
    conn_pipeline: int = 256
    max_inflight: int = 5000
    linger_ms: int = 5  # matches contracts/topics/v0 clients.receiver_producer.linger.ms
    # No delivery_timeout_ms field: that value is `acks`/`enable.idempotence`'s sibling in the
    # topics contract's must_not_hardcode list, and RawPublisher reads it from the contract
    # unconditionally (kafka_publisher.py) rather than accepting it as a parameter here.

    def __post_init__(self) -> None:
        # Every numeric value is checked here, once, so a spec that reaches a *serving* receiver is
        # refused as a `ConfigError` before anything is applied (a zero `queue.maxBytes` used to reach
        # `ByteBudget.set_limits` and raise mid-update).
        for name in ("queue_max_bytes", "max_message_bytes"):
            _positive_int(getattr(self, name), name, self.channel)
        if self.queue_max_messages is not None:
            _positive_int(self.queue_max_messages, "queue_max_messages", self.channel)
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not 0 <= self.port <= 65535:
            raise ConfigError(f"channel {self.channel!r}: source.port must be an integer in 0..65535")
        if not isinstance(self.ack_timeout_s, int | float) or self.ack_timeout_s <= 0:
            raise ConfigError(f"channel {self.channel!r}: source.ack.timeoutMs must be a positive number")
        # A frame under max_message_bytes must always be able to fit the (empty) byte budget on
        # its own, or the first max-size message to arrive would be a ByteBudget.BudgetExceeded
        # crash mid-connection instead of a config error caught at startup. The channel-spec schema
        # already makes this combination unreachable for a producer-validated spec (maxMessageBytes
        # tops out at 1_000_000, queue.maxBytes starts at 1_048_576) — this check exists for the
        # settings this module builds without going through that schema (tests, a future caller).
        if self.max_message_bytes > self.queue_max_bytes:
            raise ConfigError(
                f"channel {self.channel!r}: maxMessageBytes ({self.max_message_bytes}) exceeds "
                f"queue.maxBytes ({self.queue_max_bytes}); a single max-size message could never "
                "be admitted to the queue"
            )


def _positive_int(value: object, field: str, channel: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(f"channel {channel!r}: {field} must be a positive integer")


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def mllp_settings_from_channel_spec(spec: Mapping[str, Any], *, env: str, instance: str = "mllp-0") -> MllpSettings:
    """Build `MllpSettings` from a parsed channel-spec document. Raises `ConfigError` for anything
    this receiver cannot honour; never silently ignores a field the schema allows."""
    metadata = _require(spec, "metadata", "channel spec")
    channel = _require(metadata, "name", "metadata")
    body = _require(spec, "spec", "channel spec")
    return _settings(
        channel,
        int(metadata.get("version", 0)),
        _require(body, "ordering", "spec"),
        _require(body, "source", "spec"),
        env=env,
        instance=instance,
    )


def mllp_settings_from_spec(spec: ChannelSpec, *, env: str, instance: str = "mllp-0") -> MllpSettings:
    """Build `MllpSettings` from the runtime's parsed spec (the `_tb.<env>.channels` record)."""
    if not spec.source:
        raise ConfigError(f"channel {spec.name!r}: spec.source is absent")
    return _settings(spec.name, spec.version, spec.ordering, spec.source, env=env, instance=instance)


def _settings(
    channel: str, channel_version: int, ordering: str, source: Mapping[str, Any], *, env: str, instance: str
) -> MllpSettings:
    if ordering != "none":
        raise ConfigError(
            f"channel {channel!r}: ordering {ordering!r} is not supported by this receiver yet — "
            "per-key ordering needs the keyExpression evaluator (W2-4); ordering: none only in v0"
        )

    if source.get("type") != "mllp":
        raise ConfigError(f"channel {channel!r}: source.type is {source.get('type')!r}, not 'mllp'")

    ack = _require(source, "ack", "source")
    if ack.get("mode") != "after-commit":
        raise ConfigError("source.ack.mode must be 'after-commit' (invariant 1; the schema has no other value)")
    nack_on = frozenset(ack.get("nackOn", ()))
    unsupported = nack_on - SUPPORTED_NACK_ON
    if unsupported:
        raise ConfigError(
            f"channel {channel!r}: source.ack.nackOn declares {sorted(unsupported)}, which this "
            f"wave's receiver cannot produce (supported: {sorted(SUPPORTED_NACK_ON)}); "
            "remove them from the spec rather than have them silently never fire"
        )

    tls_obj = source.get("tls")
    # `tls_obj is not None`, never a truthiness check (security review; the same class of bug as the
    # mllp LOADER's `with.tls: {}` finding, `tributary.loaders.mllp.config`): a present-but-empty
    # `source.tls: {}` must not be silently read as `tls` being absent. `net.require_safe_bind`
    # (`server.py`) already refuses to start a non-loopback listener whose `cfg.tls is None`, so this
    # specific shape could not previously reach a running plaintext listener off-box -- but it produced
    # a confusing `BindError` there instead of the clear `ConfigError` a malformed `tls` object should
    # raise right here, and a *loopback* bind with `tls: {}` would have silently accepted a listener
    # with no TLS at all, papering over an operator's clearly-intended (if malformed) request for one.
    tls = (
        TlsSpec(
            _require(tls_obj, "secretRef", "source.tls"),
            _require(tls_obj, "clientAuth", "source.tls"),
            tls_obj.get("minVersion", "1.2"),
            tuple(tls_obj.get("allowedClientSans", ())),
        )
        if tls_obj is not None
        else None
    )

    queue = source.get("queue") or {}
    queue_max_bytes = queue.get("maxBytes", DEFAULT_MAX_BYTES)

    return MllpSettings(
        channel=channel,
        channel_version=channel_version,
        bind=_require(source, "bind", "source"),
        port=_require(source, "port", "source"),
        tls=tls,
        ack_timeout_s=ack.get("timeoutMs", DEFAULT_ACK_TIMEOUT_MS) / 1000,
        nack_on=nack_on,
        max_message_bytes=source.get("maxMessageBytes", DEFAULT_MAX_MESSAGE_BYTES),
        queue_max_bytes=queue_max_bytes,
        queue_max_messages=queue.get("maxMessages"),
        env=env,
        instance=instance,
    )
