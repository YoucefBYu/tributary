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

There is no `tributary-runtime` yet (W1-4) to consume `_tb.<env>.channels` and hand this receiver its
spec, so v0's entry point is a JSON file: the published (or, for `ordering: none` walking-skeleton
channels, the authored) form, loaded with the standard library's `json`. This is a wave-1 stand-in,
not a contract: once the runtime exists, whatever feeds it a parsed spec `dict` replaces the file read
in `mllp/server.py`'s `main`, and everything below this line is unchanged.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

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
    bootstrap_servers: str
    instance: str = "mllp-0"
    idle_timeout_s: float = 60.0
    write_timeout_s: float = 10.0
    max_conns: int = 512
    conn_pipeline: int = 256
    max_inflight: int = 5000
    linger_ms: int = 5  # matches contracts/topics/v0 clients.receiver_producer.linger.ms
    # No delivery_timeout_ms field: that value is `acks`/`enable.idempotence`'s sibling in the
    # topics contract's must_not_hardcode list, and RawPublisher reads it from the contract
    # unconditionally (kafka_publisher.py) rather than accepting it as a parameter here.

    def __post_init__(self) -> None:
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


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def mllp_settings_from_channel_spec(
    spec: Mapping[str, Any], *, env: str, bootstrap_servers: str, instance: str = "mllp-0"
) -> MllpSettings:
    """Build `MllpSettings` from a parsed channel-spec document. Raises `ConfigError` for anything
    this receiver cannot honour; never silently ignores a field the schema allows."""
    metadata = _require(spec, "metadata", "channel spec")
    channel = _require(metadata, "name", "metadata")
    channel_version = metadata.get("version", 0)

    body = _require(spec, "spec", "channel spec")
    ordering = _require(body, "ordering", "spec")
    if ordering != "none":
        raise ConfigError(
            f"channel {channel!r}: ordering {ordering!r} is not supported by this receiver yet — "
            "per-key ordering needs the keyExpression evaluator (W2-4); ordering: none only in v0"
        )

    source = _require(body, "source", "spec")
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
    tls = TlsSpec(tls_obj["secretRef"], tls_obj["clientAuth"], tls_obj.get("minVersion", "1.2")) if tls_obj else None

    queue = source.get("queue") or {}
    queue_max_bytes = queue.get("maxBytes", DEFAULT_MAX_BYTES)

    return MllpSettings(
        channel=channel,
        channel_version=int(channel_version),
        bind=_require(source, "bind", "source"),
        port=_require(source, "port", "source"),
        tls=tls,
        ack_timeout_s=ack.get("timeoutMs", DEFAULT_ACK_TIMEOUT_MS) / 1000,
        nack_on=nack_on,
        max_message_bytes=source.get("maxMessageBytes", DEFAULT_MAX_MESSAGE_BYTES),
        queue_max_bytes=queue_max_bytes,
        queue_max_messages=queue.get("maxMessages"),
        env=env,
        bootstrap_servers=bootstrap_servers,
        instance=instance,
    )
