"""Turning a `contracts/channel-spec/v0` document (`spec.source.type: http`) into `HttpSettings`.

The spec arrives as a parsed `tributary.runtime.spec.ChannelSpec` from the live `_tb.<env>.channels`
reader (`http_settings_from_spec`), not from a file: invariant 3 and `http/reload.py` (which settings apply
live and which need a restart). `http_settings_from_channel_spec` keeps the published-document shape for
tests and tools; both share `_settings`. The schema is trusted for shapes it already constrains; this module
refuses the legal shapes this receiver cannot honour, and every numeric or structural value it will apply, so
a bad live update is a `ConfigError` before anything is applied.

README 3.2.3: "It is an ingest endpoint, not a FHIR server" -- one fixed `POST` path. A FHIR-ingest
channel is an `http` source whose `accept` includes `application/fhir+json`; there is no separate
`fhir` source type in the contract (3.0's table lists only `mllp`, `http`, `sftp`, `kafka`), so this
module is also what a "FHIR receiver" is built from.

Known gaps, stated rather than hidden:

* `auth: mtls` authenticates that the client certificate chains to the trust bundle in `tls.secretRef`.
  Without `tls.allowedClientSans` (set 1.3, README 3.2.7) every certificate that chains is accepted; with
  it, the leaf must carry a listed SAN, enforced after the handshake and before any byte is read.
* `ack.nackOn: spec-invalid` is schema-valid but refused, as `mllp` does: it means "NACK because the
  channel spec became invalid", and nothing detects that, so accepting it would promise a NACK that can
  never fire. The architect may overrule.
* Publish timeouts can duplicate: a 503 after `ack.timeoutMs` does not cancel the produce already handed
  to librdkafka, which may still be delivered when the broker returns (`delivery.timeout.ms` is longer
  than the ACK timeout). The sender's retry then lands a second record under a new `tb-msg-id`.
  At-least-once, never loss; downstream idempotency keyed on `tb-msg-id` does not absorb it
  (README 3.2.2 "Duplicates at ingest").
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from tributary.runtime.spec import ChannelSpec
from tributary.sdk.keys import KeyExpression

from ..backpressure import DEFAULT_MAX_BYTES
from ..errors import ConfigError
from ..keys import key_expression_from_spec
from ..net import is_loopback
from ..tls import TlsSpec

DEFAULT_ACK_TIMEOUT_MS = 10_000
DEFAULT_MAX_MESSAGE_BYTES = 1_000_000  # envelope inline limit, invariant 8

#: `parse-error` and `queue-full` are the two conditions this receiver can produce. `queue-full` (503)
#: and a malformed request or unresolved per-key key (400) fire whether or not they are declared --
#: README 3.2.2 fixes those codes -- and declaring `parse-error` additionally makes a body that is
#: not well-formed JSON a 400 when the matched type is JSON. `spec-invalid` is refused, as in
#: `mllp.config`: nothing detects it yet, so accepting it would be a promise that never fires.
SUPPORTED_NACK_ON = frozenset({"parse-error", "queue-full"})

_AUTH_MODES = frozenset({"bearer", "mtls", "none"})


@dataclass(frozen=True, slots=True)
class AuthSpec:
    mode: str  # "bearer" | "mtls" | "none"
    secret_ref: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if self.mode not in _AUTH_MODES:
            raise ValueError(f"auth mode must be one of {sorted(_AUTH_MODES)}, not {self.mode!r}")
        if self.mode == "bearer" and not self.secret_ref:
            raise ValueError("auth mode 'bearer' requires secretRef")


@dataclass(frozen=True, slots=True)
class HttpSettings:
    channel: str
    channel_version: int
    bind: str
    port: int
    tls: TlsSpec | None
    path: str
    accept: frozenset[str]
    auth: AuthSpec
    key_expression: KeyExpression | None
    nack_on: frozenset[str]
    ack_timeout_s: float
    max_message_bytes: int
    queue_max_bytes: int
    queue_max_messages: int | None
    env: str
    instance: str = "http-0"
    idle_timeout_s: float = 30.0
    request_timeout_s: float = 30.0  # whole head + body, not per read: a trickling sender cannot outlast it
    tls_handshake_timeout_s: float = 10.0
    write_timeout_s: float = 10.0
    max_conns: int = 512
    max_header_bytes: int = 16_384
    max_inflight: int = 5000
    linger_ms: int = 5
    #: `spec.ordering` and, for `per-key`, the parsed `spec.partitionKey`. Fixed for the life of the process
    #: (`http/reload.py`): the raw producer's key rule is set at construction (invariant 5).
    ordering: str = "none"
    partition_key: str | None = None

    def __post_init__(self) -> None:
        # Every value that is applied live is checked here, once, so a bad update is refused as a
        # `ConfigError` before anything is applied (the MLLP zero-`queue.maxBytes` lesson).
        if (self.ordering == "per-key") != (self.key_expression is not None):
            raise ConfigError(f"channel {self.channel!r}: ordering per-key needs a key expression, none forbids one")
        for name in ("queue_max_bytes", "max_message_bytes"):
            _positive_int(getattr(self, name), name, self.channel)
        if self.queue_max_messages is not None:
            _positive_int(self.queue_max_messages, "queue_max_messages", self.channel)
        if isinstance(self.port, bool) or not isinstance(self.port, int) or not 0 <= self.port <= 65535:
            raise ConfigError(f"channel {self.channel!r}: source.port must be an integer in 0..65535")
        if not isinstance(self.bind, str) or not self.bind:
            raise ConfigError(f"channel {self.channel!r}: source.bind must be a non-empty string")
        if not isinstance(self.ack_timeout_s, int | float) or self.ack_timeout_s <= 0:
            raise ConfigError(f"channel {self.channel!r}: source.ack.timeoutMs must be a positive number")
        if not isinstance(self.path, str):
            raise ConfigError(f"channel {self.channel!r}: source.path must be a string")
        if not self.accept or not all(isinstance(t, str) and t for t in self.accept):
            raise ConfigError(f"channel {self.channel!r}: source.accept must list at least one content type")
        if self.max_message_bytes > self.queue_max_bytes:
            raise ConfigError(
                f"channel {self.channel!r}: maxMessageBytes ({self.max_message_bytes}) exceeds "
                f"queue.maxBytes ({self.queue_max_bytes}); a single max-size message could never "
                "be admitted to the queue"
            )
        if self.max_message_bytes > DEFAULT_MAX_MESSAGE_BYTES:
            raise ConfigError(
                f"channel {self.channel!r}: maxMessageBytes exceeds the {DEFAULT_MAX_MESSAGE_BYTES}-byte "
                "inline limit (invariant 8; claim-check is not built)"
            )
        if not self.path.startswith("/") or "?" in self.path:
            raise ConfigError(f"channel {self.channel!r}: path must be absolute and carry no query string")
        if self.auth.mode == "none" and not is_loopback(self.bind):
            raise ConfigError(f"channel {self.channel!r}: auth mode 'none' is only permitted on a loopback bind")
        if self.auth.mode == "mtls" and (self.tls is None or self.tls.client_auth != "required"):
            raise ConfigError(f"channel {self.channel!r}: auth.mode 'mtls' requires tls.clientAuth: required")


def _positive_int(value: object, field: str, channel: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(f"channel {channel!r}: {field} must be a positive integer")


def _require(mapping: Mapping[str, Any], key: str, where: str) -> Any:
    try:
        return mapping[key]
    except KeyError:
        raise ConfigError(f"{where}: missing required field {key!r}") from None


def http_settings_from_channel_spec(spec: Mapping[str, Any], *, env: str, instance: str = "http-0") -> HttpSettings:
    """Build `HttpSettings` from a parsed channel-spec document. Raises `ConfigError` for anything
    this receiver cannot honour; never silently ignores a field the schema allows."""
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
    )


def http_settings_from_spec(spec: ChannelSpec, *, env: str, instance: str = "http-0") -> HttpSettings:
    """Build `HttpSettings` from the runtime's parsed spec (the `_tb.<env>.channels` record)."""
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
) -> HttpSettings:
    if source.get("type") != "http":
        raise ConfigError(f"channel {channel!r}: source.type is {source.get('type')!r}, not 'http'")

    ack = _require(source, "ack", "source")
    if ack.get("mode") != "after-commit":
        raise ConfigError("source.ack.mode must be 'after-commit' (invariant 1; the schema has no other value)")
    nack_on = frozenset(ack.get("nackOn", ()))
    if nack_on - SUPPORTED_NACK_ON:
        raise ConfigError(
            f"channel {channel!r}: source.ack.nackOn declares {sorted(nack_on - SUPPORTED_NACK_ON)}, which "
            f"this receiver cannot produce (supported: {sorted(SUPPORTED_NACK_ON)}); remove them from the "
            "spec rather than have them silently never fire"
        )

    tls_obj = source.get("tls")
    tls = (
        TlsSpec(
            _require(tls_obj, "secretRef", "source.tls"),
            _require(tls_obj, "clientAuth", "source.tls"),
            tls_obj.get("minVersion", "1.2"),
            tuple(tls_obj.get("allowedClientSans", ())),
        )
        if tls_obj is not None  # `is not None`, never truthiness: `tls: {}` still means "configured"
        else None
    )

    auth_obj = _require(source, "auth", "source")
    auth = AuthSpec(mode=_require(auth_obj, "type", "source.auth"), secret_ref=auth_obj.get("secretRef"))

    accept = frozenset(_require(source, "accept", "source"))
    if not accept:
        raise ConfigError(f"channel {channel!r}: source.accept must list at least one content type")

    key_expression = key_expression_from_spec(
        {"ordering": ordering, "partitionKey": partition_key},
        channel=channel,
        source_type="http",
        content_types=accept,
    )

    queue = source.get("queue") or {}
    queue_max_bytes = queue.get("maxBytes", DEFAULT_MAX_BYTES)

    return HttpSettings(
        channel=channel,
        channel_version=channel_version,
        bind=_require(source, "bind", "source"),
        port=_require(source, "port", "source"),
        tls=tls,
        path=_require(source, "path", "source"),
        accept=accept,
        auth=auth,
        key_expression=key_expression,
        nack_on=nack_on,
        ack_timeout_s=ack.get("timeoutMs", DEFAULT_ACK_TIMEOUT_MS) / 1000,
        max_message_bytes=source.get("maxMessageBytes", DEFAULT_MAX_MESSAGE_BYTES),
        queue_max_bytes=queue_max_bytes,
        queue_max_messages=queue.get("maxMessages"),
        env=env,
        instance=instance,
        ordering=ordering,
        partition_key=partition_key,
    )
