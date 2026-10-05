"""Typed exceptions for `tributary-receivers`.

Same rule as the SDK (`tributary.sdk.errors`): no PHI in an exception message. Nothing here embeds a
payload, a record key or a header value; config errors quote field names and enum values from the
channel spec, which are operator-authored, not sender-controlled.
"""

from __future__ import annotations


class ReceiverError(Exception):
    """Base class for every error this package raises."""


class ConfigError(ReceiverError):
    """A channel spec cannot be turned into a running receiver: missing, contradictory or
    unsupported-in-this-wave configuration. Raised at startup, before any socket is opened."""


class PermanentPublishError(ReceiverError):
    """The record can never be produced as built (it breaks the envelope contract). Unlike a broker outage,
    retrying the same input cannot succeed, so callers must not answer it as "Kafka unavailable"
    (HTTP 503 + Retry-After, an SFTP file left for the next poll, a Kafka-source backoff). The message
    carries no value."""


class BindError(ReceiverError):
    """Refused to bind: the bind rule (no wildcard default, no plaintext on a non-loopback
    address) rejected the requested address, or the address is otherwise not ours to use."""


class SecretUnresolvedError(ReceiverError, FileNotFoundError):
    """A `secretRef` does not resolve to a file (not mounted yet, or a wrong key). A `FileNotFoundError` so callers
    that already treat a missing file as one keep working; distinct so a lifecycle can refuse to start (exit 2,
    `SECRET_UNRESOLVED`) instead of retrying it as if a bind had failed. The message names a ref and a key only."""

    code = "SECRET_UNRESOLVED"


#: Log-only codes for why a channel spec could not be read (not `tb-error-code` values; observability/v0 L6).
CHANNELS_UNAUTHORIZED = "CHANNELS_UNAUTHORIZED"  # the principal may not describe or read `_tb.<env>.channels`
CHANNELS_TOPIC_MISSING = "CHANNELS_TOPIC_MISSING"  # the topic does not exist (yet)
CHANNEL_NOT_DEPLOYED = "CHANNEL_NOT_DEPLOYED"  # the topic is readable and holds no spec for this channel
CHANNEL_SPEC_UNAVAILABLE = "CHANNEL_SPEC_UNAVAILABLE"  # anything else (a catch-up timeout, say)


def spec_unavailable_error(code: str) -> Exception:
    """A `ChannelSpecError` carrying `error_code`, so `lifecycle` can log why without reading message text."""
    from tributary.runtime.errors import ChannelSpecError

    exc = ChannelSpecError("channel spec unavailable")
    exc.error_code = code  # type: ignore[attr-defined]
    return exc
