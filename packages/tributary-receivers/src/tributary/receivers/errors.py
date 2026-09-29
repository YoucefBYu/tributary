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
