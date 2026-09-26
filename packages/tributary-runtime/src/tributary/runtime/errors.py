"""Typed exceptions for the runtime.

Same rule as the SDK's: no exception message carries a record key, a payload, or a header value
(invariant 6). Topic names, channel names, destination names and message ids are safe and are what an
operator needs; everything else is a size or a code.
"""

from __future__ import annotations

from tributary.sdk.errors import EnvelopeError, TributaryError, safe_token

__all__ = [
    "ChannelSpecError",
    "ConfigError",
    "DeliveryError",
    "DlqUnwritableError",
    "PartitionMismatchError",
    "RuntimeConfigurationError",
    "TopicNameError",
    "safe_token",
]


class RuntimeConfigurationError(TributaryError):
    """The worker cannot start with the configuration it was given."""


class ConfigError(RuntimeConfigurationError):
    """A contract defaults file is missing, unreadable, or does not have the shape the runtime needs."""


class TopicNameError(RuntimeConfigurationError):
    """A topic name does not fit the grammar in `contracts/topics/v0`."""


class ChannelSpecError(RuntimeConfigurationError):
    """A record on `_tb.<env>.channels` is not a usable channel spec."""


class PartitionMismatchError(RuntimeConfigurationError):
    """An `out.<dest>` topic has a different partition count from `raw` on a per-key channel.

    Refusing to start is the point: a different count changes the key-to-partition mapping, so two
    messages for one patient can be delivered by different workers out of order — invariant 5 violated
    with nothing in the logs to show for it (`contracts/topics/v0` 2).
    """


class DeliveryError(TributaryError):
    """A destination or a produce attempt failed. Carries a stable `tb-error-code`, never sink text."""

    def __init__(self, error_code: str, *, retryable: bool = True, detail: str = "") -> None:
        super().__init__(f"{safe_token(error_code)}{f': {detail}' if detail else ''}")
        self.error_code = error_code
        self.retryable = retryable
        self.detail = detail


class DlqUnwritableError(EnvelopeError):
    """A dead letter cannot be written because its record breaks the envelope even when reduced.

    A raw record may exceed the envelope's 1,000,000-byte inline limit and still be accepted by the
    broker (`max.message.bytes` is 1,048,576), and no `dlq` projection of it fits. The loop halts the
    partition rather than crash: a crash replays the same record for ever, and committing would drop it.
    """
