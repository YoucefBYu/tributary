"""Typed exceptions for the runtime.

Same rule as the SDK's: no exception message carries a record key, a payload, or a header value
(invariant 6). Topic names, channel names, destination names and message ids are safe and are what an
operator needs; everything else is a size or a code.
"""

from __future__ import annotations

from tributary.sdk.errors import EnvelopeError, TributaryError, safe_token

__all__ = [
    "ENGINE_KAFKA_CODES",
    "ChannelSpecError",
    "ChannelsUnavailableError",
    "ConfigError",
    "DeliveryError",
    "DlqUnwritableError",
    "EngineKafkaError",
    "KafkaSecurityError",
    "PartitionMismatchError",
    "RuntimeConfigurationError",
    "TopicNameError",
    "safe_token",
]


class RuntimeConfigurationError(TributaryError):
    """The worker cannot start with the configuration it was given.

    `code` is a stable, upper-case token an operator can alert on and a runbook can index. It is what
    `engine_kafka.load_engine_kafka_or_exit` logs first; the message is a fixed sentence built from names.
    """

    code: str = "RUNTIME_CONFIG_INVALID"


class ConfigError(RuntimeConfigurationError):
    """A contract defaults file is missing, unreadable, or does not have the shape the runtime needs."""

    code = "CONFIG_INVALID"


class KafkaSecurityError(RuntimeConfigurationError):
    """A Kafka client configuration would connect unauthenticated, unverified or weakened.

    Raised by `KafkaSettings` at construction (`contracts/engine-kafka/v0` section 4). The message names
    the rule and, at most, a property name or entry index: never a host, a credential, a path or a value.
    """

    code = "KAFKA_LINK_REFUSED"


class EngineKafkaError(RuntimeConfigurationError):
    """The engine Kafka connection document (`TB_ENGINE_KAFKA_FILE`) is missing, invalid or unresolvable.

    Same message rule as `KafkaSecurityError`: JSON paths, rule names and `secretRef` names only. `code`
    is one of `ENGINE_KAFKA_CODES` (see `engine_kafka`'s module doc for when each is raised).
    """

    code = "ENGINE_KAFKA_DOCUMENT_INVALID"

    def __init__(self, message: str, *, code: str = "ENGINE_KAFKA_DOCUMENT_INVALID") -> None:
        super().__init__(message)
        self.code = code if code in ENGINE_KAFKA_CODES else "ENGINE_KAFKA_DOCUMENT_INVALID"


#: Every `EngineKafkaError.code`. Stable: dashboards and runbooks key on them.
ENGINE_KAFKA_CODES: frozenset[str] = frozenset(
    {
        "ENGINE_KAFKA_RETIRED_INPUT",
        "ENGINE_KAFKA_FILE_UNSET",
        "ENGINE_KAFKA_FILE_UNREADABLE",
        "ENGINE_KAFKA_DOCUMENT_INVALID",
        "ENGINE_KAFKA_SECRET_UNRESOLVED",
        "ENGINE_KAFKA_LINK_REFUSED",
    }
)


class TopicNameError(RuntimeConfigurationError):
    """A topic name does not fit the grammar in `contracts/topics/v0`."""

    code = "TOPIC_NAME_INVALID"


class ChannelSpecError(RuntimeConfigurationError):
    """A record on `_tb.<env>.channels` is not a usable channel spec."""

    code = "CHANNEL_SPEC_INVALID"


class ChannelsUnavailableError(TributaryError):
    """`_tb.<env>.channels` cannot be read right now; the specs already applied are still being served.

    Not a `ChannelSpecError`: nothing is wrong with any spec, and a caller that refuses a spec on
    `ChannelSpecError` must not refuse one because a broker is down. Invariant 3 is why this is an
    exception a caller may catch and carry on from: the cache keeps every applied spec, and
    `ChannelCache.get` / `LiveSpecSource.current` keep answering from it. What the caller learns is that
    the answer may be stale. `error_code` is a librdkafka error name or a fixed token, never broker text.
    """

    code = "CHANNELS_UNAVAILABLE"

    def __init__(self, topic: str, *, error_code: str, unavailable_for_s: float) -> None:
        super().__init__(
            f"{safe_token(topic)} unreadable ({safe_token(error_code)}) for {unavailable_for_s:.1f}s; "
            "serving the last applied channel specs"
        )
        self.topic = topic
        self.error_code = error_code
        self.unavailable_for_s = unavailable_for_s


class PartitionMismatchError(RuntimeConfigurationError):
    """An `out.<dest>` topic has a different partition count from `raw` on a per-key channel.

    Refusing to start is the point: a different count changes the key-to-partition mapping, so two
    messages for one patient can be delivered by different workers out of order — invariant 5 violated
    with nothing in the logs to show for it (`contracts/topics/v0` 2).
    """

    code = "PARTITION_MISMATCH"


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
