"""Which Kafka-source settings a new `spec.version` may change in a running receiver, and which it may not.

Invariant 3: the receiver's spec comes from the compacted `_tb.<env>.channels` topic and is refreshed while it
runs. A republished spec must never need a restart for the settings that can change safely, and must never be
half-applied for the ones that cannot.

**Live (each record takes one snapshot when it is handled; nothing is applied mid-record):**

| Setting | Spec field | Takes effect |
|---|---|---|
| ACK conditions and timeout | `source.ack.nackOn`, `source.ack.timeoutMs` | the next record |
| Size limit | `source.maxMessageBytes` | the next record |
| Content type | `source.contentType` | the next record (the key locator is re-checked against it) |
| Stamped channel version | `metadata.version` | the next record published |

**Tombstone pauses consumption.** The record in flight is published and its offset recorded; then the consumer
commits **only what was acked** and closes. Nothing fetched but not published is committed, so a redeploy
re-reads it. The process stays up, not ready. A later spec for the channel starts a new consumer, after the old
one has committed and closed, and it resumes from the committed offset.

**Restart required (logged as `spec_restart_required`, then a clean exit 0 for the supervisor):**

| Setting | Why it cannot change in place |
|---|---|
| `source.kafka.topic` | the subscription: a different topic is a different stream, and the committed offsets belong to the old one |
| the consumer group | derived from the env and channel (`tb.<env>.<channel>.receiver`), fixed with the process |
| `source.kafka.connection` (`bootstrapServers`, `security`: protocol, mechanism, secretRef, TLS) | the foreign cluster's connection and credentials are built into the consumer once |
| `source.kafka.startFrom` | `auto.offset.reset` is fixed at consumer construction; a live change would be quietly not applied |
| `spec.ordering`, `spec.partitionKey` | the raw producer's key rule is fixed at construction; a key that changes under a running channel could reorder a patient's records (invariant 5) |

**Refused (last good spec keeps serving):** anything `kafka_source_settings_from_spec` raises for: a non-`kafka`
source, `nackOn` other than `parse-error`, a topic that is platform or channel shaped, a non-loopback `PLAINTEXT`
connection, a TLS minimum version librdkafka cannot enforce, a non-positive limit.

Field names only are ever returned or logged here, never values: a topic name and a bootstrap list are
operator input, and a `secretRef` is a path.
"""

from __future__ import annotations

from .config import KafkaSourceSettings

RESTART_REQUIRED_FIELDS = (
    "source_bootstrap",
    "security",
    "topic",
    "start_from",
    "ordering",
    "partition_key",
)

LIVE_FIELDS = (
    "channel_version",
    "content_type",
    "nack_on",
    "max_message_bytes",
    "ack_timeout_s",
)


def restart_required_fields(old: KafkaSourceSettings, new: KafkaSourceSettings) -> list[str]:
    """The restart-only fields that differ, by name (never a value)."""
    return [name for name in RESTART_REQUIRED_FIELDS if getattr(old, name) != getattr(new, name)]


def live_fields_changed(old: KafkaSourceSettings, new: KafkaSourceSettings) -> list[str]:
    """The live fields that differ, by name."""
    return [name for name in LIVE_FIELDS if getattr(old, name) != getattr(new, name)]
