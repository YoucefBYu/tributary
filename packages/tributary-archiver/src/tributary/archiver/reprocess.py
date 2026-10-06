"""Reprocess one message: re-publish its original payload to its channel's `raw` topic.

Flow: `reprocess_facts` (two bounded aggregate queries) -> refusal rules -> raw coordinates from the events -> `PayloadSource.fetch` ->
`reinjected` -> publish with `acks=all` and an idempotent producer -> wait for the broker acknowledgement.
Identity, idempotency key and headers: see `republish`. Delivery is at-least-once.

**Where the payload comes from.** The `raw` record the archive saw: the earliest event of the message whose
`topic` is `<env>.<channel>.raw` and that carries `partition` and `offset` (the worker's `received`/`queued`
events do). No such event, no reprocess: `ReprocessRefused(NO_PAYLOAD_COORDINATES)`. The payload is the
ORIGINAL (pre-transform) record, so a message that failed in a step is re-run from the start after a code fix.

**Refused, on purpose** (ADR-003 follow-up 10, until a destination-scoped re-entry exists):

* `DESTINATION_SCOPED`: the message has a `dlq` event naming a destination. Re-injecting into `raw` would
  re-deliver to EVERY destination under an id no loader has seen, which loader idempotency cannot absorb.
  NOT BUILT: destination-scoped re-entry needs a contract change (`tb-destination` is forbidden on `raw`;
  architect). The refusal is the safe state.
* `CHILD_MESSAGE`: the message has a `parent_id` (a split child, or an earlier reprocess). Re-running the
  splitter on a child is wrong, and the header cannot tell the two cases apart (ADR-003 follow-up 10, extended).
  This over-refuses a reprocess of a reprocess; reprocess the root message instead.

**Ordering (invariant 5).** The record goes back into the message's own channel, which is live. If that channel is
`ordering: per-key` the re-injected record lands after newer records for the same key, so reprocess is refused with
`ORDERING_WOULD_BREAK` unless `allow_reorder=True`; the flag is recorded in the audit `requested` event. Unknown
channels are refused. Rule and shadow handling: `ordering`. Checked after the refusal rules and before the audit event.

**Failure.** After `requested`, a failure flushes in a best-effort step, logs `message_reprocess_failed` with the
acknowledged count (0 or 1), emits an audit `failed` event with it, and sets `error.republished` where the error
type carries it.

**Audit.** Content access: `audit(requested)` fails closed before the payload is read.
"""

from __future__ import annotations

import uuid
from contextlib import suppress
from dataclasses import dataclass

from tributary.runtime.errors import TopicNameError
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import raw_topic
from tributary.sdk import headers as h

from .audit import ACTOR, AuditEvent, AuditHook
from .errors import ArchiverError, MessageNotFound, PayloadUnavailable, PublishError, ReprocessRefused
from .ordering import ORDERING_WOULD_BREAK, TraitsLookup, order_safe
from .payload_source import PayloadRef, PayloadSource
from .query import ArchiveQueries
from .republish import RawPublisher, reinjected, reprocess_id, request_id_is_valid


@dataclass(frozen=True, slots=True)
class ReprocessResult:
    channel: str
    original_message_id: str
    new_message_id: str
    topic: str
    partition: int
    offset: int


class ReprocessService:
    def __init__(
        self,
        queries: ArchiveQueries,
        source: PayloadSource,
        publisher: RawPublisher,
        *,
        env: str,
        audit: AuditHook,
        log: JsonLogger,
        traits: TraitsLookup,
    ) -> None:
        self._traits = traits
        self._queries = queries
        self._source = source
        self._publisher = publisher
        self._env = env
        self._audit = audit
        self._log = log

    def reprocess(
        self,
        message_id: str,
        *,
        channel: str,
        request_id: str,
        actor: str,
        allow_reorder: bool = False,
    ) -> ReprocessResult:
        """`channel` is required: the caller has authorised access to THAT channel before any payload is read."""
        if not request_id_is_valid(request_id) or ACTOR.fullmatch(actor) is None:
            raise ReprocessRefused("INVALID_REQUEST")
        try:
            original = str(uuid.UUID(message_id))
            topic = raw_topic(self._env, channel)
        except (ValueError, TopicNameError):
            raise ReprocessRefused("INVALID_REQUEST") from None
        facts = self._queries.reprocess_facts(original, channel=channel, raw_topic=topic)
        if not facts.found:
            raise MessageNotFound(original)
        if facts.destination_dlq:
            raise ReprocessRefused("DESTINATION_SCOPED")
        if facts.has_parent:
            raise ReprocessRefused("CHILD_MESSAGE")
        if facts.raw_partition is None or facts.raw_offset is None:
            raise ReprocessRefused("NO_PAYLOAD_COORDINATES")
        ref = PayloadRef(topic, facts.raw_partition, facts.raw_offset, original)
        found_channel = channel
        if not order_safe(self._traits, channel=found_channel, allow_reorder=allow_reorder):
            raise ReprocessRefused(ORDERING_WOULD_BREAK)

        # Fail closed: a content access that cannot be audited does not happen.
        self._audit(
            AuditEvent(
                "message.reprocess",
                "requested",
                actor,
                found_channel,
                original,
                request_id=request_id,
                allow_reorder=allow_reorder,
            )
        )
        start_delivered = self._publisher.delivered
        new_id = reprocess_id(original, request_id)
        try:
            record = self._source.fetch(ref)
            if record.get(h.TB_CHANNEL) != found_channel:
                raise PayloadUnavailable("RECORD_MISMATCH")
            self._publisher.publish(ref.topic, reinjected(record, new_id=new_id, parent_id=original))
            (delivery,) = self._publisher.flush()
        except Exception as exc:  # any failure after `requested`: report the partial count, then re-raise
            with suppress(PublishError):  # a queued record is acknowledged or not; do not mask the cause
                self._publisher.flush()
            done = self._publisher.delivered - start_delivered
            if isinstance(exc, ArchiverError):
                exc.republished = done
            self._log.warning(
                "message_reprocess_failed",
                channel=found_channel,
                message_id=original,
                error_code=getattr(exc, "code", "ERROR"),
                republished=done,
            )
            self._audit_followup(
                AuditEvent(
                    "message.reprocess", "failed", actor, found_channel, original, request_id=request_id, count=done
                )
            )
            raise
        self._log.info(
            "message_reprocessed",
            channel=found_channel,
            message_id=new_id,
            parent_id=original,
            partition=delivery.partition,
        )
        self._audit_followup(
            AuditEvent("message.reprocess", "completed", actor, found_channel, original, request_id=request_id, count=1)
        )
        return ReprocessResult(found_channel, original, new_id, ref.topic, delivery.partition, delivery.offset)

    def _audit_followup(self, event: AuditEvent) -> None:
        try:
            self._audit(event)
        except Exception as exc:  # best effort once the work is done; class name only
            self._log.warning("audit_followup_failed", error_type=type(exc).__name__)
