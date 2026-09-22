"""Lifecycle events on `_tb.<env>.events`, per `contracts/events/v0`.

Every stage transition emits one. Two kinds share the topic: `message` events (a message crossed a
stage) and `partition` events (blocking retry paused, resumed or halted a partition), discriminated by
`kind`, because an operator's "why did delivery stop" and a message's timeline are the same stream.

**Emission is fire-and-forget (invariant 9).** The archive is off the critical path, so a full producer
queue drops the event and increments a counter; it never blocks a message and never fails one. Events
may therefore be lost and, after a restart, duplicated — consumers are idempotent on
`(message_id, stage, destination, attempt)` (events contract 2.2).

PHI: an event carries ids, codes, sizes and short attrs. Never payload bytes, never the record key,
and `error_message` is redacted static text — an f-string built from an exception is a Critical finding
(events contract section 3), so `DeliveryError.error_code` plus a fixed message is what reaches here.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import orjson
from tributary.sdk.errors import AttrError, TributaryError, safe_token
from tributary.sdk.guards import validate_attrs
from tributary.sdk.headers import format_timestamp

EVENT_VERSION = 0

MESSAGE_STAGES: frozenset[str] = frozenset(
    {
        "received",
        "queued",
        "filtered",
        "transformed",
        "routed",
        "pending",
        "sent",
        "retrying",
        "replied",
        "error",
        "dlq",
    }
)
PARTITION_STAGES: frozenset[str] = frozenset({"partition_paused", "partition_resumed", "partition_halted"})
STATUSES: frozenset[str] = frozenset({"ok", "error", "timeout", "dropped"})

MAX_ERROR_MESSAGE = 512

Status = Literal["ok", "error", "timeout", "dropped"]


class EventError(TributaryError):
    """An event would not satisfy `contracts/events/v0`. Raised at construction, before emission."""


def now_text() -> str:
    """The contract's RFC 3339 UTC timestamp with millisecond precision.

    Same formatter as `tb-received-at`, so one time parser covers both contracts (events contract 2).
    """
    return format_timestamp(datetime.now(UTC))


def _check_attrs(attrs: Mapping[str, str]) -> None:
    """Apply the SDK's attrs guard, re-raised as an `EventError`.

    One error type for "this event does not satisfy the contract", so a caller does not have to know
    that the attrs rules are implemented in the SDK. The guard's message is kept; it names the attribute
    and the rule, never the value.
    """
    try:
        validate_attrs(attrs)
    except AttrError as exc:
        raise EventError(str(exc)) from exc


def _check_common(stage: str, status: str, error_code: str | None, error_message: str | None) -> None:
    if status not in STATUSES:
        raise EventError(f"unknown status {safe_token(status)}")
    if status in ("error", "timeout") and not error_code:
        raise EventError(
            f"stage {safe_token(stage)} with status {safe_token(status)} needs an error_code: "
            "alerting groups on the code"
        )
    if error_message is not None:
        if len(error_message) > MAX_ERROR_MESSAGE:
            raise EventError(f"error_message is {len(error_message)} characters, at most {MAX_ERROR_MESSAGE}")
        if any(ord(char) < 0x20 for char in error_message):
            raise EventError("error_message must not contain control characters")


@dataclass(frozen=True, slots=True)
class MessageEvent:
    """One message crossed one lifecycle stage."""

    stage: str
    status: Status
    channel: str
    channel_version: int
    instance: str
    message_id: str
    ts: str = field(default_factory=now_text)
    destination: str | None = None
    attempt: int | None = None
    content_type: str | None = None
    size_bytes: int | None = None
    claim_check: str | None = None
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    duration_ms: float | None = None
    traceparent: str | None = None
    parent_id: str | None = None
    correlation_id: str | None = None
    attrs: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.stage not in MESSAGE_STAGES:
            raise EventError(f"{safe_token(self.stage)} is not a message stage")
        _check_common(self.stage, self.status, self.error_code, self.error_message)
        if self.attrs:
            _check_attrs(self.attrs)
        if self.claim_check is not None and self.size_bytes not in (None, 0):
            raise EventError("a claim-checked payload has no inline bytes: size_bytes must be 0")

    @property
    def key(self) -> bytes:
        """Message events are keyed by `tb-msg-id`, so one message's events stay ordered."""
        return self.message_id.encode("utf-8")

    def to_dict(self) -> dict[str, Any]:
        return _compact(
            {
                "v": EVENT_VERSION,
                "kind": "message",
                "ts": self.ts,
                "channel": self.channel,
                "channel_version": self.channel_version,
                "stage": self.stage,
                "status": self.status,
                "instance": self.instance,
                "message_id": self.message_id,
                "parent_id": self.parent_id,
                "correlation_id": self.correlation_id,
                "destination": self.destination,
                "attempt": self.attempt,
                "content_type": self.content_type,
                "size_bytes": self.size_bytes,
                "claim_check": self.claim_check,
                "topic": self.topic,
                "partition": self.partition,
                "offset": self.offset,
                "error_code": self.error_code,
                "error_message": self.error_message,
                "duration_ms": self.duration_ms,
                "traceparent": self.traceparent,
                "attrs": dict(self.attrs) if self.attrs else None,
            }
        )


@dataclass(frozen=True, slots=True)
class PartitionEvent:
    """Blocking retry paused, resumed or halted a partition.

    A paused partition is invisible unless it is an event (ADR-003 section 1), and `halt-partition`
    requires an operator, so `partition_halted` is what the alert rule fires on.
    """

    stage: str
    status: Status
    channel: str
    channel_version: int
    instance: str
    topic: str
    partition: int
    ts: str = field(default_factory=now_text)
    message_id: str | None = None
    offset: int | None = None
    destination: str | None = None
    attempt: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    duration_ms: float | None = None
    traceparent: str | None = None
    attrs: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if self.stage not in PARTITION_STAGES:
            raise EventError(f"{safe_token(self.stage)} is not a partition stage")
        _check_common(self.stage, self.status, self.error_code, self.error_message)
        if self.attrs:
            _check_attrs(self.attrs)

    @property
    def key(self) -> bytes:
        """`<topic>:<partition>`, so one partition's events stay ordered (events contract 1)."""
        return f"{self.topic}:{self.partition}".encode()

    def to_dict(self) -> dict[str, Any]:
        return _compact(
            {
                "v": EVENT_VERSION,
                "kind": "partition",
                "ts": self.ts,
                "channel": self.channel,
                "channel_version": self.channel_version,
                "stage": self.stage,
                "status": self.status,
                "instance": self.instance,
                "message_id": self.message_id,
                "destination": self.destination,
                "attempt": self.attempt,
                "topic": self.topic,
                "partition": self.partition,
                "offset": self.offset,
                "error_code": self.error_code,
                "error_message": self.error_message,
                "duration_ms": self.duration_ms,
                "traceparent": self.traceparent,
                "attrs": dict(self.attrs) if self.attrs else None,
            }
        )


LifecycleEvent = MessageEvent | PartitionEvent


def _compact(document: Mapping[str, Any]) -> dict[str, Any]:
    """Drop absent fields. The contract says omit, and `additionalProperties: false` plus a null would
    otherwise make every optional field a validation risk for no information gained."""
    return {key: value for key, value in document.items() if value is not None}


def serialize(event: LifecycleEvent) -> bytes:
    """Single-line JSON, UTF-8, as the events contract requires."""
    return orjson.dumps(event.to_dict())


class EventEmitter(Protocol):
    """Where lifecycle events go. Implementations must not block the data plane."""

    def emit(self, event: LifecycleEvent) -> None: ...

    def close(self) -> None:
        """Best-effort flush at shutdown, bounded so a slow events topic cannot delay a drain."""


class NullEmitter:
    """Drops every event. For a worker running with events disabled, and for unit tests that ignore them."""

    def emit(self, event: LifecycleEvent) -> None:
        return None

    def close(self) -> None:
        return None


class RecordingEmitter:
    """Keeps events in memory so a test can assert the stage sequence."""

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    def emit(self, event: LifecycleEvent) -> None:
        self.events.append(event)

    def close(self) -> None:
        return None

    def stages(self) -> list[str]:
        return [event.stage for event in self.events]
