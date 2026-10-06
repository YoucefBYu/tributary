"""The audit hook for access to message content (CLAUDE.md: access to content is an auditable action).

The API supplies an `AuditHook`; this package calls it and never writes audit records itself, because where
they go (`_tb.<env>.audit`, retention) is a control-plane decision still open (wave-3 7.2).

* `phase="requested"` is emitted BEFORE any payload is read or published and **fails closed**: if the hook
  raises, the operation does not run. A content access that cannot be recorded does not happen.
* `phase="failed"` is emitted when the work stops after `requested`, with the acknowledged count (best effort).
* `phase="completed"` is emitted after the work, with counts, best effort: a hook failure there is logged
  by class name and does not turn a delivered re-publish into an error.

An `AuditEvent` carries identifiers and counts only: no payload, no key, no header value (invariant 6).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Final, Literal, Protocol

ACTOR: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/=-]{0,127}")
Action = Literal["message.reprocess", "channel.replay"]
Phase = Literal["requested", "completed", "failed"]


@dataclass(frozen=True, slots=True)
class AuditEvent:
    action: Action
    phase: Phase
    actor: str
    channel: str | None = None
    message_id: str | None = None
    #: A request id the caller minted (`request_id` or `replay_id`): joins `requested` to `completed`.
    request_id: str | None = None
    #: Records the broker acknowledged (`completed`, and `failed` after a partial run).
    count: int | None = None
    #: The caller asked to reorder a per-key channel (ordering.py). Always recorded on `requested`.
    allow_reorder: bool = False
    #: Replay only: the topic written (always the source channel's own `raw`) and the requested range `[start, end)`.
    target_topic: str | None = None
    range_start: datetime | None = None
    range_end: datetime | None = None
    ts: datetime = field(default_factory=lambda: datetime.now(UTC))


class AuditHook(Protocol):
    def __call__(self, event: AuditEvent) -> None: ...


def discard(event: AuditEvent) -> None:
    """No-op hook for tools with no audit sink. The API must not use it."""
