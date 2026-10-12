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

#: `tributary.audit/v0` `actor` is the token's `sub`: printable ASCII, 1-255 (ADR-009 D7). The grammar of the
#: audit schema and `openapi/v0`, so a valid subject such as `idp|1234` is accepted.
ACTOR: Final = re.compile(r"[\x21-\x7e]{1,255}")
_ERROR_CODE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,63}")
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
    #: `failed` only: the raised error's stable `code` (`error_code_of`), never its message.
    error_code: str | None = None
    ts: datetime = field(default_factory=lambda: datetime.now(UTC))


def error_code_of(exc: BaseException) -> str:
    """The stable code for an audit `failed` record: the error's own `code` when it fits `audit/v0`'s pattern,
    else `INTERNAL_ERROR`. Never the exception message (invariant 6)."""
    code = getattr(exc, "code", None)
    return code if isinstance(code, str) and _ERROR_CODE.fullmatch(code) else "INTERNAL_ERROR"


class AuditHook(Protocol):
    def __call__(self, event: AuditEvent) -> None: ...


def discard(event: AuditEvent) -> None:
    """No-op hook for tools with no audit sink. The API must not use it."""
