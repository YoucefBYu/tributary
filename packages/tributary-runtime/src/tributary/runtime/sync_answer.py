"""What a synchronous sender is answered, as a pure state machine (channel-spec README 3.2.6, ADR-003 D10).

One `SyncRequest` per pending entry. The receiver feeds it the five things that can happen to a request
— its own `raw` delivery callback, a reply record, the reply timer, a revoke of the advertised reply
partition, and the sender going away — in whatever order they arrive, and it returns the **one** answer
at most once. It has no clock, no socket and no Kafka, so the W2-8 property test can drive every
interleaving through it; the receiver's pending map (connector-engineer) owns timers, the correlation
lookup and `maxPending`, and is meant to delegate the decision here.

The rules, each from the amended contract:

* `AR` = commit not confirmed (resend is safe). `AE` = committed (resend duplicates). `AA` = committed
  and routed.
* **Neither `AA` nor `AE` precedes the successful `raw` callback** (invariant 1, amendment). A reply or
  a revoke that arrives first is held until the callback, which then answers from it, or `AR` if the
  callback failed.
* Only a reply with `tb-reply-status: accepted` becomes `AA`. Absent or unknown answers `AE`, with the
  reply's `tb-error-code` if it has one (envelope 2.3.3: tolerated, never trusted).
* The reply clock starts at the successful callback, so a timer event before it means nothing.
* A sender that disconnected gets no answer: the entry resolves as abandoned, never as a false answer.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from tributary.sdk.codes import REPLY_ROUTE_LOST, REPLY_TIMEOUT

Answer = Literal["AA", "AE", "AR"]


@dataclass(frozen=True, slots=True)
class Resolution:
    """The one outcome of a request. `answer` is `None` when the sender is gone (nothing is written)."""

    answer: Answer | None
    code: str | None = None


ABANDONED = Resolution(answer=None)


@dataclass(frozen=True, slots=True)
class _Reply:
    status: str | None
    code: str | None


def answer_for_reply(status: str | None, code: str | None) -> Resolution:
    """A committed request's answer from its reply record: `AA` only for `accepted`."""
    if status == "accepted":
        return Resolution("AA")
    return Resolution("AE", code)


class SyncRequest:
    """The answer state of one pending synchronous request. Every method returns the resolution it
    caused, or `None`; after the first resolution every method returns `None`."""

    __slots__ = ("_committed", "_reply", "_revoked", "resolution")

    def __init__(self) -> None:
        self._committed: bool | None = None  # None until the raw delivery callback
        self._reply: _Reply | None = None
        self._revoked = False
        self.resolution: Resolution | None = None

    @property
    def resolved(self) -> bool:
        return self.resolution is not None

    def commit_succeeded(self) -> Resolution | None:
        """The receiver's own `raw` delivery callback succeeded. The reply clock starts now."""
        if self.resolved or self._committed is not None:
            return None
        self._committed = True
        if self._reply is not None:
            return self._resolve(answer_for_reply(self._reply.status, self._reply.code))
        if self._revoked:
            return self._resolve(Resolution("AE", REPLY_ROUTE_LOST))
        return None

    def commit_failed(self) -> Resolution | None:
        """The `raw` callback failed, or `ack.timeoutMs` passed without one: nothing is confirmed."""
        if self.resolved or self._committed is not None:
            return None
        self._committed = False
        return self._resolve(Resolution("AR"))

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        """A reply record matched this request (correlation id and `tb-msg-id`). The first one counts."""
        if self.resolved:
            return None
        if self._committed:
            return self._resolve(answer_for_reply(status, code))
        if self._reply is None:
            self._reply = _Reply(status, code)  # held until the callback: never answer ahead of it
        return None

    def reply_timed_out(self) -> Resolution | None:
        """`reply.timeoutMs` after the successful callback. Before it the timer is not running."""
        if self.resolved or not self._committed:
            return None
        return self._resolve(Resolution("AE", REPLY_TIMEOUT))

    def partition_revoked(self) -> Resolution | None:
        """The reply partition this request advertised was revoked: its reply is unreachable."""
        if self.resolved:
            return None
        self._revoked = True
        if self._committed:
            return self._resolve(Resolution("AE", REPLY_ROUTE_LOST))
        return None  # wait for the callback: AE on success, AR on failure (amendment)

    def disconnected(self) -> Resolution | None:
        """The sender's connection closed. Nothing can be written; the entry ends."""
        if self.resolved:
            return None
        return self._resolve(ABANDONED)

    def _resolve(self, resolution: Resolution) -> Resolution:
        self.resolution = resolution
        return resolution
