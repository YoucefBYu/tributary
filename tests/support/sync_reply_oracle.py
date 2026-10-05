"""The sync-reply answer oracle, shared by every suite that checks the answer rules (W3-20).

Before W3-20 this code existed twice: in `packages/tributary-runtime/tests/test_reply_properties.py` (the
model, `SyncRequest`) and as a copy in `packages/tributary-receivers/tests/reply/test_properties.py` (the
receiver's real `PendingMap`), because one test module cannot import another across packages under
`--import-mode=importlib`. A rule fixed in one and not the other would have been a quiet divergence. This module
is the single home: both suites import it, and `tests/support/test_sync_reply_oracle.py` holds the oracle's own
controls.

What is here, and what is not:

* `REPLIES`, `NOISE`, `traces()` (the strategy), `drive()`, `oracle()`, `violations()` and `revoke_gaps()`:
  the answer property. `oracle` is written from `contracts/channel-spec/v0` 3.2.6 without reading
  `SyncRequest`, and must stay that way: an oracle derived from the implementation judges nothing.
* `BROKEN_REQUESTS`: seven broken `SyncRequest`s. Each is a bug a correct implementation must not have and the
  property must catch (the proposal's control, the amendment's control, an absent status trusted, last reply
  wins, swapped codes, an unrecognised status trusted, a held reply answered on a failed commit).
* Not here: anything about a specific receiver's map (`MapEntry`, the broken `PendingMap`s). Those need
  `tributary.receivers` and stay in the receivers' suite; they plug into `violations()` through its `factory`.

`violations` takes a factory of anything that answers the six events in `RequestLike`, so the same judge runs
over the model and over an adapter of a receiver's entry.

Synthetic: no payloads, only control events and answer codes.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from hypothesis import strategies as st
from tributary.runtime.sync_answer import Resolution, SyncRequest, answer_for_reply
from tributary.sdk.codes import REPLY_ROUTE_LOST, REPLY_TIMEOUT, TRANSFORM_ERROR

__all__ = [
    "BROKEN_REQUESTS",
    "NOISE",
    "REPLIES",
    "AnswersHeldReplyOnFailedCommit",
    "AnswersOnReplyAlone",
    "AnswersRevokeEagerly",
    "LastReplyWins",
    "RequestLike",
    "SwapsAeCodes",
    "TrustsAbsentStatus",
    "TrustsUnrecognisedStatus",
    "drive",
    "oracle",
    "revoke_gaps",
    "traces",
    "violations",
]


class RequestLike(Protocol):
    """The six events of `SyncRequest`, which a receiver's pending-map entry is adapted to."""

    def commit_succeeded(self) -> Resolution | None: ...

    def commit_failed(self) -> Resolution | None: ...

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None: ...

    def reply_timed_out(self) -> Resolution | None: ...

    def partition_revoked(self) -> Resolution | None: ...

    def disconnected(self) -> Resolution | None: ...


REPLIES = {
    "reply_accepted": ("accepted", None),
    "reply_rejected": ("rejected", "LAB_QUERY_UNSUPPORTED"),
    "reply_failed": ("failed", TRANSFORM_ERROR),
    "reply_absent": (None, None),
    "reply_unknown": ("partially-accepted", None),
}
NOISE = [*REPLIES, "timeout", "revoke", "disconnect"]


@st.composite
def traces(draw: st.DrawFn) -> list[str]:
    """Noise events in any order, exactly one `raw` callback somewhere, and a final timer firing."""
    noise = draw(st.lists(st.sampled_from(NOISE), max_size=8))
    commit = draw(st.sampled_from(["commit_ok", "commit_failed"]))
    position = draw(st.integers(min_value=0, max_value=len(noise)))
    # The timer always fires eventually unless something resolved first: it closes every trace.
    return [*noise[:position], commit, *noise[position:], "timeout"]


def drive(request: RequestLike, event: str) -> Resolution | None:
    if event == "commit_ok":
        return request.commit_succeeded()
    if event == "commit_failed":
        return request.commit_failed()
    if event in REPLIES:
        status, code = REPLIES[event]
        return request.reply_received(status, code)
    if event == "timeout":
        return request.reply_timed_out()
    if event == "revoke":
        return request.partition_revoked()
    if event == "disconnect":
        return request.disconnected()
    raise AssertionError(event)


def oracle(cause: str, first_reply: str | None, revoked: bool) -> Resolution | None:
    """The one right answer, written from channel-spec 3.2.6's table without reading `SyncRequest`.

    `cause` is the event that resolved the request. A reply-derived answer comes from the **first**
    reply (held before the callback or received after it): `AA` only for `accepted`, else `AE` with that
    reply's own code. A revoke answers `REPLY_ROUTE_LOST`, the timer `REPLY_TIMEOUT`. `None`: nothing
    justifies an answer at this event.
    """
    if cause == "disconnect":
        return Resolution(answer=None)
    if cause == "commit_failed":
        return Resolution("AR")
    if cause == "timeout":
        return Resolution("AE", REPLY_TIMEOUT)
    if cause == "revoke":
        return Resolution("AE", REPLY_ROUTE_LOST)
    if first_reply is not None:  # cause is a reply, or the callback releasing a held one
        status, code = REPLIES[first_reply]
        return Resolution("AA") if status == "accepted" else Resolution("AE", code)
    if cause == "commit_ok" and revoked:  # the callback releasing a held revoke
        return Resolution("AE", REPLY_ROUTE_LOST)
    return None


def violations(trace: list[str], factory: Callable[[], RequestLike]) -> list[str]:
    """Every broken rule in one run of `trace`. Empty means the implementation held."""
    request = factory()
    found: list[str] = []
    resolutions: list[Resolution] = []
    committed = False
    disconnected = False
    revoked = False
    first_reply: str | None = None
    for event in trace:
        committed = committed or event == "commit_ok"
        if first_reply is None and event in REPLIES and not resolutions:
            first_reply = event
        revoked = revoked or (event == "revoke" and not resolutions)
        resolution = drive(request, event)
        disconnected = disconnected or event == "disconnect"
        if resolution is None:
            continue
        resolutions.append(resolution)
        if resolution.answer in ("AA", "AE") and not committed:
            found.append(f"{resolution.answer} before the raw callback succeeded (invariant 1)")
        if resolution.answer == "AR" and committed:
            found.append("AR after a confirmed commit: tells the sender a resend is safe when it is not")
        if resolution.answer == "AA" and first_reply != "reply_accepted":
            found.append("AA without the first reply being accepted")
        if resolution.answer is not None and disconnected:
            found.append("an answer written to a sender that had gone")
        expected = oracle(event, first_reply, revoked)
        if len(resolutions) == 1 and resolution != expected:
            found.append(f"{resolution} where the rules give {expected} (cause {event})")
    if len(resolutions) != 1:
        found.append(f"{len(resolutions)} resolutions, expected exactly one")
    return found


def revoke_gaps(trace: list[str], factory: Callable[[], RequestLike]) -> list[str]:
    """After the callback, a revoke of the advertised partition answers `AE` / `REPLY_ROUTE_LOST` **at once**
    (the oracle only judges answers that were given, so a revoke that is ignored needs its own check)."""
    request, committed, settled = factory(), False, False
    found: list[str] = []
    for event in trace:
        resolution = drive(request, event)
        if resolution is not None:
            settled = True
        elif event == "revoke" and committed and not settled:
            found.append("a revoke after the callback did not answer")
        committed = committed or event == "commit_ok"
    return found


# -- the broken SyncRequests: controls, each of which the property must catch ---------------------------


class AnswersOnReplyAlone(SyncRequest):
    """The proposal's control: resolves on the reply without waiting for the raw callback."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        if self.resolved:
            return None
        self.resolution = answer_for_reply(status, code)
        return self.resolution


class AnswersRevokeEagerly(SyncRequest):
    """The amendment's control: `AE` / `REPLY_ROUTE_LOST` on a revoke before the callback."""

    def partition_revoked(self) -> Resolution | None:
        if self.resolved:
            return None
        self.resolution = Resolution("AE", REPLY_ROUTE_LOST)
        return self.resolution


class TrustsAbsentStatus(SyncRequest):
    """Envelope 2.3.3's control: reads an absent `tb-reply-status` as accepted."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        return super().reply_received("accepted" if status is None else status, code)


class LastReplyWins(SyncRequest):
    """A later reply overwrites the one held before the callback (the rule is: the first one counts)."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        if self.resolved or self._committed:
            return super().reply_received(status, code)
        self._reply = None  # forget the held one, so the base class holds this one instead
        return super().reply_received(status, code)


class SwapsAeCodes(SyncRequest):
    """The timer answers `REPLY_ROUTE_LOST` and a revoke `REPLY_TIMEOUT`: right answer, wrong code."""

    def reply_timed_out(self) -> Resolution | None:
        resolved = super().reply_timed_out()
        return None if resolved is None else self._resolve(Resolution("AE", REPLY_ROUTE_LOST))

    def partition_revoked(self) -> Resolution | None:
        resolved = super().partition_revoked()
        return None if resolved is None else self._resolve(Resolution("AE", REPLY_TIMEOUT))


class TrustsUnrecognisedStatus(SyncRequest):
    """`AA` for any status that is not `rejected` or `failed`, so `partially-accepted` becomes `AA`."""

    def reply_received(self, status: str | None, code: str | None) -> Resolution | None:
        trusted = "accepted" if status not in (None, "rejected", "failed") else status
        return super().reply_received(trusted, code)


class AnswersHeldReplyOnFailedCommit(SyncRequest):
    """A reply held before the callback is answered even when the callback then fails (never `AR`)."""

    def commit_failed(self) -> Resolution | None:
        if not self.resolved and self._committed is None and self._reply is not None:
            self._committed = False
            return self._resolve(answer_for_reply(self._reply.status, self._reply.code))
        return super().commit_failed()


BROKEN_REQUESTS: list[type[SyncRequest]] = [
    AnswersOnReplyAlone,
    AnswersRevokeEagerly,
    TrustsAbsentStatus,
    LastReplyWins,
    SwapsAeCodes,
    TrustsUnrecognisedStatus,
    AnswersHeldReplyOnFailedCommit,
]
