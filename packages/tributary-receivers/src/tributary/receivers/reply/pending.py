"""The pending map of synchronous reply mode: which requests are waiting, and the one answer each gets.

One `Pending` per in-flight synchronous request, keyed by `tb-correlation-id`. **Every decision about what
the sender is answered is `tributary.runtime.sync_answer.SyncRequest`'s**, not re-derived here: this module
feeds it the six things that can happen to a request (the receiver's own `raw` delivery callback succeeding or
failing, a reply record, the reply timer, a revoke of the advertised reply partition, the sender going away)
and settles the entry with the resolution it returns. That is what lets `tests/test_reply_properties.py`'s
property prove the receiver's rules and not just a model of them (the test drives this map through the same
traces).

What the map adds around the state machine (channel-spec README 3.2.6, ADR-003 D10):

* **Admission.** `open` refuses with `REPLY_UNAVAILABLE` when this process holds no reply partition (nothing is
  published) and with `REPLY_BUSY` at `maxPending`. Both are `AR`: nothing was committed.
* **The reply clock** starts at the successful `raw` callback, never earlier, and is a `loop.call_later` whose
  handle is cancelled when the entry settles. A timer that fires for a settled entry does nothing.
* **Matching.** A reply resolves an entry only when its correlation id is in the map **and** its `tb-msg-id`
  equals the entry's; anything else is dropped and counted. A late reply (after the timeout, after a worker
  restart replayed it) finds no entry.
* **`body: step`.** The first reply's body is held with the entry and charged to `queue.maxBytes`
  (`ByteBudget.try_charge`) until the answer is written. If the budget cannot take it, or the body would break
  MLLP framing, the reply is replaced by a `failed` one (`REPLY_BUSY` / `REPLY_BODY_MISSING`) so the sender
  gets an honest `AE`; the message was committed either way.
* **PHI.** The sender's MSH-10 lives only in the entry, in memory. The map never logs and never puts a value in
  a header; the correlation id and `tb-msg-id` it exposes are engine-minted.

Everything runs on the event loop thread. The reply consumer's thread reaches it only through
`call_soon_threadsafe` (`feed.py`), so nothing here is locked.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from typing import Protocol

from tributary.runtime.sync_answer import Resolution, SyncRequest
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_BODY_MISSING, REPLY_BUSY, REPLY_UNAVAILABLE
from tributary.sdk.ids import new_correlation_id

from ..backpressure import ByteBudget
from ..metrics import Metrics
from .feed import ReplyRecord
from .settings import ReplySettings

#: A reply's `tb-error-code` is tolerated, never trusted (envelope 2.3.3): it reaches the sender in `MSA-3`,
#: so only a registered-shape code (`^[A-Z][A-Z0-9_]{0,63}$`) passes. Anything else is answered with no code.
_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")

#: MLLP framing bytes a body must not contain (`0x0B` starts a frame, `0x1C` ends it).
_FRAMING = (b"\x0b", b"\x1c")


class Cancellable(Protocol):
    def cancel(self) -> None: ...


Schedule = Callable[[float, Callable[[], None]], Cancellable]


class PartitionSource(Protocol):
    def choose_partition(self) -> int | None:
        """A reply partition this process holds and has a fetch position on, or `None`."""
        ...


@dataclass(slots=True)
class Outcome:
    """What `_ack_writer` needs to answer one request: the resolution, the sender's control id, the engine's
    `tb-msg-id` for `MSA-3`, and a step body when the answer is `AA`. `body_charged` is returned to the budget by
    the writer after the answer is written (`take_charge` makes that idempotent)."""

    resolution: Resolution
    sender_control_id: str
    msg_id: str | None
    body: bytes = b""
    body_charged: int = 0

    def take_charge(self) -> int:
        charged, self.body_charged = self.body_charged, 0
        return charged


@dataclass(eq=False, slots=True)
class Pending:
    correlation_id: str
    sender_control_id: str
    partition: int
    timeout_s: float
    connection: object
    request: SyncRequest
    outcome: asyncio.Future[Outcome]
    msg_id: str | None = None
    timer: Cancellable | None = None
    reply_seen: bool = False
    body: bytes = b""
    body_charged: int = 0
    body_mode: str = "ack"

    def bind(self, msg_id: str) -> None:
        """Record the `tb-msg-id` of the `raw` record, before it is produced: no reply can name it earlier."""
        self.msg_id = msg_id

    def routing_headers(self, *, ack_timeout_s: float, now: datetime | None = None) -> dict[str, str]:
        """The three reply-routing headers for the `raw` record. The deadline is no earlier than the real one
        (produce time + `ack.timeoutMs` + `reply.timeoutMs`): the reply clock starts at the callback, which
        cannot come later than `ack.timeoutMs` after the produce (envelope README 3, `tb-reply-deadline`)."""
        moment = now if now is not None else datetime.now(UTC)
        deadline = moment + timedelta(seconds=ack_timeout_s + self.timeout_s)
        return {
            h.TB_CORRELATION_ID: self.correlation_id,
            h.TB_REPLY_PARTITION: str(self.partition),
            h.TB_REPLY_DEADLINE: h.format_timestamp(deadline),
        }


class PendingMap:
    def __init__(
        self,
        settings: ReplySettings,
        *,
        feed: PartitionSource,
        loop: asyncio.AbstractEventLoop,
        budget: ByteBudget,
        metrics: Metrics,
        schedule: Schedule | None = None,
        request_factory: Callable[[], SyncRequest] = SyncRequest,
    ) -> None:
        self._settings = settings
        self._feed = feed
        self._loop = loop
        self._budget = budget
        self._metrics = metrics
        self._schedule: Schedule = schedule if schedule is not None else loop.call_later
        self._request_factory = request_factory
        self._by_id: dict[str, Pending] = {}
        self._retired = False

    def __len__(self) -> int:
        return len(self._by_id)

    @property
    def retired(self) -> bool:
        return self._retired

    def retire(self) -> None:
        """The server that owns this map stopped taking requests: the feed may drop it once it is empty."""
        self._retired = True

    @property
    def settings(self) -> ReplySettings:
        return self._settings

    def configure(self, settings: ReplySettings) -> None:
        """A live update (`timeoutMs`, `maxPending`, `body`): applies to requests opened from now on."""
        self._settings = settings

    # -- admission --------------------------------------------------------------------------------

    def open(self, *, sender_control_id: str, connection: object) -> Pending | str:
        """A pending entry for a new request, or the refusal code (`REPLY_UNAVAILABLE` / `REPLY_BUSY`) when it
        must be answered `AR` **before anything is published**."""
        partition = self._feed.choose_partition()
        if partition is None:
            self._metrics.nacked_reply_unavailable += 1
            return REPLY_UNAVAILABLE
        if len(self._by_id) >= self._settings.max_pending:
            self._metrics.nacked_reply_busy += 1
            return REPLY_BUSY
        entry = Pending(
            correlation_id=new_correlation_id(),
            sender_control_id=sender_control_id,
            partition=partition,
            timeout_s=self._settings.timeout_s,
            connection=connection,
            request=self._request_factory(),
            outcome=self._loop.create_future(),
            body_mode=self._settings.body,
        )
        self._by_id[entry.correlation_id] = entry
        self._metrics.reply_pending = len(self._by_id)
        return entry

    # -- the six events ---------------------------------------------------------------------------

    def commit(self, entry: Pending, *, ok: bool) -> Resolution | None:
        """The receiver's own `raw` delivery callback. On success the reply clock starts."""
        if ok:
            resolution = entry.request.commit_succeeded()
            if resolution is None and not entry.request.resolved and entry.timer is None:
                entry.timer = self._schedule(entry.timeout_s, partial(self._on_timer, entry))
        else:
            resolution = entry.request.commit_failed()
        return self._settle(entry, resolution)

    def deliver_reply(self, record: ReplyRecord) -> tuple[bool, Resolution | None]:
        """A reply record. `(matched, resolution)`: unmatched means the id is unknown or `tb-msg-id` differs,
        and the record is dropped (a late reply, another process's, a forged one)."""
        entry = self._by_id.get(record.correlation_id)
        if entry is None or entry.msg_id is None or entry.msg_id != record.msg_id:
            return False, None  # the feed counts it once, after every sink has declined
        if entry.reply_seen:
            return True, None  # the first reply counts (SyncRequest ignores a later one; no second charge)
        entry.reply_seen = True
        status, code, body = record.status, _safe_code(record.code), record.body
        if status == "accepted" and entry.body_mode == "step" and body:
            if any(byte in body for byte in _FRAMING):
                status, code, body = "failed", REPLY_BODY_MISSING, b""  # would break the sender's framing
            elif self._budget.try_charge(len(body)):
                entry.body, entry.body_charged = body, len(body)
            else:
                status, code, body = "failed", REPLY_BUSY, b""  # committed, but the body cannot be held
        return True, self._settle(entry, entry.request.reply_received(status, code))

    def timer_fire(self, entry: Pending) -> Resolution | None:
        return self._timer_fired(entry)

    def _on_timer(self, entry: Pending) -> None:
        self._timer_fired(entry)

    def partitions_revoked(self, partitions: frozenset[int]) -> list[Resolution]:
        """The advertised partitions are gone: their entries' replies are unreachable."""
        settled: list[Resolution] = []
        for entry in [e for e in self._by_id.values() if e.partition in partitions]:
            resolution = self._settle(entry, entry.request.partition_revoked())
            if resolution is not None:
                settled.append(resolution)
        return settled

    def abandon(self, connection: object) -> None:
        """The sender's connection is gone (or its handler ended): nothing can be written, the entries end."""
        for entry in [e for e in self._by_id.values() if e.connection is connection]:
            self._settle(entry, entry.request.disconnected())

    def abandon_all(self) -> None:
        for entry in list(self._by_id.values()):
            self._settle(entry, entry.request.disconnected())

    # -- internals --------------------------------------------------------------------------------

    def _timer_fired(self, entry: Pending) -> Resolution | None:
        entry.timer = None
        return self._settle(entry, entry.request.reply_timed_out())

    def _settle(self, entry: Pending, resolution: Resolution | None) -> Resolution | None:
        if resolution is None:
            return None
        self._by_id.pop(entry.correlation_id, None)
        self._metrics.reply_pending = len(self._by_id)
        if entry.timer is not None:
            entry.timer.cancel()
            entry.timer = None
        outcome = Outcome(
            resolution=resolution,
            sender_control_id=entry.sender_control_id,
            msg_id=entry.msg_id,
            body=entry.body if resolution.answer == "AA" else b"",
            body_charged=entry.body_charged,
        )
        entry.body, entry.body_charged = b"", 0
        if not entry.outcome.done():
            entry.outcome.set_result(outcome)
        return resolution


def _safe_code(code: str | None) -> str | None:
    return code if code is not None and _CODE.match(code) else None
