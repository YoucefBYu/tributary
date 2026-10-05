"""`ByteBudget`: a receiver-wide memory bound in bytes, not items (gate requirement 5, SR-3).

Phase 0's S2 spike bounded each connection's pipeline by *item count* (`conn_pipeline`, a queue depth)
with no bound on the bytes those items could hold. The gate report worked the arithmetic: 512
connections times a pipeline depth of 256 times a 1 MB message is 128 GB of frames a single receiver
process could be holding at once, none of it a real memory bound. `contracts/channel-spec/v0`
`source.queue` now requires `maxBytes` for exactly this reason (its own `$comment` reads "an item
count is not a memory bound when an item may be up to 1 MB").

One `ByteBudget` is shared by every connection a server handles. A frame reserves its size before it
is queued for publish and releases it only once its ACK has been written — the full lifetime the gate
finding was about — so the total number of frames buffered anywhere in the process, across every
connection, is bounded by `maxBytes` regardless of how many connections or how deep any one
connection's pipeline is. `maxMessages`, when the spec sets it, is a secondary count cap for the same
budget (useful when many small messages would otherwise pass the byte cap easily); it is optional
because bytes alone is already a memory bound and a count alone is not (same reasoning as maxBytes
being required and maxMessages not).

`contracts/channel-spec/v0` leaves the *default* `maxBytes` (when the `queue` object is absent) to
this package with a `TODO(connector-engineer): set and document it from a measurement`. `DEFAULT_MAX_BYTES`
below is a **judgement call, not a measurement**: 64 MiB is enough for 64 messages at the 1 MB inline
limit, or many thousands of typical few-hundred-byte HL7 messages, on a receiver Phase 0 never load
tested past 1k msg/s in a way that pinned steady-state buffered bytes. Re-measure before relying on it
for a sizing decision (`docs/spikes/S2-mllp-kafka.md` follow-up 1: a Linux re-run of the latency
matrix is the natural place to also watch this number).
"""

from __future__ import annotations

import asyncio

DEFAULT_MAX_BYTES = 64 * 1024 * 1024


class BudgetExceeded(Exception):
    """A single reservation request alone is larger than the budget: it could never fit, so it is a
    configuration error (`maxMessageBytes` bigger than `queue.maxBytes`), not backpressure."""


class ByteBudget:
    """Async-safe bounded resource. `reserve` blocks (bounded by `timeout`) while the budget is full,
    which is what turns "the destination partition is stalled" into TCP backpressure on the sender
    rather than unbounded buffering here. A timed-out reservation is reported as `queue-full`, one of
    the channel spec's `ack.nackOn` conditions, by the caller."""

    def __init__(self, max_bytes: int = DEFAULT_MAX_BYTES, max_messages: int | None = None) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive when set")
        self._max_bytes = max_bytes
        self._max_messages = max_messages
        self._used_bytes = 0
        self._used_messages = 0
        self._cond = asyncio.Condition()

    @property
    def used_bytes(self) -> int:
        return self._used_bytes

    @property
    def used_messages(self) -> int:
        return self._used_messages

    def set_limits(self, max_bytes: int, max_messages: int | None) -> None:
        """Change the limits in place (a live spec update). Synchronous on purpose: on the single event
        loop thread it is atomic with respect to every coroutine, so a caller can change these and the
        matching message-size limit in one step. Reservations already held are kept; a shrink only makes
        later reservations wait. Follow with `await wake()` so waiters re-check."""
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive when set")
        self._max_bytes = max_bytes
        self._max_messages = max_messages

    async def wake(self) -> None:
        async with self._cond:
            self._cond.notify_all()

    async def reserve(self, n_bytes: int, timeout: float) -> bool:
        """Reserve `n_bytes` within `timeout` seconds. `True` and the budget is charged; `False` on
        timeout and nothing is charged. Never raises for an ordinary full queue."""
        if n_bytes > self._max_bytes:
            raise BudgetExceeded(f"{n_bytes} bytes exceeds the {self._max_bytes}-byte budget on its own")

        def fits() -> bool:
            return self._used_bytes + n_bytes <= self._max_bytes and (
                self._max_messages is None or self._used_messages < self._max_messages
            )

        try:
            async with self._cond:
                await asyncio.wait_for(self._cond.wait_for(fits), timeout)
                self._used_bytes += n_bytes
                self._used_messages += 1
                return True
        except TimeoutError:
            return False

    def try_charge(self, n_bytes: int) -> bool:
        """Charge `n_bytes` at once, without waiting and without taking a message slot, or charge nothing and
        return `False`. For bytes that are *added* to a frame already admitted (a synchronous reply body held
        until its answer is written): they count against `maxBytes` like the frame itself, but a full budget
        refuses them rather than queueing, since the frame's own slot is what the sender is waiting on. Release
        with `release_bytes`. Synchronous so a reply handler on the event loop never yields while charging."""
        if n_bytes < 0:
            raise ValueError("n_bytes must not be negative")
        if self._used_bytes + n_bytes > self._max_bytes:
            return False
        self._used_bytes += n_bytes
        return True

    async def release_bytes(self, n_bytes: int) -> None:
        """Return bytes charged by `try_charge` (no message slot is released)."""
        if n_bytes <= 0:
            return
        async with self._cond:
            self._used_bytes = max(0, self._used_bytes - n_bytes)
            self._cond.notify_all()

    async def release(self, n_bytes: int) -> None:
        async with self._cond:
            self._used_bytes = max(0, self._used_bytes - n_bytes)
            self._used_messages = max(0, self._used_messages - 1)
            self._cond.notify_all()
