"""Per-record stage timestamps for the synchronous-reply path, off by default (W3-24).

`WorkerSettings.stage_timing=True` (the worker's `main` sets it from `TB_STAGE_TIMING=1`) makes the worker
write one `stage_timing` log line per record that it answers, with wall-clock epoch milliseconds for each
worker stage:

* `raw_ts_ms`: the consumed `raw` record's Kafka timestamp, with its type (`create`: the receiver's
  `produce()` call; `log_append`: the broker's append, i.e. the raw commit, when the topic uses
  `message.timestamp.type=LogAppendTime`).
* `polled_ms`: `poll()` returned the record to the loop. `poll_wait_ms`: how long that `poll()` blocked.
* `handle_ms`: the handler started. `flushed_ms`: every `out` record was acknowledged (absent when a head
  step drops the message, which produces nothing).
* `reply_ms`: the reply was handed to the producer (`produce()` returned).

Transactional channels have no `flushed_ms` and no line at all: their records are flushed by the loop's
commit, not per record, and they never answer (sync reply is refused on a transactional pipeline).

Ids and timestamps only (invariant 6): `message_id` is the engine-minted `tb-msg-id`; no key, payload or
header value from the sender is logged. Wall clock (`time.time`) on purpose: the stamps are joined with
the receiver's `tb-received-at`, broker append times and the sender's clock on one host. The timer is
owned by one worker loop and touched only from its thread, so nothing is locked.

The line is not a metric and not a contract surface: it is a diagnostic. A per-stage histogram would be an
`contracts/observability` change (architect).
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any, Final

from tributary.sdk.context import Logger

ENV_FLAG: Final = "TB_STAGE_TIMING"

#: confluent-kafka `Message.timestamp()` type codes.
_TS_TYPES: Final[dict[int, str]] = {1: "create", 2: "log_append"}


def enabled_from_env(environ: Mapping[str, str]) -> bool:
    """`TB_STAGE_TIMING` set to `1`, `true` or `yes` (any case). Called by the worker's `main` only."""
    value = environ.get(ENV_FLAG, "")
    return value.strip().lower() in {"1", "true", "yes"}


def _ms(seconds: float) -> float:
    return round(seconds * 1000.0, 2)


class StageTimer:
    """Collects the stamps of the record in hand and writes them once it is answered."""

    __slots__ = ("_clock", "_log", "_stamps", "written")

    def __init__(self, log: Logger, *, clock: Callable[[], float] = time.time) -> None:
        self._log = log
        self._clock = clock
        self._stamps: dict[str, Any] = {}
        self.written = 0

    def polled(self, raw: Any, *, poll_started: float) -> None:
        """The loop got a record from `poll()`. Starts a new record's stamps; never raises."""
        now = self._clock()
        stamps: dict[str, Any] = {"polled_ms": _ms(now), "poll_wait_ms": _ms(now - poll_started)}
        try:
            ts_type, ts_ms = raw.timestamp()
        except Exception:  # a stand-in record without a timestamp: the other stamps still count
            ts_type, ts_ms = 0, None
        if ts_type in _TS_TYPES and isinstance(ts_ms, int):
            stamps["raw_ts_ms"] = ts_ms
            stamps["raw_ts_type"] = _TS_TYPES[ts_type]
        self._stamps = stamps

    def mark(self, stage: str) -> None:
        """Stamp `<stage>_ms` now on the record in hand (`handle`, `flushed`)."""
        if self._stamps:
            self._stamps[f"{stage}_ms"] = _ms(self._clock())

    def replied(self, message_id: str | None, *, status: str) -> None:
        """The reply was handed to the producer: stamp it and write the line. One line per record."""
        if not self._stamps:
            return
        stamps, self._stamps = self._stamps, {}
        stamps["reply_ms"] = _ms(self._clock())
        self.written += 1
        self._log.info("stage_timing", message_id=message_id, reply_status=status, **stamps)

    def clock(self) -> float:
        """The timer's clock, for a caller that must stamp `poll_started` on the same scale."""
        return self._clock()
