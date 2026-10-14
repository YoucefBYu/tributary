"""The window an event or audit `ts` must fall in before it is stored.

Both tables are `PARTITION BY toYYYYMM(ts)`. A syntactically valid `ts` (years 0001-9999) would otherwise let one
record, or a batch of records, spread over more than `max_partitions_per_insert_block` (100) month partitions;
ClickHouse then refuses the whole insert (TOO_MANY_PARTS) and the batch retries forever. A `ts` outside the window
is rejected with a fixed code before it can reach an insert.

The window is anchored on the Kafka record's own timestamp when there is one, else on the clock: never before
2000-01-01 and not more than `SKEW` after the anchor. Audit adds a `LOOKBACK` (its `ts` is the decision time, a
producer stamps it within moments of producing); events do not (a `received` event carries `tb-received-at`,
which a replay can make old). Either way the loop cuts inserts by month, so a spread-out batch is still safe.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Final

EARLIEST: Final = datetime(2000, 1, 1, tzinfo=UTC)
LATEST_RANGE: Final = datetime(2299, 12, 31, 23, 59, 59, tzinfo=UTC)  # DateTime64 ends in 2299
LOOKBACK: Final = timedelta(days=30)
SKEW: Final = timedelta(days=1)


def record_time(message: Any) -> datetime | None:
    """The Kafka record's timestamp, or None if the message carries none."""
    getter = getattr(message, "timestamp", None)
    if getter is None:
        return None
    stamp = getter()
    milliseconds = stamp[1] if isinstance(stamp, tuple) and len(stamp) == 2 else None
    if not isinstance(milliseconds, int) or milliseconds <= 0:
        return None
    try:
        return datetime.fromtimestamp(milliseconds / 1000.0, UTC)
    except (OverflowError, OSError, ValueError):
        return None


def in_window(
    ts: datetime,
    anchor: datetime | None = None,
    *,
    now: datetime | None = None,
    lookback: timedelta | None = None,
) -> bool:
    """`ts` is not before 2000-01-01, not more than `SKEW` after the anchor (the Kafka record time, else now), and
    when a `lookback` is given and there is an anchor, not more than that before it. Events pass no lookback: a
    `received` event legitimately carries `tb-received-at`, which a replay or reprocess can make arbitrarily old.
    The insert is cut by month anyway (`archiver.MAX_MONTHS_PER_BLOCK`); this bound only refuses absurd values."""
    base = anchor or now or datetime.now(UTC)
    lowest = max(EARLIEST, base - lookback) if (anchor is not None and lookback is not None) else EARLIEST
    return lowest <= ts <= base + SKEW
