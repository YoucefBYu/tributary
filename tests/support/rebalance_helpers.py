"""Watching a cooperative-sticky rebalance settle from outside the process, by reading each worker's
own structured log rather than guessing from timing. Shared by every chaos scenario that needs to know
"has the group actually finished moving partitions around" before it trusts a downstream assertion.

`ConsumerLoop`'s `on_assign`/`on_revoke` callbacks are *incremental*: a `partitions_assigned` log line's
`count` is only what was newly handed to that member in one generation, and a brand-new member routinely
gets **0** partitions in generation 1 (the current owner has to revoke them via its own `poll()` before
the broker can grant them in generation 2). Reading the first `partitions_assigned` event per process as
the final assignment therefore reads a genuine, healthy join as "assigned nothing" -- found writing
`test_cooperative_rebalance_stable_join.py` (env-gotchas memory has the incident). `net_assigned`
accumulates every assign (add) and revoke (subtract) event instead, and `wait_until_settled` will not
call the result stable until it has held for several consecutive polls taken no earlier than
`MIN_SETTLE_WAIT_S` after the wait started -- long enough for the group's heartbeat-driven rebalance
detection to have had a real chance to run, not just long enough to sample a still-unchanged start state.
"""

from __future__ import annotations

import time

from tests.support.processes import ManagedProcess

#: A brand-new member joining a group is only *noticed* by the existing members through their own
#: heartbeat responses -- `heartbeat.interval.ms` (`contracts/topics/v0/topic-defaults.json`
#: `clients.worker_consumer`, 5000) -- not on their next `poll()`. Declaring the assignment "stable"
#: before at least a few of those intervals have had a chance to land would just measure "no rebalance
#: has happened *yet*" as if it meant "no rebalance is going to happen".
MIN_SETTLE_WAIT_S = 15.0


def net_assigned(process: ManagedProcess) -> int:
    """The partitions this process currently owns, from its own log: every `partitions_assigned`
    `count` adds, every `partitions_revoked` `count` subtracts. Relies on `collect_pending` having been
    called recently enough that the relevant lines are already in `.lines` (see `wait_until_settled`)."""
    total = 0
    for line in process.lines:
        event = line.get("event")
        if event == "partitions_assigned":
            total += int(line.get("count", 0))
        elif event == "partitions_revoked":
            total -= int(line.get("count", 0))
    return total


def wait_until_settled(
    processes: dict[str, ManagedProcess],
    *,
    expected_total: int,
    timeout_s: float = 90.0,
    quiet_polls: int = 3,
    poll_interval_s: float = 0.5,
    min_wait_s: float = MIN_SETTLE_WAIT_S,
) -> dict[str, int]:
    """Poll every process's pending output until the group's combined net assignment covers
    `expected_total` partitions and holds unchanged for `quiet_polls` consecutive samples taken no
    earlier than `min_wait_s` after this call started -- "stable" as every caller of this module means
    it (see the module docstring for why the grace period is load-bearing, not just a slower test)."""
    started = time.monotonic()
    deadline = started + timeout_s
    stable_rounds = 0
    previous: dict[str, int] | None = None
    counts: dict[str, int] = {}
    while time.monotonic() < deadline:
        for process in processes.values():
            process.collect_pending()
        counts = {name: net_assigned(process) for name, process in processes.items()}
        eligible = time.monotonic() - started >= min_wait_s
        if eligible and sum(counts.values()) == expected_total and counts == previous:
            stable_rounds += 1
            if stable_rounds >= quiet_polls:
                return counts
        else:
            stable_rounds = 0
        previous = counts
        time.sleep(poll_interval_s)
    raise AssertionError(
        f"assignment across {sorted(processes)} never settled to {expected_total} partitions "
        f"within {timeout_s}s (last observed: {counts})"
    )
