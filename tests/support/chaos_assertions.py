"""The headline checks W2-9's chaos scenarios all make, factored out once so each scenario and its
mutation control run the exact same code path -- a check that is only ever exercised against a healthy
run cannot be trusted to fail when it should (PHASE-0-DECISION.md section 4: "a green test whose fault
never fired is indistinguishable from a passing system").

`tests/chaos/test_chaos_assertion_mutation_controls.py` is the mutation control for this module: it
feeds each function a deliberately broken input (a dropped id, a reordered sequence) and asserts the
function actually raises. Nothing in `packages/*/src` is touched or stood in for -- these functions are
test-owned verification logic, not a replacement for the runtime under test.
"""

from __future__ import annotations

from itertools import pairwise


def assert_no_acknowledged_loss(published_ids: set[str], delivered_ids: set[str]) -> None:
    """Invariant 1/4: every id this scenario put on `raw` (or had ACKed by a receiver) must show up
    somewhere downstream, no matter how many times a fault redelivered it."""
    missing = published_ids - delivered_ids
    assert not missing, f"invariant 1 violated: {len(missing)} acknowledged message(s) never delivered: {missing}"


def duplicate_rate(published_count: int, delivered_count: int) -> float:
    """The fraction of deliveries that were replays, for the report line every scenario prints.

    `delivered_count` is always `>= published_count` under at-least-once; a `delivered_count` below
    `published_count` is a loss, which `assert_no_acknowledged_loss` catches separately and this
    function does not re-check (division here would just produce a confusing negative rate)."""
    if published_count <= 0:
        return 0.0
    return max(0.0, (delivered_count - published_count) / published_count)


def assert_per_key_order_preserved(first_delivery_by_key: dict[bytes, list[int]]) -> None:
    """Invariant 5, checked S5's way: the FIRST delivery of each sequence number, per key, must be
    increasing. A redelivered sequence number is a duplicate, not a reordering -- that is what
    at-least-once means, and it is exactly why invariant 4 requires loader idempotency on `tb-msg-id`
    rather than relying on delivery order alone. Callers pass only first deliveries (see
    `tests.support.kafka_helpers.first_delivery_order`), so this function does not need to de-duplicate
    itself; it exists to keep that rule written down next to the assertion that depends on it.
    """
    for key, sequences in first_delivery_by_key.items():
        assert sequences == sorted(sequences), f"per-key order broken for key {key!r}: {sequences}"


def assert_each_key_on_one_partition(partitions_by_key: dict[bytes, set[int]]) -> None:
    """Invariant 5: a key's records all sit on one partition. A key seen on two partitions means ordering
    for it cannot hold (Kafka orders within a partition only), whatever the sequence numbers say."""
    split = {key: sorted(parts) for key, parts in partitions_by_key.items() if len(parts) != 1}
    assert not split, f"keys not confined to one partition: {split}"


def keys_hit_by_fault(
    partitions_by_key: dict[bytes, set[int]],
    partitions_led_by_killed: frozenset[int],
    acked_indexes_by_key: dict[bytes, list[int]],
    *,
    kill_index: int,
) -> set[bytes]:
    """Keys that make a keyed chaos run mean something: the key lives on a partition the killed broker led,
    and it has an ACKed send on each side of the kill (`index < kill_index` and `index >= kill_index`). A
    key on an unaffected partition, or one with no traffic after the kill, says nothing about order across
    a leader change."""
    return {
        key
        for key, partitions in partitions_by_key.items()
        if partitions & partitions_led_by_killed
        and any(i < kill_index for i in acked_indexes_by_key.get(key, []))
        and any(i >= kill_index for i in acked_indexes_by_key.get(key, []))
    }


def assert_fault_fired(condition: bool, *, description: str) -> None:
    """A named assertion for the "did the fault actually happen" half of every chaos scenario
    (PHASE-0-DECISION section 4). Kept as its own function -- rather than a bare `assert` at the call
    site -- so every scenario's fault-confirmation reads the same way and is easy to grep for."""
    assert condition, f"fault did not fire: {description} -- this run proves nothing about the invariant"


def count_order_violations(first_delivery_by_key: dict[bytes, list[int]]) -> int:
    """Per-key adjacent inversions among FIRST deliveries (0 when `assert_per_key_order_preserved`
    would pass). Counted rather than asserted so the report row can carry the number."""
    return sum(
        1 for sequences in first_delivery_by_key.values() for earlier, later in pairwise(sequences) if later < earlier
    )


def record_chaos_row(
    *,
    scenario: str,
    sent: int,
    delivered_total: int,
    delivered_distinct: int,
    order_violations: int | None,
    fault_fired: bool,
) -> dict[str, object]:
    """Print one machine-readable report row and, when `TB_CHAOS_ROWS_FILE` is set, append it as JSON
    Lines. Used by W2-9's duplicate-rate report (docs/spikes/wave-2-duplicate-rate.md). Ids only, no
    payload content (invariant 6). `order_violations=None` means the scenario has no per-key order
    assertion (the receiver-originated broker-kill scenario)."""
    import json
    import os

    duplicates = delivered_total - delivered_distinct
    row: dict[str, object] = {
        "scenario": scenario,
        "sent": sent,
        "delivered_total": delivered_total,
        "delivered_distinct": delivered_distinct,
        "duplicates": duplicates,
        "duplicate_rate": duplicate_rate(published_count=sent, delivered_count=delivered_total),
        "order_violations": order_violations,
        "fault_fired": fault_fired,
    }
    line = json.dumps(row, sort_keys=True)
    print(f"CHAOS_ROW {line}")
    path = os.environ.get("TB_CHAOS_ROWS_FILE")
    if path:
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(line + "\n")
    return row
