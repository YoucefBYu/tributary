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


def assert_fault_fired(condition: bool, *, description: str) -> None:
    """A named assertion for the "did the fault actually happen" half of every chaos scenario
    (PHASE-0-DECISION section 4). Kept as its own function -- rather than a bare `assert` at the call
    site -- so every scenario's fault-confirmation reads the same way and is easy to grep for."""
    assert condition, f"fault did not fire: {description} -- this run proves nothing about the invariant"
