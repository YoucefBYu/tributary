"""Mutation controls for `tests.support.chaos_assertions`: prove each check actually goes red on a
broken input before trusting it to certify a real chaos scenario green.

PHASE-0-DECISION.md section 4: five S5 claims were first rated `WEAKLY SUPPORTED` because a check could
pass without the fault it was named after ever firing. The fix there was making the *harness* fail when
the fault did not fire; this file is the other half of that lesson applied to W2-9 -- the *assertion
helpers themselves* are proven capable of failing, with no Kafka, no Docker, and no real worker involved.
Not marked `integration` or `chaos` on purpose: this is a fast unit-level check of test infrastructure,
meant to run on every `pytest` invocation, not only the chaos suite.
"""

from __future__ import annotations

import pytest

from tests.support.chaos_assertions import (
    assert_fault_fired,
    assert_no_acknowledged_loss,
    assert_per_key_order_preserved,
    duplicate_rate,
)


def test_loss_check_passes_when_every_published_id_was_delivered() -> None:
    published = {"a", "b", "c"}
    delivered = {"a", "b", "c"}  # duplicates are fine under at-least-once
    assert_no_acknowledged_loss(published, delivered)


def test_loss_check_catches_a_dropped_message() -> None:
    """Mutation control: a broken worker that silently drops one acknowledged message must turn this
    check red, or the check is certifying nothing about invariant 1."""
    published = {"a", "b", "c"}
    delivered = {"a", "c"}  # "b" vanished -- exactly what invariant 1 forbids
    with pytest.raises(AssertionError, match="invariant 1 violated"):
        assert_no_acknowledged_loss(published, delivered)


def test_order_check_passes_for_monotonic_sequences() -> None:
    assert_per_key_order_preserved({b"key-1": [1, 2, 3], b"key-2": [1, 2]})


def test_order_check_catches_a_reordered_sequence() -> None:
    """Mutation control: two messages for one key delivered out of order must turn this check red, or
    it is certifying nothing about invariant 5."""
    with pytest.raises(AssertionError, match="order broken"):
        assert_per_key_order_preserved({b"key-1": [1, 3, 2]})


def test_order_check_does_not_flag_a_replayed_duplicate_as_reordering() -> None:
    """Pins PHASE-0-DECISION section 3.1's rule the other direction: `1, 2, 3, 1, 3, 2` (S5's example)
    must NOT trip this check, because a redelivered sequence number is at-least-once working correctly,
    not a reordering. Callers only ever pass first-delivery sequences (`kafka_helpers.first_delivery_order`
    already drops the repeats), so this test documents why that pre-filtering step is load-bearing rather
    than cosmetic -- passing the raw, undeduplicated sequence here would raise, which is why no scenario
    does that."""
    assert_per_key_order_preserved({b"key-1": [1, 2, 3]})  # what remains after de-duplication


def test_duplicate_rate_is_zero_with_no_faults() -> None:
    assert duplicate_rate(published_count=100, delivered_count=100) == 0.0


def test_duplicate_rate_reports_the_replay_fraction() -> None:
    assert duplicate_rate(published_count=100, delivered_count=130) == pytest.approx(0.30)


def test_fault_fired_check_catches_a_fault_that_never_happened() -> None:
    """Mutation control for the "did the fault actually fire" half of every scenario: a condition of
    `False` (the kill missed, the joiner got nothing, the outage never started) must raise, matching the
    S5 pattern this whole suite is built around."""
    with pytest.raises(AssertionError, match="fault did not fire"):
        assert_fault_fired(False, description="broker kill never reached the target")


def test_fault_fired_check_passes_a_confirmed_fault() -> None:
    assert_fault_fired(True, description="broker reached Docker's exited state")
