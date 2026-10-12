from __future__ import annotations

import pytest

from tests.support.outage_verdict import outage_verdict


def test_median_inside_the_control_range_passes() -> None:
    verdict = outage_verdict([100, 110, 120, 130, 140], [90, 105, 125, 126, 300])
    assert verdict.passed and verdict.stopped_median == 125


def test_the_range_edges_count_as_inside() -> None:
    assert outage_verdict([100, 140], [100, 100, 100]).passed
    assert outage_verdict([100, 140], [140, 140, 140]).passed


def test_median_below_the_control_minimum_fails() -> None:
    assert not outage_verdict([100, 110, 120], [99, 99, 99]).passed


def test_a_faster_stopped_arm_passes_one_sided_but_is_outside_the_range() -> None:
    verdict = outage_verdict([100, 110, 120], [121, 121, 121])
    assert verdict.passed and not verdict.inside_range


def test_one_slow_stopped_run_does_not_fail_a_median_but_three_do() -> None:
    assert outage_verdict([100, 110, 120], [5, 105, 106, 107, 108]).passed
    assert not outage_verdict([100, 110, 120], [5, 6, 7, 105, 106]).passed  # median 7


def test_empty_arms_are_refused() -> None:
    with pytest.raises(ValueError):
        outage_verdict([], [1.0])
