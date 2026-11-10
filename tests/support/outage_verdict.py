"""The W3-9a pass rule (wave-3 section 4, criterion 2), one-sided per the lead's ruling after set 1: the stopped
runs' median throughput is not below the control runs' minimum (invariant 3 asks that the data plane not degrade;
a faster stopped arm is not a failure). The original two-sided rule is kept as `inside_range`. One function, so the
real check and its mutation control apply the same rule."""

from __future__ import annotations

import statistics
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Verdict:
    passed: bool
    inside_range: bool
    stopped_median: float
    control_min: float
    control_max: float
    control_median: float

    @property
    def stopped_over_control_median(self) -> float:
        return self.stopped_median / self.control_median

    def describe(self) -> str:
        return (
            f"stopped median {self.stopped_median:.0f} msg/s vs control range "
            f"[{self.control_min:.0f}, {self.control_max:.0f}] (median {self.control_median:.0f}): "
            f"{'PASS' if self.passed else 'FAIL'} (two-sided: {'inside' if self.inside_range else 'outside'})"
        )


def outage_verdict(control: list[float], stopped: list[float]) -> Verdict:
    if not control or not stopped:
        raise ValueError("both arms need at least one run")
    low, high = min(control), max(control)
    median = statistics.median(stopped)
    return Verdict(
        passed=median >= low,
        inside_range=low <= median <= high,
        stopped_median=median,
        control_min=low,
        control_max=high,
        control_median=statistics.median(control),
    )
