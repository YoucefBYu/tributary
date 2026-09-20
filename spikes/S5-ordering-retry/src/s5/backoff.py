"""Exponential backoff with jitter. Pure math, no I/O, fully property-testable."""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import StrEnum


class Jitter(StrEnum):
    NONE = "none"
    FULL = "full"  # AWS "full jitter": uniform(0, ceiling)
    EQUAL = "equal"  # half fixed, half random
    DECORRELATED = "decorrelated"  # uniform(base, prev*3), capped


@dataclass(frozen=True, slots=True)
class Backoff:
    """Delay schedule for `blocking` retry.

    ``attempt`` is 0-based: attempt 0 is the delay *before the first retry*,
    i.e. after the initial delivery already failed once.
    """

    base: float = 0.5
    factor: float = 2.0
    cap: float = 30.0
    jitter: Jitter = Jitter.FULL

    def __post_init__(self) -> None:
        if self.base <= 0:
            raise ValueError("base must be > 0")
        if self.factor < 1:
            raise ValueError("factor must be >= 1")
        if self.cap < self.base:
            raise ValueError("cap must be >= base")

    def ceiling(self, attempt: int) -> float:
        """Un-jittered upper bound for ``attempt``. Never exceeds ``cap``."""
        if attempt < 0:
            raise ValueError("attempt must be >= 0")
        # Guard against float overflow for absurd attempt counts.
        if attempt > 64:
            return self.cap
        return min(self.cap, self.base * (self.factor**attempt))

    def delay(self, attempt: int, rng: random.Random, previous: float | None = None) -> float:
        ceiling = self.ceiling(attempt)
        match self.jitter:
            case Jitter.NONE:
                return ceiling
            case Jitter.FULL:
                return rng.uniform(0.0, ceiling)
            case Jitter.EQUAL:
                half = ceiling / 2.0
                return half + rng.uniform(0.0, half)
            case Jitter.DECORRELATED:
                prev = self.base if previous is None else max(previous, self.base)
                return min(self.cap, rng.uniform(self.base, prev * 3.0))

    def schedule(self, attempts: int, rng: random.Random) -> list[float]:
        out: list[float] = []
        prev: float | None = None
        for attempt in range(attempts):
            prev = self.delay(attempt, rng, previous=prev)
            out.append(prev)
        return out
