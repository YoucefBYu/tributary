"""A per-destination circuit breaker (`with.circuitBreaker`).

Closed -> Open after `failure_threshold` consecutive failures; Open rejects every call without
attempting one until `open_seconds` has elapsed, then allows exactly one probe (Half-Open); that
probe's outcome decides Closed (success) or Open again (failure), doubling the wait is not modelled
in v0 — a fixed `open_seconds` is what the schema declares. A destination with no `circuitBreaker`
configured never opens (`CircuitBreaker.disabled()`): every call is attempted and classified, same
as one with an infinite `failure_threshold`.

`clock` is injectable so tests do not sleep real seconds.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from enum import Enum, auto


class State(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    def __init__(
        self, failure_threshold: int, open_seconds: float, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self._threshold = failure_threshold
        self._open_seconds = open_seconds
        self._clock = clock
        self._state = State.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @classmethod
    def disabled(cls) -> CircuitBreaker:
        return cls(failure_threshold=2**31, open_seconds=0.0)

    @property
    def state(self) -> State:
        if (
            self._state is State.OPEN
            and self._opened_at is not None
            and self._clock() - self._opened_at >= self._open_seconds
        ):
            return State.HALF_OPEN
        return self._state

    def allow(self) -> bool:
        """`True` if a call may be attempted right now. A `True` in the `HALF_OPEN` state is the
        one probe call; call `record_success`/`record_failure` with its outcome."""
        return self.state is not State.OPEN

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._state = State.CLOSED
        self._opened_at = None

    def record_failure(self) -> None:
        if self.state is State.HALF_OPEN:
            # the probe failed: stay open for another full window, not a fresh failure count
            self._state = State.OPEN
            self._opened_at = self._clock()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold:
            self._state = State.OPEN
            self._opened_at = self._clock()
