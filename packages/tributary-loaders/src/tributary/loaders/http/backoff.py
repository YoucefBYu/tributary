"""Backoff delay computation for blocking retry (`retry.backoff`, `retry.policy: blocking`).

A pure function, deliberately: the sleep itself belongs to whatever holds the partition open while
retrying (`consumer.py`'s `OutConsumer` in this wave; a future `tributary-runtime` stage later), so
this is unit-testable with no event loop and no clock mocking beyond passing `attempt` in.
"""

from __future__ import annotations

Backoff = str  # "exponential" | "fixed"


def next_delay_ms(backoff: Backoff, attempt: int, *, initial_delay_ms: int, max_delay_ms: int) -> int:
    """The delay before retry number `attempt` (1-based: the first retry is `attempt=1`, i.e. the
    second delivery attempt overall). `exponential` doubles each time from `initial_delay_ms`;
    `fixed` is `initial_delay_ms` every time. Both are capped at `max_delay_ms`."""
    if attempt < 1:
        raise ValueError("attempt must be >= 1")
    if backoff == "fixed":
        delay = initial_delay_ms
    elif backoff == "exponential":
        delay = initial_delay_ms * (2 ** (attempt - 1))
    else:
        raise ValueError(f"backoff must be 'exponential' or 'fixed', not {backoff!r}")
    return min(delay, max_delay_ms)
