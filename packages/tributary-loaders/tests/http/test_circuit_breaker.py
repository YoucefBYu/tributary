from __future__ import annotations

from tributary.loaders.http.circuit_breaker import CircuitBreaker, State


def test_starts_closed_and_allows() -> None:
    b = CircuitBreaker(failure_threshold=3, open_seconds=10)
    assert b.state is State.CLOSED
    assert b.allow() is True


def test_opens_after_threshold_consecutive_failures() -> None:
    b = CircuitBreaker(failure_threshold=3, open_seconds=10)
    b.record_failure()
    b.record_failure()
    assert b.state is State.CLOSED
    b.record_failure()
    assert b.state is State.OPEN
    assert b.allow() is False


def test_a_success_resets_the_failure_count() -> None:
    b = CircuitBreaker(failure_threshold=3, open_seconds=10)
    b.record_failure()
    b.record_failure()
    b.record_success()
    b.record_failure()
    b.record_failure()
    assert b.state is State.CLOSED  # would have opened without the reset


def test_half_open_after_the_window_then_success_closes() -> None:
    now = [0.0]
    b = CircuitBreaker(failure_threshold=1, open_seconds=5, clock=lambda: now[0])
    b.record_failure()
    assert b.state is State.OPEN
    now[0] = 4.9
    assert b.state is State.OPEN
    now[0] = 5.0
    assert b.state is State.HALF_OPEN
    assert b.allow() is True
    b.record_success()
    assert b.state is State.CLOSED


def test_half_open_probe_failure_reopens_for_a_full_window() -> None:
    now = [0.0]
    b = CircuitBreaker(failure_threshold=1, open_seconds=5, clock=lambda: now[0])
    b.record_failure()
    now[0] = 5.0
    assert b.state is State.HALF_OPEN
    b.record_failure()
    assert b.state is State.OPEN
    now[0] = 9.9
    assert b.state is State.OPEN
    now[0] = 10.0
    assert b.state is State.HALF_OPEN


def test_disabled_breaker_never_opens() -> None:
    b = CircuitBreaker.disabled()
    for _ in range(1000):
        b.record_failure()
    assert b.allow() is True
