from __future__ import annotations

import pytest
from tributary.loaders.http.backoff import next_delay_ms


def test_fixed_backoff_is_constant() -> None:
    for attempt in (1, 2, 5):
        assert next_delay_ms("fixed", attempt, initial_delay_ms=1000, max_delay_ms=60_000) == 1000


def test_exponential_backoff_doubles() -> None:
    assert next_delay_ms("exponential", 1, initial_delay_ms=250, max_delay_ms=60_000) == 250
    assert next_delay_ms("exponential", 2, initial_delay_ms=250, max_delay_ms=60_000) == 500
    assert next_delay_ms("exponential", 3, initial_delay_ms=250, max_delay_ms=60_000) == 1000


def test_exponential_backoff_is_capped() -> None:
    assert next_delay_ms("exponential", 20, initial_delay_ms=250, max_delay_ms=60_000) == 60_000


def test_attempt_must_be_positive() -> None:
    with pytest.raises(ValueError, match="attempt"):
        next_delay_ms("fixed", 0, initial_delay_ms=100, max_delay_ms=1000)


def test_unknown_backoff_raises() -> None:
    with pytest.raises(ValueError, match="backoff"):
        next_delay_ms("random", 1, initial_delay_ms=100, max_delay_ms=1000)  # type: ignore[arg-type]
