"""Properties of the retry delay schedule. Pure math, no Kafka, no clock."""

from __future__ import annotations

import random

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st
from s5.backoff import Backoff, Jitter

@st.composite
def _backoffs(draw: st.DrawFn) -> Backoff:
    # cap >= base has to hold by construction: Backoff validates in __init__,
    # so a .filter() on builds() would blow up before the filter ran.
    base = draw(st.floats(min_value=0.001, max_value=5.0, allow_nan=False))
    return Backoff(
        base=base,
        factor=draw(st.floats(min_value=1.0, max_value=4.0, allow_nan=False)),
        cap=draw(st.floats(min_value=base, max_value=600.0, allow_nan=False)),
        jitter=draw(st.sampled_from(list(Jitter))),
    )


configs = _backoffs()

attempts = st.integers(min_value=0, max_value=40)


@given(cfg=configs, attempt=attempts)
def test_ceiling_is_bounded_by_cap_and_never_negative(cfg: Backoff, attempt: int) -> None:
    ceiling = cfg.ceiling(attempt)
    assert 0 < ceiling <= cfg.cap


@given(cfg=configs, attempt=attempts)
def test_ceiling_is_monotonic_in_attempt(cfg: Backoff, attempt: int) -> None:
    assert cfg.ceiling(attempt + 1) >= cfg.ceiling(attempt)


@given(cfg=configs)
def test_first_ceiling_is_the_base(cfg: Backoff) -> None:
    assert cfg.ceiling(0) == pytest.approx(min(cfg.base, cfg.cap))


@given(cfg=configs)
def test_absurd_attempt_counts_do_not_overflow(cfg: Backoff) -> None:
    assert cfg.ceiling(10_000) == cfg.cap


def test_negative_attempt_is_rejected() -> None:
    with pytest.raises(ValueError):
        Backoff().ceiling(-1)


@pytest.mark.parametrize(
    "kwargs",
    [{"base": 0.0}, {"base": -1.0}, {"factor": 0.5}, {"base": 10.0, "cap": 1.0}],
)
def test_invalid_configuration_is_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError):
        Backoff(**kwargs)


@given(cfg=configs, attempt=attempts, seed=st.integers(0, 2**32 - 1))
def test_delay_stays_inside_the_documented_band(cfg: Backoff, attempt: int, seed: int) -> None:
    """Each jitter mode has a band; a delay outside it is a bug in the policy.

    The bands matter operationally: FULL can return ~0 (a retry storm risk
    that `blocking` tolerates because only one partition is involved), EQUAL
    never drops below half the ceiling.
    """
    rng = random.Random(seed)
    ceiling = cfg.ceiling(attempt)
    delay = cfg.delay(attempt, rng, previous=None)
    match cfg.jitter:
        case Jitter.NONE:
            assert delay == ceiling
        case Jitter.FULL:
            assert 0.0 <= delay <= ceiling
        case Jitter.EQUAL:
            assert ceiling / 2.0 <= delay <= ceiling + 1e-9
        case Jitter.DECORRELATED:
            assert min(cfg.base, cfg.cap) <= delay <= cfg.cap


@given(cfg=configs, n=st.integers(1, 20), seed=st.integers(0, 2**32 - 1))
def test_schedule_is_deterministic_for_a_seed(cfg: Backoff, n: int, seed: int) -> None:
    assert cfg.schedule(n, random.Random(seed)) == cfg.schedule(n, random.Random(seed))


@given(cfg=configs, n=st.integers(1, 20), seed=st.integers(0, 2**32 - 1))
def test_schedule_never_exceeds_the_worst_case_total(cfg: Backoff, n: int, seed: int) -> None:
    """Bounding total stall time is what makes `blocking` safe to operate.

    An operator sizing `max` needs to know the worst case a partition can be
    stalled: sum of ceilings, which is bounded by ``n * cap``.
    """
    schedule = cfg.schedule(n, random.Random(seed))
    assert len(schedule) == n
    assert sum(schedule) <= n * cfg.cap + 1e-6


@given(seed=st.integers(0, 2**32 - 1))
def test_full_jitter_actually_spreads(seed: int) -> None:
    """Jitter must de-synchronise partitions retrying against one dead sink."""
    cfg = Backoff(base=1.0, factor=2.0, cap=30.0, jitter=Jitter.FULL)
    rng = random.Random(seed)
    delays = [cfg.delay(4, rng) for _ in range(200)]
    assume(len(set(delays)) > 1)
    assert max(delays) - min(delays) > cfg.ceiling(4) / 4
