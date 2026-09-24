"""Property tests for the delay-mechanism model. Fast: no Kafka."""

from __future__ import annotations

from collections import Counter

from hypothesis import given, settings
from hypothesis import strategies as st
from s7.model import MECHANISMS, Arrival, count_inversions, simulate, steady_arrivals
from s7.plan import POISON, FailurePlan, Tiers

tiers_strategy = st.lists(st.floats(min_value=0.1, max_value=50.0), min_size=1, max_size=4).map(
    lambda delays: Tiers(tuple(sorted(delays)))
)
plans = st.builds(
    FailurePlan,
    seed=st.integers(0, 10_000),
    fail_first=st.tuples(st.floats(0, 0.3), st.floats(0, 0.2), st.floats(0, 0.1)),
    poison=st.floats(0, 0.05),
    outages=st.lists(
        st.tuples(st.floats(0, 20), st.floats(0.1, 5)).map(lambda w: (w[0], w[0] + w[1])), max_size=2
    ).map(tuple),
)
workloads = st.builds(
    steady_arrivals,
    rate=st.floats(5, 60),
    duration=st.floats(1, 10),
    keys=st.integers(1, 20),
    seed=st.integers(0, 1000),
)


@settings(max_examples=150, deadline=None)
@given(workloads, plans, tiers_strategy, st.sampled_from(MECHANISMS), st.integers(1, 6))
def test_every_message_is_delivered_or_dead_lettered_exactly_once(
    arrivals: list[Arrival], plan: FailurePlan, tiers: Tiers, mechanism: str, max_attempts: int
) -> None:
    result = simulate(arrivals, plan, tiers, mechanism, max_attempts=max_attempts)  # type: ignore[arg-type]
    succeeded = result.succeeded()
    dead = Counter(result.dlq)
    for arrival in arrivals:
        assert (arrival.msg_id in succeeded) != (arrival.msg_id in dead)
    assert all(count == 1 for count in dead.values())
    tries = Counter(attempt.msg_id for attempt in result.attempts)
    for msg_id in dead:
        assert tries[msg_id] == max_attempts
    for arrival in arrivals:
        if plan.leading_failures(arrival.msg_id) >= POISON:
            assert arrival.msg_id in dead


@settings(max_examples=150, deadline=None)
@given(workloads, plans, tiers_strategy, st.sampled_from(MECHANISMS))
def test_no_attempt_is_early(arrivals: list[Arrival], plan: FailurePlan, tiers: Tiers, mechanism: str) -> None:
    result = simulate(arrivals, plan, tiers, mechanism)  # type: ignore[arg-type]
    assert all(attempt.lateness >= -1e-9 for attempt in result.attempts)


@settings(max_examples=150, deadline=None)
@given(workloads, plans, tiers_strategy)
def test_pause_lateness_is_bounded_by_the_tier_spread(arrivals: list[Arrival], plan: FailurePlan, tiers: Tiers) -> None:
    """Head-of-line blocking can cost at most max(delay) - min(delay): the report's headline bound."""
    result = simulate(arrivals, plan, tiers, "pause")
    spread = max(tiers.delays) - min(tiers.delays)
    assert all(attempt.lateness <= spread + 1e-9 for attempt in result.retries())


@settings(max_examples=150, deadline=None)
@given(workloads, plans, tiers_strategy, st.sampled_from(("buffer", "tiers")))
def test_buffer_and_tiers_have_no_head_of_line_blocking(
    arrivals: list[Arrival], plan: FailurePlan, tiers: Tiers, mechanism: str
) -> None:
    result = simulate(arrivals, plan, tiers, mechanism)  # type: ignore[arg-type]
    assert all(abs(attempt.lateness) < 1e-9 for attempt in result.attempts)


@settings(max_examples=100, deadline=None)
@given(workloads, plans, tiers_strategy, st.sampled_from(MECHANISMS))
def test_messages_that_never_fail_keep_their_order(
    arrivals: list[Arrival], plan: FailurePlan, tiers: Tiers, mechanism: str
) -> None:
    result = simulate(arrivals, plan, tiers, mechanism)  # type: ignore[arg-type]
    failed = {attempt.msg_id for attempt in result.attempts if not attempt.ok}
    clean = [(a.key, a.seq, a.at) for a in result.attempts if a.ok and a.msg_id not in failed]
    assert count_inversions(clean) == 0


def test_pause_blocks_a_short_retry_behind_a_long_one() -> None:
    """Retry 2 of "a" (12 s) is appended just before retry 1 of "b" (1 s) on the same partition."""
    tiers = Tiers((1.0, 12.0))
    arrivals = [Arrival(0.0, "a", 0, 0), Arrival(1.05, "b", 0, 1)]
    # every attempt in [0, 1.1) fails: a@0 fails -> retry due 1.0; a@1.0 fails -> retry due 13.0;
    # b@1.05 fails -> retry due 2.05, but it sits behind a's 13.0 record under `pause`.
    plan = FailurePlan(fail_first=(), poison=0.0, outages=((0.0, 1.1),))
    paused = simulate(arrivals, plan, tiers, "pause", partitions=1)
    buffered = simulate(arrivals, plan, tiers, "buffer", partitions=1)
    b_pause = [a for a in paused.retries() if a.msg_id == "b"][0]
    b_buffer = [a for a in buffered.retries() if a.msg_id == "b"][0]
    assert b_pause.lateness > 10.0
    assert b_buffer.lateness == 0.0


def test_simulation_is_deterministic() -> None:
    arrivals = steady_arrivals(rate=50, duration=5, keys=10)
    plan = FailurePlan(outages=((1.0, 2.0),))
    for mechanism in MECHANISMS:
        first = simulate(arrivals, plan, Tiers((0.5, 2.0)), mechanism)
        second = simulate(arrivals, plan, Tiers((0.5, 2.0)), mechanism)
        assert first.attempts == second.attempts


def test_failure_plan_is_a_pure_function_of_the_message_id() -> None:
    plan = FailurePlan(seed=3)
    assert [plan.leading_failures(f"m{i}") for i in range(200)] == [
        FailurePlan(seed=3).leading_failures(f"m{i}") for i in range(200)
    ]
    counts = Counter(min(plan.leading_failures(f"m{i}"), 9) for i in range(20_000))
    assert 0.005 < counts[9] / 20_000 < 0.02  # poison ~1%
    assert 0.08 < counts[1] / 20_000 < 0.12  # fail-first-1 ~10%
