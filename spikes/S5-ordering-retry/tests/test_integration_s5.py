"""Integration tests: the same properties, against the real spike cluster.

`test_properties.py` explores hundreds of fault schedules against the model in
`s5.fakekafka`. These tests do the opposite trade: a handful of scenarios, real
brokers, real worker processes, real `kill -9`. Their job is to catch the case
where the model is wrong about librdkafka or about Kafka 4.0, which no amount
of model-side property testing can find.

Requires the spike cluster (`spikes/common/docker-compose.yml`). Each test
creates its own ``spike.s5.<scenario>-<hhmmss>.*`` topics and deletes them
afterwards, so nothing is left behind for the next spike to trip over.

Sizes are deliberately small (a few hundred messages): these are correctness
checks, not measurements. `s5.runner` with a bigger ``--count`` produces the
numbers the report quotes.
"""

from __future__ import annotations

import pytest
from s5 import runner
from s5.runner import Options, RunResult
from tbcommon import admin

pytestmark = [pytest.mark.integration, pytest.mark.timeout(900)]

SMALL = Options(count=600, keys=24, partitions=4, session_timeout_ms=10_000)


@pytest.fixture(scope="module", autouse=True)
def cluster() -> None:
    admin.wait_for_cluster()


def _assert_core_properties(result: RunResult) -> None:
    """The three things every scenario must satisfy, whatever else it tests."""
    assert result.report.order_preserved, (
        f"per-key reordering in {result.scenario}: {list(result.report.reordered_keys)[:5]}"
    )
    assert result.report.no_loss, (
        f"{len(result.report.lost)} message(s) lost in {result.scenario}"
    )
    assert not result.commit_violations, "\n".join(result.commit_violations[:3])
    assert not result.phi_findings, f"PHI markers in lifecycle events: {result.phi_findings}"
    assert result.report.delivered > 0, "a run that delivered nothing proves nothing"


def test_baseline_delivers_everything_in_order_and_drains() -> None:
    result = runner.run("baseline", SMALL)
    _assert_core_properties(result)
    assert not result.undrained, f"partitions left uncommitted: {result.undrained}"
    assert result.report.unique_msg_ids == result.produced


def test_blocking_retry_pauses_and_resumes_without_skipping() -> None:
    """The librdkafka trap, on a real broker.

    A 25% transient failure rate forces hundreds of pause/seek/resume cycles.
    If ``resume()`` really refetches from last-consumed+1 and the seek were
    missing, the failed messages would silently vanish -- so `no_loss` here is
    the assertion that the seek-before-resume works against Kafka 4.0.
    """
    result = runner.run("blocking_retry", SMALL)
    _assert_core_properties(result)
    assert result.notes["pauses"] > 0, "the scenario never actually paused a partition"
    assert result.notes["resumes"] > 0
    assert not result.undrained


def test_poison_messages_reach_the_dlq_and_the_partition_keeps_moving() -> None:
    result = runner.run("poison_dlq", SMALL)
    _assert_core_properties(result)
    assert result.dlq_ids == result.expected_dlq_ids, (
        f"DLQ mismatch: {len(result.dlq_ids - result.expected_dlq_ids)} unexpected, "
        f"{len(result.expected_dlq_ids - result.dlq_ids)} missing"
    )
    assert not result.dlq_ids & result.delivered_ids, (
        "a dead-lettered message must not also count as delivered"
    )
    assert not result.undrained, "the DLQ exists so an ordered partition can continue"


def test_halt_on_exhaust_stops_the_partition_instead_of_reordering_it() -> None:
    result = runner.run("halt", SMALL)
    # Loss is judged against what was reachable below the halt point, so the
    # core assertions still apply.
    _assert_core_properties(result)
    assert not result.dlq_ids, "halt-partition must not dead-letter"
    assert result.undrained, "nothing halted, so the scenario did not test halting"
    assert result.notes["halted_partitions"] >= 1


def test_killing_a_worker_replays_from_the_last_commit_and_loses_nothing() -> None:
    """``kill -9`` mid-run. Duplicates are allowed; loss and reordering are not."""
    result = runner.run("kill", SMALL)
    _assert_core_properties(result)
    assert not result.undrained
    assert result.worker_stats["worker-a"] == {"killed": True}, (
        "worker-a wrote a stats file, so it exited cleanly and was never really killed"
    )
    # Reported, not asserted to be zero: replay from the last commit is the
    # whole at-least-once bargain.
    print("duplicate rate after kill:", result.report.duplicate_rate)


def test_a_joining_member_rebalances_cooperatively_without_reordering() -> None:
    result = runner.run("rebalance", SMALL)
    _assert_core_properties(result)
    assert not result.undrained
    revokes = [v for v in result.notes["revokes"].values() if v]
    assert revokes, "no partition ever moved, so no rebalance was exercised"


def test_static_restart_inside_the_session_timeout_does_not_disturb_the_survivor() -> None:
    result = runner.run("static_restart", SMALL)
    _assert_core_properties(result)
    assert not result.undrained


@pytest.mark.chaos
def test_a_broker_kill_during_delivery_loses_nothing() -> None:
    result = runner.run("broker_kill", SMALL)
    _assert_core_properties(result)
    assert not result.undrained


@pytest.mark.chaos
def test_transactional_stage_is_exactly_once_across_a_kill() -> None:
    """EOS is a broker property, so it is only testable here.

    The stage is killed with a transaction open and restarted with the same
    ``transactional.id``. Read with ``read_committed``, the output must contain
    each input message exactly once -- the aborted batch's records must be
    invisible and its input offsets must have rolled back with them.
    """
    result = runner.run("txn_crash", SMALL)
    assert result.report.order_preserved, f"reordered: {list(result.report.reordered_keys)[:5]}"
    assert result.report.no_loss, f"{len(result.report.lost)} message(s) never reached `out`"
    assert result.report.duplicates == 0, (
        f"{result.report.duplicates} duplicate(s) in a read_committed read: not exactly-once"
    )
    assert not result.undrained
