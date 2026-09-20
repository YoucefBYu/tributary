"""Property tests for the invariants the ordered loader must defend.

These run against `s5.fakekafka`, the in-process model of the librdkafka
behaviours the algorithm depends on (`test_fakekafka.py` pins those behaviours
down; `test_integration_s5.py` re-checks them against a real broker). The point
of the split is coverage: hundreds of randomised fault schedules run here in
seconds, which is not affordable against a broker.

Properties, each mapped to the invariant it defends:

* **Invariant 5 (ordering).** Per key, the first delivery of each `tb-x-seq` is
  non-decreasing -- across transient failures, pause/resume cycles, cooperative
  rebalances, membership loss and `kill -9`.
* **Invariant 4 (at-least-once, no loss).** Every produced message ends up
  either in the sink or in the DLQ. Duplicates are permitted and measured;
  losses are not.
* **Commit-after-delivery.** A commit of offset `O` on a partition implies every
  offset below `O` was already durably handled. Checked *online*, in the commit
  hook, so a violation is caught at the moment it happens rather than inferred.
* **Invariant 6 (no PHI).** Assertions only ever touch ids, keys and sequence
  numbers. Nothing here decodes a payload.
"""

from __future__ import annotations

import itertools
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from s5 import fakekafka, generator
from s5.backoff import Backoff, Jitter
from s5.blocking_loader import BlockingLoader, LoaderConfig, OnExhaust
from s5.fakekafka import FakeCluster, SimulatedCrash
from s5.sink import FailurePlan, FileSink, read_delivery_log
from tbcommon import checks

RAW = "spike.s5prop.raw"
DLQ = "spike.s5prop.dlq"
GROUP = "s5-prop"

#: Backoff small enough that a few hundred pause/resume cycles stay fast, and
#: un-jittered so a failing example replays identically.
FAST_BACKOFF = Backoff(base=0.001, factor=2.0, cap=0.004, jitter=Jitter.NONE)

#: High enough that a transient failure rate of 0.3 effectively never exhausts
#: retries (0.3**20 ~ 3e-11), which keeps "went to the DLQ" meaning "poison".
MAX_ATTEMPTS = 20

#: `tmp_path` is created once per test *function*, but hypothesis runs a
#: function many times. Sharing one delivery log across examples silently merges
#: their results -- a 5-message scenario reporting 113 deliveries, with keys from
#: other examples showing up as reorderings. Every example gets its own file.
_run_counter = itertools.count()


# --------------------------------------------------------------------- harness
@dataclass
class Tracker:
    """What has been durably handled, and any commit-ordering violations.

    ``handled`` accumulates across loader instances on purpose: after a crash
    the successor re-delivers, and a commit it makes legitimately covers offsets
    that the dead worker had already written downstream.
    """

    handled: fakekafka.Handled = field(default_factory=fakekafka.Handled)
    violations: list[str] = field(default_factory=list)
    commits: int = 0


class TrackingSink:
    """Wraps :class:`FileSink` and records source coordinates as handled.

    Composition rather than subclassing: the loader only needs ``deliver`` and
    ``close``, and ``FileSink`` is a dataclass whose ``__post_init__`` opens the
    file, which makes inheritance fiddlier than it is worth.
    """

    def __init__(self, sink: FileSink, tracker: Tracker) -> None:
        self._sink = sink
        self._tracker = tracker

    def deliver(self, **kw: Any) -> bool:
        first = self._sink.deliver(**kw)
        # Only after deliver() returned, i.e. after the write was fsynced.
        self._tracker.handled.add(kw["topic"], kw["partition"], kw["offset"])
        return first

    def close(self) -> None:
        self._sink.close()


def _install_commit_hook(cluster: FakeCluster, tracker: Tracker, dlq_topic: str) -> None:
    """Assert commit-after-delivery at the instant of every commit."""

    def hook(event: fakekafka.CommitEvent) -> None:
        tracker.commits += 1
        done = set(tracker.handled.offsets.get((event.topic, event.partition), set()))
        # A message routed to the DLQ is also "handled": the loader flushes the
        # DLQ produce before storing the offset, so by now it is durable.
        for record in fakekafka.dlq_records(cluster, dlq_topic):
            if record.source_topic == event.topic and record.source_partition == event.partition:
                done.add(record.source_offset)
        missing = [o for o in range(event.committed) if o not in done]
        if missing:
            tracker.violations.append(
                f"commit {event.topic}[{event.partition}]@{event.committed} "
                f"by {event.consumer} precedes handling of offsets {missing[:8]}"
            )

    cluster.commit_hook = hook


def _loader(
    cluster: FakeCluster,
    tracker: Tracker,
    log_path: Path,
    *,
    plan: FailurePlan,
    instance_id: str,
    on_exhaust: OnExhaust = OnExhaust.DLQ_AND_CONTINUE,
    commit_every: int = 1,
) -> BlockingLoader:
    sink = TrackingSink(FileSink(path=log_path, plan=plan, fsync=False), tracker)
    cfg = LoaderConfig(
        topic=RAW,
        dlq_topic=DLQ,
        group=GROUP,
        instance_id=instance_id,
        max_attempts=MAX_ATTEMPTS,
        on_exhaust=on_exhaust,
        backoff=FAST_BACKOFF,
        commit_every=commit_every,
        commit_interval_s=0.0,
        poll_timeout_s=0.0,
        idle_exit_s=0.02,
    )
    # `sink` is duck-typed; the loader only calls deliver()/close().
    return BlockingLoader(cfg, sink, driver=cluster, worker=instance_id)  # type: ignore[arg-type]


def _drain(loader: BlockingLoader) -> bool:
    """Run a loader to quiescence. Returns True if it died of a simulated kill."""
    try:
        loader.run()
    except SimulatedCrash:
        return True
    return False


@dataclass
class Outcome:
    report: checks.OrderingReport
    delivered_ids: set[str]
    dlq_ids: set[str]
    tracker: Tracker
    stats: dict[str, float | int]


def run_scenario(
    tmp_path: Path,
    *,
    partitions: int,
    count: int,
    keys: int,
    transient_rate: float,
    poison_every: int | None,
    faults: list[tuple[int, str]],
    seed: int,
) -> Outcome:
    """Publish, then drain under a fault schedule, then drain clean.

    ``faults`` is a list of ``(poll_number, kind)`` applied to the first
    loader's consumer. The final clean drain (no injected faults, no transient
    failures) is what makes "no loss" a meaningful assertion: it guarantees the
    run reached quiescence rather than simply stopping early.
    """
    cluster = FakeCluster(default_partitions=partitions)
    cluster.create_topic(RAW, partitions)
    cluster.create_topic(DLQ, max(1, partitions // 2))

    plan_out = generator.publish(
        cluster.producer(), RAW, count=count, keys=keys, seed=seed, poison_every=poison_every
    )

    tracker = Tracker()
    _install_commit_hook(cluster, tracker, DLQ)
    log_path = tmp_path / f"deliveries-{next(_run_counter)}.jsonl"

    failing = FailurePlan(
        poison=plan_out.poison, transient_rate=transient_rate, seed=seed
    )
    clean = FailurePlan(poison=plan_out.poison, transient_rate=0.0, seed=seed)

    first = _loader(cluster, tracker, log_path, plan=failing, instance_id="worker-a")
    consumer = cluster.consumers[-1]
    for at_poll, kind in faults:
        if kind == "rebalance":
            consumer.schedule(at_poll, consumer.rebalance)
        elif kind == "lose":
            consumer.schedule(at_poll, consumer.lose)
        elif kind == "crash":
            consumer.schedule(at_poll, consumer.crash)
    _drain(first)

    # Successors until nothing is left. Two are enough: the first may itself be
    # cut short by a scheduled fault, the second runs clean.
    for name in ("worker-b", "worker-c"):
        _drain(_loader(cluster, tracker, log_path, plan=clean, instance_id=name))

    records = read_delivery_log(log_path)
    observed = [
        checks.Observation(key=str(r["key"]), seq=int(r["seq"]), msg_id=str(r["msg_id"]))
        for r in records
    ]
    return Outcome(
        report=checks.check(observed, expected=plan_out.deliverable),
        delivered_ids={o.msg_id for o in observed},
        dlq_ids={r.msg_id for r in fakekafka.dlq_records(cluster, DLQ)},
        tracker=tracker,
        stats=first.stats.as_dict(),
    )


# ------------------------------------------------------------------ strategies
fault_kinds = st.sampled_from(["rebalance", "lose", "crash"])


@st.composite
def scenarios(draw: st.DrawFn) -> dict[str, Any]:
    n_faults = draw(st.integers(min_value=0, max_value=2))
    return {
        "partitions": draw(st.integers(min_value=1, max_value=4)),
        "count": draw(st.integers(min_value=5, max_value=40)),
        "keys": draw(st.integers(min_value=1, max_value=6)),
        "transient_rate": draw(st.sampled_from([0.0, 0.1, 0.35])),
        "poison_every": draw(st.one_of(st.none(), st.integers(min_value=5, max_value=15))),
        "faults": draw(
            st.lists(
                st.tuples(st.integers(min_value=1, max_value=30), fault_kinds),
                min_size=n_faults,
                max_size=n_faults,
            )
        ),
        "seed": draw(st.integers(min_value=1, max_value=10_000)),
    }


#: 60 keeps the default suite around half a minute. The numbers the report
#: quotes come from a deeper run: `S5_MAX_EXAMPLES=500 uv run pytest tests/test_properties.py`.
MAX_EXAMPLES = int(os.environ.get("S5_MAX_EXAMPLES", "60"))

PROPERTY_SETTINGS = settings(
    max_examples=MAX_EXAMPLES,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)


# ------------------------------------------------------------------ properties
@given(scenario=scenarios())
@PROPERTY_SETTINGS
def test_per_key_order_holds_under_arbitrary_fault_schedules(
    tmp_path: Path, scenario: dict[str, Any]
) -> None:
    """Invariant 5. The headline property of S5."""
    outcome = run_scenario(tmp_path, **scenario)
    assert outcome.report.order_preserved, (
        f"per-key reordering: {outcome.report.reordered_keys} (scenario={scenario})"
    )


@given(scenario=scenarios())
@PROPERTY_SETTINGS
def test_nothing_is_lost_every_message_reaches_the_sink_or_the_dlq(
    tmp_path: Path, scenario: dict[str, Any]
) -> None:
    """Invariant 4. Loss is the one outcome at-least-once does not excuse."""
    outcome = run_scenario(tmp_path, **scenario)
    accounted = outcome.delivered_ids | outcome.dlq_ids
    missing = [i for i in outcome.report.lost if i not in accounted]
    assert not missing, f"{len(missing)} message(s) in neither sink nor DLQ (scenario={scenario})"


@given(scenario=scenarios())
@PROPERTY_SETTINGS
def test_no_offset_is_committed_before_the_work_below_it_is_durable(
    tmp_path: Path, scenario: dict[str, Any]
) -> None:
    """Commit-after-delivery, asserted online by the commit hook."""
    outcome = run_scenario(tmp_path, **scenario)
    assert not outcome.tracker.violations, "\n".join(outcome.tracker.violations[:5])
    assert outcome.tracker.commits > 0, "a run that never committed proves nothing"


@given(scenario=scenarios())
@PROPERTY_SETTINGS
def test_poison_messages_end_up_in_the_dlq_and_only_poison_does(
    tmp_path: Path, scenario: dict[str, Any]
) -> None:
    """Invariant 5's escape hatch: the DLQ is how an ordered partition unblocks."""
    cluster_poison = scenario["poison_every"]
    outcome = run_scenario(tmp_path, **scenario)
    if cluster_poison is None:
        assert not outcome.dlq_ids, "nothing was poison, so the DLQ must be empty"
    # A poison message must never be reported as delivered to the sink.
    assert not (outcome.dlq_ids & outcome.delivered_ids), (
        "a message both reached the sink and was dead-lettered"
    )


# --------------------------------------------------------- worked examples
def test_a_transient_failure_pauses_and_resumes_without_skipping(tmp_path: Path) -> None:
    """The exact bug the loader exists to prevent, end to end.

    One partition, a failure rate high enough to force pauses, and then a check
    that every sequence number still arrived in order with nothing skipped.
    """
    outcome = run_scenario(
        tmp_path,
        partitions=1,
        count=25,
        keys=1,
        transient_rate=0.4,
        poison_every=None,
        faults=[],
        seed=11,
    )
    assert outcome.stats["pauses"] > 0, "the scenario did not actually exercise pause/resume"
    assert outcome.stats["resumes"] > 0
    assert outcome.report.order_preserved
    assert outcome.report.no_loss
    assert outcome.delivered_ids and not outcome.dlq_ids
    # Nothing was skipped: seq 0..24 all present exactly once per first delivery.
    assert outcome.report.unique_msg_ids == 25


def test_a_kill_replays_from_the_last_commit_and_loses_nothing(tmp_path: Path) -> None:
    """kill -9 mid-run: duplicates are allowed, loss is not."""
    outcome = run_scenario(
        tmp_path,
        partitions=2,
        count=30,
        keys=4,
        transient_rate=0.0,
        poison_every=None,
        faults=[(5, "crash")],
        seed=7,
    )
    assert outcome.report.no_loss, f"lost {len(outcome.report.lost)} message(s) across a kill"
    assert outcome.report.order_preserved
    assert not outcome.tracker.violations


def test_membership_loss_does_not_commit_someone_elses_partitions(tmp_path: Path) -> None:
    outcome = run_scenario(
        tmp_path,
        partitions=2,
        count=20,
        keys=3,
        transient_rate=0.0,
        poison_every=None,
        faults=[(4, "lose")],
        seed=3,
    )
    assert outcome.report.order_preserved
    assert outcome.report.no_loss
    assert not outcome.tracker.violations


def test_poison_message_reaches_the_dlq_with_source_coordinates(tmp_path: Path) -> None:
    """DLQ entries must carry enough to replay, and no payload-derived PHI."""
    cluster = FakeCluster(default_partitions=1)
    cluster.create_topic(RAW, 1)
    cluster.create_topic(DLQ, 1)
    plan_out = generator.publish(
        cluster.producer(), RAW, count=6, keys=1, seed=5, poison_every=3
    )
    tracker = Tracker()
    _install_commit_hook(cluster, tracker, DLQ)
    loader = _loader(
        cluster,
        tracker,
        tmp_path / "d.jsonl",
        plan=FailurePlan(poison=plan_out.poison, seed=5),
        instance_id="w",
    )
    _drain(loader)

    entries = fakekafka.dlq_records(cluster, DLQ)
    assert entries, "poison never reached the DLQ"
    assert {e.msg_id for e in entries} == plan_out.poison_ids()
    for entry in entries:
        assert entry.reason == "poison-message"
        assert entry.source_topic == RAW
        assert entry.source_offset >= 0
    # The partition kept moving afterwards: DLQ is what unblocks an ordered
    # partition, so the non-poison messages must all have been delivered.
    assert loader.stats.delivered == 6 - len(plan_out.poison)
    assert not tracker.violations


def test_halt_on_exhaust_stops_the_partition_instead_of_dead_lettering(tmp_path: Path) -> None:
    """The order-preserving alternative to a DLQ: stop, and page a human.

    Nothing after the bad message may be delivered, because doing so would
    reorder the partition -- that is the whole point of `halt-partition`.
    """
    cluster = FakeCluster(default_partitions=1)
    cluster.create_topic(RAW, 1)
    cluster.create_topic(DLQ, 1)
    plan_out = generator.publish(
        cluster.producer(), RAW, count=8, keys=1, seed=9, poison_every=3
    )
    tracker = Tracker()
    _install_commit_hook(cluster, tracker, DLQ)
    loader = _loader(
        cluster,
        tracker,
        tmp_path / "d.jsonl",
        plan=FailurePlan(poison=plan_out.poison, seed=9),
        instance_id="w",
        on_exhaust=OnExhaust.HALT_PARTITION,
    )
    _drain(loader)

    assert loader.stats.halted_partitions == 1
    assert loader.stats.dlq == 0
    assert not fakekafka.dlq_records(cluster, DLQ)
    # The first poison message is at index 2 (poison_every=3 -> every 3rd),
    # so exactly the messages before it were delivered.
    first_poison_seq = min(seq for _key, seq in plan_out.poison)
    assert loader.stats.delivered == first_poison_seq
    assert not tracker.violations


@pytest.mark.parametrize("commit_every", [1, 5])
def test_duplicate_rate_is_bounded_by_the_commit_interval(
    tmp_path: Path, commit_every: int
) -> None:
    """Committing less often trades duplicates for throughput. Measure it.

    This is a measurement, not a threshold: the report quotes the numbers.
    """
    cluster = FakeCluster(default_partitions=1)
    cluster.create_topic(RAW, 1)
    cluster.create_topic(DLQ, 1)
    plan_out = generator.publish(cluster.producer(), RAW, count=40, keys=1, seed=13)
    tracker = Tracker()
    _install_commit_hook(cluster, tracker, DLQ)
    log_path = tmp_path / "d.jsonl"

    first = _loader(
        cluster,
        tracker,
        log_path,
        plan=FailurePlan(seed=13),
        instance_id="a",
        commit_every=commit_every,
    )
    cluster.consumers[-1].schedule(20, cluster.consumers[-1].crash)
    _drain(first)
    _drain(_loader(cluster, tracker, log_path, plan=FailurePlan(seed=13), instance_id="b"))

    records = read_delivery_log(log_path)
    observed = [
        checks.Observation(key=str(r["key"]), seq=int(r["seq"]), msg_id=str(r["msg_id"]))
        for r in records
    ]
    report = checks.check(observed, expected=plan_out.expected)
    assert report.no_loss
    assert report.order_preserved
    # At most commit_every-1 messages can be in the uncommitted window.
    assert report.duplicates <= max(1, commit_every - 1), report.summary()
    assert not tracker.violations
