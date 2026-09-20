"""Fault-injection scenarios against the real 3-broker spike cluster.

`tests/test_properties.py` proves the *algorithm* over hundreds of randomised
fault schedules against `s5.fakekafka`. This module proves that the semantics
the model assumes are what Apache Kafka 4.0 actually does, on a handful of
scenarios, with real worker processes that are really killed.

What every scenario establishes, from evidence on disk and in the cluster:

* **per-key order** -- across the merged delivery logs of all workers, the first
  delivery of each ``tb-x-seq`` per key is non-decreasing (`tbcommon.checks`);
* **no loss** -- every produced message id is in a delivery log or in the DLQ;
* **commit-after-delivery** -- for every partition, every offset below the
  group's committed offset was written downstream or dead-lettered. Checked
  after the fact here; the fake checks the same thing online, in the commit hook;
* **drained** -- committed offset == high watermark, so the run *finished*
  rather than merely stopped;
* **duplicate rate** -- reported, never asserted to be zero (invariant 4).

Usage::

    uv run python -m s5.runner kill --count 4000
    uv run python -m s5.runner all --count 2000

Topics live under ``spike.s5.<run-id>.*`` and are deleted on exit unless
``--keep-topics`` is given.
"""

from __future__ import annotations

import argparse
import json
import logging
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson
from tbcommon import admin, checks, clients, config, faults

from . import readback, topics
from .generator import Plan, publish
from .sink import read_delivery_log
from .topics import Layout

log = logging.getLogger("s5.runner")

#: Every S5 topic starts with this, so one prefix match cleans the cluster up.
TOPIC_NAMESPACE = f"{config.topic_prefix()}.{topics.CHANNEL}."

RUNS_DIR = Path(__file__).resolve().parents[2] / "runs"


# ------------------------------------------------------------------- processes
@dataclass
class Worker:
    """A loader subprocess plus the evidence it leaves behind."""

    name: str
    proc: subprocess.Popen[bytes]
    log_path: Path
    stats_path: Path
    stderr: Any
    killed_wall: float | None = None

    def kill(self) -> float:
        """``kill -9`` equivalent. No atexit, no final commit, no LeaveGroup.

        On Windows ``Popen.kill`` is ``TerminateProcess``, which like SIGKILL
        gives the process no chance to run ``consumer.close()``. That is the
        point: the group must notice through the session timeout, exactly as it
        would when a pod is OOM-killed.
        """
        self.killed_wall = time.time()
        self.proc.kill()
        self.proc.wait(timeout=30)
        log.info("killed worker %s", self.name)
        return self.killed_wall

    def stats(self) -> dict[str, Any] | None:
        """``None`` means the worker never exited cleanly -- i.e. it was killed."""
        if not self.stats_path.exists():
            return None
        return json.loads(self.stats_path.read_text(encoding="utf-8"))


class WorkerPool:
    """Starts, watches and reaps loader processes for one scenario."""

    def __init__(
        self, run_dir: Path, layout: Layout, group: str, poison_file: Path | None = None
    ) -> None:
        self.run_dir = run_dir
        self.layout = layout
        self.group = group
        #: Passed to every worker it starts. Wiring this per scenario was the
        #: original arrangement and it silently produced a run where nothing
        #: failed, nothing reached the DLQ and the verdict was still "ok".
        self.poison_file = poison_file
        self.workers: list[Worker] = []

    def start(self, name: str, *, static: bool = True, extra: Sequence[str] = ()) -> Worker:
        log_path = self.run_dir / f"deliveries-{name}.jsonl"
        stats_path = self.run_dir / f"stats-{name}.json"
        stderr = (self.run_dir / f"{name}.stderr.log").open("wb")
        argv = [
            sys.executable,
            "-m",
            "s5.worker",
            "--topic", self.layout.raw,
            "--dlq-topic", self.layout.dlq,
            "--group", self.group,
            "--events-topic", self.layout.events,
            "--log", str(log_path),
            "--stats", str(stats_path),
            *(("--instance-id", name) if static else ()),
            *(("--poison-file", str(self.poison_file)) if self.poison_file else ()),
            *extra,
        ]
        proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=stderr)  # noqa: S603
        worker = Worker(name, proc, log_path, stats_path, stderr)
        self.workers.append(worker)
        log.info("started worker %s (pid %d)", name, proc.pid)
        return worker

    def deliveries(self) -> int:
        total = 0
        for worker in self.workers:
            if worker.log_path.exists():
                with worker.log_path.open("rb") as fh:
                    total += sum(1 for _ in fh)
        return total

    def wait_for_deliveries(self, at_least: int, timeout_s: float = 180.0) -> int:
        """Block until enough work has happened to make a fault interesting."""
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            seen = self.deliveries()
            if seen >= at_least:
                return seen
            if all(w.proc.poll() is not None for w in self.workers):
                return seen
            time.sleep(0.05)
        return self.deliveries()

    def wait_all(self, timeout_s: float = 300.0) -> None:
        deadline = time.monotonic() + timeout_s
        for worker in self.workers:
            remaining = max(1.0, deadline - time.monotonic())
            try:
                worker.proc.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                log.warning("worker %s did not exit; terminating", worker.name)
                worker.proc.kill()
                worker.proc.wait(timeout=30)
        for worker in self.workers:
            worker.stderr.close()


# -------------------------------------------------------------------- analysis
@dataclass
class RunResult:
    scenario: str
    layout: Layout
    group: str
    run_dir: Path
    produced: int
    report: checks.OrderingReport = field(default_factory=checks.OrderingReport)
    delivered_ids: set[str] = field(default_factory=set)
    dlq_ids: set[str] = field(default_factory=set)
    expected_dlq_ids: set[str] = field(default_factory=set)
    committed: dict[int, int] = field(default_factory=dict)
    high_watermarks: dict[int, int] = field(default_factory=dict)
    commit_violations: list[str] = field(default_factory=list)
    undrained: dict[int, tuple[int, int]] = field(default_factory=dict)
    worker_stats: dict[str, Any] = field(default_factory=dict)
    event_stages: dict[str, int] = field(default_factory=dict)
    phi_findings: list[str] = field(default_factory=list)
    notes: dict[str, Any] = field(default_factory=dict)
    wall_s: float = 0.0

    @property
    def ok(self) -> bool:
        """The verdict the report quotes. Duplicates are not a failure."""
        return (
            self.report.order_preserved
            and self.report.no_loss
            and not self.commit_violations
            and not self.phi_findings
            # Exactly the poison messages, no more and no fewer. Without this an
            # injection harness that forgot to inject anything reports success.
            and self.dlq_ids == self.expected_dlq_ids
        )

    def summary(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "topics": self.layout.raw,
            "group": self.group,
            "produced": self.produced,
            "wall_s": round(self.wall_s, 2),
            "delivery": self.report.summary(),
            "dlq": {
                "seen": len(self.dlq_ids),
                "expected": len(self.expected_dlq_ids),
                "unexpected": len(self.dlq_ids - self.expected_dlq_ids),
                "missing": len(self.expected_dlq_ids - self.dlq_ids),
            },
            "commit_after_delivery_violations": self.commit_violations[:5],
            # str keys: orjson refuses int dict keys, and a halted run is
            # exactly the case that puts entries in here.
            "undrained_partitions": {
                str(p): {"committed": c, "high_watermark": h} for p, (c, h) in self.undrained.items()
            },
            "workers": self.worker_stats,
            "event_stages": self.event_stages,
            "phi_findings": self.phi_findings,
            "notes": self.notes,
            "ok": self.ok,
        }


#: Strings that must never appear in a lifecycle event. MSH-3 and a segment id
#: are the cheapest evidence that a payload leaked into the archive path.
PHI_MARKERS = ("TRIBUTARY-SYNTH", "PID|", "MSH|")


def analyse(
    scenario: str,
    layout: Layout,
    group: str,
    run_dir: Path,
    plan: Plan,
    pool: WorkerPool,
    *,
    wall_s: float,
    expect_halt: bool = False,
    notes: dict[str, Any] | None = None,
) -> RunResult:
    records = read_delivery_log(run_dir / "deliveries-*.jsonl")
    observed = [
        checks.Observation(key=str(r["key"]), seq=int(r["seq"]), msg_id=str(r["msg_id"]))
        for r in records
    ]
    result = RunResult(
        scenario=scenario,
        layout=layout,
        group=group,
        run_dir=run_dir,
        produced=len(plan.expected),
        wall_s=wall_s,
        notes=notes or {},
    )
    result.delivered_ids = {o.msg_id for o in observed}

    # Throughput from the delivery log, not from `wall_s`: the latter includes
    # process startup and the idle_exit_s wait at the end, which would make
    # every scenario look several times slower than it is.
    if records:
        stamps = [float(r["ts"]) for r in records]
        span = max(stamps) - min(stamps)
        result.notes["delivery_span_s"] = round(span, 2)
        result.notes["throughput_msg_s"] = round(len(records) / span, 1) if span > 0 else None

    dlq = readback.read_all(layout.dlq)
    result.dlq_ids = {r.msg_id for r in dlq}
    result.expected_dlq_ids = set() if expect_halt else plan.poison_ids()

    result.committed = readback.committed_offsets(group, layout.raw)
    marks = readback.watermarks(layout.raw)
    result.high_watermarks = {p: high for p, (_low, high) in marks.items()}

    # A halted partition is *supposed* to stop, so "everything was delivered" is
    # not the expectation there. What must still hold is that the delivered set
    # is exactly the prefix below the commit point: nothing skipped, nothing
    # delivered from behind the halt. So the expectation is read back from the
    # topic rather than assumed.
    expected = plan.deliverable
    if expect_halt:
        raw = readback.read_all(layout.raw)
        reachable = {r.msg_id for r in raw if r.offset < result.committed.get(r.partition, 0)}
        expected = [o for o in plan.deliverable if o.msg_id in reachable]
        result.notes["blocked_behind_halt"] = len(plan.expected) - len(reachable)
    result.report = checks.check(observed, expected=expected)

    # ---- commit-after-delivery, from the group's committed offsets -----------
    handled: dict[int, set[int]] = {}
    for record in records:
        handled.setdefault(int(record["partition"]), set()).add(int(record["offset"]))
    for entry in dlq:
        part = int(entry.headers.get("tb-source-partition", "-1"))
        handled.setdefault(part, set()).add(int(entry.headers.get("tb-source-offset", "-1")))
    for part, committed in sorted(result.committed.items()):
        if committed < 0:
            continue
        done = handled.get(part, set())
        missing = [o for o in range(marks[part][0], committed) if o not in done]
        if missing:
            result.commit_violations.append(
                f"{layout.raw}[{part}] committed @{committed} but offsets {missing[:8]} "
                f"({len(missing)} total) were never delivered or dead-lettered"
            )
        if committed != marks[part][1]:
            result.undrained[part] = (committed, marks[part][1])

    for worker in pool.workers:
        result.worker_stats[worker.name] = worker.stats() or {"killed": True}

    events = readback.read_events(layout.events)
    result.event_stages = readback.stage_counts(events)
    for event in events:
        blob = orjson.dumps(event).decode(errors="replace")
        for marker in PHI_MARKERS:
            if marker in blob:
                result.phi_findings.append(f"marker {marker!r} in a {event.get('stage')} event")
                break
    return result


def takeover_gap_s(
    run_dir: Path, dead: str, killed_wall: float, *, by_others_only: bool = True
) -> float | None:
    """Wall seconds from a kill until the dead worker's partitions move again.

    ``by_others_only`` distinguishes the two questions S5 has to answer
    separately. True: how long before *another* member picks the partitions up,
    which for a static member is bounded below by ``session.timeout.ms``. False:
    how long before those partitions flow again at all, which is what a restart
    with the same ``group.instance.id`` is supposed to shorten.
    """
    records = read_delivery_log(run_dir / "deliveries-*.jsonl")
    owned = {
        int(r["partition"])
        for r in records
        if r["worker"] == dead and float(r["ts"]) < killed_wall
    }
    after = [
        float(r["ts"])
        for r in records
        if int(r["partition"]) in owned
        and float(r["ts"]) >= killed_wall
        and (r["worker"] != dead or not by_others_only)
    ]
    return round(min(after) - killed_wall, 3) if after else None


# ------------------------------------------------------------------- scenarios
@dataclass(frozen=True, slots=True)
class Options:
    count: int = 2000
    keys: int = 64
    partitions: int = 6
    seed: int = 1
    keep_topics: bool = False
    session_timeout_ms: int = 10_000


def _setup(scenario: str, opts: Options) -> tuple[Layout, str, Path, Plan]:
    run_id = f"{scenario}-{time.strftime('%H%M%S')}"
    layout = topics.layout(run_id, partitions=opts.partitions)
    group = f"s5-{run_id}"
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    admin.ensure_topics(layout.specs())
    log.info("topics ready: %s (%d partitions)", layout.raw, opts.partitions)
    plan = publish(
        clients.make_producer(),
        layout.raw,
        count=opts.count,
        keys=opts.keys,
        seed=opts.seed,
        poison_every=_poison_every(scenario),
    )
    if plan.poison:
        (run_dir / "poison.json").write_bytes(orjson.dumps(sorted(plan.poison)))
    log.info("published %d messages over %d keys", len(plan.expected), opts.keys)
    return layout, group, run_dir, plan


def _pool(run_dir: Path, layout: Layout, group: str, plan: Plan) -> WorkerPool:
    """Build a pool with the failure injection already wired in.

    Every scenario goes through here so that none of them can be written
    without passing the poison set to its workers.
    """
    poison_file = run_dir / "poison.json"
    return WorkerPool(
        run_dir, layout, group, poison_file=poison_file if plan.poison else None
    )


def _poison_every(scenario: str) -> int | None:
    if scenario in ("poison_dlq", "halt"):
        return 250 if scenario == "poison_dlq" else 400
    return None


def _common(opts: Options, *, idle_exit_s: float = 12.0) -> list[str]:
    return [
        "--idle-exit-s", str(idle_exit_s),
        "--max-runtime-s", "240",
        "--session-timeout-ms", str(opts.session_timeout_ms),
    ]


def scenario_baseline(opts: Options) -> RunResult:
    """No faults at all. Establishes throughput and that the harness is honest."""
    layout, group, run_dir, plan = _setup("baseline", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    pool.start("worker-a", extra=_common(opts))
    pool.start("worker-b", extra=_common(opts))
    pool.wait_all()
    return analyse("baseline", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)


def scenario_baseline_batched(opts: Options) -> RunResult:
    """Baseline again, committing every 50 deliveries instead of every one.

    `commit_every=1` costs an OffsetCommit round trip per message, which on this
    host is the dominant cost. This scenario exists to put a number on the
    trade the runtime will have to make: throughput against the size of the
    replay window after a kill.
    """
    layout, group, run_dir, plan = _setup("baseline_batched", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    extra = [*_common(opts), "--commit-every", "50", "--commit-interval-s", "0.5"]
    pool.start("worker-a", extra=extra)
    pool.start("worker-b", extra=extra)
    pool.wait_all()
    result = analyse(
        "baseline_batched", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0
    )
    result.notes["commit_every"] = 50
    result.notes["commits"] = sum(
        (s or {}).get("stats", {}).get("commits", 0) for s in result.worker_stats.values()
    )
    return result


def scenario_blocking_retry(opts: Options) -> RunResult:
    """A flaky destination: every failure pauses its partition and seeks back.

    This is the scenario that would expose the librdkafka resume-skips-the-
    failed-message trap: without the seek, messages would vanish and `lost`
    would be non-empty.
    """
    layout, group, run_dir, plan = _setup("blocking_retry", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    extra = [
        *_common(opts, idle_exit_s=15.0),
        "--transient-rate", "0.25",
        "--max-attempts", "30",
        "--backoff-base", "0.02",
        "--backoff-cap", "0.2",
    ]
    pool.start("worker-a", extra=extra)
    pool.start("worker-b", extra=extra)
    pool.wait_all()
    result = analyse(
        "blocking_retry", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0
    )
    pauses = sum(
        (s or {}).get("stats", {}).get("pauses", 0) for s in result.worker_stats.values()
    )
    result.notes["pauses"] = pauses
    result.notes["resumes"] = sum(
        (s or {}).get("stats", {}).get("resumes", 0) for s in result.worker_stats.values()
    )
    result.notes["offset_skips_corrected"] = sum(
        (s or {}).get("stats", {}).get("offset_skips_corrected", 0)
        for s in result.worker_stats.values()
    )
    return result


def scenario_poison_dlq(opts: Options) -> RunResult:
    """Permanently-failing messages must leave the partition, via the DLQ."""
    layout, group, run_dir, plan = _setup("poison_dlq", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    extra = [*_common(opts), "--max-attempts", "3", "--backoff-base", "0.02", "--backoff-cap", "0.1"]
    pool.start("worker-a", extra=extra)
    pool.start("worker-b", extra=extra)
    pool.wait_all()
    return analyse("poison_dlq", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)


def scenario_halt(opts: Options) -> RunResult:
    """`halt-partition`: the order-preserving alternative to dead-lettering."""
    layout, group, run_dir, plan = _setup("halt", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    extra = [
        *_common(opts, idle_exit_s=8.0),
        "--on-exhaust", "halt-partition",
        "--max-attempts", "3",
        "--backoff-base", "0.02",
        "--backoff-cap", "0.1",
    ]
    pool.start("worker-a", extra=extra)
    pool.wait_all()
    result = analyse(
        "halt", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0, expect_halt=True
    )
    result.notes["halted_partitions"] = sum(
        (s or {}).get("stats", {}).get("halted_partitions", 0)
        for s in result.worker_stats.values()
    )
    return result


def scenario_kill(opts: Options) -> RunResult:
    """``kill -9`` a worker mid-run; a fresh member takes the partitions over."""
    layout, group, run_dir, plan = _setup("kill", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    a = pool.start("worker-a", extra=_common(opts, idle_exit_s=20.0))
    pool.start("worker-b", extra=_common(opts, idle_exit_s=20.0))
    pool.wait_for_deliveries(max(50, opts.count // 4))
    killed = a.kill()
    pool.start("worker-c", extra=_common(opts, idle_exit_s=20.0))
    pool.wait_all()
    result = analyse("kill", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)
    result.notes["killed"] = "worker-a"
    result.notes["takeover_gap_s"] = takeover_gap_s(run_dir, "worker-a", killed)
    result.notes["session_timeout_ms"] = opts.session_timeout_ms
    return result


def scenario_static_hold(opts: Options) -> RunResult:
    """Static membership's price: a dead member's partitions stay dead.

    Nobody replaces worker-a, so the gap measured here is the session timeout,
    not a rebalance. Documented so the runtime picks `session.timeout.ms` with
    open eyes: it is the recovery-time floor for an ordered channel.
    """
    layout, group, run_dir, plan = _setup("static_hold", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    a = pool.start("worker-a", extra=_common(opts, idle_exit_s=25.0))
    pool.start("worker-b", extra=_common(opts, idle_exit_s=25.0))
    pool.wait_for_deliveries(max(50, opts.count // 4))
    killed = a.kill()
    pool.wait_all()
    result = analyse("static_hold", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)
    result.notes["killed"] = "worker-a"
    result.notes["takeover_gap_s"] = takeover_gap_s(run_dir, "worker-a", killed)
    result.notes["session_timeout_ms"] = opts.session_timeout_ms
    return result


def scenario_static_restart(opts: Options) -> RunResult:
    """Restart inside the session timeout with the same ``group.instance.id``.

    The claim under test: the member rejoins with its own assignment and the
    group does not rebalance at all, so the only cost is the downtime itself.
    """
    layout, group, run_dir, plan = _setup("static_restart", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    a = pool.start("worker-a", extra=_common(opts, idle_exit_s=20.0))
    b = pool.start("worker-b", extra=_common(opts, idle_exit_s=20.0))
    pool.wait_for_deliveries(max(50, opts.count // 4))
    killed = a.kill()
    time.sleep(2.0)  # well inside session.timeout.ms
    pool.start("worker-a", extra=_common(opts, idle_exit_s=20.0))  # same instance id
    pool.wait_all()
    result = analyse(
        "static_restart", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0
    )
    result.notes["restarted"] = "worker-a"
    result.notes["downtime_s"] = 2.0
    # Not takeover: the same member comes back and resumes its own partitions.
    result.notes["recovery_gap_s"] = takeover_gap_s(
        run_dir, "worker-a", killed, by_others_only=False
    )
    result.notes["survivor_revokes"] = (b.stats() or {}).get("stats", {}).get("revokes")
    result.notes["session_timeout_ms"] = opts.session_timeout_ms
    return result


def scenario_rebalance(opts: Options) -> RunResult:
    """A third member joins mid-run: cooperative-sticky, incremental assignment."""
    layout, group, run_dir, plan = _setup("rebalance", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    pool.start("worker-a", extra=_common(opts, idle_exit_s=15.0))
    pool.start("worker-b", extra=_common(opts, idle_exit_s=15.0))
    pool.wait_for_deliveries(max(50, opts.count // 5))
    pool.start("worker-c", extra=_common(opts, idle_exit_s=15.0))
    pool.wait_all()
    result = analyse("rebalance", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)
    result.notes["revokes"] = {
        name: (s or {}).get("stats", {}).get("revokes") for name, s in result.worker_stats.items()
    }
    result.notes["lost"] = {
        name: (s or {}).get("stats", {}).get("lost") for name, s in result.worker_stats.items()
    }
    return result


def scenario_broker_kill(opts: Options) -> RunResult:
    """SIGKILL the leader of partition 0 while the loader is mid-flight."""
    layout, group, run_dir, plan = _setup("broker_kill", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    extra = [*_common(opts, idle_exit_s=30.0), "--transient-rate", "0.05", "--max-attempts", "30"]
    pool.start("worker-a", extra=extra)
    pool.start("worker-b", extra=extra)
    pool.wait_for_deliveries(max(50, opts.count // 4))
    service = faults.leader_service_for(layout.raw, 0)
    log.info("killing broker %s (leader of %s[0])", service, layout.raw)
    down_at = time.monotonic()
    faults.kill_broker(service)
    time.sleep(6.0)
    faults.start_broker(service)
    faults.wait_healthy()
    # Measured here, not after wait_all(): taking it at the end would silently
    # report the whole remaining run as broker downtime.
    recovered_s = round(time.monotonic() - down_at, 1)
    pool.wait_all()
    result = analyse(
        "broker_kill", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0
    )
    result.notes["broker_killed"] = service
    result.notes["broker_down_to_healthy_s"] = recovered_s
    # `lost` counts *partitions* handed to the on_lost callback, not messages:
    # killing the broker that hosts the group coordinator drops members out of
    # the group, and on_lost must not commit those partitions.
    result.notes["partitions_lost_from_group"] = {
        name: (s or {}).get("stats", {}).get("lost") for name, s in result.worker_stats.items()
    }
    return result


def scenario_txn(opts: Options, *, crash: bool = False) -> RunResult:
    """Read-process-write with offsets in the transaction, `raw` -> `out`.

    With ``crash``, the stage is killed mid-transaction and restarted with the
    same ``transactional.id``. The check that matters then is that the output
    topic, read with ``read_committed``, still contains each message exactly
    once: the killed process's open transaction must have been aborted, not
    committed, and its input offsets must have rolled back with it.
    """
    name = "txn_crash" if crash else "txn"
    layout, group, run_dir, plan = _setup(name, opts)
    txn_id = f"{group}.stage"
    t0 = time.monotonic()

    def launch(instance: str) -> subprocess.Popen[bytes]:
        argv = [
            sys.executable, "-m", "s5.txn_worker",
            "--in-topic", layout.raw,
            "--out-topic", layout.out,
            "--group", group,
            "--instance-id", instance,
            "--transactional-id", txn_id,
            "--events-topic", layout.events,
            "--batch-size", "50",
            "--idle-exit-s", "12",
            "--max-runtime-s", "240",
            "--stats", str(run_dir / f"stats-{instance}.json"),
        ]
        return subprocess.Popen(  # noqa: S603
            argv,
            stdout=subprocess.DEVNULL,
            stderr=(run_dir / f"{instance}.stderr.log").open("wb"),
        )

    proc = launch("txn-a")
    if crash:
        _wait_for_records(layout.out, at_least=max(50, opts.count // 5), timeout_s=120.0)
        proc.kill()
        proc.wait(timeout=30)
        log.info("killed the transactional stage mid-flight; restarting with the same txn id")
        proc = launch("txn-a")  # same instance id AND same transactional.id
    try:
        proc.wait(timeout=300)
    except subprocess.TimeoutExpired:
        log.warning("transactional stage did not exit; terminating")
        proc.kill()
        proc.wait(timeout=30)

    out = readback.read_all(layout.out, read_committed=True, timeout_s=90.0)
    observed = [checks.Observation(key=r.key, seq=r.seq, msg_id=r.msg_id) for r in out]
    result = RunResult(
        scenario=name,
        layout=layout,
        group=group,
        run_dir=run_dir,
        produced=len(plan.expected),
        wall_s=time.monotonic() - t0,
    )
    result.report = checks.check(observed, expected=plan.expected)
    result.committed = readback.committed_offsets(group, layout.raw)
    marks = readback.watermarks(layout.raw)
    result.high_watermarks = {p: high for p, (_low, high) in marks.items()}
    for part, committed in result.committed.items():
        if committed >= 0 and committed != marks[part][1]:
            result.undrained[part] = (committed, marks[part][1])
    stats_file = run_dir / "stats-txn-a.json"
    if stats_file.exists():
        result.worker_stats["txn-a"] = json.loads(stats_file.read_text(encoding="utf-8"))
    events = readback.read_events(layout.events)
    result.event_stages = readback.stage_counts(events)
    result.notes["exactly_once"] = result.report.duplicates == 0
    result.notes["out_records"] = len(out)
    result.notes["crashed_mid_transaction"] = crash
    return result


def _wait_for_records(topic: str, *, at_least: int, timeout_s: float) -> int:
    """Poll the high watermark until a topic has enough records."""
    deadline = time.monotonic() + timeout_s
    seen = 0
    while time.monotonic() < deadline:
        seen = sum(high - low for low, high in readback.watermarks(topic).values())
        if seen >= at_least:
            return seen
        time.sleep(0.25)
    return seen


SCENARIOS: dict[str, Callable[[Options], RunResult]] = {
    "baseline": scenario_baseline,
    "baseline_batched": scenario_baseline_batched,
    "blocking_retry": scenario_blocking_retry,
    "poison_dlq": scenario_poison_dlq,
    "halt": scenario_halt,
    "kill": scenario_kill,
    "static_hold": scenario_static_hold,
    "static_restart": scenario_static_restart,
    "rebalance": scenario_rebalance,
    "broker_kill": scenario_broker_kill,
    "txn": scenario_txn,
    "txn_crash": lambda opts: scenario_txn(opts, crash=True),
}


def run(scenario: str, opts: Options) -> RunResult:
    result = SCENARIOS[scenario](opts)
    (result.run_dir / "summary.json").write_bytes(
        orjson.dumps(result.summary(), option=orjson.OPT_INDENT_2)
    )
    if not opts.keep_topics:
        admin.delete_topics(result.layout.all_topics())
    return result


def cleanup_stray_topics() -> list[str]:
    """Delete every ``spike.s5.*`` topic. S5 must leave the cluster as it found it."""
    names = [
        n for n in admin.admin().list_topics(timeout=15.0).topics if n.startswith(TOPIC_NAMESPACE)
    ]
    if names:
        admin.delete_topics(names)
    return names


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="s5.runner")
    parser.add_argument("scenario", choices=[*SCENARIOS, "all", "cleanup"])
    parser.add_argument("--count", type=int, default=2000)
    parser.add_argument("--keys", type=int, default=64)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--session-timeout-ms", type=int, default=10_000)
    parser.add_argument("--keep-topics", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    if args.scenario == "cleanup":
        print(json.dumps({"deleted": cleanup_stray_topics()}, indent=2))
        return 0

    opts = Options(
        count=args.count,
        keys=args.keys,
        partitions=args.partitions,
        seed=args.seed,
        keep_topics=args.keep_topics,
        session_timeout_ms=args.session_timeout_ms,
    )
    admin.wait_for_cluster()
    names = list(SCENARIOS) if args.scenario == "all" else [args.scenario]
    failures = 0
    for name in names:
        log.info("=== scenario %s ===", name)
        result = run(name, opts)
        print(json.dumps(result.summary(), indent=2, default=str))
        failures += 0 if result.ok else 1
    return 1 if failures else 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
