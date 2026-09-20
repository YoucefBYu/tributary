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
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson
from confluent_kafka import Consumer, KafkaException, TopicPartition
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
        self.proc.kill()
        self.proc.wait(timeout=30)
        # Timestamped *after* the process is confirmed dead. TerminateProcess is
        # not instantaneous, so a timestamp taken before it lets the victim's
        # own last few deliveries land "after the kill" and be counted as
        # recovery -- which is how the first re-run measured a 3 ms recovery
        # from a 2 s outage.
        self.killed_wall = time.time()
        log.info("killed worker %s", self.name)
        return self.killed_wall

    def stats(self) -> dict[str, Any] | None:
        """``None`` means the worker never exited cleanly -- i.e. it was killed."""
        if not self.stats_path.exists():
            return None
        return json.loads(self.stats_path.read_text(encoding="utf-8"))

    def deliveries(self) -> int:
        if not self.log_path.exists():
            return 0
        with self.log_path.open("rb") as fh:
            return sum(1 for _ in fh)

    def wait_mid_commit_window(
        self, commit_every: int, *, minimum: int, timeout_s: float = 180.0
    ) -> int:
        """Block until this worker has an uncommitted window open, then return.

        Killing on a pool-wide delivery count is not good enough. The first
        re-run of the `kill` scenario killed worker-a at exactly its 260th
        delivery -- 13 x 20 -- so it had just committed, nothing was replayed
        and the scenario proved nothing. The kill has to land *between* commits,
        so wait for this worker's own delivery count to sit mid-window.

        Aims for the middle of the window to leave slack: the count keeps moving
        between the check and the kill, and deliveries here take single-digit
        milliseconds.
        """
        if commit_every <= 1:
            raise ValueError("a commit window of 1 has no inside to kill in")
        target = commit_every // 2
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            n = self.deliveries()
            if n >= minimum and n % commit_every == target:
                return n
            if self.proc.poll() is not None:
                return n  # it exited on its own; caller will notice the verdict
            time.sleep(0.002)
        return self.deliveries()


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
            "--journal", str(self.run_dir / "rebalances.jsonl"),
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
    sampled_commits: int = 0
    #: Scenarios that kill a worker with an open uncommitted window MUST produce
    #: duplicates. Zero duplicates there does not mean "no replay happened and
    #: all is well", it means the scenario failed to exercise replay at all.
    require_duplicates: bool = False
    #: Kafka-to-Kafka scenarios must be exactly-once, so for them a duplicate is
    #: a hard failure rather than the expected cost of at-least-once.
    require_exactly_once: bool = False
    #: `txn_crash` must catch the stage with a transaction actually open,
    #: otherwise it tests nothing about abort or zombie fencing.
    require_open_transaction: bool = False

    @property
    def verdict_failures(self) -> list[str]:
        """Why this run is not a pass. Empty means it is."""
        why: list[str] = []
        if not self.report.order_preserved:
            why.append(f"per-key reordering on {len(self.report.reordered_keys)} key(s)")
        if not self.report.no_loss:
            why.append(f"{len(self.report.lost)} message(s) lost")
        if self.commit_violations:
            why.append(f"{len(self.commit_violations)} commit-after-delivery violation(s)")
        if self.phi_findings:
            why.append(f"{len(self.phi_findings)} PHI finding(s)")
        # Exactly the poison messages, no more and no fewer. Without this an
        # injection harness that forgot to inject anything reports success.
        if self.dlq_ids != self.expected_dlq_ids:
            why.append(
                f"DLQ mismatch: {len(self.dlq_ids - self.expected_dlq_ids)} unexpected, "
                f"{len(self.expected_dlq_ids - self.dlq_ids)} missing"
            )
        if self.require_duplicates and self.report.duplicates == 0:
            why.append("no duplicates, so replay-after-kill was never exercised")
        if self.require_exactly_once and self.report.duplicates:
            why.append(f"{self.report.duplicates} duplicate(s) in an exactly-once stage")
        if self.require_open_transaction:
            held = int(self.notes.get("open_transaction_at_kill", {}).get(
                "records_in_open_transaction", 0
            ))
            if held <= 0:
                why.append(
                    "no transaction was open at the kill, so abort and fencing were not tested"
                )
        return why

    @property
    def ok(self) -> bool:
        """The verdict the report quotes. Duplicates alone are not a failure."""
        return not self.verdict_failures

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
            "sampled_commits": self.sampled_commits,
            "notes": self.notes,
            "verdict_failures": self.verdict_failures,
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


class CommitSampler:
    """Polls the group's committed offsets while the scenario runs.

    Without this, commit-after-delivery on the cluster is only a *final-state*
    check: it compares the last committed offset against the whole delivery log,
    which cannot catch a commit that ran ahead of its delivery and was then
    caught up with. Sampling during the kill window turns it into a sequence of
    point-in-time checks.

    The timestamp is taken *after* the fetch returns, so the recorded time is at
    or later than the moment the commit became visible. That biases the check
    towards false negatives rather than false positives: a delivery dated after
    the sample really did happen after that commit.
    """

    def __init__(self, group: str, topic: str, interval_s: float = 0.25) -> None:
        self.group = group
        self.topic = topic
        self.interval_s = interval_s
        self.samples: list[tuple[float, dict[int, int]]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="commit-sampler", daemon=True)

    def _loop(self) -> None:
        consumer = Consumer(clients.consumer_config(self.group))
        try:
            tps = [TopicPartition(self.topic, p) for p in range(readback.partition_count(self.topic))]
            while not self._stop.is_set():
                try:
                    committed = consumer.committed(tps, timeout=10)
                except KafkaException:  # a broker kill can make this fail transiently
                    self._stop.wait(self.interval_s)
                    continue
                offsets = {tp.partition: tp.offset for tp in committed if tp.offset >= 0}
                self.samples.append((time.time(), offsets))
                self._stop.wait(self.interval_s)
        finally:
            consumer.close()

    def __enter__(self) -> CommitSampler:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(timeout=30)


def check_sampled_commits(
    run_dir: Path, samples: list[tuple[float, dict[int, int]]], dlq: list[readback.Readback]
) -> list[str]:
    """Every sampled commit must be backed by deliveries that already happened."""
    records = read_delivery_log(run_dir / "deliveries-*.jsonl")
    delivered_at: dict[tuple[int, int], float] = {}
    for record in records:
        key = (int(record["partition"]), int(record["offset"]))
        ts = float(record["ts"])
        delivered_at[key] = min(delivered_at.get(key, ts), ts)
    for entry in dlq:
        part = int(entry.headers.get("tb-source-partition", "-1"))
        off = int(entry.headers.get("tb-source-offset", "-1"))
        # No timestamp for a DLQ record; the produce is flushed before the offset
        # is stored, so treat it as always-already-handled.
        delivered_at.setdefault((part, off), 0.0)

    violations: list[str] = []
    for ts, offsets in samples:
        for part, committed in offsets.items():
            for off in range(committed):
                handled = delivered_at.get((part, off))
                if handled is None:
                    violations.append(
                        f"commit {part}@{committed} observed at {ts:.3f} but offset {off} "
                        f"was never delivered or dead-lettered"
                    )
                    break
                if handled > ts:
                    violations.append(
                        f"commit {part}@{committed} observed at {ts:.3f} but offset {off} "
                        f"was only delivered at {handled:.3f}"
                    )
                    break
    return violations


def read_journal(run_dir: Path) -> list[dict[str, Any]]:
    """Every assign/revoke/lost any worker recorded, ordered by wall time."""
    path = run_dir / "rebalances.jsonl"
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    with path.open("rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entries.append(orjson.loads(line))
            except orjson.JSONDecodeError:  # a partial line from a killed worker
                continue
    entries.sort(key=lambda e: float(e["ts"]))
    return entries


def rebalance_evidence(run_dir: Path, since_wall: float) -> dict[str, Any]:
    """Did a real rebalance happen after ``since_wall``, and who moved?

    "Real" means a revoke or lost by a worker that was still running. A worker's
    own shutdown also revokes its partitions, and counting those as evidence is
    how the first `rebalance` run claimed a rebalance it never had.
    """
    entries = read_journal(run_dir)
    after = [e for e in entries if float(e["ts"]) >= since_wall]
    moves = [
        e for e in after if e["kind"] in ("revoke", "lost") and not e.get("shutting_down", False)
    ]
    return {
        "revokes_while_running_after": [
            {"worker": e["worker"], "kind": e["kind"], "partitions": e["partitions"]} for e in moves
        ],
        "assigns_after": [
            {"worker": e["worker"], "partitions": e["partitions"]}
            for e in after
            if e["kind"] == "assign"
        ],
        "partitions_moved": sorted({p for e in moves for p in e["partitions"]}),
    }


def owned_at(run_dir: Path, worker: str, wall: float) -> set[int]:
    """Partitions ``worker`` owned at wall-clock time ``wall``, per the journal.

    Necessary because "partitions this worker ever delivered on" is not the same
    set: a worker that starts alone owns everything, then gives most of it away
    when the second member joins. Measuring a recovery gap against a partition
    somebody else has owned for the last ten seconds yields ~0 ms and means
    nothing, which is exactly what the first `static_restart` re-run reported.
    """
    owned: set[int] = set()
    for entry in read_journal(run_dir):
        if float(entry["ts"]) > wall:
            break
        if entry["worker"] != worker:
            continue
        parts = set(entry["partitions"])
        owned |= parts if entry["kind"] == "assign" else set()
        owned -= set() if entry["kind"] == "assign" else parts
    return owned


def wait_for_stable_assignment(
    run_dir: Path, workers: Sequence[str], timeout_s: float = 90.0
) -> bool:
    """Block until every named worker owns at least one partition.

    Needed before introducing a *further* member. Without it the existing
    members may still be settling, and their revokes get attributed to the new
    joiner: in `rebalance-200152` worker-b's own join at 316.898 caused the
    revoke at 316.907, three seconds before worker-c was assigned anything, yet
    the analysis credited that revoke to worker-c.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if all(owned_at(run_dir, w, time.time()) for w in workers):
            return True
        time.sleep(0.05)
    return False


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
    # The journal is authoritative about ownership; fall back to the delivery
    # log only if a run predates it.
    owned = owned_at(run_dir, dead, killed_wall) or {
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
    #: Commit window for the kill scenarios. Must be > 1 or the kill replays
    #: nothing and proves nothing.
    replay_commit_every: int = 20


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
    """``kill -9`` a worker mid-run; a fresh member takes the partitions over.

    Committing every ``opts.replay_commit_every`` deliveries rather than every
    one, on purpose. With ``commit_every=1`` the uncommitted window is a single
    message, so the kill lands between commits and replays almost nothing: the
    first version of this scenario reported 0 or 1 duplicates and therefore did
    not actually exercise "replay from the last commit preserves order". A
    window of 20 forces a real replay, and the verdict now *requires*
    duplicates, so a run that replayed nothing counts as a failure to test
    rather than as a pass.
    """
    layout, group, run_dir, plan = _setup("kill", opts)
    pool = _pool(run_dir, layout, group, plan)
    extra = [
        *_common(opts, idle_exit_s=20.0),
        "--commit-every", str(opts.replay_commit_every),
        # Long enough that the time-based commit does not quietly shrink the
        # window back to ~1 and defeat the point of the scenario.
        "--commit-interval-s", "30",
    ]
    t0 = time.monotonic()
    with CommitSampler(group, layout.raw) as sampler:
        a = pool.start("worker-a", extra=extra)
        pool.start("worker-b", extra=extra)
        pool.wait_for_deliveries(max(50, opts.count // 4))
        at_kill = a.wait_mid_commit_window(
            opts.replay_commit_every, minimum=max(20, opts.count // 20)
        )
        killed = a.kill()
        pool.start("worker-c", extra=extra)
        pool.wait_all()
    result = analyse("kill", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)
    result.require_duplicates = True
    result.notes["killed"] = "worker-a"
    result.notes["deliveries_by_victim_at_kill"] = at_kill
    result.notes["commit_every"] = opts.replay_commit_every
    result.notes["takeover_gap_s"] = takeover_gap_s(run_dir, "worker-a", killed)
    result.notes["session_timeout_ms"] = opts.session_timeout_ms
    result.sampled_commits = len(sampler.samples)
    result.commit_violations.extend(
        check_sampled_commits(run_dir, sampler.samples, readback.read_all(layout.dlq))
    )
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
    # Same reason as `kill`: a one-message uncommitted window means the restart
    # replays nothing and the scenario only measures rejoin time.
    extra = [
        *_common(opts, idle_exit_s=20.0),
        "--commit-every", str(opts.replay_commit_every),
        "--commit-interval-s", "30",
    ]
    t0 = time.monotonic()
    a = pool.start("worker-a", extra=extra)
    b = pool.start("worker-b", extra=extra)
    pool.wait_for_deliveries(max(50, opts.count // 4))
    at_kill = a.wait_mid_commit_window(
        opts.replay_commit_every, minimum=max(20, opts.count // 20)
    )
    killed = a.kill()
    time.sleep(2.0)  # well inside session.timeout.ms
    pool.start("worker-a", extra=extra)  # same instance id
    pool.wait_all()
    result = analyse(
        "static_restart", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0
    )
    result.require_duplicates = True
    result.notes["restarted"] = "worker-a"
    result.notes["commit_every"] = opts.replay_commit_every
    result.notes["deliveries_by_victim_at_kill"] = at_kill
    result.notes["downtime_s"] = 2.0
    # Not takeover: the same member comes back and resumes its own partitions.
    result.notes["recovery_gap_s"] = takeover_gap_s(
        run_dir, "worker-a", killed, by_others_only=False
    )
    # `revokes` alone includes the survivor's own shutdown revoke, so the
    # meaningful number is the one taken while it was still running: a static
    # rejoin inside the session timeout should leave it at 0.
    survivor = (b.stats() or {}).get("stats", {})
    result.notes["survivor_revokes_total"] = survivor.get("revokes")
    result.notes["survivor_revokes_while_running"] = survivor.get("revokes_while_running")
    result.notes["session_timeout_ms"] = opts.session_timeout_ms
    return result


def scenario_rebalance(opts: Options) -> RunResult:
    """A third member joins mid-run: cooperative-sticky, incremental assignment.

    Two things the first version of this scenario got wrong, both of which made
    it pass without testing anything:

    * worker-c was started a quarter of the way in, which at small message
      counts meant the run was over before it was assigned anything -- it
      delivered 0 records and the "rebalance" never happened;
    * the evidence was each worker's total `revokes`, but every worker revokes
      its partitions on clean shutdown too, so that counter is non-zero whether
      or not a rebalance occurred.

    Now c joins at a tenth of the way in, and the evidence is the timestamped
    journal: a revoke by a *still-running* worker after c started, plus c
    actually delivering records.
    """
    layout, group, run_dir, plan = _setup("rebalance", opts)
    pool = _pool(run_dir, layout, group, plan)
    t0 = time.monotonic()
    pool.start("worker-a", extra=_common(opts, idle_exit_s=15.0))
    pool.start("worker-b", extra=_common(opts, idle_exit_s=15.0))
    # Both founders must own partitions before c joins, or c gets blamed for the
    # revoke that b's own join caused.
    stable = wait_for_stable_assignment(run_dir, ["worker-a", "worker-b"])
    pool.wait_for_deliveries(max(20, opts.count // 10))
    joined_at = time.time()
    pool.start("worker-c", extra=_common(opts, idle_exit_s=15.0))
    pool.wait_all()
    result = analyse("rebalance", layout, group, run_dir, plan, pool, wall_s=time.monotonic() - t0)
    result.notes["founders_stable_before_join"] = stable
    result.notes["joined_at_deliveries"] = max(20, opts.count // 10)
    result.notes["worker_c_delivered"] = (
        (result.worker_stats.get("worker-c") or {}).get("stats", {}).get("delivered", 0)
    )
    result.notes["revokes_while_running"] = {
        name: (s or {}).get("stats", {}).get("revokes_while_running")
        for name, s in result.worker_stats.items()
    }
    result.notes["rebalance_evidence"] = rebalance_evidence(run_dir, joined_at)
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

    open_txn: dict[str, int] | None = None
    proc = launch("txn-a")
    if crash:
        _wait_for_records(layout.out, at_least=max(50, opts.count // 5), timeout_s=120.0)
        proc.kill()
        proc.wait(timeout=30)
        # Prove a transaction really was open at the kill, rather than assuming
        # it. Records written inside an uncommitted transaction are visible to a
        # `read_uncommitted` reader and invisible to a `read_committed` one, so
        # a positive difference here IS the open transaction. Read before the
        # restart, because `init_transactions` in the successor aborts it.
        uncommitted = len(readback.read_all(layout.out, read_committed=False))
        committed = len(readback.read_all(layout.out, read_committed=True))
        open_txn = {
            "uncommitted_visible": uncommitted,
            "committed_visible": committed,
            "records_in_open_transaction": uncommitted - committed,
        }
        log.info("open transaction at kill held %d record(s)", uncommitted - committed)
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
    # A duplicate here is a hard failure, not the accepted cost of
    # at-least-once: this is the one place ADR-001 claims exactly-once.
    result.require_exactly_once = True
    result.notes["exactly_once"] = result.report.duplicates == 0
    result.notes["out_records"] = len(out)
    result.notes["crashed_mid_transaction"] = crash
    if open_txn is not None:
        # Without this the scenario can silently degrade into "killed between
        # transactions", which proves nothing about abort or zombie fencing.
        result.require_open_transaction = True
        result.notes["open_transaction_at_kill"] = open_txn
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
