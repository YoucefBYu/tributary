"""S7 scenarios against the 3-broker spike cluster, with real loader processes.

Every scenario checks, from evidence on disk and in the cluster:

* **no loss** - every non-poison message has a successful attempt in a journal, and every poison
  message is on the DLQ topic (read back from Kafka, not from a journal);
* **poison exact** - the DLQ holds exactly the poison set, each after exactly `max_attempts`
  attempts when nothing was killed;
* **drained** - the group's committed offsets equal the high watermarks of `out` and every retry
  topic, so the run finished rather than merely stopped;
* **lateness** - attempt time minus `tb-retry-not-before`, per retry tier;
* **reordering** - messages first delivered after a later message of their key. Expected to be
  non-zero: `retry-topic` does not preserve order, and the run shows it rather than asserting it away;
* **duplicates** - successful attempts beyond the first, per message. Reported, never asserted zero.

Fault scenarios assert that the fault fired (the Phase 0 lesson): a kill needs a non-zero exit, no
stats file, work on both sides of the kill and records held or paused at the moment of the kill.
Mutation controls run the same checks against a deliberately broken loader and pass only when the
checks **fail**.

Usage::

    uv run python -m s7.runner steady-buffer,kill-buffer
    uv run python -m s7.runner all
"""

from __future__ import annotations

import argparse
import logging
import random
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import orjson
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from tbcommon import admin, clients, config, faults, payloads
from tbcommon.envelope import uuid7

from . import checks
from .model import Arrival, count_inversions, percentile, simulate
from .plan import H_ATTEMPT, H_DESTINATION, H_ERROR_CODE, H_KEY_ORDINAL, H_MSG_ID, H_SEQ, FailurePlan, Tiers

log = logging.getLogger("s7.runner")

ROOT = Path(__file__).resolve().parents[2]
RUNS = ROOT / "runs"
RESULTS = ROOT / "results"

#: Windows resolves `localhost` to ::1 first, and each failed IPv6 connect costs about 2 s.
V4 = {"broker.address.family": "v4"}


@dataclass(frozen=True, slots=True)
class Options:
    mechanism: str
    count: int = 3000
    rate: float = 100.0
    keys: int = 200
    delays: str = "1,4,12"
    max_attempts: int = 5
    partitions: int = 3
    seed: int = 7
    outages: tuple[tuple[float, float], ...] = ()  # seconds after the publisher starts
    loader_args: tuple[str, ...] = ()
    label: str = ""


@dataclass(slots=True)
class Layout:
    run_id: str
    out: str
    retry: list[str]
    dlq: str
    group: str

    @classmethod
    def create(cls, mechanism: str, tiers: int, partitions: int) -> Layout:
        run_id = f"{int(time.time())}{random.randrange(1000):03d}"
        base = f"{config.topic_prefix()}.s7.{run_id}"
        retry = [f"{base}.retry-t{i}" for i in range(tiers)] if mechanism == "tiers" else [f"{base}.retry"]
        layout = cls(run_id, f"{base}.out", retry, f"{base}.dlq", f"{base}.loader")
        admin.ensure_topics(
            [admin.TopicSpec(name, partitions=partitions) for name in [layout.out, *layout.retry, layout.dlq]]
        )
        return layout

    def delete(self) -> None:
        admin.delete_topics([self.out, *self.retry, self.dlq])


@dataclass(slots=True)
class LoaderProcess:
    name: str
    proc: subprocess.Popen[bytes]
    stats_path: Path
    killed_at: float | None = None

    def kill(self) -> None:
        self.proc.kill()  # TerminateProcess on Windows: no atexit, no commit, no LeaveGroup
        self.proc.wait(30)
        self.killed_at = time.time()

    def stats(self) -> dict[str, Any] | None:
        return orjson.loads(self.stats_path.read_bytes()) if self.stats_path.exists() else None


class Harness:
    def __init__(self, options: Options) -> None:
        self.options = options
        self.tiers = Tiers(tuple(float(value) for value in options.delays.split(",")))
        self.layout = Layout.create(options.mechanism, len(self.tiers.delays), options.partitions)
        self.run_dir = RUNS / f"{self.layout.run_id}-{options.label or options.mechanism}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.journal = self.run_dir / "journal.jsonl"
        self.start_wall = time.time() + 2.0  # the publisher's t=0, fixed before anything starts
        self.plan = FailurePlan(
            seed=options.seed,
            outages=tuple((self.start_wall + a, self.start_wall + b) for a, b in options.outages),
        )
        (self.run_dir / "plan.json").write_bytes(orjson.dumps(self.plan.to_json()))
        self.produced: list[tuple[str, int, int]] = []  # (msg_id, key ordinal, seq)
        self.loaders: list[LoaderProcess] = []

    def start_loader(self, name: str, *extra: str) -> LoaderProcess:
        stats = self.run_dir / f"stats-{name}-{len(self.loaders)}.json"
        argv = [
            sys.executable,
            "-m",
            "s7.loader",
            "--out-topic",
            self.layout.out,
            *[arg for topic in self.layout.retry for arg in ("--retry-topic", topic)],
            "--dlq-topic",
            self.layout.dlq,
            "--group",
            self.layout.group,
            "--instance-id",
            name,
            "--mechanism",
            self.options.mechanism,
            "--delays",
            self.options.delays,
            "--max-attempts",
            str(self.options.max_attempts),
            "--plan",
            str(self.run_dir / "plan.json"),
            "--journal",
            str(self.journal),
            "--stats",
            str(stats),
            *self.options.loader_args,
            *extra,
        ]
        stderr = (self.run_dir / f"stderr-{name}-{len(self.loaders)}.log").open("wb")
        proc = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=stderr)  # noqa: S603 - fixed argv
        loader = LoaderProcess(name, proc, stats)
        self.loaders.append(loader)
        return loader

    def publish(self) -> threading.Thread:
        """Paced production in a thread, so first attempts are spread over time like real traffic."""

        def run() -> None:
            producer = Producer(clients.producer_config(**V4))
            rng = random.Random(self.options.seed)
            per_key: Counter[int] = Counter()
            errors: list[str] = []

            def on_delivery(err: KafkaError | None, _msg: object) -> None:
                if err is not None:
                    errors.append(str(err))

            while time.time() < self.start_wall:
                time.sleep(0.01)
            for index in range(self.options.count):
                target = self.start_wall + index / self.options.rate
                delay = target - time.time()
                if delay > 0:
                    time.sleep(delay)
                ordinal = rng.randrange(self.options.keys)
                seq = per_key[ordinal]
                per_key[ordinal] += 1
                msg_id = uuid7()
                key = payloads.patient_key(ordinal)
                producer.produce(
                    self.layout.out,
                    key=key.encode("utf-8"),
                    value=payloads.adt(key, f"{index:08d}"),
                    headers=[
                        (H_MSG_ID, msg_id.encode()),
                        (H_ATTEMPT, b"0"),
                        (H_SEQ, str(seq).encode()),
                        (H_KEY_ORDINAL, str(ordinal).encode()),
                    ],
                    on_delivery=on_delivery,
                )
                producer.poll(0)
                self.produced.append((msg_id, ordinal, seq))
            if producer.flush(60) or errors:
                raise RuntimeError("publisher could not deliver every record")

        thread = threading.Thread(target=run, name="publisher", daemon=True)
        thread.start()
        return thread

    def journal_lines(self) -> list[dict[str, Any]]:
        lines: list[dict[str, Any]] = []
        if not self.journal.exists():
            return lines
        for raw in self.journal.read_bytes().splitlines():
            try:
                lines.append(orjson.loads(raw))
            except orjson.JSONDecodeError:
                continue  # a partial last line from a killed loader
        return lines

    def last_state(self, worker: str) -> dict[str, Any] | None:
        states = [line for line in self.journal_lines() if line.get("state") and line.get("w") == worker]
        return states[-1] if states else None

    def wait_all(self, timeout_s: float = 600.0) -> None:
        deadline = time.monotonic() + timeout_s
        for loader in self.loaders:
            if loader.killed_at is not None:
                continue
            remaining = max(1.0, deadline - time.monotonic())
            try:
                loader.proc.wait(remaining)
            except subprocess.TimeoutExpired:
                loader.proc.kill()
                raise

    def read_dlq(self) -> tuple[Counter[str], int]:
        """DLQ message ids read back from Kafka, and how many records lack the exhaustion headers."""
        consumer = Consumer(
            clients.consumer_config(
                f"{self.layout.group}.readback", read_committed=False, **{"enable.partition.eof": True}, **V4
            )
        )
        consumer.assign([TopicPartition(self.layout.dlq, p, 0) for p in range(self.options.partitions)])
        ids: Counter[str] = Counter()
        missing = 0
        eof: set[int] = set()
        deadline = time.monotonic() + 60
        while len(eof) < self.options.partitions and time.monotonic() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error() is not None:
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    eof.add(msg.partition())
                continue
            headers = dict(msg.headers() or [])
            ids[headers[H_MSG_ID].decode()] += 1
            if not all(headers.get(name) for name in (H_ERROR_CODE, H_ATTEMPT, H_DESTINATION)):
                missing += 1
        consumer.close()
        return ids, missing

    def drained(self) -> dict[str, int]:
        """Committed offset minus high watermark, summed per topic. 0 everywhere means finished."""
        consumer = Consumer(clients.consumer_config(self.layout.group, read_committed=False, **V4))
        lag: dict[str, int] = {}
        for topic in [self.layout.out, *self.layout.retry]:
            partitions = [TopicPartition(topic, p) for p in range(self.options.partitions)]
            committed = consumer.committed(partitions, timeout=30)
            total = 0
            for tp in committed:
                _low, high = consumer.get_watermark_offsets(TopicPartition(topic, tp.partition), timeout=30)
                total += high - max(tp.offset, 0)
            lag[topic.rsplit(".", 1)[-1]] = total
        consumer.close()
        return lag


class CommitSampler:
    """Samples the group's committed offsets from outside every 250 ms, as S5 did.

    The loader's own commit journal gives an exact check; this one checks the broker's view, which is
    what a replacement member actually resumes from.
    """

    def __init__(self, harness: Harness, interval_s: float = 0.25) -> None:
        self.harness = harness
        self.interval_s = interval_s
        self.samples: list[tuple[float, str, int, int]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, name="commit-sampler", daemon=True)

    def _loop(self) -> None:
        layout = self.harness.layout
        consumer = Consumer(clients.consumer_config(layout.group, read_committed=False, **V4))
        partitions = [
            TopicPartition(topic, p)
            for topic in [layout.out, *layout.retry]
            for p in range(self.harness.options.partitions)
        ]
        while not self._stop.is_set():
            try:
                committed = consumer.committed(partitions, timeout=5)
                now = time.time()
                self.samples.extend((now, tp.topic, tp.partition, tp.offset) for tp in committed if tp.offset >= 0)
            except KafkaException:
                pass  # a broker kill makes this fail transiently; the next sample retries
            self._stop.wait(self.interval_s)
        consumer.close()

    def __enter__(self) -> CommitSampler:
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._stop.set()
        self._thread.join(30)


# ---------------------------------------------------------------------------------- analysis

CHECKS = (
    "loss",
    "dlq_exact",
    "dlq_headers",
    "exhaustion",
    "drained",
    "commit_order",
    "commit_sampled",
    "never_failed_order",
    "scheduling_lag",
    "fault_fired",
)

#: Resume must not be driven by the 200 ms poll timeout (ADR-003 follow-up 6). S5 measured 155 ms of
#: it; a p50 under 50 ms cannot be produced by a poll-timeout-driven loop.
SCHEDULING_LAG_P50_S = 0.050


@dataclass(slots=True)
class Result:
    scenario: str
    mechanism: str
    label: str
    expect_check: str | None = None
    failures: list[dict[str, str]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def fail(self, check: str, detail: str) -> None:
        assert check in CHECKS, check
        self.failures.append({"check": check, "detail": detail})

    @property
    def failed_checks(self) -> set[str]:
        return {failure["check"] for failure in self.failures}

    @property
    def ok(self) -> bool:
        # A mutation control passes only when ITS check caught the mutation.
        if self.expect_check is not None:
            return self.expect_check in self.failed_checks
        return not self.failures

    def summary(self) -> dict[str, Any]:
        return {
            "scenario": self.scenario,
            "mechanism": self.mechanism,
            "label": self.label,
            "ok": self.ok,
            "expect_check": self.expect_check,
            "failures": self.failures,
            **self.metrics,
        }


def _stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {"n": 0}
    return {
        "n": len(values),
        "p50_ms": round(percentile(values, 0.5) * 1000, 1),
        "p90_ms": round(percentile(values, 0.9) * 1000, 1),
        "p99_ms": round(percentile(values, 0.99) * 1000, 1),
        "max_ms": round(max(values) * 1000, 1),
    }


def analyse(
    harness: Harness,
    scenario: str,
    *,
    killed: bool,
    sampled: list[tuple[float, str, int, int]],
    expect_check: str | None = None,
) -> Result:
    options = harness.options
    result = Result(scenario, options.mechanism, options.label or options.mechanism, expect_check)
    lines = harness.journal_lines()
    evidence = checks.Evidence.from_lines(lines)
    produced = {msg_id: (ordinal, seq) for msg_id, ordinal, seq in harness.produced}
    poison = {msg_id for msg_id in produced if harness.plan.is_poison(msg_id)}
    deliverable = set(produced) - poison
    ok_counts: Counter[str] = Counter(a["id"] for a in evidence.attempts if a["ok"])
    dlq, dlq_missing_headers = harness.read_dlq()

    lost_deliverable = deliverable - set(ok_counts)
    lost_poison = poison - set(dlq)
    if lost_deliverable or lost_poison:
        result.fail("loss", f"{len(lost_deliverable)} deliverable not delivered, {len(lost_poison)} poison not in DLQ")
    if set(dlq) - poison:
        result.fail("dlq_exact", f"{len(set(dlq) - poison)} non-poison message(s) dead-lettered")
    if dlq_missing_headers:
        result.fail(
            "dlq_headers", f"{dlq_missing_headers} DLQ record(s) missing tb-error-code/tb-attempt/tb-destination"
        )
    tries: Counter[str] = Counter(a["id"] for a in evidence.attempts)
    if not killed:
        wrong = [msg_id for msg_id in poison if tries[msg_id] != options.max_attempts]
        if wrong:
            result.fail("exhaustion", f"{len(wrong)} poison message(s) not tried exactly max_attempts times")
    lag = harness.drained()
    if any(lag.values()):
        result.fail("drained", f"committed offsets short of the high watermark: {lag}")

    anonymous = sum(1 for ack in evidence.acks if not ack["acked"])
    if anonymous:
        # Blind evidence would read as "never acknowledged" and fail commit_order for the wrong reason.
        result.fail("commit_order", f"{anonymous} ack line(s) carry no message id: the evidence is blind")
    resolved = checks.resolution_times(evidence)
    journal_violations = checks.commit_violations(resolved, evidence.commits)
    if journal_violations:
        result.fail("commit_order", f"{len(journal_violations)} commit(s) past an unresolved record")
    sampled_violations = checks.commit_violations(resolved, sampled)
    if sampled_violations:
        result.fail(
            "commit_sampled", f"{len(sampled_violations)} sampled committed offset(s) past an unresolved record"
        )
    if not sampled:
        result.fail("commit_sampled", "no committed-offset samples were taken")

    never_failed = checks.never_failed_inversions(evidence, produced)
    if never_failed:
        result.fail("never_failed_order", f"{never_failed} never-failed message(s) delivered out of order")

    sched = checks.scheduling_lag(evidence, options.mechanism)
    if sched and percentile(sched, 0.5) > SCHEDULING_LAG_P50_S:
        result.fail("scheduling_lag", f"p50 {percentile(sched, 0.5) * 1000:.0f} ms: resume is poll- or fetch-driven")

    late = checks.lateness(evidence)
    duplicates = sum(count - 1 for count in ok_counts.values() if count > 1)

    # The model, fed this run's own first-attempt trace, predicts the head-of-line component.
    firsts: dict[str, Arrival] = {}
    for attempt in sorted(evidence.attempts, key=lambda a: a["ts"]):
        if attempt["n"] == 1 and attempt["id"] not in firsts:
            firsts[attempt["id"]] = Arrival(attempt["ts"], attempt["id"], attempt["k"], attempt["s"])
    model = simulate(
        sorted(firsts.values(), key=lambda arrival: arrival.t),
        harness.plan,
        harness.tiers,
        options.mechanism,  # type: ignore[arg-type]
        partitions=options.partitions,
        max_attempts=options.max_attempts,
    )

    result.metrics = {
        "produced": len(produced),
        "poison": len(poison),
        "attempts": len(evidence.attempts),
        "delivered_unique": len(ok_counts),
        "duplicates": duplicates,
        "duplicate_rate": round(duplicates / max(1, len(deliverable)), 4),
        "dlq_records": sum(dlq.values()),
        "dlq_duplicates": sum(count - 1 for count in dlq.values() if count > 1),
        "lost_deliverable": len(lost_deliverable),
        "lost_poison": len(lost_poison),
        "commits_journaled": len(evidence.commits),
        "commit_samples": len(sampled),
        "never_failed_inverted": never_failed,
        "retried_inverted_total": count_inversions(
            (produced[m][0], produced[m][1], ts) for m, ts in checks.first_ok(evidence).items() if m in produced
        ),
        "displacement": checks.displacement(evidence, produced),
        "lateness_by_retry": {str(n): _stats(values) for n, values in sorted(late.items())},
        "lateness_all": _stats([value for values in late.values() for value in values]),
        "scheduling_lag": _stats(sched),
        "resume_lag": _stats(checks.resume_lag(evidence)),
        "model": {
            "lateness_all": _stats([a.lateness for a in model.retries()]),
            "inverted": model.inverted(),
            "max_buffered": model.max_buffered,
        },
        "lag": lag,
        "loaders": {f"{loader.name}#{i}": loader.stats() for i, loader in enumerate(harness.loaders)},
    }
    return result


# ---------------------------------------------------------------------------------- scenarios

OUTAGES = ((8.0, 11.0), (18.0, 20.0))


def _wait_busy(harness: Harness, worker: str, *, timeout_s: float = 120.0) -> dict[str, Any]:
    """Wait until `worker` holds or has paused retry records and a third of the traffic is through.

    A fault injected into an idle loader tests nothing (the Phase 0 lesson), so every fault scenario
    waits for this before firing.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        time.sleep(0.2)
        state = harness.last_state(worker)
        if state and (state["held"] or state["paused"]) and state["attempts"] >= harness.options.count // 3:
            return state
    raise RuntimeError(f"the fault window never opened: {worker} held nothing after a third of the run")


def scenario_run(options: Options, *, expect_check: str | None = None) -> Result:
    harness = Harness(options)
    try:
        with CommitSampler(harness) as sampler:
            harness.start_loader("loader-a")
            harness.publish().join()
            harness.wait_all()
        name = "outage" if options.outages else "steady"
        return analyse(harness, name, killed=False, sampled=sampler.samples, expect_check=expect_check)
    finally:
        harness.layout.delete()


def scenario_kill(options: Options, *, expect_check: str | None = None) -> Result:
    """`kill -9` the loader in the middle of a delay window; a replacement takes over.

    The replacement uses the same `group.instance.id` - the planned-restart path, so the takeover is
    immediate and what is measured is the replay, not the session timeout (S5 measured that).
    """
    harness = Harness(options)
    try:
        with CommitSampler(harness) as sampler:
            victim = harness.start_loader("loader-a")
            publisher = harness.publish()
            at_kill = _wait_busy(harness, "loader-a")
            victim.kill()
            time.sleep(1.0)
            harness.start_loader("loader-a")
            publisher.join()
            harness.wait_all()
        result = analyse(harness, "kill", killed=True, sampled=sampler.samples, expect_check=expect_check)
        assert victim.killed_at is not None
        attempts = [line for line in harness.journal_lines() if "n" in line and "id" in line]
        before = sum(1 for line in attempts if line["ts"] < victim.killed_at)
        after = sum(1 for line in attempts if line["ts"] > victim.killed_at)
        fired = {
            "returncode": victim.proc.returncode,
            "stats_file_absent": victim.stats() is None,
            "held_at_kill": at_kill["held"],
            "paused_at_kill": at_kill["paused"],
            "attempts_before_kill": before,
            "attempts_after_kill": after,
        }
        result.metrics["fault"] = fired
        if victim.proc.returncode == 0 or victim.stats() is not None or not before or not after:
            result.fail("fault_fired", f"the kill did not behave as a kill: {fired}")
        return result
    finally:
        harness.layout.delete()


def scenario_leader_kill(options: Options) -> Result:
    """SIGKILL the broker leading retry partition 0 mid-delay-window, bring it back after 10 s."""
    harness = Harness(options)
    try:
        with CommitSampler(harness) as sampler:
            harness.start_loader("loader-a")
            publisher = harness.publish()
            at_kill = _wait_busy(harness, "loader-a")
            retry_topic = harness.layout.retry[0]
            service = faults.leader_service_for(retry_topic, 0)
            killed_node = admin.partition_leaders(retry_topic)[0]
            killed_at = time.time()
            faults.kill_broker(service)
            time.sleep(3.0)
            new_leader = admin.partition_leaders(retry_topic)[0]
            time.sleep(7.0)
            faults.start_broker(service)
            publisher.join()
            harness.wait_all()
            faults.wait_healthy()
        result = analyse(harness, "leader-kill", killed=False, sampled=sampler.samples)
        attempts = [line for line in harness.journal_lines() if "n" in line and "id" in line]
        fired = {
            "killed_service": service,
            "killed_node": killed_node,
            "leader_after_kill": new_leader,
            "held_at_kill": at_kill["held"],
            "paused_at_kill": at_kill["paused"],
            "attempts_during_outage": sum(1 for line in attempts if killed_at < line["ts"] < killed_at + 10),
        }
        result.metrics["fault"] = fired
        if new_leader == killed_node:
            result.fail("fault_fired", f"leadership never moved off the killed broker: {fired}")
        return result
    finally:
        try:
            faults.wait_healthy()
        finally:
            harness.layout.delete()


def scenario_rebalance(options: Options) -> Result:
    """A second member joins mid-run: a cooperative, planned handover of some partitions."""
    harness = Harness(options)
    try:
        with CommitSampler(harness) as sampler:
            harness.start_loader("loader-a")
            publisher = harness.publish()
            _wait_busy(harness, "loader-a")
            harness.start_loader("loader-b")
            publisher.join()
            harness.wait_all()
        result = analyse(harness, "rebalance", killed=False, sampled=sampler.samples)
        revokes = [line for line in harness.journal_lines() if "revoke" in line and line["ts"] < time.time()]
        mid_run = [line for line in revokes if line["w"] == "loader-a"]
        moved = sum(1 for line in harness.journal_lines() if "moved" in line)
        result.metrics["fault"] = {"revokes": len(revokes), "reenqueued_on_revoke": moved}
        attempts_b = sum(1 for line in harness.journal_lines() if line.get("w") == "loader-b" and "n" in line)
        if not mid_run or not attempts_b:
            result.fail("fault_fired", "no partition moved to the joiner: the rebalance never happened")
        return result
    finally:
        harness.layout.delete()


PLAN: list[tuple[str, str, dict[str, Any]]] = [
    # (scenario, mechanism, overrides)
    ("run", "pause", {"label": "steady-pause"}),
    ("run", "buffer", {"label": "steady-buffer"}),
    ("run", "tiers", {"label": "steady-tiers"}),
    ("run", "pause", {"label": "outage-pause", "outages": OUTAGES}),
    ("run", "buffer", {"label": "outage-buffer", "outages": OUTAGES}),
    ("run", "tiers", {"label": "outage-tiers", "outages": OUTAGES}),
    (
        "run",
        "pause",
        {"label": "outage-pause-fetchwait50", "outages": OUTAGES, "loader_args": ("--fetch-wait-ms", "50")},
    ),
    ("kill", "pause", {"label": "kill-pause", "outages": OUTAGES}),
    ("kill", "buffer", {"label": "kill-buffer", "outages": OUTAGES}),
    ("kill", "tiers", {"label": "kill-tiers", "outages": OUTAGES}),
    ("leader-kill", "buffer", {"label": "leader-kill-buffer", "outages": OUTAGES}),
    ("leader-kill", "pause", {"label": "leader-kill-pause", "outages": OUTAGES}),
    ("rebalance", "buffer", {"label": "rebalance-buffer", "outages": OUTAGES}),
    (
        "rebalance",
        "buffer",
        {"label": "rebalance-buffer-no-reenqueue", "outages": OUTAGES, "loader_args": ("--no-reenqueue",)},
    ),
    ("rebalance", "pause", {"label": "rebalance-pause", "outages": OUTAGES}),
    # Mutation controls: each passes only if the named check goes red.
    ("control:loss", "buffer", {"label": "mutation-drop-retry", "loader_args": ("--mutate", "drop-retry")}),
    (
        "control-kill:loss",
        "buffer",
        {"label": "mutation-commit-high", "outages": OUTAGES, "loader_args": ("--mutate", "commit-high")},
    ),
    (
        "control:commit_order",
        "buffer",
        {"label": "mutation-commit-unflushed", "outages": OUTAGES, "loader_args": ("--mutate", "commit-unflushed")},
    ),
    ("control:exhaustion", "buffer", {"label": "mutation-dlq-early", "loader_args": ("--mutate", "dlq-early")}),
    ("control:never_failed_order", "pause", {"label": "mutation-swap-out", "loader_args": ("--mutate", "swap-out")}),
    (
        "control:scheduling_lag",
        "buffer",
        {"label": "control-naive-poll", "outages": OUTAGES, "loader_args": ("--naive-poll",)},
    ),
]


def run_one(scenario: str, options: Options) -> Result:
    kind, _, expect = scenario.partition(":")
    expect_check = expect or None
    if kind in ("run", "control"):
        return scenario_run(options, expect_check=expect_check)
    if kind in ("kill", "control-kill"):
        return scenario_kill(options, expect_check=expect_check)
    if kind == "leader-kill":
        return scenario_leader_kill(options)
    if kind == "rebalance":
        return scenario_rebalance(options)
    raise ValueError(scenario)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="s7.runner")
    parser.add_argument("what", help="`all`, or a comma-separated list of labels from PLAN")
    parser.add_argument("--count", type=int, default=3000)
    parser.add_argument("--rate", type=float, default=100.0)
    parser.add_argument("--out", type=Path, default=RESULTS / "cluster-runs.json")
    args = parser.parse_args(argv)
    admin.wait_for_cluster()
    wanted = None if args.what == "all" else set(args.what.split(","))
    results: list[dict[str, Any]] = []
    for scenario, mechanism, overrides in PLAN:
        if wanted is not None and overrides["label"] not in wanted:
            continue
        options = Options(mechanism=mechanism, count=args.count, rate=args.rate, **overrides)
        log.info("running %s", options.label)
        try:
            summary = run_one(scenario, options).summary()
        except Exception as exc:  # noqa: BLE001 - one broken scenario must not hide the others
            log.exception("scenario %s crashed", options.label)
            summary = {"label": options.label, "mechanism": mechanism, "ok": False, "crashed": type(exc).__name__}
        results.append(summary)
        print(orjson.dumps(summary, option=orjson.OPT_INDENT_2).decode(), flush=True)
        args.out.parent.mkdir(parents=True, exist_ok=True)
        existing = orjson.loads(args.out.read_bytes()) if args.out.exists() else []
        merged = [item for item in existing if item["label"] != summary["label"]] + [summary]
        args.out.write_bytes(orjson.dumps(merged, option=orjson.OPT_INDENT_2))
    return 0 if all(item["ok"] for item in results) else 1


if __name__ == "__main__":  # pragma: no cover - CLI
    raise SystemExit(main())
