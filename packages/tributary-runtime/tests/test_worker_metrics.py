"""The worker's `/metrics` exposition against `contracts/observability/v0/metrics.json`, in both directions.

As `test_codes_contract.py` does for error codes: the contract is the source and the exposition is checked
against it, each direction its own assertion. A real `PipelineHandler` and `ConsumerLoop` (on the fakes, no
Kafka) run records that are routed, filtered, dead-lettered, retried and finally halt a partition, so every
worker metric has at least one series and its label names and values can be checked, not just its name.

Controls: a registry drifted from the contract (a renamed metric, a dropped label) refuses at construction or
at the first observation, and the diff function flags an exposition carrying an unregistered name.
"""

from __future__ import annotations

import copy
import io
import json
import re
from typing import Any

import pytest
from confluent_kafka import TopicPartition
from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.metrics import MetricLabelError, MetricRegistrationError, MetricsContract, MetricsRegistry
from tributary.runtime.producer import RecordProducer
from tributary.runtime.records import to_kafka_headers
from tributary.runtime.spec import ChannelSpec, RetryPolicy
from tributary.runtime.worker import KafkaDlqWriter, PipelineHandler
from tributary.runtime.worker_metrics import WorkerMetrics, WorkerSources
from tributary.sdk import Drop, Message, step
from tributary.sdk.testing import make_message

from .fakes import FakeConsumer, FakeProducer, FakeRecord
from .ops_support import METRICS_JSON, Exposition, parse_exposition
from .test_consumer_loop import Clock
from .test_worker import spec

DOCUMENT: dict[str, Any] = json.loads(METRICS_JSON.read_text(encoding="utf-8"))
CHANNEL = "adt-passthrough"
TOPIC = f"dev.{CHANNEL}.raw"
HALT_FAST = RetryPolicy(policy="blocking", max_attempts=2, on_exhaust="halt-partition", initial_delay_ms=10)


@step(name="route-or-drop")  # spec grammar (`$defs/step` name): a Python name with `_` is not a step name
def route_or_drop(msg: Message) -> Message | Drop:
    if msg.payload == b"drop":
        return Drop(reason="filtered on purpose")
    if msg.payload == b"explode":
        raise ValueError("SYNTH^JANE mrn 12345")
    return msg


def contract_diff(exposition: Exposition, document: dict[str, Any], component: str) -> list[str]:
    """Every way the exposition and the registry disagree, as fixed sentences. Empty means they agree."""
    problems: list[str] = []
    registered = {m["name"]: m for m in document["metrics"] if component in m["emitters"]}
    for name, kind in exposition.types.items():
        entry = registered.get(name)
        if entry is None:
            problems.append(f"exported but not registered for {component}: {name}")
        elif entry["type"] != kind:
            problems.append(f"type differs: {name}")
    for name in registered:
        if name not in exposition.types:
            problems.append(f"registered but not exported: {name}")
        elif not any(exposition.family_of(s.name) == name for s in exposition.samples):
            problems.append(f"registered but no series: {name}")
    labels = document["labels"]
    for sample in exposition.samples:
        family = exposition.family_of(sample.name)
        entry = registered.get(family)
        if entry is None:
            continue
        names = {k for k in sample.labels if not (k == "le" and sample.name.endswith("_bucket"))}
        if names != set(entry["labels"]):
            problems.append(f"label names differ: {family}")
            continue
        for label in entry["labels"]:
            value = sample.labels[label]
            rule = labels[label]
            if rule.get("per_metric"):
                ok = value in entry["label_values"][label]
            elif "enum" in rule:
                ok = value in rule["enum"]
            else:
                ok = re.fullmatch(rule["pattern"], value) is not None
            if not ok:
                problems.append(f"value outside the closed list: {family}.{label}")
    return problems


def raw(offset: int, payload: bytes) -> FakeRecord:
    message = make_message(payload, key=b"SYNTH-KEY-1", channel=CHANNEL)
    return FakeRecord(
        _topic=TOPIC,
        _partition=0,
        _offset=offset,
        _key=b"SYNTH-KEY-1",
        _value=payload,
        _headers=to_kafka_headers(message),
    )


class Harness:
    """A worker without Kafka: the real handler and loop, a counting emitter, the metrics bound as `build()` does."""

    def __init__(self, metrics: WorkerMetrics, *, channel_spec: ChannelSpec | None = None) -> None:
        self.clock = Clock()
        self.spec = channel_spec or spec(version=3)
        self.consumer = FakeConsumer()
        self.producer = FakeProducer()
        self.events = RecordingEmitter()
        log = JsonLogger("worker", stream=io.StringIO())
        emitter = metrics.counted(self.events)
        records = RecordProducer(self.producer, log=log)
        self.handler = PipelineHandler(
            env="dev",
            spec_provider=lambda: self.spec,
            producer=records,
            emitter=emitter,
            log=log,
            instance="worker:test@localhost",
            steps_override=(route_or_drop,),
            metrics=metrics,
        )
        self.handler.prepare(self.spec)
        self.loop = ConsumerLoop(
            self.consumer,
            self.handler,
            topics=[TOPIC],
            retry=HALT_FAST,
            settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
            emitter=emitter,
            log=log,
            channel=CHANNEL,
            channel_version=3,
            instance="worker:test@localhost",
            dlq=KafkaDlqWriter(records, f"dev.{CHANNEL}.dlq", log=log, source="worker:test@localhost"),
            clock=self.clock,
        )
        self.loop.start()
        loop, handler = self.loop, self.handler
        metrics.bind(
            WorkerSources(
                loop_stats=lambda: loop.stats,
                partition_counts=loop.partition_counts,
                reply_stats=lambda: None,
                applied_version=lambda: handler.applied_version,
                spec_available=lambda: True,
            )
        )
        self.metrics = metrics

    def run(self, iterations: int = 1, *, advance_s: float = 0.0) -> None:
        for _ in range(iterations):
            self.loop.run(max_iterations=1)
            self.clock.advance(advance_s)

    def scrape(self) -> Exposition:
        return parse_exposition(self.metrics.render().decode())


def strict_metrics(document: dict[str, Any] | None = None) -> WorkerMetrics:
    registry = MetricsRegistry(
        MetricsContract.from_document(copy.deepcopy(document or DOCUMENT)), component="worker", strict=True
    )
    return WorkerMetrics(registry, channel=CHANNEL, version="0.0.0+test")


def exercised() -> Harness:
    """Routed, filtered, dead-lettered (step failure), then a produce that never lands until the partition halts."""
    harness = Harness(strict_metrics())
    for offset, payload in enumerate([b"ok", b"drop", b"explode"]):
        harness.consumer.append(raw(offset, payload))
    harness.run(3)
    harness.producer.remaining = 10_000  # every flush times out: PRODUCE_TIMEOUT, retryable
    harness.consumer.append(raw(3, b"ok"))
    harness.run(6, advance_s=1.0)
    return harness


def test_the_exposition_and_the_registry_agree_in_both_directions() -> None:
    exposition = exercised().scrape()
    assert contract_diff(exposition, DOCUMENT, "worker") == []


def test_every_exported_name_is_registered_for_the_worker() -> None:
    exposition = exercised().scrape()
    worker = {m["name"] for m in DOCUMENT["metrics"] if "worker" in m["emitters"]}
    assert set(exposition.types) - worker == set(), "exported names nobody registered"


def test_every_registered_worker_metric_is_exported_with_a_series() -> None:
    exposition = exercised().scrape()
    worker = {m["name"] for m in DOCUMENT["metrics"] if "worker" in m["emitters"]}
    with_series = {exposition.family_of(s.name) for s in exposition.samples}
    assert worker - with_series == set(), "registered worker metrics with no series"


def test_the_values_say_what_happened() -> None:
    harness = exercised()
    e = harness.scrape()
    base = {"component": "worker", "channel": CHANNEL}
    assert e.value("tb_partitions_halted", **base, destination="") == 1
    assert e.value("tb_partitions_paused", **base, destination="") == 0
    assert e.value("tb_spec_version", **base) == 3
    assert e.value("tb_spec_source_unproven", **base) == 0
    assert e.value("tb_build_info", component="worker", version="0.0.0+test") == 1
    assert e.value("tb_worker_records_total", channel=CHANNEL, outcome="handled") == 2
    assert e.value("tb_worker_records_total", channel=CHANNEL, outcome="dead_lettered") == 1
    assert e.value("tb_worker_records_total", channel=CHANNEL, outcome="retried") == 2
    assert e.value("tb_commits_total", **base, outcome="ok") == 3
    assert e.value("tb_messages_total", **base, stage="filtered", status="dropped", destination="") == 1
    assert e.value("tb_messages_total", **base, stage="routed", status="ok", destination="http-stub") == 1
    assert e.value("tb_messages_total", **base, stage="dlq", status="error", destination="") == 1
    assert e.value("tb_messages_total", **base, stage="partition_halted", status="error", destination="") == 1
    # One pipeline execution reached `transformed` and was delivered: the first record.
    assert e.value("tb_stage_duration_seconds_count", **base, stage="transformed", destination="") == 1
    step_calls = {s.labels["outcome"]: s.value for s in e.series("tb_step_duration_seconds_count")}
    # ok: offsets 0, then 3 on each of its two attempts (the step runs before the produce fails).
    failing_calls = harness.spec.pipeline.step_retries + 1  # a failing step is retried in place, each a call
    assert step_calls == {"ok": 3, "dropped": 1, "failed": failing_calls}
    assert "SYNTH" not in harness.metrics.render().decode() and "12345" not in harness.metrics.render().decode()


def test_a_forced_halt_shows_tb_partitions_halted_1_and_a_paused_partition_counts_while_it_waits() -> None:
    harness = Harness(strict_metrics())
    harness.producer.remaining = 10_000
    harness.consumer.append(raw(0, b"ok"))
    harness.run(1)
    base = {"component": "worker", "channel": CHANNEL, "destination": ""}
    paused = harness.scrape()
    assert (paused.value("tb_partitions_paused", **base), paused.value("tb_partitions_halted", **base)) == (1, 0)
    harness.run(4, advance_s=1.0)
    halted = harness.scrape()
    assert (halted.value("tb_partitions_paused", **base), halted.value("tb_partitions_halted", **base)) == (0, 1)
    # Revocation hands the partition (and its halt) to another member: no longer counted here.
    harness.loop._on_revoke(harness.consumer, [TopicPartition(TOPIC, 0)])
    assert harness.scrape().value("tb_partitions_halted", **base) == 0


def test_a_redeploy_moves_tb_spec_version_on_the_next_record_or_while_idle() -> None:
    harness = Harness(strict_metrics())
    base = {"component": "worker", "channel": CHANNEL}
    assert harness.scrape().value("tb_spec_version", **base) == 3
    harness.spec = spec(version=4)
    harness.handler.refresh()  # the loop's idle tick
    assert harness.scrape().value("tb_spec_version", **base) == 4
    harness.spec = spec(version=5)
    harness.consumer.append(raw(0, b"ok"))
    harness.run(1)
    assert harness.scrape().value("tb_spec_version", **base) == 5


# -- controls --------------------------------------------------------------------------------------


def test_control_a_renamed_metric_in_the_registry_refuses_the_exporter() -> None:
    document = copy.deepcopy(DOCUMENT)
    next(m for m in document["metrics"] if m["name"] == "tb_partitions_halted")["name"] = "tb_partitions_stuck"
    with pytest.raises(MetricRegistrationError):
        strict_metrics(document)


def test_control_a_dropped_label_in_the_registry_fails_the_first_observation() -> None:
    document = copy.deepcopy(DOCUMENT)
    entry = next(m for m in document["metrics"] if m["name"] == "tb_messages_total")
    entry["labels"] = [label for label in entry["labels"] if label != "destination"]
    harness = Harness(strict_metrics(document))
    harness.consumer.append(raw(0, b"ok"))
    with pytest.raises(MetricLabelError):
        harness.run(1)


def test_control_the_diff_flags_an_unregistered_name_and_a_missing_one() -> None:
    exposition = exercised().scrape()
    exposition.types["tb_worker_unlisted_total"] = "counter"
    del exposition.types["tb_partitions_halted"]
    problems = contract_diff(exposition, DOCUMENT, "worker")
    assert "exported but not registered for worker: tb_worker_unlisted_total" in problems
    assert "registered but not exported: tb_partitions_halted" in problems
