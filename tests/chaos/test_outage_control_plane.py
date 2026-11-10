"""W3-9a (wave 3 exit criterion 2, invariants 3 and 9): a pipeline worker keeps its full rate while
ClickHouse, PostgreSQL and the API are all stopped.

Topology per run (all real processes and containers, fresh each run): Kafka (shared by the module), one
ClickHouse, one TLS PostgreSQL, the archiver (`python -m tributary.archiver`, consuming `_tb.<env>.events`
into ClickHouse), the API (`python -m tributary.api`, on PostgreSQL), and one pipeline worker on an
`ordering: per-key` channel. The archiver and the API stay in the topology so stopping their stores is a real
fault for live components, not the absence of a consumer.

Method. Warm-up: 48 keyed messages go through, and the archiver is shown to have written rows to ClickHouse
and the API to answer `/readyz` (so the control arm is wired and the stopped arm was working before the
fault). Then, in a `stopped` run, the API process, PostgreSQL and ClickHouse are stopped and each is shown down
(`assert_fault_fired`); a `control` run changes nothing and shows all three up. Then `N` synthetic keyed
messages are published to `raw` in one burst and the worker's throughput is `N / (time until the Nth record
is read from out.<dest>)`, measured from just before the publish. Runs alternate control, stopped, control,
... so host drift lands on both arms. Pass (plan section 4, criterion 2): the stopped runs' median is not below the
control runs' minimum (one-sided, lead's ruling after set 1; `tests.support.outage_verdict`). Every run also asserts no ACKed message lost
(each id published is on `out`), 0 per-key order violations among first deliveries, one partition per key,
and the same worker process alive throughout. Duplicates are counted and reported.

Mutation control. `test_outage_control_plane_mutation_control_is_red` runs the same harness with a worker
whose pipeline step waits on ClickHouse (the archive in the message path, an invariant 9 violation), and the
same verdict function must report FAIL. A check that cannot go red proves nothing.

Marked `chaos`, not `integration`: ten runs with fresh containers take several minutes, far beyond a
per-push budget, so this belongs in the on-demand and nightly chaos job (`integration_expected.json` does
not change).
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.sdk.testing import make_message

from benchmarks.harness import environment
from tests.support.channel_spec import ordered_document
from tests.support.chaos_assertions import (
    assert_each_key_on_one_partition,
    assert_fault_fired,
    assert_no_acknowledged_loss,
    count_order_violations,
)
from tests.support.control_plane_stack import ControlPlaneStack, port_refuses
from tests.support.kafka_helpers import await_broker, create_topics_with_retry, free_port, header_map, publish_messages
from tests.support.outage_verdict import outage_verdict
from tests.support.processes import spawn_pipeline_worker

pytestmark = pytest.mark.chaos

CHANNEL = "w9a-adt"
DESTINATION = "http-stub"
PARTITIONS = 4
KEYS = [f"SYNTH-{index:02d}" for index in range(16)]
WARM_PER_KEY = 12
SEQ = "tb-usr-seq"
RUNS_PER_ARM = int(os.environ.get("TB_W9A_RUNS", "5"))  # 5 is the plan; fewer only for a smoke run
N_MESSAGES = int(os.environ.get("TB_W9A_MESSAGES", "10000"))
SETTLE_S = 3.0  # lets the archiver reach its failure state before the window opens, in both arms
MUTANT_MESSAGES = 300
BUNDLE_DIR = Path(__file__).resolve().parents[1] / "support" / "bundle_w9a"


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_name("tb-qa-w39a-kafka")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", f"PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "chaos-w39a-outage")
        .with_bind_ports(port, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


def _messages(count_per_key: int, *, keys: list[str], first_seq: int = 0) -> list[Any]:
    return [
        make_message(b'{"synthetic": true}', key=key.encode(), channel=CHANNEL, headers={SEQ: str(seq)})
        for seq in range(first_seq, first_seq + count_per_key)
        for key in keys
    ]


def _create_topics_and_publish_spec(kafka: str, env: str, *, mutant: bool) -> None:
    topics = [
        NewTopic(raw_topic(env, CHANNEL), PARTITIONS, 1),
        NewTopic(out_topic(env, CHANNEL, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(env, CHANNEL), 1, 1),
        NewTopic(events_topic(env), 1, 1),
        NewTopic(channels_topic(env), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    create_topics_with_retry(kafka, topics)
    document = ordered_document(
        mllp_port=free_port(), http_url="http://127.0.0.1:9/never-dialled", channel=CHANNEL, destination=DESTINATION
    )
    if mutant:
        document["spec"]["pipeline"]["steps"] = [{"name": "archive-in-path", "ref": "w9a_mutant.steps.archive_in_path"}]
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(env), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0


def _read_out(
    kafka: str, env: str, *, wanted: int, timeout_s: float, warm: bool = False
) -> tuple[list[Any], float | None]:
    """Read `out.<dest>` from the beginning until `wanted` records of one class (warm-up keys start with WARM) arrived. Returns the
    records and the monotonic time the last wanted one was read (None on timeout)."""
    out = out_topic(env, CHANNEL, DESTINATION)
    consumer = Consumer(
        {
            "bootstrap.servers": kafka,
            "group.id": f"w9a-verify-{env}",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1,
            "fetch.wait.max.ms": 20,
        }
    )
    consumer.assign([TopicPartition(out, p, 0) for p in range(PARTITIONS)])
    records: list[Any] = []
    measured = 0
    finished: float | None = None
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline and finished is None:
            for record in consumer.consume(num_messages=2000, timeout=0.05):
                if record.error() is not None:
                    continue
                records.append(record)
                if (record.key() or b"").startswith(b"WARM") == warm:
                    measured += 1
                    if measured >= wanted:
                        finished = time.monotonic()
                        break
    finally:
        consumer.close()
    return records, finished


def _run_once(kafka: str, tmp: Path, *, index: int, mode: str, count: int, mutant: bool) -> dict[str, Any]:
    assert mode in {"control", "stopped"}
    tag = f"{'m' if mutant else 'r'}{index}{mode[0]}"
    env = f"w9a{tag}"
    workdir = tmp / tag
    workdir.mkdir()
    _create_topics_and_publish_spec(kafka, env, mutant=mutant)
    stack = ControlPlaneStack(workdir, kafka, run=tag, env=env)
    worker = None
    try:
        stack.start_containers()
        assert stack.clickhouse is not None and stack.postgres is not None
        archiver = stack.start_archiver()
        stack.start_api()
        stack.wait_api_ready()
        worker = spawn_pipeline_worker(
            env=env,
            channel=CHANNEL,
            bootstrap=kafka,
            instance_id=f"w9a-{tag}",
            timeout_s=60,
            extra_env={
                "PYTHONPATH": os.pathsep.join(filter(None, [str(BUNDLE_DIR), os.environ.get("PYTHONPATH")])),
                "TB_W9A_MUTANT_CH_PING": f"http://127.0.0.1:{stack.clickhouse.port}/ping",
            },
        )
        worker_pid = worker.process.pid

        warm = _messages(WARM_PER_KEY, keys=[f"WARM-{i}" for i in range(4)])
        warm_ids = publish_messages(kafka, raw_topic(env, CHANNEL), warm)
        _, warm_done = _read_out(kafka, env, wanted=len(warm), timeout_s=90, warm=True)
        assert warm_done is not None, "the warm-up batch never reached out.<dest>"
        deadline = time.monotonic() + 60
        while stack.clickhouse.archived_rows() == 0 and time.monotonic() < deadline:
            time.sleep(0.5)
        rows_before = stack.clickhouse.archived_rows()
        assert rows_before > 0, "the archiver wrote no row to ClickHouse before the fault: the topology is not wired"
        assert stack.api_ready()

        if mode == "stopped":
            stack.stop_all_services()
            assert_fault_fired(
                stack.api is not None
                and stack.api.process.poll() is not None
                and port_refuses("127.0.0.1", stack.api_port),
                description="the API process is still answering",
            )
            assert_fault_fired(
                port_refuses("127.0.0.1", stack.postgres.port), description="PostgreSQL still accepts connections"
            )
            assert_fault_fired(
                port_refuses("127.0.0.1", stack.clickhouse.port), description="ClickHouse still accepts connections"
            )
        else:
            assert not port_refuses("127.0.0.1", stack.postgres.port) and stack.api_ready()
            assert stack.clickhouse.archived_rows() > 0
        time.sleep(SETTLE_S)
        assert archiver.process.poll() is None, "the archiver must still be running (it is part of the topology)"

        batch = _messages(count // len(KEYS), keys=KEYS)
        ids = warm_ids + [m.require("tb-msg-id") for m in batch]
        t0 = time.monotonic()
        publish_messages(kafka, raw_topic(env, CHANNEL), batch)
        records, finished = _read_out(kafka, env, wanted=len(batch), timeout_s=300)
        assert finished is not None, f"only {len(records)} records reached out.<dest> within 300 s"
        throughput = len(batch) / (finished - t0)

        assert worker.process.pid == worker_pid and worker.process.poll() is None
        halted = [line for line in worker.collect_pending() if line.get("event") == "partition_halted"]
        assert not halted
        # Let the reader see the tail (duplicates after the Nth measured record) before accounting.
        records, _ = _read_out(kafka, env, wanted=len(batch), timeout_s=60)
        delivered_ids = {header_map(r)["tb-msg-id"] for r in records}
        assert_no_acknowledged_loss(set(ids), delivered_ids)
        first: dict[bytes, list[int]] = {}
        partitions: dict[bytes, set[int]] = {}
        for record in records:
            key = record.key()
            seq = int(header_map(record)[SEQ])
            seen = first.setdefault(key, [])
            if seq not in seen:
                seen.append(seq)
            partitions.setdefault(key, set()).add(record.partition())
        assert_each_key_on_one_partition(partitions)
        violations = count_order_violations(first)
        assert violations == 0, f"{violations} per-key order violation(s) while the control plane was {mode}"
        duplicates = len(records) - len(delivered_ids)
        assert duplicates == 0, f"{duplicates} duplicate(s) on out.<dest> while the control plane was {mode}"
        rows_after = stack.clickhouse.archived_rows() if mode == "control" else None
        return {
            "run": tag,
            "mode": mode,
            "mutant": mutant,
            "messages": len(batch),
            "msg_per_s": throughput,
            "seconds": finished - t0,
            "order_violations": violations,
            "lost": len(set(ids) - delivered_ids),
            "duplicates": duplicates,
            "archived_rows_before": rows_before,
            "archived_rows_after": rows_after,
            "fault_fired": mode == "stopped",
        }
    finally:
        if worker is not None:
            worker.terminate_and_wait()
        stack.teardown()


def _emit(scenario: str, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    line = json.dumps(
        {"scenario": scenario, "env": environment(), "summary": summary, "rows": rows}, sort_keys=True, default=str
    )
    print(f"W39A_ROW {line}")
    path = os.environ.get("TB_CHAOS_ROWS_FILE")
    if path:
        with Path(path).open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


def _alternate(kafka: str, tmp: Path, *, runs_per_arm: int, count: int, mutant: bool) -> list[dict[str, Any]]:
    rows = []
    for index in range(runs_per_arm):
        for mode in ("control", "stopped"):
            rows.append(_run_once(kafka, tmp, index=index, mode=mode, count=count, mutant=mutant))
    return rows


def _arm(rows: list[dict[str, Any]], mode: str) -> list[float]:
    return [float(r["msg_per_s"]) for r in rows if r["mode"] == mode]


def test_outage_control_plane_worker_keeps_full_rate(kafka: str, tmp_path: Path) -> None:
    rows = _alternate(kafka, tmp_path, runs_per_arm=RUNS_PER_ARM, count=N_MESSAGES, mutant=False)
    verdict = outage_verdict(_arm(rows, "control"), _arm(rows, "stopped"))
    _emit(
        "w3-9a-outage-control-plane",
        rows,
        {
            "verdict": verdict.describe(),
            "passed": verdict.passed,
            "stopped_over_control_median": round(verdict.stopped_over_control_median, 3),
            # Informational, not the plan's rule: the stopped median is not below the control minimum.
            "stopped_not_below_control_min": verdict.stopped_median >= verdict.control_min,
            "control_msg_s": [round(v) for v in _arm(rows, "control")],
            "stopped_msg_s": [round(v) for v in _arm(rows, "stopped")],
            "order_violations": sum(int(r["order_violations"]) for r in rows),
            "lost": sum(int(r["lost"]) for r in rows),
            "duplicates": sum(int(r["duplicates"]) for r in rows),
            "messages_per_run": N_MESSAGES,
        },
    )
    assert verdict.passed, verdict.describe()


def test_outage_control_plane_mutation_control_is_red(kafka: str, tmp_path: Path) -> None:
    """The same harness and the same verdict, with the archive wired into the message path: it must FAIL."""
    rows = _alternate(kafka, tmp_path, runs_per_arm=2, count=MUTANT_MESSAGES, mutant=True)
    verdict = outage_verdict(_arm(rows, "control"), _arm(rows, "stopped"))
    _emit(
        "w3-9a-mutation-control",
        rows,
        {
            "verdict": verdict.describe(),
            "passed": verdict.passed,
            "control_msg_s": [round(v) for v in _arm(rows, "control")],
            "stopped_msg_s": [round(v) for v in _arm(rows, "stopped")],
        },
    )
    assert not verdict.passed, f"the mutant (archive in the message path) was not caught: {verdict.describe()}"
