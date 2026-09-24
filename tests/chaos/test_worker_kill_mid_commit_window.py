"""W2-9 chaos scenario 1: kill the pipeline worker mid-commit-window and prove invariants 1, 4 and 5
hold across the kill -- no acknowledged message lost, per-key order preserved, and the run reports its
duplicate rate.

Real subprocesses, real entry point: the worker is spawned via
`tests.support.processes.spawn_pipeline_worker`, the same `python -m`-equivalent path
`deploy/compose/docker-compose.yml` runs, with no `steps_override` -- it resolves
`uses: tributary.core.passthrough` from the published channel spec exactly as a deployed worker would
(see that module's docstring for why an overridden step would hide the exact bug this suite once found).

**Mid-commit-window, not just "before the first commit ever"**: this scenario lets the worker complete
at least one full `commit_every` cycle first, so there is a genuine committed baseline, and only then
kills it partway through the *second* window. `packages/tributary-runtime/tests/test_integration_kafka.py`
already covers the "killed before any commit" edge case; this one is deliberately the steady-state case,
proven by asserting `0 < duplicates < published` -- some records really were committed and were not
replayed, and some records really were replayed. Either bound failing means the kill did not land where
this scenario claims it did (PHASE-0-DECISION section 4: assert the fault fired, do not assume it).

Channel is `ordering: per-key` (`tests.support.channel_spec.ordered_document`). The MLLP receiver cannot
originate a per-key channel yet (no `keyExpression` evaluator, W2-4), so this scenario publishes
synthetic records straight to `raw` with its own keys via `tests.support.kafka_helpers.publish_messages`
-- the worker and the ordering checks in `check_partition_counts`/`validate_for_produce` do not care
where the key came from.

Single-node (lite, RF=1) Kafka: this scenario's fault is the worker process, not the broker: the
3-broker durability floor belongs to `test_kafka_broker_kill.py`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.sdk import headers as h
from tributary.sdk.testing import make_message

from tests.support.channel_spec import ordered_document
from tests.support.chaos_assertions import (
    assert_fault_fired,
    assert_no_acknowledged_loss,
    assert_per_key_order_preserved,
    duplicate_rate,
)
from tests.support.kafka_helpers import (
    await_broker,
    drain_topic,
    first_delivery_order,
    free_port,
    header_map,
    publish_messages,
    wait_for_at_least,
)
from tests.support.processes import ManagedProcess, spawn_pipeline_worker

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough-ordered-workerkill"
DESTINATION = "http-stub"
PARTITIONS = 3
KEYS = [f"SYNTH-WK-{index}".encode() for index in range(5)]
PER_KEY = 24  # 5 keys x 24 = 120 records, well past one commit_every(50) cycle
SEQ_HEADER = "tb-usr-seq"


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    """Single-node Apache Kafka 4.x in KRaft mode, the lite profile -- matches
    `tests/e2e/test_mllp_to_http_e2e.py::kafka`."""
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093")
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
        .with_env("CLUSTER_ID", "chaos-worker-kill")
        .with_bind_ports(9092, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def topics(kafka: str) -> str:
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
        NewTopic(out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    for _topic, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=30)
        except Exception as exc:  # pragma: no cover - already exists on a re-run
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    return kafka


@pytest.fixture(scope="module")
def channel_spec_document() -> dict[str, Any]:
    return ordered_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/ingest",  # never dialled: no loader runs in this scenario
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )


@pytest.fixture(scope="module")
def deployed_spec(topics: str, channel_spec_document: dict[str, Any]) -> dict[str, Any]:
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(channel_spec_document).encode())
    assert producer.flush(30) == 0
    return channel_spec_document


def _publish_batch(bootstrap: str) -> list[str]:
    messages = [
        make_message(
            b'{"synthetic": true}',
            key=key,
            channel=CHANNEL,
            headers={SEQ_HEADER: str(sequence)},
        )
        for sequence in range(PER_KEY)
        for key in KEYS
    ]
    return publish_messages(bootstrap, raw_topic(ENV, CHANNEL), messages)


def test_worker_kill_mid_commit_window_loses_nothing_and_keeps_order(
    topics: str, deployed_spec: dict[str, Any]
) -> None:
    defaults = load_topic_defaults()
    commit_every = defaults.consumer.commit_every
    out = out_topic(ENV, CHANNEL, DESTINATION)

    published = _publish_batch(topics)
    total = len(published)
    assert total > commit_every, "the batch must span more than one commit window, or this proves nothing"

    # Past the first commit boundary (a genuine committed baseline exists) but short of the whole batch
    # (the worker must still be mid-flight when it dies).
    kill_at = commit_every + max(1, (total - commit_every) // 2)
    assert commit_every < kill_at < total, "kill_at must land strictly inside the second commit window"

    worker = spawn_pipeline_worker(env=ENV, channel=CHANNEL, bootstrap=topics, instance_id="pipeline-kill-1")
    try:
        wait_for_at_least(topics, out, partitions=PARTITIONS, count=kill_at, timeout_s=90.0)
    finally:
        exit_code = worker.kill_and_wait(timeout_s=30.0)
    assert_fault_fired(exit_code != 0, description=f"worker exited {exit_code} -- a SIGKILL never exits 0")

    # Static membership (group.instance.id = the worker's instance_id): the restart fences the dead
    # member and resumes rather than waiting out session.timeout.ms (S5's measured recovery path).
    restarted: ManagedProcess = spawn_pipeline_worker(
        env=ENV, channel=CHANNEL, bootstrap=topics, instance_id="pipeline-kill-1", timeout_s=60.0
    )
    try:
        wait_for_at_least(topics, out, partitions=PARTITIONS, count=total, timeout_s=120.0)
    finally:
        restarted.terminate_and_wait()

    delivered = drain_topic(topics, out, partitions=PARTITIONS, expected=total, timeout_s=30.0)
    delivered_ids = [header_map(record)[h.TB_MSG_ID] for record in delivered]

    assert_no_acknowledged_loss(set(published), set(delivered_ids))

    duplicates = len(delivered_ids) - len(set(delivered_ids))
    rate = duplicate_rate(published_count=total, delivered_count=len(delivered_ids))
    print(f"worker-kill duplicate rate: {rate:.1%} ({duplicates} of {total} published, killed at ~{kill_at})")

    assert_fault_fired(
        duplicates > 0, description="no duplicates on restart -- the kill did not interrupt an uncommitted window"
    )
    assert_fault_fired(
        duplicates < total,
        description="every record was replayed -- nothing was actually committed before the kill, so this is "
        "the 'before any commit' case, not mid-window",
    )

    order = first_delivery_order(delivered, sequence_header=SEQ_HEADER)
    assert set(order) == set(KEYS), "every key must appear in what was delivered"
    assert_per_key_order_preserved(order)
