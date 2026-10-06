"""W2-9 chaos scenario 2: a third worker joins a channel's consumer group after the first two have
reached a **stable** cooperative-sticky assignment, and the rebalance moves live partitions to it
without losing a message, duplicating one, or reordering a key.

Follows S5's exact fix for its own first-round failure (PHASE-0-DECISION section 4): *"A rebalance
scenario where the joiner is assigned nothing measures nothing."* This scenario therefore:

1. Waits for a **settled, cumulative** split before the joiner starts, not just one `partitions_assigned`
   log line. `ConsumerLoop`'s cooperative-sticky callbacks are *incremental*: `on_assign`'s `count` is
   only the partitions newly handed to that member in one generation, and cooperative rebalancing
   routinely takes two generations -- the new member can be handed **zero** partitions in the first
   generation (the current owner has to revoke them via its own `poll()` before the broker can grant them
   in the second). An earlier version of this test read the first `partitions_assigned` event per process
   as if it were the final assignment and failed immediately on exactly that 0-in-generation-one case.
   `tests.support.rebalance_helpers.wait_until_settled` instead accumulates every `partitions_assigned`
   (add) and `partitions_revoked` (subtract) event per process (`ManagedProcess.collect_pending`) until
   the group's combined total reaches `PARTITIONS` and stays there for several consecutive polls -- see
   that module's docstring for the full incident and the grace-period reasoning, shared with
   `test_cooperative_rebalance_commit_refused_mid_join.py`.
2. Asserts the joiner's own **net, settled** partition count is greater than zero -- a joiner assigned
   nothing would make every downstream assertion vacuously true.
3. Publishes traffic only *between* settled rebalances, never during one, so a duplicate in the drained
   output can only mean the handover was not the zero-duplicate operation `ConsumerLoop._on_revoke`'s
   docstring claims it is -- not a race this test manufactured.

Real subprocesses (`tests.support.processes.spawn_pipeline_worker`), the shared loss/order checks from
`tests.support.chaos_assertions` (mutation-controlled in `test_chaos_assertion_mutation_controls.py`),
`ordering: per-key` published straight to `raw` -- see `test_worker_kill_mid_commit_window.py`'s
docstring for why the MLLP receiver is not used here.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.sdk import headers as h
from tributary.sdk.testing import make_message

from tests.support.channel_spec import ordered_document
from tests.support.chaos_assertions import (
    assert_fault_fired,
    assert_no_acknowledged_loss,
    assert_per_key_order_preserved,
    count_order_violations,
    duplicate_rate,
    record_chaos_row,
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
from tests.support.rebalance_helpers import wait_until_settled

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough-ordered-rebalance"
DESTINATION = "http-stub"
PARTITIONS = 6  # S5 used 6 partitions/3 members for the same reason: enough to prove a real split
KEYS = [f"SYNTH-RB-{index}".encode() for index in range(12)]
PER_KEY_PER_BATCH = 10
SEQ_HEADER = "tb-usr-seq"


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
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
        .with_env("CLUSTER_ID", "chaos-rebalance")
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
def deployed_spec(topics: str) -> dict[str, Any]:
    document = ordered_document(
        mllp_port=free_port(),
        http_url="http://127.0.0.1:1/ingest",  # never dialled: no loader runs in this scenario
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def _publish_batch(bootstrap: str, *, start_seq: int, count_per_key: int) -> list[str]:
    messages = [
        make_message(
            b'{"synthetic": true}',
            key=key,
            channel=CHANNEL,
            headers={SEQ_HEADER: str(sequence)},
        )
        for sequence in range(start_seq, start_seq + count_per_key)
        for key in KEYS
    ]
    return publish_messages(bootstrap, raw_topic(ENV, CHANNEL), messages)


def test_joiner_against_a_stable_assignment_moves_partitions_without_loss_or_duplication(
    topics: str, deployed_spec: dict[str, Any]
) -> None:
    out = out_topic(ENV, CHANNEL, DESTINATION)
    processes: dict[str, ManagedProcess] = {}
    try:
        founder1 = spawn_pipeline_worker(env=ENV, channel=CHANNEL, bootstrap=topics, instance_id="pipeline-founder-1")
        processes["founder1"] = founder1
        solo = wait_until_settled(processes, expected_total=PARTITIONS)
        assert solo["founder1"] == PARTITIONS, "the lone founder must own every partition at first"

        founder2 = spawn_pipeline_worker(env=ENV, channel=CHANNEL, bootstrap=topics, instance_id="pipeline-founder-2")
        processes["founder2"] = founder2
        pair = wait_until_settled(processes, expected_total=PARTITIONS)
        assert_fault_fired(
            pair["founder2"] > 0,
            description="the second founder settled with zero partitions -- no rebalance actually split the group",
        )
        assert pair["founder1"] + pair["founder2"] == PARTITIONS, (
            "every partition must be owned by exactly one of the two founders once settled"
        )

        batch_a = _publish_batch(topics, start_seq=0, count_per_key=PER_KEY_PER_BATCH)
        wait_for_at_least(topics, out, partitions=PARTITIONS, count=len(batch_a), timeout_s=90.0)

        joiner = spawn_pipeline_worker(
            env=ENV, channel=CHANNEL, bootstrap=topics, instance_id="pipeline-joiner", timeout_s=60.0
        )
        processes["joiner"] = joiner
        trio = wait_until_settled(processes, expected_total=PARTITIONS)
        assert_fault_fired(
            trio["joiner"] > 0,
            description="the joiner settled with zero partitions -- this run proves nothing about a live rebalance",
        )
        assert sum(trio.values()) == PARTITIONS, "every partition must be owned by exactly one member once settled"

        # Only now, once the join has fully settled, is more traffic sent -- so any duplicate in the
        # final drain reflects the handover itself, not a race this test created.
        batch_b = _publish_batch(topics, start_seq=PER_KEY_PER_BATCH, count_per_key=PER_KEY_PER_BATCH)

        published = batch_a + batch_b
        wait_for_at_least(topics, out, partitions=PARTITIONS, count=len(published), timeout_s=120.0)
    finally:
        for process in reversed(list(processes.values())):
            process.terminate_and_wait()

    delivered = drain_topic(topics, out, partitions=PARTITIONS, expected=len(published), timeout_s=30.0)
    delivered_ids = [header_map(record)[h.TB_MSG_ID] for record in delivered]

    assert_no_acknowledged_loss(set(published), set(delivered_ids))

    duplicates = len(delivered_ids) - len(set(delivered_ids))
    rate = duplicate_rate(published_count=len(published), delivered_count=len(delivered_ids))
    print(f"rebalance duplicate rate: {rate:.1%} ({duplicates} of {len(published)} published)")
    assert duplicates == 0, (
        "a planned cooperative handover commits before revoking (ConsumerLoop._on_revoke), so traffic "
        "sent only between settled rebalances must produce zero duplicates -- any here is a regression"
    )

    order = first_delivery_order(delivered, sequence_header=SEQ_HEADER)
    record_chaos_row(
        scenario="cooperative-rebalance-stable-join",
        sent=len(published),
        delivered_total=len(delivered_ids),
        delivered_distinct=len(set(delivered_ids)),
        order_violations=count_order_violations(order),
        fault_fired=trio["joiner"] > 0,
    )
    assert set(order) == set(KEYS), "every key must appear in what was delivered"
    assert_per_key_order_preserved(order)
