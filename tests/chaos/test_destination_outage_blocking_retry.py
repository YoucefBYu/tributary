"""W2-9 chaos scenario 3: an HTTP destination goes down while a `blocking`-retry loader is mid-stream,
and recovers once it comes back. Proves invariant 5's `blocking` guarantee (order preserved, nothing
skipped) and invariant 1/4 (nothing acknowledged onto `out.<dest>` is ever dropped because a destination
was briefly unreachable).

Real loader process, real entry point (`tests.support.processes.spawn_http_loader`, the same
`tributary.loaders.http` `deploy/compose/docker-compose.yml` runs). No worker or receiver: this
scenario is specifically about the loader's retry behaviour, so synthetic records are published straight
to `out.<dest>` (`tests.support.kafka_helpers.publish_messages`) -- see
`test_worker_kill_mid_commit_window.py`'s docstring for why bypassing upstream components is the
established pattern here.

**The outage is real, not simulated in-process**: the first `StubHttpServer` is stopped (its socket
closed), so every in-flight retry gets a genuine `ECONNREFUSED` from the OS, then a second instance binds
the *same* port once the scenario decides the destination has "recovered"
(`tests.support.http_stub.StubHttpServer(port=...)`). The retry policy's `maxAttempts` is raised well
past what the outage window can exhaust (100, against a ~250 ms fixed backoff) specifically so the
partition never halts -- `onExhaust: halt-partition` staying dormant for the whole test is itself
asserted (no `partition_halted` log line, checked against the loader's full output via
`ManagedProcess.collect_pending`), because a scenario that silently halted and then quietly "recovered"
via a code path this test never exercised would prove nothing about `blocking` retry.

Because the HTTP loader does not forward Kafka headers to the destination (`HttpLoader._headers` sets
only `Content-Type`, an optional idempotency header and auth -- never `tb-msg-id`), a message's identity
here is carried in its own payload bytes (`key`, `seq`), which is what every assertion below reads back
out of the stub's captured request bodies.

**"Sustained", proven per partition, not just counted across the batch**: `_wait_for_sustained_retry`
waits for one SINGLE partition's `partition_paused` `attempt` to reach 2, not merely for two
`partition_paused` lines anywhere in the log -- the batch spans multiple partitions, and two different
partitions each failing their first attempt within milliseconds of each other would satisfy a bare
event count without any one partition actually surviving a real second failed attempt under blocking
retry (code review, chaos-suite gate, Medium).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic
from tributary.sdk.testing import make_message

from tests.support.channel_spec import ordered_document
from tests.support.chaos_assertions import (
    assert_fault_fired,
    assert_no_acknowledged_loss,
    assert_per_key_order_preserved,
    duplicate_rate,
)
from tests.support.http_stub import CapturedRequest, StubHttpServer
from tests.support.kafka_helpers import await_broker, free_port, publish_messages
from tests.support.processes import ManagedProcess, spawn_http_loader

pytestmark = [pytest.mark.integration, pytest.mark.chaos]

ENV = "dev"
CHANNEL = "adt-passthrough-ordered-outage"
DESTINATION = "http-stub"
PARTITIONS = 3
KEYS = [f"SYNTH-OUT-{index}".encode() for index in range(4)]
PER_KEY_PER_BATCH = 3

#: Well past what the outage window in this test can exhaust (a fixed ~125-250ms backoff): the point is
#: to observe sustained blocking retry and recovery, never `halt-partition`.
RETRY_OVERRIDE = {
    "policy": "blocking",
    "backoff": "fixed",
    "initialDelayMs": 250,
    "maxDelayMs": 250,
    "maxAttempts": 100,
    "onExhaust": "halt-partition",
}


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
        .with_env("CLUSTER_ID", "chaos-destination-outage")
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
def destination_port() -> int:
    """Chosen once: the channel spec's URL is fixed at publish time, so the stub that "recovers" after
    the outage must rebind this exact port."""
    return free_port()


@pytest.fixture(scope="module")
def deployed_spec(topics: str, destination_port: int) -> dict[str, Any]:
    document = ordered_document(
        mllp_port=free_port(),
        http_url=f"http://127.0.0.1:{destination_port}/ingest",
        channel=CHANNEL,
        destination=DESTINATION,
        version=1,
    )
    document["spec"]["destinations"][0]["retry"] = dict(RETRY_OVERRIDE)
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def _publish_batch(bootstrap: str, *, start_seq: int, count_per_key: int) -> list[tuple[bytes, int]]:
    """Publishes straight to `out.<dest>`, and returns the `(key, seq)` identity of each record --
    the loader does not forward `tb-msg-id` to the destination (module docstring), so this pair is
    the only identity the stub's captured bodies can be checked against."""
    identities: list[tuple[bytes, int]] = []
    messages = []
    for sequence in range(start_seq, start_seq + count_per_key):
        for key in KEYS:
            payload = json.dumps({"key": key.decode(), "seq": sequence}).encode()
            messages.append(
                make_message(
                    payload, key=key, channel=CHANNEL, destination=DESTINATION, content_type="application/json"
                )
            )
            identities.append((key, sequence))
    publish_messages(bootstrap, out_topic(ENV, CHANNEL, DESTINATION), messages)
    return identities


def _wait_for_request_count(stub: StubHttpServer, *, count: int, timeout_s: float = 30.0) -> list[CapturedRequest]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        requests = stub.requests
        if len(requests) >= count:
            return requests
        time.sleep(0.2)
    raise AssertionError(
        f"stub on port {stub.port} only received {len(stub.requests)}/{count} requests in {timeout_s}s"
    )


def _identity(request: CapturedRequest) -> tuple[bytes, int]:
    body = json.loads(request.body)
    return body["key"].encode(), int(body["seq"])


def _wait_for_sustained_retry(loader: ManagedProcess, *, timeout_s: float = 30.0) -> dict[str, Any]:
    """Waits for a SINGLE partition's `partition_paused` `attempt` to reach 2, not merely for two
    `partition_paused` lines total.

    `partition_paused` fires once per failed attempt *per partition* (`ConsumerLoop._begin_retry`), and
    this scenario's batch spans multiple partitions -- two different partitions each failing their
    first attempt within milliseconds of each other would satisfy "two `partition_paused` events" while
    proving nothing about a *sustained* outage on any one of them (code review, chaos-suite gate,
    Medium). Gating on one partition reaching `attempt >= 2` is what actually shows the same partition
    surviving a real second failed attempt under blocking retry.
    """
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        for record in loader.collect_pending():
            if record.get("event") == "partition_paused" and int(record.get("attempt", 0)) >= 2:
                return record
        time.sleep(0.2)
    raise AssertionError(
        f"no single partition reached a second `partition_paused` attempt within {timeout_s}s: "
        f"{[line for line in loader.lines if line.get('event') == 'partition_paused']}"
    )


def test_destination_outage_under_blocking_retry_loses_nothing_and_recovers(
    topics: str, deployed_spec: dict[str, Any], destination_port: int
) -> None:
    stub_before = StubHttpServer(port=destination_port)
    stub_before.start()
    loader: ManagedProcess | None = None
    stub_after: StubHttpServer | None = None
    try:
        batch_a = _publish_batch(topics, start_seq=0, count_per_key=PER_KEY_PER_BATCH)
        loader = spawn_http_loader(channel=CHANNEL, destination=DESTINATION, env=ENV, bootstrap=topics)
        _wait_for_request_count(stub_before, count=len(batch_a))

        # The outage: the destination that was serving batch_a is gone.
        stub_before.stop()

        batch_b = _publish_batch(topics, start_seq=PER_KEY_PER_BATCH, count_per_key=PER_KEY_PER_BATCH)

        # The fault, confirmed rather than assumed (PHASE-0-DECISION section 4): a single partition
        # must actually survive a second failed attempt under blocking retry, proving this is a
        # sustained outage, not one lucky blip on one partition while another merely failed once.
        _wait_for_sustained_retry(loader, timeout_s=30.0)

        # Recovery: a fresh stub takes the same port the channel spec already points at.
        stub_after = StubHttpServer(port=destination_port)
        stub_after.start()

        delivered_after = _wait_for_request_count(stub_after, count=len(batch_b), timeout_s=60.0)
        delivered_before = stub_before.requests
        # Drained before termination: `wait_for_event` only appended lines up to the two
        # `partition_paused` matches above, so anything printed since (including a `partition_halted`
        # this scenario must not see) would otherwise never reach `.lines` at all.
        loader.collect_pending()
    finally:
        if loader is not None:
            loader.terminate_and_wait()
        # Unconditional, not only on the success path: if `_wait_for_request_count(stub_before, ...)`
        # above ever times out, execution never reaches the explicit `stub_before.stop()` call, and the
        # server thread plus its bound port would otherwise leak for the rest of the session (code
        # review, chaos-suite gate, High). `StubHttpServer.stop()` is idempotent, so this is safe even
        # when the explicit mid-test call already ran.
        stub_before.stop()
        if stub_after is not None:
            stub_after.stop()

    halted = [line for line in loader.lines if line.get("event") == "partition_halted"]
    assert not halted, f"the retry budget was exhausted and the partition halted: {halted}"

    published = batch_a + batch_b
    # `stub_before` finished serving batch_a and was stopped before batch_b was even published, and
    # `stub_after` only exists afterwards -- concatenating in this order is genuine chronological order,
    # not an assumption (`tests.support.http_stub.CapturedRequest.received_at` exists for a stronger
    # check if this ever needs to span more than two stub instances).
    combined = list(delivered_before) + list(delivered_after)
    delivered_identities = [_identity(request) for request in combined]

    assert_no_acknowledged_loss(
        {f"{key!r}:{seq}" for key, seq in published},
        {f"{key!r}:{seq}" for key, seq in delivered_identities},
    )

    duplicates = len(delivered_identities) - len(set(delivered_identities))
    rate = duplicate_rate(published_count=len(published), delivered_count=len(delivered_identities))
    print(f"destination-outage duplicate rate: {rate:.1%} ({duplicates} of {len(published)} published)")

    assert_fault_fired(
        len(delivered_before) == len(batch_a),
        description="the pre-outage stub did not receive exactly batch_a -- the outage boundary was not clean",
    )

    order: dict[bytes, list[int]] = {}
    for key, seq in delivered_identities:
        seen = order.setdefault(key, [])
        if seq not in seen:
            seen.append(seq)
    assert_per_key_order_preserved(order)
