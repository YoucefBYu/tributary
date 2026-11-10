"""W1-4 verify line, against a real broker: raw -> out with events, and order preserved across a kill.

Marked `integration`: it needs Docker. `uv run pytest -m integration packages/tributary-runtime`.

Two claims are tested, each with a control:

1. **A message flows `raw` -> steps -> `out.<dest>` and the lifecycle events are emitted**, with the
   `out` record validated against `contracts/envelope/v0` and every event against
   `contracts/events/v0` — the real emitters, not fixtures (gate requirement 10).
2. **Killing the worker mid-stream and restarting preserves per-key order and loses nothing.** The kill
   is only meaningful if it interrupts an uncommitted window, so the test asserts that the restart
   actually replayed something; a run that replays nothing has not tested the thing it is named after
   (PHASE-0-DECISION section 4).

Ordering is checked the way S5 checked it: the first delivery of each sequence number must be
increasing per key. A redelivered sequence number is a duplicate, not a reordering — that is what
at-least-once means, and why invariant 4 requires loader idempotency on `tb-msg-id`.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from jsonschema import Draft202012Validator
from testcontainers.core.container import DockerContainer
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.runtime.worker import PipelineWorker, WorkerSettings
from tributary.sdk import Message, step
from tributary.sdk import headers as h
from tributary.sdk.ids import new_traceparent, uuid7
from tributary.sdk.projection import record_projection
from tributary.sdk.testing import make_message

pytestmark = pytest.mark.integration

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
ENV = "dev"
CHANNEL = "adt-passthrough"
DESTINATION = "http-stub"
PARTITIONS = 3
KEYS = [f"SYNTH-KEY-{index}" for index in range(4)]
PER_KEY = 8

#: The ordering affordance travels in the contract's channel-defined space, which is propagated and
#: never interpreted by the engine (envelope contract 2.1). S5 used `tb-x-seq`; that space is
#: spike-local and the runtime now strips it on produce, so a test that relied on it would be testing
#: a header the engine is required to drop.
SEQ_HEADER = "tb-usr-seq"
EXPERIMENTAL_PROBE = "tb-x-seq"


@step
def passthrough(message: Message) -> None:
    """The wave-1 walking-skeleton step: it changes nothing, so anything that changes is the runtime."""
    return None


def _free_port() -> int:
    """A port the broker can advertise.

    KRaft needs `advertised.listeners` to name the address clients will actually use, and that has to
    be known *before* the container starts — hence a port chosen here and published explicitly rather
    than a Docker-assigned one read back afterwards.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    """A single-node Apache Kafka 4.x in KRaft mode, managed by testcontainers.

    `DockerContainer` with `apache/kafka:4.0.0` rather than testcontainers' `KafkaContainer`, whose
    start script is written for `confluentinc/cp-kafka` and its `/etc/confluent/docker/run` entry
    point. CLAUDE.md's stack is Apache Kafka 4.x (KRaft) and Phase 0 measured against 4.0.0, so the
    broker under test is the broker the design was measured on.

    RF=1 and `min.insync.replicas=1` here are the lite profile: a single-node test cluster cannot
    satisfy the production durability floor, and pretending otherwise would make this test evidence
    for something it does not check.
    """
    port = _free_port()
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
        .with_env("CLUSTER_ID", "MkU3OEVBNTcwNTJENDM2Qk")
        .with_bind_ports(9092, port)
        # A unique name: several agents and CI jobs share one Docker daemon.
        .with_name(f"tb-runtime-it-kafka-{uuid.uuid4().hex[:12]}")
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        _await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


def _await_broker(bootstrap: str, *, timeout_s: float = 90.0) -> None:
    """Wait for metadata rather than for a log line: a started process is not a reachable broker."""
    deadline = time.monotonic() + timeout_s
    last: Exception | None = None
    while time.monotonic() < deadline:
        try:
            AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=5)
            return
        except Exception as exc:  # any failure here means the broker is not ready yet
            last = exc
            time.sleep(1.0)
    raise RuntimeError(f"Kafka did not become reachable: {type(last).__name__ if last else 'timeout'}")


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
    """Publish the walking-skeleton channel spec to `_tb.<env>.channels`, keyed by name."""
    document = json.loads(
        (CONTRACTS / "channel-spec" / "v0" / "examples" / "valid" / "walking-skeleton-loopback.json").read_text(
            encoding="utf-8"
        )
    )
    document["metadata"] = {**document["metadata"], "version": 1}
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return document


def publish(bootstrap: str, count_per_key: int, *, start: int = 0) -> list[str]:
    """Produce `count_per_key` synthetic messages per key, each carrying its sequence number."""
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    ids: list[str] = []
    for sequence in range(start, start + count_per_key):
        for key in KEYS:
            message = make_message(
                b'{"synthetic": true}',
                key=key.encode(),
                channel=CHANNEL,
                msg_id=uuid7(),
                traceparent=new_traceparent(),
                # The probe rides along on every record: the engine must tolerate it inbound and
                # must not hand it on (envelope contract 2.1 against 1.2).
                headers={SEQ_HEADER: str(sequence), EXPERIMENTAL_PROBE: "spike-local"},
            )
            ids.append(message.msg_id)
            producer.produce(
                raw_topic(ENV, CHANNEL),
                key=message.key,
                value=message.payload,
                headers=[(name, value.encode()) for name, value in message.headers.items()],
            )
    assert producer.flush(30) == 0
    return ids


def drain(bootstrap: str, topic: str, *, expected: int, timeout_s: float = 30.0) -> list[Any]:
    """Read a topic from the beginning, stopping at partition EOF.

    EOF, not the high watermark: the watermark counts transaction control records that are never handed
    to the application, so a watermark loop can fail to terminate.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"verify-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    consumer.subscribe([topic])
    collected: list[Any] = []
    eofs: set[int] = set()
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            raw = consumer.poll(1.0)
            if raw is None:
                if collected and len(collected) >= expected:
                    break
                continue
            if raw.error() is not None:
                eofs.add(raw.partition())
                if len(eofs) >= PARTITIONS and len(collected) >= expected:
                    break
                continue
            collected.append(raw)
    finally:
        consumer.close()
    return collected


def header_map(raw: Any) -> dict[str, str]:
    return {name: value.decode("utf-8") for name, value in (raw.headers() or [])}


def worker(bootstrap: str, instance_id: str) -> PipelineWorker:
    return PipelineWorker(
        settings=WorkerSettings(
            env=ENV,
            channel=CHANNEL,
            kafka=KafkaSettings(bootstrap_servers=bootstrap),
            instance_id=instance_id,
            poll_timeout_s=0.1,
        ),
        steps_override=(passthrough,),
        defaults=load_topic_defaults(),
    )


def first_delivery_order(records: list[Any]) -> dict[bytes, list[int]]:
    """The sequence numbers per key, keeping only the FIRST delivery of each (S5's ordering check)."""
    order: dict[bytes, list[int]] = {}
    for raw in records:
        key = raw.key()
        sequence = int(header_map(raw)[SEQ_HEADER])
        seen = order.setdefault(key, [])
        if sequence not in seen:
            seen.append(sequence)
    return order


def test_a_message_flows_from_raw_to_out_with_events(deployed_spec: dict[str, Any], topics: str) -> None:
    expected_ids = set(publish(topics, PER_KEY))
    running = worker(topics, "pipeline-1")
    running.build()
    running.run(max_iterations=400)

    delivered = drain(topics, out_topic(ENV, CHANNEL, DESTINATION), expected=len(expected_ids))
    delivered_ids = {header_map(raw)[h.TB_MSG_ID] for raw in delivered}
    assert expected_ids <= delivered_ids, "at-least-once: every published message reached out.<dest>"

    envelope = Draft202012Validator(
        json.loads((CONTRACTS / "envelope" / "v0" / "envelope.schema.json").read_text(encoding="utf-8"))
    )
    for raw in delivered[:10]:
        headers = header_map(raw)
        assert headers[h.TB_DESTINATION] == DESTINATION
        assert headers[h.TB_ATTEMPT] == "0"
        message = Message(headers=headers, payload=raw.value(), key=raw.key())
        assert list(envelope.iter_errors(record_projection(message, topic_class="out"))) == []

    events = Draft202012Validator(
        json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
    )
    emitted = [json.loads(raw.value()) for raw in drain(topics, events_topic(ENV), expected=len(expected_ids))]
    stages = {event["stage"] for event in emitted}
    assert {"transformed", "routed"} <= stages
    for event in emitted[:20]:
        assert list(events.iter_errors(event)) == []


def received_events(bootstrap: str, ids: set[str]) -> dict[str, list[dict[str, Any]]]:
    """Every `received` event for `ids`, in events-topic order (one partition here), by message id."""
    by_id: dict[str, list[dict[str, Any]]] = {}
    for raw in drain(bootstrap, events_topic(ENV), expected=len(ids)):
        event = json.loads(raw.value())
        if event.get("stage") == "received" and event.get("message_id") in ids:
            by_id.setdefault(event["message_id"], []).append(event)
    return by_id


def test_every_raw_record_gets_a_received_event_with_its_coordinates(
    deployed_spec: dict[str, Any], topics: str
) -> None:
    """Events contract 2.1 (set 1.8): the worker emits `received` then `queued` for each consumed `raw` record.

    The coordinates are checked against the record as it sits on `raw`, so the event points at the exact
    record a reprocess would read. `received.ts` is that record's `tb-received-at` at millisecond precision.
    `received`, `queued`, `transformed` in that order for the same message: one key, one events partition,
    both arrival events emitted before any step runs.
    """
    published = set(publish(topics, 2, start=50))
    running = worker(topics, "pipeline-received")
    running.build()
    running.run(max_iterations=300)

    coordinates = {
        header_map(raw)[h.TB_MSG_ID]: (
            raw.topic(),
            raw.partition(),
            raw.offset(),
            len(raw.value() or b""),
            h.format_timestamp(h.parse_timestamp(h.TB_RECEIVED_AT, header_map(raw)[h.TB_RECEIVED_AT])),
        )
        for raw in drain(topics, raw_topic(ENV, CHANNEL), expected=len(published))
        if header_map(raw).get(h.TB_MSG_ID) in published
    }
    assert set(coordinates) == published, "every published record is really on raw"

    schema = Draft202012Validator(
        json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
    )
    emitted = [json.loads(raw.value()) for raw in drain(topics, events_topic(ENV), expected=3 * len(published))]
    by_id = received_events(topics, published)
    assert set(by_id) == published, "one received event per consumed raw record"
    for message_id, copies in by_id.items():
        topic, partition, offset, size, received_at = coordinates[message_id]
        for event in copies:
            assert list(schema.iter_errors(event)) == []
            assert (event["kind"], event["status"], event["channel"]) == ("message", "ok", CHANNEL)
            assert event["channel_version"] == deployed_spec["metadata"]["version"]
            assert (event["topic"], event["partition"], event["offset"]) == (topic, partition, offset)
            assert event["attempt"] == 0 and event["size_bytes"] == size
            assert "attrs" not in event and "destination" not in event
            assert event["ts"] == received_at, "received.ts is the receiver's commit time"
        timeline = [event for event in emitted if event.get("message_id") == message_id]
        stages = [event["stage"] for event in timeline]
        assert stages.index("received") < stages.index("queued") < stages.index("transformed")
        queued = timeline[stages.index("queued")]
        assert list(schema.iter_errors(queued)) == []
        assert (queued["topic"], queued["partition"], queued["offset"]) == (topic, partition, offset)


def test_an_experimental_header_is_tolerated_inbound_and_stripped_on_produce(
    deployed_spec: dict[str, Any], topics: str
) -> None:
    """Envelope contract 2.1 against 1.2, end to end.

    A `tb-x-*` header on `raw` must not make the record undeliverable — rejecting it would let any
    producer poison a channel with a header the contract calls "never load-bearing" — and it must not
    survive the hop either, because it must never reach a released package. Tolerated inbound,
    stripped on produce. `tb-usr-*`, the channel-defined space, is propagated.
    """
    inbound = drain(topics, raw_topic(ENV, CHANNEL), expected=1)
    assert any(EXPERIMENTAL_PROBE in header_map(record) for record in inbound), (
        "the probe must really be on raw, or this test asserts nothing"
    )

    delivered = drain(topics, out_topic(ENV, CHANNEL, DESTINATION), expected=1)
    assert delivered, "the record was delivered despite carrying an experimental header"
    assert all(EXPERIMENTAL_PROBE not in header_map(record) for record in delivered)
    assert all(SEQ_HEADER in header_map(record) for record in delivered)


def test_an_undecodable_record_is_quarantined_to_the_dlq_with_a_record_event(
    deployed_spec: dict[str, Any], topics: str
) -> None:
    """The quarantine path end to end, against the contracts (envelope 5.2, events `kind: record`).

    The record carries a non-UTF-8 header value, so the worker cannot identify it as a Tributary
    message at all. What must happen: a **new** wrapper record on the DLQ whose payload holds the
    original bytes, a freshly minted UUIDv7 as its id, and a `kind: record` / `stage: quarantined`
    event. What must not happen: the partition stalling, or any of those bytes reaching a log line.
    """
    marker = b"PID|1|SYNTH-QUARANTINE-MARKER^JANE"
    producer = Producer({"bootstrap.servers": topics, "acks": "all", "enable.idempotence": True})
    producer.produce(
        raw_topic(ENV, CHANNEL),
        key=b"SYNTH-KEY-0",
        value=marker,
        headers=[("tb-channel", bytes([0xFF, 0xFE])), ("tb-msg-id", uuid7().encode())],
    )
    assert producer.flush(30) == 0

    running = worker(topics, "pipeline-quarantine")
    running.build()
    running.run(max_iterations=200)

    envelope = Draft202012Validator(
        json.loads((CONTRACTS / "envelope" / "v0" / "envelope.schema.json").read_text(encoding="utf-8"))
    )
    dead = drain(topics, dlq_topic(ENV, CHANNEL), expected=1)
    wrappers = [record for record in dead if header_map(record).get(h.TB_ERROR_CODE) == "RECORD_UNDECODABLE"]
    assert wrappers, "the undecodable record was quarantined to the DLQ"

    wrapper = wrappers[-1]
    headers = header_map(wrapper)
    assert headers[h.TB_CONTENT_TYPE] == "application/octet-stream"
    assert headers[h.TB_CHANNEL] == CHANNEL, "the channel comes from the subscription, not the record"
    assert wrapper.key() == headers[h.TB_MSG_ID].encode(), "keyed by the minted tb-msg-id"
    assert headers[h.TB_STAGE] == "quarantined", "fixed by contract, not the stage that read it"
    blob = wrapper.value()
    assert blob is not None and blob.startswith(b"TBQ1"), "framed per envelope 5.2.1"
    assert marker in blob, "the original payload bytes are preserved"
    assert b"SYNTH-KEY-0" in blob, "and so is the original key, in the blob and nowhere else"
    projected = Message(headers=headers, payload=wrapper.value(), key=wrapper.key())
    assert list(envelope.iter_errors(record_projection(projected, topic_class="dlq"))) == []

    events = Draft202012Validator(
        json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
    )
    emitted = [json.loads(record.value()) for record in drain(topics, events_topic(ENV), expected=1)]
    quarantined = [event for event in emitted if event.get("stage") == "quarantined"]
    assert quarantined, "a kind: record event was emitted, so the failure is not log-only"
    for event in quarantined:
        assert event["kind"] == "record"
        assert event["status"] == "error"
        assert "attrs" not in event
        assert event["error_code"] == "RECORD_UNDECODABLE"
        assert list(events.iter_errors(event)) == []


WORKER_PROGRAM = """
import os
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka
from tributary.runtime.worker import PipelineWorker, WorkerSettings
from tributary.sdk import step

@step
def passthrough(message):
    return None

worker = PipelineWorker(
    settings=WorkerSettings(
        env=os.environ["TB_ENV"],
        channel=os.environ["TB_CHANNEL"],
        kafka=load_engine_kafka(),
        instance_id=os.environ["TB_INSTANCE_ID"],
        poll_timeout_s=0.1,
    ),
    steps_override=(passthrough,),
    defaults=load_topic_defaults(),
)
worker.build()
worker.run()
"""


def spawn_worker(bootstrap: str, instance_id: str) -> subprocess.Popen[bytes]:
    """A worker in its own process, so it can be killed rather than asked politely to stop.

    Its link comes from an `engine-kafka/v0` document, as a deployed worker's does (ADR-008): PLAINTEXT is
    valid here only because the testcontainers broker is advertised on 127.0.0.1.
    """
    document = Path(tempfile.mkdtemp(prefix="tb-engine-kafka-")) / "engine-kafka.json"
    document.write_text(
        json.dumps(
            {
                "apiVersion": "tributary.dev/v1",
                "kind": "EngineKafkaConnection",
                "bootstrapServers": [bootstrap],
                "security": {"protocol": "PLAINTEXT"},
            }
        ),
        encoding="utf-8",
    )
    environment = {
        **{name: value for name, value in os.environ.items() if name != "TB_BOOTSTRAP_SERVERS"},
        "TB_ENV": ENV,
        "TB_CHANNEL": CHANNEL,
        "TB_ENGINE_KAFKA_FILE": str(document),
        "TB_INSTANCE_ID": instance_id,
        "TB_LOG_LEVEL": "warning",
    }
    return subprocess.Popen([sys.executable, "-c", WORKER_PROGRAM], env=environment)  # noqa: S603


def wait_for_at_least(bootstrap: str, topic: str, *, count: int, timeout_s: float = 60.0) -> int:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        delivered = len(drain(bootstrap, topic, expected=count, timeout_s=5.0))
        if delivered >= count:
            return delivered
        time.sleep(0.5)
    raise AssertionError(f"{topic} did not reach {count} records in {timeout_s}s")


def test_kill_and_restart_preserves_per_key_order_and_loses_nothing(deployed_spec: dict[str, Any], topics: str) -> None:
    """Invariant 5 under a real `kill`, with the fault asserted rather than assumed.

    The worker runs in its own process and is killed with no chance to commit or drain. That is the
    difference between this and a graceful stop, and it is the whole test: a scenario that stops the
    loop politely measures shutdown, not replay, while appearing to prove ordering under failure
    (PHASE-0-DECISION section 4). `commit_every` is 50 and fewer than 50 records are in flight, so a
    kill before the first commit replays everything it had already delivered — which is why the
    duplicate count is asserted to be non-zero: no duplicates would mean the kill landed somewhere
    harmless and the test proved nothing.
    """
    out = out_topic(ENV, CHANNEL, DESTINATION)
    baseline = len(drain(topics, out, expected=0, timeout_s=10.0))
    published = publish(topics, PER_KEY, start=100)

    killed = spawn_worker(topics, "pipeline-kill")
    try:
        wait_for_at_least(topics, out, count=baseline + 4)
    finally:
        killed.kill()
        killed.wait(timeout=30)
    assert killed.returncode != 0, "the worker must have been killed, not have exited cleanly"

    # Same group.instance.id: static membership, so the successor fences the dead member instead of
    # waiting out session.timeout.ms (S5 measured 3.21 s for a 2.0 s planned restart).
    restarted = worker(topics, "pipeline-kill")
    restarted.build()
    restarted.run(max_iterations=800)

    delivered = drain(topics, out, expected=baseline + len(published))
    replayed = [raw for raw in delivered if int(header_map(raw).get(SEQ_HEADER, -1)) >= 100]
    delivered_ids = [header_map(raw)[h.TB_MSG_ID] for raw in replayed]

    assert set(published) <= set(delivered_ids), "no acknowledged message was lost across the kill"

    duplicates = len(delivered_ids) - len(set(delivered_ids))
    print(f"duplicates across the kill: {duplicates} of {len(published)} published")
    assert duplicates > 0, "the kill did not interrupt an uncommitted window, so this run tested recovery from nothing"

    for _key, sequences in first_delivery_order(replayed).items():
        assert sequences == sorted(sequences), f"per-key order broken for a key ({len(sequences)} records)"

    # A replayed record is consumed again, so its `received` event may be emitted twice (the killed
    # worker's copy survives only if its fire-and-forget queue was sent before the kill). Every copy must
    # carry the same archive dedup key and raw coordinates, which is what makes the duplicate harmless.
    received = received_events(topics, set(published))
    assert set(received) == set(published), "every replayed record has a received event"
    for copies in received.values():
        assert len({(e["message_id"], e["stage"], e.get("destination"), e["attempt"]) for e in copies}) == 1
        assert len({(e["topic"], e["partition"], e["offset"]) for e in copies}) == 1
    print(f"received duplicates across the kill: {sum(len(c) - 1 for c in received.values())}")
