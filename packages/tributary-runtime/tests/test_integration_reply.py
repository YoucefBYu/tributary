"""W2-8 worker reply side against a real broker: a reply lands on the partition the request named.

Marked `integration` (Docker). `uv run pytest -m integration packages/tributary-runtime -k reply`.

A real `PipelineWorker` consumes sync-mode `raw` records (as the receiver will produce them, envelope
example `raw-mllp-sync-request`) and must produce exactly one `reply` record per request, **on the
partition named by `tb-reply-partition`**, keyed by `tb-correlation-id`, with `tb-reply-status` and the
envelope's reply headers, and with neither the request's key nor its MSH-10 anywhere in it. The
controls: requests naming different partitions (a worker that ignored the header would land them on
the key's hash partition, which differs for at least one), an async record in the same run (no reply),
and a request naming a partition the topic does not have (no reply, message still routed, a
`replied` event with `REPLY_ROUTE_INVALID`).
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from jsonschema import Draft202012Validator
from testcontainers.core.container import DockerContainer
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic, reply_topic
from tributary.runtime.worker import PipelineWorker, WorkerSettings
from tributary.sdk import Message, step
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_ROUTE_INVALID
from tributary.sdk.ids import new_correlation_id, new_traceparent, uuid7
from tributary.sdk.projection import record_projection
from tributary.sdk.testing import make_message

from .reply_harness import CONTRACTS, CONTROL_ID_MARKER, KEY_MARKER, REQUEST_PAYLOAD, spec_document
from .test_integration_kafka import _await_broker, _free_port

pytestmark = pytest.mark.integration

ENV = "dev"
CHANNEL = "lab-sync"
DESTINATION = "http-stub"
PARTITIONS = 3
REPLY_PARTITIONS = 3


@step
def passthrough(message: Message) -> None:
    return None


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    """Single-node Apache Kafka 4.0.0 in KRaft mode, the same broker `test_integration_kafka` uses."""
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
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        _await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def deployed(kafka: str) -> str:
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
        NewTopic(out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(reply_topic(ENV, CHANNEL), REPLY_PARTITIONS, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    for _topic, future in admin.create_topics(wanted).items():
        future.result(timeout=30)

    document = spec_document(reply={}, version=1)
    document["metadata"] = {**document["metadata"], "name": CHANNEL}
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0
    return kafka


def request(partition: str | None) -> Message:
    deadline = h.format_timestamp(datetime.now(UTC) + timedelta(minutes=5))
    headers = {} if partition is None else {h.TB_REPLY_PARTITION: partition, h.TB_REPLY_DEADLINE: deadline}
    return make_message(
        REQUEST_PAYLOAD,
        key=KEY_MARKER,
        channel=CHANNEL,
        msg_id=uuid7(),
        content_type="application/hl7-v2+er7",
        traceparent=new_traceparent(),
        correlation_id=new_correlation_id(),
        headers=headers,
    )


def publish(bootstrap: str, messages: list[Message]) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    for message in messages:
        producer.produce(
            raw_topic(ENV, CHANNEL),
            key=message.key,
            value=message.payload,
            headers=[(name, value.encode()) for name, value in message.headers.items()],
        )
    assert producer.flush(30) == 0


def read_all(bootstrap: str, topic: str, partitions: int, *, expected: int, timeout_s: float = 30.0) -> list[Any]:
    """Every record on every partition from the beginning, by explicit assignment (no group needed)."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"verify-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    consumer.assign([TopicPartition(topic, index, 0) for index in range(partitions)])
    collected: list[Any] = []
    eofs: set[int] = set()
    deadline = time.monotonic() + timeout_s
    try:
        while time.monotonic() < deadline:
            raw = consumer.poll(0.5)
            if raw is None:
                continue
            if raw.error() is not None:
                eofs.add(raw.partition())
                if len(eofs) >= partitions and len(collected) >= expected:
                    break
                continue
            collected.append(raw)
    finally:
        consumer.close()
    return collected


def headers_of(raw: Any) -> dict[str, str]:
    return {name: value.decode("utf-8") for name, value in (raw.headers() or [])}


def test_each_reply_lands_on_the_partition_its_request_named(deployed: str) -> None:
    sync = [request(str(index % REPLY_PARTITIONS)) for index in range(6)]
    async_record = request(None)
    out_of_range = request("9")  # well-formed (0-63) but the topic has 3 partitions
    publish(deployed, [*sync, async_record, out_of_range])

    worker = PipelineWorker(
        settings=WorkerSettings(
            env=ENV,
            channel=CHANNEL,
            kafka=KafkaSettings(bootstrap_servers=deployed),
            instance_id="reply-it-1",
            poll_timeout_s=0.1,
        ),
        steps_override=(passthrough,),
        defaults=load_topic_defaults(),
    )
    worker.build()
    worker.run(max_iterations=300)

    replies = read_all(deployed, reply_topic(ENV, CHANNEL), REPLY_PARTITIONS, expected=len(sync))
    by_correlation = {headers_of(raw)[h.TB_CORRELATION_ID]: raw for raw in replies}
    assert len(replies) == len(sync), "exactly one reply per sync request; none for async or bad routes"

    envelope = Draft202012Validator(
        json.loads((CONTRACTS / "envelope" / "v0" / "envelope.schema.json").read_text(encoding="utf-8"))
    )
    for message in sync:
        raw = by_correlation[message.require(h.TB_CORRELATION_ID)]
        headers = headers_of(raw)
        assert raw.partition() == int(message.require(h.TB_REPLY_PARTITION)), "targeted, not hashed"
        assert raw.key() == message.require(h.TB_CORRELATION_ID).encode()
        assert headers[h.TB_MSG_ID] == message.msg_id
        assert headers[h.TB_REPLY_STATUS] == "accepted"
        assert h.TB_ERROR_CODE not in headers
        assert headers[h.TB_STAGE] == "replied"
        assert not {h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE} & set(headers)
        assert raw.value() in (None, b"")
        rendered = json.dumps(headers)
        assert KEY_MARKER.decode() not in rendered and CONTROL_ID_MARKER not in rendered
        projected = Message(headers=headers, payload=raw.value() or b"", key=raw.key())
        assert list(envelope.iter_errors(record_projection(projected, topic_class="reply"))) == []

    delivered = read_all(deployed, out_topic(ENV, CHANNEL, DESTINATION), PARTITIONS, expected=8)
    delivered_ids = {headers_of(raw)[h.TB_MSG_ID] for raw in delivered}
    assert {m.msg_id for m in [*sync, async_record, out_of_range]} <= delivered_ids, (
        "every message is routed, including the one whose reply route was invalid (envelope 2.3.3)"
    )
    assert all(not {h.TB_REPLY_PARTITION, h.TB_REPLY_DEADLINE} & set(headers_of(raw)) for raw in delivered)

    events = [json.loads(raw.value()) for raw in read_all(deployed, events_topic(ENV), 1, expected=1)]
    replied = [event for event in events if event["stage"] == "replied"]
    invalid = [event for event in replied if event.get("error_code") == REPLY_ROUTE_INVALID]
    assert [event["message_id"] for event in invalid] == [out_of_range.msg_id]
    ok = {event["message_id"] for event in replied if event["status"] == "ok"}
    assert ok == {message.msg_id for message in sync}, "every delivered reply was confirmed by its callback"
