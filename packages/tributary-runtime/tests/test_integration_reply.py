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

The failure answers, each on its own channel (own topics, own consumer group):

* `rejected`: a `Reject` is dead-lettered, then answered `rejected` with the channel's code;
* `failed`: a step that raises is dead-lettered, then answered `failed` / `TRANSFORM_ERROR`, and the
  exception text (a synthetic name) reaches neither the reply nor an event;
* dlq-then-reply: in both, the `dlq` record and its event precede the reply's `replied` event on
  `_tb.<env>.events` (one partition, one idempotent producer, so its offsets are emission order), and
  no `out` record exists;
* `body: step`: the step's `ctx.reply` body is the reply's payload with its content type; without a
  call the reply is `failed` / `REPLY_BODY_MISSING`, the message is still routed and not dead-lettered.
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
from tributary.sdk import Message, Reject, step
from tributary.sdk import headers as h
from tributary.sdk.codes import REPLY_BODY_MISSING, REPLY_ROUTE_INVALID, TRANSFORM_ERROR
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


HL7 = "application/hl7-v2+er7"
ACK_BODY = b"MSH|^~\\&|ENGINE|LAB|TRIBUTARY-SYNTH|LAB|20260928||ACK^Q11|ACK-1|P|2.5.1\rMSA|AA|SYNTH-ACK-CONTROL\r"
EXCEPTION_MARKER = "SYNTH^EXCEPTION^MARKER"


@step
def passthrough(message: Message) -> None:
    return None


@step
def reject_lab(message: Message) -> None:
    raise Reject("LAB_QUERY_UNSUPPORTED")


@step
def explode(message: Message) -> None:
    raise ValueError(EXCEPTION_MARKER)


@step
def answer(message: Message, ctx: Any) -> None:
    ctx.reply(ACK_BODY, content_type=HL7)


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


def deploy(bootstrap: str, channel: str, *, reply: dict[str, Any]) -> None:
    """One channel's topics and its spec record on `_tb.<env>.channels` (created once per broker)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    wanted = [
        NewTopic(raw_topic(ENV, channel), PARTITIONS, 1),
        NewTopic(out_topic(ENV, channel, DESTINATION), PARTITIONS, 1),
        NewTopic(dlq_topic(ENV, channel), 1, 1),
        NewTopic(reply_topic(ENV, channel), REPLY_PARTITIONS, 1),
    ]
    existing = admin.list_topics(timeout=10).topics
    for shared in (
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ):
        if shared.topic not in existing:
            wanted.append(shared)
    for _topic, future in admin.create_topics(wanted).items():
        future.result(timeout=30)

    document = spec_document(reply=reply, version=1)
    document["metadata"] = {**document["metadata"], "name": channel}
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=channel.encode(), value=json.dumps(document).encode())
    assert producer.flush(30) == 0


@pytest.fixture(scope="module")
def deployed(kafka: str) -> str:
    deploy(kafka, CHANNEL, reply={})
    return kafka


def run_worker(bootstrap: str, channel: str, steps: tuple[Any, ...], *, max_iterations: int = 300) -> None:
    worker = PipelineWorker(
        settings=WorkerSettings(
            env=ENV,
            channel=channel,
            kafka=KafkaSettings(bootstrap_servers=bootstrap),
            instance_id=f"reply-it-{channel}",
            poll_timeout_s=0.1,
        ),
        steps_override=steps,
        defaults=load_topic_defaults(),
    )
    worker.build()
    worker.run(max_iterations=max_iterations)


def request(partition: str | None, channel: str = CHANNEL) -> Message:
    deadline = h.format_timestamp(datetime.now(UTC) + timedelta(minutes=5))
    headers = {} if partition is None else {h.TB_REPLY_PARTITION: partition, h.TB_REPLY_DEADLINE: deadline}
    return make_message(
        REQUEST_PAYLOAD,
        key=KEY_MARKER,
        channel=channel,
        msg_id=uuid7(),
        content_type=HL7,
        traceparent=new_traceparent(),
        correlation_id=new_correlation_id(),
        headers=headers,
    )


def publish(bootstrap: str, messages: list[Message], channel: str = CHANNEL) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    for message in messages:
        producer.produce(
            raw_topic(ENV, channel),
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

    run_worker(deployed, CHANNEL, (passthrough,))

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


# -- failure answers, dlq-then-reply, body: step ------------------------------------------------------


def _events_for(bootstrap: str, ids: set[str]) -> list[tuple[int, dict[str, Any]]]:
    """(offset, event) for the given message ids, in `_tb.<env>.events` offset order."""
    raws = read_all(bootstrap, events_topic(ENV), 1, expected=1)
    found = [(raw.offset(), json.loads(raw.value())) for raw in raws]
    return sorted((offset, event) for offset, event in found if event.get("message_id") in ids)


def _answered_after_dlq(bootstrap: str, channel: str, steps: tuple[Any, ...]) -> tuple[Message, dict[str, str], Any]:
    """Run one sync request through a failing pipeline; return it, its reply headers and its reply."""
    deploy(bootstrap, channel, reply={})
    sent = request("1", channel)
    publish(bootstrap, [sent], channel)
    run_worker(bootstrap, channel, steps)

    (reply,) = read_all(bootstrap, reply_topic(ENV, channel), REPLY_PARTITIONS, expected=1)
    (dead,) = read_all(bootstrap, dlq_topic(ENV, channel), 1, expected=1)
    assert headers_of(dead)[h.TB_MSG_ID] == sent.msg_id
    assert read_all(bootstrap, out_topic(ENV, channel, DESTINATION), PARTITIONS, expected=0, timeout_s=5.0) == []
    assert reply.partition() == 1 and reply.key() == sent.require(h.TB_CORRELATION_ID).encode()

    events = _events_for(bootstrap, {sent.msg_id})
    stages = [event["stage"] for _offset, event in events]
    assert "dlq" in stages and "replied" in stages, stages
    assert stages.index("dlq") < stages.index("replied"), f"dlq-then-reply: {stages}"
    assert dead.timestamp()[1] <= reply.timestamp()[1], "the reply was produced after the dlq record"
    return sent, headers_of(reply), reply


def test_a_rejected_message_is_dead_lettered_then_answered_rejected(kafka: str) -> None:
    _sent, headers, reply = _answered_after_dlq(kafka, "lab-sync-reject", (reject_lab,))
    assert headers[h.TB_REPLY_STATUS] == "rejected"
    assert headers[h.TB_ERROR_CODE] == "LAB_QUERY_UNSUPPORTED"
    assert reply.value() in (None, b"")


def test_a_failing_step_is_dead_lettered_then_answered_failed_without_its_text(kafka: str) -> None:
    sent, headers, reply = _answered_after_dlq(kafka, "lab-sync-fail", (explode,))
    assert headers[h.TB_REPLY_STATUS] == "failed"
    assert headers[h.TB_ERROR_CODE] == TRANSFORM_ERROR
    rendered = json.dumps(headers) + (reply.value() or b"").decode()
    assert EXCEPTION_MARKER not in rendered and KEY_MARKER.decode() not in rendered
    events = json.dumps([event for _offset, event in _events_for(kafka, {sent.msg_id})])
    assert EXCEPTION_MARKER not in events and CONTROL_ID_MARKER not in events


def test_body_step_answers_with_the_steps_body_and_fails_the_reply_only_without_one(kafka: str) -> None:
    channel = "lab-sync-body"
    deploy(kafka, channel, reply={"body": "step"})
    with_body = request("0", channel)
    publish(kafka, [with_body], channel)
    run_worker(kafka, channel, (answer,))

    without_body = request("2", channel)
    publish(kafka, [without_body], channel)
    run_worker(kafka, channel, (passthrough,))  # same group: resumes after the first request

    replies = read_all(kafka, reply_topic(ENV, channel), REPLY_PARTITIONS, expected=2)
    by_msg = {headers_of(raw)[h.TB_MSG_ID]: raw for raw in replies}
    assert set(by_msg) == {with_body.msg_id, without_body.msg_id}

    answered = by_msg[with_body.msg_id]
    assert answered.partition() == 0 and answered.value() == ACK_BODY
    assert headers_of(answered)[h.TB_CONTENT_TYPE] == HL7
    assert headers_of(answered)[h.TB_REPLY_STATUS] == "accepted"

    missing = headers_of(by_msg[without_body.msg_id])
    assert (missing[h.TB_REPLY_STATUS], missing[h.TB_ERROR_CODE]) == ("failed", REPLY_BODY_MISSING)
    assert by_msg[without_body.msg_id].value() in (None, b"")

    routed = read_all(kafka, out_topic(ENV, channel, DESTINATION), PARTITIONS, expected=2)
    assert {headers_of(raw)[h.TB_MSG_ID] for raw in routed} == {with_body.msg_id, without_body.msg_id}
    assert read_all(kafka, dlq_topic(ENV, channel), 1, expected=0, timeout_s=5.0) == [], (
        "the reply fails, not the message"
    )
    replied = {
        event["message_id"]: event
        for _o, event in _events_for(kafka, {without_body.msg_id})
        if event["stage"] == "replied"
    }
    assert (replied[without_body.msg_id]["status"], replied[without_body.msg_id]["error_code"]) == (
        "error",
        REPLY_BODY_MISSING,
    )
    events = json.dumps([event for _o, event in _events_for(kafka, {with_body.msg_id})])
    assert "SYNTH-ACK-CONTROL" not in events, "a step body is payload: never in an event"
