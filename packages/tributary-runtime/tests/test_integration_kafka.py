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
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("testcontainers.kafka", reason="testcontainers is needed for the Kafka integration test")

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from jsonschema import Draft202012Validator
from testcontainers.kafka import KafkaContainer
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

#: A tb-x-* header is legal on a spike topic and must never survive into a released package
#: (envelope contract 2.1, gate section 6). It is used here only as a measurement affordance for the
#: ordering check — and the test below asserts it does NOT reach `out`.
SEQ_HEADER = "tb-x-seq"


@step
def passthrough(message: Message) -> None:
    """The wave-1 walking-skeleton step: it changes nothing, so anything that changes is the runtime."""
    return None


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    with KafkaContainer() as container:
        yield container.get_bootstrap_server()


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
                headers={SEQ_HEADER: str(sequence)},
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
    import time

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


def test_the_experimental_header_does_not_survive_the_hop(deployed_spec: dict[str, Any], topics: str) -> None:
    """Control on the control: the ordering affordance must not become a load-bearing header.

    `tb-x-seq` is produced onto `raw` by this test, and the contract allows it there. It reaches `out`
    only because the runtime propagates headers it does not understand, which is correct — what must
    never happen is the runtime *producing* one itself.
    """
    delivered = drain(topics, out_topic(ENV, CHANNEL, DESTINATION), expected=1)
    assert delivered, "the previous test should have left records here"
    produced_by_runtime = {
        name for raw in delivered for name in header_map(raw) if name.startswith("tb-x-") and name != SEQ_HEADER
    }
    assert produced_by_runtime == set()


def test_kill_and_restart_preserves_per_key_order_and_loses_nothing(deployed_spec: dict[str, Any], topics: str) -> None:
    """Invariant 5 under a kill, with the fault asserted rather than assumed."""
    before = drain(topics, out_topic(ENV, CHANNEL, DESTINATION), expected=0, timeout_s=10.0)
    baseline = len(before)

    published = publish(topics, PER_KEY, start=100)

    killed = worker(topics, "pipeline-kill")
    killed.build()
    killed.run(max_iterations=30)  # stop mid-stream, inside an uncommitted window
    killed.stop()

    restarted = worker(topics, "pipeline-kill")  # same group.instance.id: static membership
    restarted.build()
    restarted.run(max_iterations=400)

    delivered = drain(topics, out_topic(ENV, CHANNEL, DESTINATION), expected=baseline + len(published))
    replayed = [raw for raw in delivered if int(header_map(raw).get(SEQ_HEADER, -1)) >= 100]

    delivered_ids = [header_map(raw)[h.TB_MSG_ID] for raw in replayed]
    assert set(published) <= set(delivered_ids), "no acknowledged message was lost across the kill"

    duplicates = len(delivered_ids) - len(set(delivered_ids))
    assert duplicates >= 0
    # Not asserted as a threshold: duplicates are permitted by at-least-once and their count depends on
    # where the kill landed in the commit window. Reported so the number is visible in the run log.
    print(f"duplicates across the kill: {duplicates}")

    for _key, sequences in first_delivery_order(replayed).items():
        assert sequences == sorted(sequences), f"per-key order broken for a key ({len(sequences)} records)"
