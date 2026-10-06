"""W2-5 against a real broker: split children and destination steps, through the shipping built-ins.

Marked `integration`: it needs Docker. `uv run pytest -m integration packages/tributary-runtime`.

No `steps_override`: the channel names `tributary.core.split`, `tributary.core.enrich` and
`tributary.core.validate` by `uses:`, so entry-point resolution and `with` binding are exercised as in
production. Claims, each checked on the topics themselves:

1. every split child reaches every destination it should, with its deterministic `tb-msg-id`, its
   `tb-parent-id`, and a record that validates against `contracts/envelope/v0`;
2. a destination step's `tb-usr-*` header appears on that destination's records only;
3. a destination step's `Reject` dead-letters that child **for that destination only**, with
   `tb-destination` set and the destination step's input as the payload (README 3.5), while the other
   destinations still receive it;
4. every lifecycle event validates against `contracts/events/v0`.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from jsonschema import Draft202012Validator
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, raw_topic
from tributary.runtime.worker import PipelineWorker, WorkerSettings
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.ids import derived_uuid7, new_traceparent, uuid7
from tributary.sdk.projection import record_projection
from tributary.sdk.testing import make_message

from .test_integration_kafka import ENV, drain, header_map, kafka  # noqa: F401 - kafka is a fixture

pytestmark = pytest.mark.integration

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
CHANNEL = "lab-fanout"
DESTINATIONS = ("fhir", "rows", "hl7-only")
PARTITIONS = 3
PARENTS = 5
CHILDREN = 3


def channel_document() -> dict[str, Any]:
    base = json.loads(
        (CONTRACTS / "channel-spec" / "v0" / "examples" / "valid" / "walking-skeleton-loopback.json").read_text(
            encoding="utf-8"
        )
    )
    template = base["spec"]["destinations"][0]
    base["metadata"] = {"name": CHANNEL, "version": 1}
    base["spec"]["pipeline"]["steps"] = [
        {"name": "entries", "uses": "tributary.core.split", "with": {"by": "json-array", "pointer": "/entry"}}
    ]
    destinations = []
    for name in DESTINATIONS:
        destination = {**json.loads(json.dumps(template)), "name": name}
        if name == "rows":
            destination["steps"] = [{"name": "route", "uses": "tributary.core.enrich", "with": {"usr": ["sink=rows"]}}]
        if name == "hl7-only":
            destination["steps"] = [
                {"name": "must-be-hl7", "uses": "tributary.core.validate", "with": {"requireSegments": ["MSH"]}}
            ]
        destinations.append(destination)
    base["spec"]["destinations"] = destinations
    return base


@pytest.fixture(scope="module")
def fanout(kafka: str) -> str:  # noqa: F811 - the imported fixture, requested by name
    admin = AdminClient({"bootstrap.servers": kafka})
    wanted = [
        NewTopic(raw_topic(ENV, CHANNEL), PARTITIONS, 1),
        *(NewTopic(out_topic(ENV, CHANNEL, name), PARTITIONS, 1) for name in DESTINATIONS),
        NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
        NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
    ]
    for _topic, future in admin.create_topics(wanted).items():
        try:
            future.result(timeout=30)
        except Exception as exc:  # pragma: no cover - shared topics already exist in this module's broker
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise
    producer = Producer({"bootstrap.servers": kafka, "acks": "all", "enable.idempotence": True})
    producer.produce(channels_topic(ENV), key=CHANNEL.encode(), value=json.dumps(channel_document()).encode())
    assert producer.flush(30) == 0
    return kafka


def publish(bootstrap: str) -> list[Message]:
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    parents = []
    for index in range(PARENTS):
        # Written by hand, not json.dumps: `1.50` must be in the source for the exact-slice check.
        entries = ", ".join(f'{{"seq": "{index}-{child}", "v": 1.50}}' for child in range(CHILDREN))
        body = f'{{"resourceType": "Bundle", "entry": [{entries}]}}'
        message = make_message(
            body.encode(),
            content_type="application/fhir+json",
            channel=CHANNEL,
            msg_id=uuid7(),
            traceparent=new_traceparent(),
        )
        parents.append(message)
        producer.produce(
            raw_topic(ENV, CHANNEL),
            key=None,
            value=message.payload,
            headers=[(name, value.encode()) for name, value in message.headers.items()],
        )
    assert producer.flush(30) == 0
    return parents


def test_split_children_and_destination_steps_end_to_end(fanout: str) -> None:
    parents = publish(fanout)
    running = PipelineWorker(
        settings=WorkerSettings(
            env=ENV,
            channel=CHANNEL,
            kafka=KafkaSettings(bootstrap_servers=fanout),
            instance_id="fanout-1",
            poll_timeout_s=0.1,
        ),
        defaults=load_topic_defaults(),
    )
    running.build()
    running.run(max_iterations=300)

    expected = {derived_uuid7(parent.msg_id, child): parent.msg_id for parent in parents for child in range(CHILDREN)}
    envelope = Draft202012Validator(
        json.loads((CONTRACTS / "envelope" / "v0" / "envelope.schema.json").read_text(encoding="utf-8"))
    )

    for name in ("fhir", "rows"):
        delivered = drain(fanout, out_topic(ENV, CHANNEL, name), expected=len(expected))
        by_id = {header_map(raw)[h.TB_MSG_ID]: raw for raw in delivered}
        assert set(expected) <= set(by_id), f"every child reached {name}"
        for msg_id, parent_id in expected.items():
            headers = header_map(by_id[msg_id])
            assert headers[h.TB_PARENT_ID] == parent_id
            assert headers[h.TB_DESTINATION] == name
            assert (headers.get("tb-usr-sink") == "rows") is (name == "rows"), "tb-usr-* stays on its destination"
            assert b'"v": 1.50' in by_id[msg_id].value(), "children are exact source slices"
            message = Message(headers=headers, payload=by_id[msg_id].value(), key=by_id[msg_id].key())
            assert list(envelope.iter_errors(record_projection(message, topic_class="out"))) == []

    assert drain(fanout, out_topic(ENV, CHANNEL, "hl7-only"), expected=0, timeout_s=5) == []
    dead = drain(fanout, dlq_topic(ENV, CHANNEL), expected=len(expected))
    dead_by_id = {header_map(raw)[h.TB_MSG_ID]: raw for raw in dead}
    assert set(expected) <= set(dead_by_id)
    for raw in dead_by_id.values():
        headers = header_map(raw)
        assert headers[h.TB_DESTINATION] == "hl7-only"
        assert headers[h.TB_ERROR_CODE] == "VALIDATE_NOT_HL7"
        assert raw.value().startswith(b'{"seq": '), "the dlq payload is the destination step's input (the child)"
        message = Message(headers=headers, payload=raw.value(), key=raw.key())
        assert list(envelope.iter_errors(record_projection(message, topic_class="dlq"))) == []

    events = Draft202012Validator(
        json.loads((CONTRACTS / "events" / "v0" / "event.schema.json").read_text(encoding="utf-8"))
    )
    emitted = [json.loads(raw.value()) for raw in drain(fanout, events_topic(ENV), expected=len(expected) * 4)]
    mine = [event for event in emitted if event.get("channel") == CHANNEL]
    assert {"transformed", "routed", "dlq"} <= {event["stage"] for event in mine}
    for event in mine:
        assert list(events.iter_errors(event)) == []
    assert all(
        event.get("parent_id") in {parent.msg_id for parent in parents} for event in mine if event["stage"] == "dlq"
    )
