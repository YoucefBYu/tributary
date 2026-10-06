"""The Kafka-source receiver's spec lifecycle against a real broker (invariant 3): the spec comes only from
`_tb.<env>.channels`, consumption starts once it has been read, a new version applies live, a tombstone pauses
consumption (committing only what was acked), and a redeploy resumes in the same process. The "foreign"
source cluster is the same test broker (loopback PLAINTEXT). Real receiver process, synthetic data only."""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import raw_topic

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_receiver
from tests.support.processes import ManagedProcess
from tests.support.receiver_specs import kafka_source_document

pytestmark = pytest.mark.integration

ENV = "itest"
MODULE = "tributary.receivers.kafka_source"


def produce(bootstrap: str, topic: str, numbers: range) -> None:
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    for n in numbers:
        producer.produce(topic, value=json.dumps({"n": n, "note": "TRIBUTARY-SYNTH"}).encode())
    assert producer.flush(30) == 0


def raw_numbers(kafka: str, channel: str, expected: int) -> list[int]:
    records = drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=expected)
    return sorted(json.loads(r.value())["n"] for r in records)


@pytest.fixture
def receivers() -> Iterator[list[ManagedProcess]]:
    procs: list[ManagedProcess] = []
    yield procs
    for proc in procs:
        proc.terminate_and_wait()


def wait_until(predicate: Callable[[], bool], what: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        time.sleep(0.1)


def test_publish_change_live_tombstone_redeploy(kafka: str, receivers: list[ManagedProcess]) -> None:
    ensure_channels_topic(kafka, ENV)
    channel, source_topic = "ksrc-spec-live", "upstream.ksrc-spec-live"
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, channel), 1, 1), NewTopic(source_topic, 1, 1)])
    produce(kafka, source_topic, range(0, 3))

    def doc(version: int, **kw: object) -> dict[str, object]:
        return kafka_source_document(channel, version=version, bootstrap=kafka, topic=source_topic, **kw)

    proc = start_receiver(MODULE, channel=channel, env=ENV, bootstrap=kafka, instance="ksrc-live")
    receivers.append(proc)
    proc.wait_for_event("spec_unavailable", timeout_s=60)
    time.sleep(0.5)
    assert not any(line.get("event") == "listening" for line in proc.lines)

    publish_channel_spec(kafka, ENV, channel, doc(1))
    listening = proc.wait_for_event("listening", timeout_s=60)
    assert listening["channel_version"] == 1
    assert raw_numbers(kafka, channel, 3) == [0, 1, 2]

    # Live: a new version (the size limit) applies in the same process, and later records carry it.
    pid = proc.process.pid
    publish_channel_spec(kafka, ENV, channel, doc(2, maxMessageBytes=900_000))
    applied = proc.wait_for_event("spec_applied", timeout_s=60)
    assert applied["changed"] == "channel_version,max_message_bytes"
    produce(kafka, source_topic, range(3, 5))
    records = drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=5)
    versions = {json.loads(r.value())["n"]: dict(r.headers())["tb-channel-version"] for r in records}
    assert versions[0] == b"1" and versions[4] == b"2"
    assert proc.process.pid == pid and proc.process.poll() is None

    # Tombstone: consumption pauses; records produced now are not consumed.
    publish_channel_spec(kafka, ENV, channel, None)
    proc.wait_for_event("channel_undeployed", timeout_s=60)
    time.sleep(1.5)  # let the consumer commit and close
    produce(kafka, source_topic, range(5, 8))
    time.sleep(2.0)
    assert raw_numbers(kafka, channel, 5) == [0, 1, 2, 3, 4]
    assert proc.process.poll() is None

    # Redeploy: a new consumer resumes from the committed offset: 5, 6, 7 arrive, and nothing is repeated.
    publish_channel_spec(kafka, ENV, channel, doc(3))
    proc.wait_for_event("listening", timeout_s=60)
    wait_until(
        lambda: len(drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=8, timeout_s=10)) >= 8,
        "8 records",
    )
    assert raw_numbers(kafka, channel, 8) == list(range(8))
    assert proc.process.pid == pid

    # Restart-only: a different source topic exits 0 for the supervisor.
    changed = kafka_source_document(channel, version=5, bootstrap=kafka, topic="upstream.other")
    publish_channel_spec(kafka, ENV, channel, changed)
    assert proc.wait_for_event("spec_restart_required", timeout_s=60)["fields"] == "topic"
    assert proc.process.wait(timeout=30) == 0
