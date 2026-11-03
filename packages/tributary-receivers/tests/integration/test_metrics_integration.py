"""W3-26 against a real broker: a spawned MLLP receiver's `/metrics` counts what it ACKed and what it refused, with
only names from `contracts/observability/v0/metrics.json`. Synthetic data only.

`uv run pytest -m integration packages/tributary-receivers -k metrics` (Docker).
"""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest
from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import raw_topic

from tests.support.channel_spec import walking_skeleton_document
from tests.support.kafka_helpers import create_topics_with_retry, free_port
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_mllp_receiver
from tests.support.processes import ManagedProcess

from .readiness_rig import ops_port, scrape

pytestmark = pytest.mark.integration

ENV = "mtest"
SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def send(port: int, payload: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=15) as sock:
        sock.sendall(SB + payload + EB + CR)
        buf = b""
        while not buf.endswith(EB + CR):
            chunk = sock.recv(4096)
            if not chunk:
                break
            buf += chunk
        return buf


@pytest.fixture
def procs() -> Iterator[list[ManagedProcess]]:
    started: list[ManagedProcess] = []
    yield started
    for proc in started:
        proc.terminate_and_wait()


def test_mllp_metrics_count_an_ack_after_commit_and_a_parse_error_nack(kafka: str, procs: list[ManagedProcess]) -> None:
    ensure_channels_topic(kafka, ENV)
    channel, port = "metrics-mllp", free_port()
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, channel), 1, 1)])
    proc = start_mllp_receiver(channel=channel, env=ENV, bootstrap=kafka, instance="metrics-mllp-0")
    procs.append(proc)
    ops = ops_port(proc)
    doc = walking_skeleton_document(
        mllp_port=port, http_url="http://127.0.0.1:1/x", channel=channel, destination="stub", version=1
    )
    doc["spec"]["source"]["ack"]["nackOn"] = ["parse-error", "queue-full"]
    publish_channel_spec(kafka, ENV, channel, doc)
    proc.wait_for_event("listening", timeout_s=60)

    assert b"MSA|AA|" in send(port, adt("METRICS-1"))
    assert b"MSA|AR|" in send(port, b"this is not hl7 at all")

    series = scrape(ops)
    scope = f'channel="{channel}",receiver_type="mllp"'
    assert series[f"tb_receiver_connections_total{{{scope}}}"] == 2
    assert series[f'tb_receiver_responses_total{{{scope},outcome="acked",reason="committed"}}'] == 1
    assert series[f'tb_receiver_responses_total{{{scope},outcome="nacked",reason="parse_error"}}'] == 1
    assert series[f"tb_ack_latency_seconds_count{{{scope}}}"] >= 1
    assert series[f'tb_ready{{component="receiver",channel="{channel}"}}'] == 1
    assert not any(name.startswith(("process_", "python_")) for name in series)
