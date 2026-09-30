"""The HTTP receiver's spec lifecycle against a real broker (invariant 3): the spec comes only from
`_tb.<env>.channels`, the listener opens after it has been read, a new version applies live, a tombstone
closes the listener, and a redeploy opens a fresh one in the same process.

Real receiver process (`python -m tributary.receivers.http`), real Kafka, synthetic data only.
"""

from __future__ import annotations

import http.client
import socket
from collections.abc import Iterator

import pytest
from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import raw_topic

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, free_port
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_receiver
from tests.support.processes import ManagedProcess
from tests.support.receiver_specs import http_document

pytestmark = pytest.mark.integration

ENV = "itest"
MODULE = "tributary.receivers.http"
BODY = b'{"resourceType": "Patient", "id": "SYNTH1"}'


def post(port: int, path: str = "/ingest", body: bytes = BODY) -> int:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
    try:
        conn.request("POST", path, body=body, headers={"Content-Type": "application/fhir+json"})
        return conn.getresponse().status
    finally:
        conn.close()


@pytest.fixture
def receivers() -> Iterator[list[ManagedProcess]]:
    procs: list[ManagedProcess] = []
    yield procs
    for proc in procs:
        proc.terminate_and_wait()


def test_publish_change_live_tombstone_redeploy(kafka: str, receivers: list[ManagedProcess]) -> None:
    ensure_channels_topic(kafka, ENV)
    channel, port = "http-spec-live", free_port()
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, channel), 1, 1)])
    proc = start_receiver(MODULE, channel=channel, env=ENV, bootstrap=kafka, instance="http-live")
    receivers.append(proc)

    # No spec yet: not ready, nothing listens.
    proc.wait_for_event("spec_unavailable", timeout_s=60)
    with pytest.raises(OSError), socket.create_connection(("127.0.0.1", port), timeout=2):
        pass

    publish_channel_spec(kafka, ENV, channel, http_document(channel, version=1, port=port))
    listening = proc.wait_for_event("listening", timeout_s=60)
    assert listening["channel_version"] == 1 and listening["port"] == port
    assert post(port) == 202
    assert post(port, "/v2") == 404

    # Live: the path changes in the same process, no restart.
    pid = proc.process.pid
    publish_channel_spec(kafka, ENV, channel, http_document(channel, version=2, port=port, path="/v2"))
    applied = proc.wait_for_event("spec_applied", timeout_s=60)
    assert applied["changed"] == "channel_version,path"
    assert post(port, "/v2") == 202 and post(port) == 404
    assert proc.process.pid == pid and proc.process.poll() is None
    assert [line for line in proc.lines if line.get("event") == "listening"] == [listening]

    records = drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=2)
    assert {dict(r.headers())["tb-channel-version"] for r in records} == {b"1", b"2"}

    # Tombstone: the listener closes, the process stays up waiting for a redeploy.
    publish_channel_spec(kafka, ENV, channel, None)
    proc.wait_for_event("channel_undeployed", timeout_s=60)
    with pytest.raises(OSError), socket.create_connection(("127.0.0.1", port), timeout=2):
        pass
    assert proc.process.poll() is None

    # Redeploy: a fresh listener, same process.
    publish_channel_spec(kafka, ENV, channel, http_document(channel, version=3, port=port, path="/v3"))
    proc.wait_for_event("listening", timeout_s=60)
    assert post(port, "/v3") == 202
    assert len(drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=3)) == 3
    assert proc.process.pid == pid


def test_a_port_change_exits_zero_for_the_supervisor(kafka: str, receivers: list[ManagedProcess]) -> None:
    ensure_channels_topic(kafka, ENV)
    channel, port = "http-spec-restart", free_port()
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, channel), 1, 1)])
    publish_channel_spec(kafka, ENV, channel, http_document(channel, version=1, port=port))
    proc = start_receiver(MODULE, channel=channel, env=ENV, bootstrap=kafka, instance="http-restart")
    receivers.append(proc)
    proc.wait_for_event("listening", timeout_s=60)
    assert post(port) == 202

    publish_channel_spec(kafka, ENV, channel, http_document(channel, version=2, port=free_port()))
    assert proc.wait_for_event("spec_restart_required", timeout_s=60)["fields"] == "port"
    assert proc.process.wait(timeout=30) == 0
