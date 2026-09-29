"""The MLLP receiver's spec lifecycle against a real broker (invariant 3): the spec comes only from
`_tb.<env>.channels`, the listener opens after it has been read, a new version applies live, a restart-only
change exits 0, and a tombstone refuses new connections while committed messages are still ACKed.

Real receiver processes (`python -m tributary.receivers.mllp`), real Kafka, synthetic data only.
"""

from __future__ import annotations

import socket
import time
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import raw_topic

from tests.support.channel_spec import walking_skeleton_document
from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, free_port
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_mllp_receiver
from tests.support.processes import ManagedProcess

pytestmark = pytest.mark.integration

ENV = "itest"
SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
GARBAGE = b"this is not hl7 at all"


def adt(cid: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{cid}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def spec(channel: str, port: int, version: int, nack_on: list[str]) -> dict[str, Any]:
    doc = walking_skeleton_document(
        mllp_port=port, http_url="http://127.0.0.1:1/x", channel=channel, destination="stub", version=version
    )
    doc["spec"]["source"]["ack"]["nackOn"] = nack_on
    return doc


def exchange(sock: socket.socket, payload: bytes, timeout: float = 15.0) -> str:
    """Send one frame on `sock`; the MSA-1 code, or `""` if the peer closed without answering."""
    sock.sendall(SB + payload + EB + CR)
    sock.settimeout(timeout)
    buf = b""
    while not buf.endswith(EB + CR):
        chunk = sock.recv(4096)
        if not chunk:
            return ""
        buf += chunk
    return buf.split(b"\rMSA|")[1].split(b"|")[0].decode()


def send_one(port: int, payload: bytes) -> str:
    with socket.create_connection(("127.0.0.1", port), timeout=15) as sock:
        return exchange(sock, payload)


def control_ids(records: list[Any]) -> list[str]:
    return [r.value().split(b"|")[9].decode() for r in records]


@pytest.fixture
def channels(kafka: str) -> str:
    ensure_channels_topic(kafka, ENV)
    return kafka


def make_channel(kafka: str, name: str) -> str:
    create_topics_with_retry(kafka, [NewTopic(raw_topic(ENV, name), 1, 1)])
    return name


@pytest.fixture
def receivers() -> Iterator[list[ManagedProcess]]:
    procs: list[ManagedProcess] = []
    yield procs
    for proc in procs:
        proc.terminate_and_wait()


def test_spec_from_the_topic_gates_the_listener_and_a_new_version_applies_live(
    channels: str, receivers: list[ManagedProcess]
) -> None:
    kafka, channel, port = channels, make_channel(channels, "spec-live"), free_port()
    proc = start_mllp_receiver(channel=channel, env=ENV, bootstrap=kafka, instance="mllp-live")
    receivers.append(proc)

    # No spec on the topic yet: not ready. Nothing listens, so nothing can be ACKed.
    proc.wait_for_event("spec_unavailable", timeout_s=60)
    with pytest.raises(OSError), socket.create_connection(("127.0.0.1", port), timeout=2):
        pass
    assert not any(line.get("event") == "listening" for line in proc.lines)

    publish_channel_spec(kafka, ENV, channel, spec(channel, port, 1, ["parse-error", "queue-full"]))
    listening = proc.wait_for_event("listening", timeout_s=60)
    assert listening["channel_version"] == 1 and listening["port"] == port

    assert send_one(port, adt("LIVE-1")) == "AA"
    assert send_one(port, GARBAGE) == "AR"  # version 1: parse-error is a NACK condition
    committed = drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=1)
    assert control_ids(committed) == ["LIVE-1"]  # the ACKed message is committed, the NACKed one is not

    # Version 2 drops parse-error from nackOn: the same input now gets a different answer, same process.
    pid = proc.process.pid
    publish_channel_spec(kafka, ENV, channel, spec(channel, port, 2, ["queue-full"]))
    applied = proc.wait_for_event("spec_applied", timeout_s=60)
    assert applied["channel_version"] == 2 and applied["changed"] == "channel_version,nack_on"
    assert send_one(port, GARBAGE) == "AA"
    assert proc.process.pid == pid and proc.process.poll() is None
    assert [line for line in proc.lines if line.get("event") == "listening"] == [listening]

    records = drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=2)
    assert len(records) == 2
    versions = {dict(r.headers())["tb-channel-version"] for r in records}
    assert versions == {b"1", b"2"}  # each record is stamped with the spec version that admitted it


def test_a_port_change_exits_zero_for_the_supervisor(channels: str, receivers: list[ManagedProcess]) -> None:
    kafka, channel, port = channels, make_channel(channels, "spec-restart"), free_port()
    publish_channel_spec(kafka, ENV, channel, spec(channel, port, 1, ["queue-full"]))
    proc = start_mllp_receiver(channel=channel, env=ENV, bootstrap=kafka, instance="mllp-restart")
    receivers.append(proc)
    proc.wait_for_event("listening", timeout_s=60)
    assert send_one(port, adt("RST-1")) == "AA"

    publish_channel_spec(kafka, ENV, channel, spec(channel, free_port(), 2, ["queue-full"]))
    event = proc.wait_for_event("spec_restart_required", timeout_s=60)
    assert event["fields"] == "port"
    assert proc.process.wait(timeout=30) == 0
    assert control_ids(drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=1)) == ["RST-1"]


def test_tombstone_refuses_new_connections_and_never_acks_what_it_did_not_commit(
    channels: str, receivers: list[ManagedProcess]
) -> None:
    kafka, channel, port = channels, make_channel(channels, "spec-gone"), free_port()
    publish_channel_spec(kafka, ENV, channel, spec(channel, port, 1, ["queue-full"]))
    proc = start_mllp_receiver(channel=channel, env=ENV, bootstrap=kafka, instance="mllp-gone")
    receivers.append(proc)
    proc.wait_for_event("listening", timeout_s=60)

    idle = socket.create_connection(("127.0.0.1", port), timeout=15)
    burst = socket.create_connection(("127.0.0.1", port), timeout=15)
    try:
        assert exchange(idle, adt("GONE-0")) == "AA"

        # A pipelined burst is in flight when the tombstone lands. Whatever the interleaving, a message is
        # ACKed exactly when it was committed: no AA for a dropped message, no commit that got an AR.
        sent = [f"BURST-{i:03d}" for i in range(200)]
        burst.sendall(b"".join(SB + adt(cid) + EB + CR for cid in sent))
        publish_channel_spec(kafka, ENV, channel, None)
        proc.wait_for_event("channel_undeployed", timeout_s=60)

        with pytest.raises(OSError), socket.create_connection(("127.0.0.1", port), timeout=2):
            pass  # the listener is closed: new connections are refused

        assert exchange(idle, adt("GONE-1")) == "AR"  # an open connection is told, not silently dropped
        acks = read_acks(burst)
    finally:
        idle.close()
        burst.close()

    assert len(acks) <= len(sent)
    accepted = [cid for cid, code in zip(sent, acks, strict=False) if code == "AA"]
    committed = control_ids(drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=1 + len(accepted)))
    assert sorted(committed) == sorted(["GONE-0", *accepted])
    assert "GONE-1" not in committed
    assert proc.process.poll() is None  # not-ready, waiting for a redeploy; not crashed

    # A redeploy opens a fresh listener in the same process.
    publish_channel_spec(kafka, ENV, channel, spec(channel, port, 2, ["queue-full"]))
    proc.wait_for_event("listening", timeout_s=60)
    assert send_one(port, adt("GONE-2")) == "AA"


def read_acks(sock: socket.socket, timeout: float = 20.0) -> list[str]:
    """Every ACK code the peer sends until it closes the connection or goes quiet."""
    sock.settimeout(2.0)
    buf = b""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            chunk = sock.recv(65536)
        except TimeoutError:
            break
        if not chunk:
            break
        buf += chunk
    return [part.split(b"|")[0].decode() for part in buf.split(b"\rMSA|")[1:]]
