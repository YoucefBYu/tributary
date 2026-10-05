"""Shared pieces of the synchronous-reply integration tests: a deployed sync channel on a real broker, real
receiver and worker processes, and a small MLLP client that reports what the sender sees.

Synthetic data only: MSH-3 `TRIBUTARY-SYNTH`, control ids minted per message."""

from __future__ import annotations

import json
import socket
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import dlq_topic, events_topic, out_topic, raw_topic, reply_topic

from tests.support.channel_spec import walking_skeleton_document
from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, header_map
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_receiver
from tests.support.processes import ManagedProcess, spawn_pipeline_worker

ENV = "synctest"
DESTINATION = "stub"
SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"

#: `tributary.core.validate` rejects every message that lacks this segment: a deterministic `Reject`.
REJECTING_STEP = {
    "name": "need-zzz",
    "uses": "tributary.core.validate",
    "with": {"requireSegments": ["ZZZ"]},
}


def message(control_id: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{control_id}|P|2.5\r"
        "PID|1||SYNTH0001^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def deploy(
    kafka: str,
    channel: str,
    *,
    reply: dict[str, Any] | None,
    reply_partitions: int = 3,
    steps: Sequence[dict[str, Any]] | None = None,
    version: int = 1,
    port: int = 0,
) -> dict[str, Any]:
    """Create the channel's topics and publish its spec (an `mllp` source on `port`, 0 = ephemeral)."""
    ensure_channels_topic(kafka, ENV)
    wanted = [
        NewTopic(raw_topic(ENV, channel), 1, 1),
        NewTopic(out_topic(ENV, channel, DESTINATION), 1, 1),
        NewTopic(dlq_topic(ENV, channel), 1, 1),
        NewTopic(events_topic(ENV), 1, 1),
    ]
    if reply is not None:
        wanted.append(NewTopic(reply_topic(ENV, channel), reply_partitions, 1))
    create_topics_with_retry(kafka, wanted)
    document = walking_skeleton_document(
        mllp_port=port, http_url="http://127.0.0.1:1/x", channel=channel, destination=DESTINATION, version=version
    )
    source = document["spec"]["source"]
    source["bind"] = "127.0.0.1"
    if reply is not None:
        source["reply"] = reply
    if steps is not None:
        document["spec"]["pipeline"]["steps"] = list(steps)
    publish_channel_spec(kafka, ENV, channel, document)
    return document


def start_receiver_process(
    kafka: str, channel: str, instance: str, *, metrics_file: Path | None = None, wait: bool = True
) -> ManagedProcess:
    extra = ["--metrics-file", str(metrics_file)] if metrics_file is not None else []
    proc = start_receiver(
        "tributary.receivers.mllp", channel=channel, env=ENV, bootstrap=kafka, instance=instance, extra_args=extra
    )
    if wait:
        proc.wait_for_event("listening", timeout_s=60)
    return proc


def listening_port(proc: ManagedProcess) -> int:
    for line in proc.lines:
        if line.get("event") == "listening":
            return int(line["port"])
    raise AssertionError("receiver has not reported listening")


def start_worker(kafka: str, channel: str, instance: str = "sync-worker") -> ManagedProcess:
    return spawn_pipeline_worker(env=ENV, channel=channel, bootstrap=kafka, instance_id=instance, timeout_s=60)


def await_partitions_held(proc: ManagedProcess, timeout_s: float = 60.0) -> None:
    """The receiver's reply consumer holds a partition with an established position (its own log event)."""
    proc.wait_for_event("reply_partitions_held", timeout_s=timeout_s)


class Ack:
    """One MSA the sender received: MSA-1 code, MSA-2 control id, MSA-3 text."""

    def __init__(self, frame: bytes) -> None:
        parts = frame.split(b"\rMSA|")[1].split(b"\r")[0].split(b"|")
        self.code = parts[0].decode()
        self.control_id = parts[1].decode()
        self.text = parts[2].decode() if len(parts) > 2 else ""

    @property
    def reply_code(self) -> str:
        return self.text.split(" ")[0] if " " in self.text else ""

    @property
    def msg_id(self) -> str:
        return self.text.split(" ")[-1]


class Sender:
    """A persistent MLLP connection that sends one message and waits for its answer."""

    def __init__(self, port: int) -> None:
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=30)
        self.buffer = b""

    def send(self, control_id: str) -> None:
        self.sock.sendall(SB + message(control_id) + EB + CR)

    def read(self, timeout_s: float = 60.0) -> Ack | None:
        """The next ACK, or `None` when the peer closed without one."""
        self.sock.settimeout(timeout_s)
        while EB + CR not in self.buffer:
            chunk = self.sock.recv(65536)
            if not chunk:
                return None
            self.buffer += chunk
        frame, _, self.buffer = self.buffer.partition(EB + CR)
        return Ack(frame)

    def exchange(self, control_id: str, timeout_s: float = 60.0) -> Ack:
        self.send(control_id)
        ack = self.read(timeout_s)
        assert ack is not None, "the receiver closed without answering"
        return ack

    def close(self) -> None:
        self.sock.close()


def raw_records(kafka: str, channel: str, *, expected: int = 0) -> list[Any]:
    return drain_topic(kafka, raw_topic(ENV, channel), partitions=1, expected=expected, timeout_s=30)


def raw_ids(kafka: str, channel: str, *, expected: int = 0) -> set[str]:
    return {header_map(r)["tb-msg-id"] for r in raw_records(kafka, channel, expected=expected)}


def read_metrics(path: Path) -> dict[str, Any]:
    try:
        return dict(json.loads(path.read_text(encoding="utf-8")))
    except (OSError, ValueError):
        return {}


def wait_for(predicate: Any, *, timeout_s: float, what: str) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError(f"timed out waiting for {what}")
