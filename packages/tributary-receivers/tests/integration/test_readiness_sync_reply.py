"""W3-17 for a synchronous-reply MLLP receiver (`reply_consumer` check) on a real broker.

* A process that holds no reply partition is 503 `reply_consumer: no_partition`; the one that holds it is 200.
* A reply consumer fenced by a duplicate `group.instance.id` goes `_FATAL`: the process logs it and exits 1.
* A broker outage (a private broker, stopped and started): `/livez` stays 200 and answers at once, `/readyz` is 503
  with the producer `all_brokers_down` and the reply consumer `disconnected`, and both recover when it returns.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from confluent_kafka import Consumer
from testcontainers.core.container import DockerContainer

from tests.support.processes import ManagedProcess

from .readiness_rig import await_ready, get, ops_port, readyz, reasons
from .sync_reply_rig import ENV, Sender, deploy, listening_port, start_receiver_process

pytestmark = pytest.mark.integration


@pytest.fixture
def procs() -> Iterator[list[ManagedProcess]]:
    started: list[ManagedProcess] = []
    yield started
    for proc in started:
        proc.terminate_and_wait()


def sync_ready(status: int, doc: dict[str, Any]) -> bool:
    return status == 200 and reasons(doc).get("reply_consumer", "absent") is None


def test_a_process_without_a_reply_partition_is_not_ready_and_the_holder_is(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "rdy-sr-holder"
    deploy(kafka, channel, reply={"timeoutMs": 20000}, reply_partitions=1)
    receivers = [start_receiver_process(kafka, channel, f"{channel}-rx{i}", wait=False) for i in range(2)]
    procs.extend(receivers)
    ports = [ops_port(r) for r in receivers]

    def holds(port: int) -> bool:
        status, doc = readyz(port)
        return sync_ready(status, doc)

    deadline = time.monotonic() + 90
    while time.monotonic() < deadline and sum(holds(p) for p in ports) < 1:
        time.sleep(0.2)
    time.sleep(1.0)  # the other member has had its rebalance
    answers = [readyz(p) for p in ports]
    holder = [i for i, (s, d) in enumerate(answers) if sync_ready(s, d)]
    idle = [i for i in range(2) if i not in holder]
    assert len(holder) == 1 and len(idle) == 1, answers
    status, doc = answers[idle[0]]
    assert status == 503 and doc["sync_reply"] is True
    assert reasons(doc)["reply_consumer"] == "no_partition"
    assert reasons(doc)["spec"] is None and reasons(doc)["raw_producer"] is None  # only the reply check fails
    status, doc = answers[holder[0]]
    assert doc["sync_reply"] is True and [c["name"] for c in doc["checks"]] == [
        "spec",
        "component",
        "raw_producer",
        "reply_consumer",
    ]
    # The ready one is listening, as its probe says.
    assert receivers[holder[0]].wait_for_event("listening", timeout_s=30)["port"] > 0


def test_a_fenced_reply_consumer_is_fatal_the_process_logs_it_and_exits_nonzero(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "rdy-sr-fenced"
    deploy(kafka, channel, reply={"timeoutMs": 20000})
    receiver = start_receiver_process(kafka, channel, f"{channel}-rx", wait=False)
    procs.append(receiver)
    port = ops_port(receiver)
    await_ready(port, sync_ready, "the reply partitions to be held")

    # A second member of the replier group with the receiver's own `<instance>-p<pid>` fences it.
    fencer = Consumer(
        {
            "bootstrap.servers": kafka,
            "group.id": f"tb.{ENV}.{channel}.replier",
            "group.instance.id": f"{channel}-rx-p{receiver.process.pid}",
            "partition.assignment.strategy": "cooperative-sticky",  # the group's protocol, or the join is refused
            "auto.offset.reset": "latest",
        }
    )
    try:
        fencer.subscribe([f"{ENV}.{channel}.reply"])
        deadline = time.monotonic() + 90
        while receiver.process.poll() is None and time.monotonic() < deadline:
            fencer.poll(0.5)
        assert receiver.process.poll() == 1, "a fenced replier exits 1 so a supervisor restarts it"
    finally:
        fencer.close()
    receiver.collect_pending()
    events = [line.get("event") for line in receiver.lines]
    assert "reply_consumer_fatal" in events and "kafka_client_fatal" in events


def test_a_broker_outage_is_not_ready_while_livez_stays_up_and_recovery_restores_it(
    private_kafka: tuple[str, DockerContainer], procs: list[ManagedProcess]
) -> None:
    bootstrap, container = private_kafka
    channel = "rdy-sr-outage"
    deploy(bootstrap, channel, reply={"timeoutMs": 20000})
    receiver = start_receiver_process(bootstrap, channel, f"{channel}-rx", wait=False)
    procs.append(receiver)
    port = ops_port(receiver)
    await_ready(port, sync_ready, "ready on a healthy broker", timeout_s=90)
    receiver.wait_for_event("listening", timeout_s=30)
    sender = Sender(listening_port(receiver))  # an admitted sender; it must not make /livez depend on Kafka
    try:
        container.get_wrapped_container().stop(timeout=5)
        doc = await_ready(
            port,
            lambda s, d: s == 503 and reasons(d)["raw_producer"] == "all_brokers_down",
            "the producer to report every broker down",
            timeout_s=120,
        )
        assert reasons(doc)["spec"] is None  # a channels outage never gates (R3), and neither does a lost broker here
        doc = await_ready(
            port, lambda s, d: reasons(d)["reply_consumer"] == "disconnected", "the reply consumer to disconnect"
        )
        # /livez never touches Kafka: it answers promptly with the broker gone (R-liveness).
        started = time.monotonic()
        assert get(port, "/livez") == (200, {"v": 0, "status": "alive", "component": "receiver"})
        assert time.monotonic() - started < 1.0
        started = time.monotonic()
        readyz(port)
        assert time.monotonic() - started < 1.0, "/readyz makes no Kafka call on the request path"

        container.get_wrapped_container().start()
        await_ready(port, sync_ready, "ready again after the broker returned", timeout_s=180)
    finally:
        sender.close()
