"""W2-8 receiver side of synchronous reply mode, end to end on a real broker: a real MLLP receiver process, a real
pipeline worker process, the real `reply` topic and `replier` group.

`uv run pytest -m integration packages/tributary-receivers -k sync_reply` (Docker). Each test deploys its own
channel (own topics, own consumer groups).

What is proven here, from the sender's side of the socket:

* AA: the sender is answered `AA` with its own control id in MSA-2, only after the worker routed it, and the
  receiver's raw record is in `raw` with no control id and no record key in any header.
* Reject -> AE: a pipeline `Reject` is dead-lettered, then answered `AE` with the channel's code.
* Timeout -> AE `REPLY_TIMEOUT` when no worker answers, with the committed message in `raw`; a late reply after the
  timeout is dropped, never written to the sender.
* No reply partition -> `AR` `REPLY_UNAVAILABLE`, before anything is published (the raw topic is unchanged).
* A worker restart mid-request: requests waiting while the worker is down are answered `AA` after it returns,
  each exactly once.
"""

from __future__ import annotations

import statistics
import time
from pathlib import Path

import pytest
from tributary.runtime.topics import dlq_topic
from tributary.sdk.codes import REPLY_TIMEOUT, REPLY_UNAVAILABLE

from tests.support.kafka_helpers import drain_topic, header_map
from tests.support.processes import ManagedProcess

from .sync_reply_rig import (
    ENV,
    REJECTING_STEP,
    Sender,
    await_partitions_held,
    deploy,
    listening_port,
    raw_ids,
    raw_records,
    read_metrics,
    start_receiver_process,
    start_worker,
    wait_for,
)

pytestmark = pytest.mark.integration


@pytest.fixture
def procs() -> list[ManagedProcess]:
    started: list[ManagedProcess] = []
    yield started  # type: ignore[misc]
    for proc in started:
        proc.terminate_and_wait()


def serve(kafka: str, procs: list[ManagedProcess], channel: str, *, worker: bool = True) -> tuple[ManagedProcess, int]:
    receiver = start_receiver_process(kafka, channel, f"{channel}-rx")
    procs.append(receiver)
    await_partitions_held(receiver)
    if worker:
        procs.append(start_worker(kafka, channel, f"{channel}-wk"))
    return receiver, listening_port(receiver)


def test_sync_reply_aa_path_answers_after_routing_and_keeps_the_control_id_out_of_kafka(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "sr-aa"
    deploy(kafka, channel, reply={"timeoutMs": 20000})
    receiver, port = serve(kafka, procs, channel)
    sender = Sender(port)
    sent = [f"SYNTH-CTRL-{index:04d}" for index in range(25)]
    acks = [sender.exchange(control_id) for control_id in sent]
    sender.close()

    assert [(a.code, a.control_id) for a in acks] == [("AA", control_id) for control_id in sent]
    msg_ids = {a.msg_id for a in acks}
    assert len(msg_ids) == len(sent), "MSA-3 carries the engine's tb-msg-id, one per message"
    records = raw_records(kafka, channel, expected=len(sent))
    assert {header_map(r)["tb-msg-id"] for r in records} == msg_ids, "every AA'd message is in raw"
    for record in records:
        headers = header_map(record)
        assert {"tb-correlation-id", "tb-reply-partition", "tb-reply-deadline"} <= set(headers)
        assert not any(control_id in value for value in headers.values() for control_id in sent)
        assert record.key() is None
    receiver.collect_pending()
    logged = b"".join(receiver.raw)
    assert logged and b"SYNTH-CTRL-" not in logged and b"TESTPATIENT" not in logged, "no sender value in any log line"


def test_sync_reply_reject_is_dead_lettered_then_answered_ae_with_the_code(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "sr-reject"
    deploy(kafka, channel, reply={"timeoutMs": 20000}, steps=[REJECTING_STEP])
    _receiver, port = serve(kafka, procs, channel)
    sender = Sender(port)
    ack = sender.exchange("SYNTH-CTRL-REJECT")
    sender.close()

    assert (ack.code, ack.control_id) == ("AE", "SYNTH-CTRL-REJECT")
    assert ack.reply_code == "VALIDATE_SEGMENT_MISSING", "MSA-3 is <CODE> <tb-msg-id>"
    assert ack.msg_id in raw_ids(kafka, channel, expected=1), "AE means committed"
    dead = drain_topic(kafka, dlq_topic(ENV, channel), partitions=1, expected=1, timeout_s=30)
    assert [header_map(r)["tb-msg-id"] for r in dead] == [ack.msg_id]


def test_sync_reply_timeout_answers_ae_reply_timeout_and_a_late_reply_is_dropped(
    kafka: str, procs: list[ManagedProcess], tmp_path: Path
) -> None:
    channel = "sr-timeout"
    deploy(kafka, channel, reply={"timeoutMs": 1000})
    metrics = tmp_path / "rx.json"
    receiver = start_receiver_process(kafka, channel, f"{channel}-rx", metrics_file=metrics)
    procs.append(receiver)
    await_partitions_held(receiver)
    sender = Sender(listening_port(receiver))

    started = time.monotonic()
    ack = sender.exchange("SYNTH-CTRL-TIMEOUT")  # no worker is running
    elapsed = time.monotonic() - started
    assert (ack.code, ack.control_id) == ("AE", "SYNTH-CTRL-TIMEOUT")
    assert ack.reply_code == REPLY_TIMEOUT
    assert 0.9 <= elapsed < 3.0, f"timeoutMs counted from the raw callback ({elapsed:.2f}s)"
    assert ack.msg_id in raw_ids(kafka, channel, expected=1), "AE means committed"

    procs.append(start_worker(kafka, channel, f"{channel}-wk"))  # the worker now routes the committed message
    wait_for(
        lambda: read_metrics(metrics).get("replies_unmatched", 0) >= 1,
        timeout_s=60,
        what="the late reply to reach the receiver and be dropped",
    )
    sender.sock.settimeout(1.0)
    with pytest.raises(TimeoutError):
        sender.sock.recv(1)  # nothing further is written to the sender
    snapshot = read_metrics(metrics)
    assert snapshot["reply_timeouts"] == 1 and snapshot["reply_pending"] == 0
    sender.close()


def test_sync_reply_a_process_without_a_reply_partition_refuses_before_publishing(
    kafka: str, procs: list[ManagedProcess], tmp_path: Path
) -> None:
    """One reply partition, two receiver processes: one holds it, the other cannot take sync traffic."""
    channel = "sr-nopartition"
    deploy(kafka, channel, reply={"timeoutMs": 20000}, reply_partitions=1)
    files = [tmp_path / "a.json", tmp_path / "b.json"]
    receivers = [start_receiver_process(kafka, channel, f"{channel}-rx{i}", metrics_file=files[i]) for i in range(2)]
    procs.extend(receivers)
    procs.append(start_worker(kafka, channel, f"{channel}-wk"))

    def settled() -> bool:
        ready = sorted(read_metrics(path).get("reply_ready", -1) for path in files)
        return ready == [0, 1]

    wait_for(settled, timeout_s=90, what="one process to hold the only reply partition and the other none")
    holder, idle = (0, 1) if read_metrics(files[0]).get("reply_ready") == 1 else (1, 0)

    before = raw_ids(kafka, channel)
    refused = Sender(listening_port(receivers[idle]))
    ack = refused.exchange("SYNTH-CTRL-NOPART")
    refused.close()
    assert (ack.code, ack.control_id, ack.text) == ("AR", "SYNTH-CTRL-NOPART", REPLY_UNAVAILABLE)
    assert raw_ids(kafka, channel) == before, "nothing was published for the refused request"

    served = Sender(listening_port(receivers[holder]))
    assert served.exchange("SYNTH-CTRL-HELD").code == "AA"
    served.close()
    wait_for(
        lambda: read_metrics(files[idle]).get("nacked_reply_unavailable", 0) == 1,
        timeout_s=15,
        what="the refusal to be counted",
    )


def test_sync_reply_requests_waiting_through_a_worker_restart_are_answered_once_each(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "sr-restart"
    deploy(kafka, channel, reply={"timeoutMs": 60000})
    receiver, port = serve(kafka, procs, channel, worker=False)
    worker = start_worker(kafka, channel, f"{channel}-wk")
    warm = Sender(port)
    assert warm.exchange("SYNTH-CTRL-WARM").code == "AA"

    worker.kill_and_wait()  # kill -9: its group session is still live for a while
    senders = [Sender(port) for _ in range(10)]
    controls = [f"SYNTH-CTRL-WAIT-{index:02d}" for index in range(10)]
    for sender, control_id in zip(senders, controls, strict=True):
        sender.send(control_id)
    wait_for(lambda: len(raw_ids(kafka, channel)) >= 11, timeout_s=30, what="the waiting requests to be committed")
    time.sleep(1.0)
    procs.append(start_worker(kafka, channel, f"{channel}-wk"))  # same static id: takes its partition back

    answers = [sender.read(timeout_s=90) for sender in senders]
    assert all(ack is not None for ack in answers)
    assert [(a.code, a.control_id) for a in answers if a] == [("AA", c) for c in controls]
    for sender in senders:
        sender.sock.settimeout(1.0)
        with pytest.raises(TimeoutError):
            sender.sock.recv(1)  # exactly one answer each, even though the worker replayed its uncommitted window
        sender.close()
    warm.close()
    del receiver


def test_sync_reply_aa_round_trip_latency_is_reported(kafka: str, procs: list[ManagedProcess], tmp_path: Path) -> None:
    """Not a gate. p50 / p99 of the sender-observed AA round trip, five senders at a modest rate (about 50 msg/s
    in total). The numbers are printed (run with `-s`) and attached to the report as a user property."""
    channel = "sr-latency"
    deploy(kafka, channel, reply={"timeoutMs": 20000})
    metrics = tmp_path / "latency.json"
    receiver = start_receiver_process(kafka, channel, f"{channel}-rx", metrics_file=metrics)
    procs.append(receiver)
    await_partitions_held(receiver)
    procs.append(start_worker(kafka, channel, f"{channel}-wk"))
    port = listening_port(receiver)
    warm = Sender(port)
    for index in range(10):
        assert warm.exchange(f"SYNTH-WARM-{index}").code == "AA"
    warm.close()

    import threading

    samples: list[float] = []
    lock = threading.Lock()
    failures: list[str] = []

    def run(sender_index: int) -> None:
        sender = Sender(port)
        for index in range(60):
            started = time.perf_counter()
            ack = sender.exchange(f"SYNTH-LAT-{sender_index}-{index}")
            elapsed = (time.perf_counter() - started) * 1000
            with lock:
                samples.append(elapsed)
                if ack.code != "AA":
                    failures.append(ack.code)
            time.sleep(0.08)  # think time: five senders -> roughly 50 messages per second
        sender.close()

    threads = [threading.Thread(target=run, args=(i,)) for i in range(5)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert failures == []
    ordered = sorted(samples)
    p50 = statistics.median(ordered)
    p99 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]
    wait_for(
        lambda: read_metrics(metrics).get("ack_latency_ms_count", 0) >= 310, timeout_s=15, what="the receiver's samples"
    )
    raw = read_metrics(metrics)
    slow = sum(1 for value in ordered if value > 200)
    print(
        f"\nSYNC_REPLY_LATENCY n={len(ordered)} p50_ms={p50:.1f} p99_ms={p99:.1f} max_ms={ordered[-1]:.1f} "
        f"over_200ms={slow} | receiver raw-commit p50_ms={raw['ack_latency_ms_p50']:.1f} "
        f"p99_ms={raw['ack_latency_ms_p99']:.1f}"
    )
    assert len(ordered) == 300
