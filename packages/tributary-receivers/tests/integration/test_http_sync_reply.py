"""W3-19 HTTP synchronous reply on a real broker: a real HTTP receiver process, a real pipeline worker process, the
real `reply` topic and `replier` group (channel-spec README 3.2.6, "What the sender gets (HTTP)").

`uv run pytest -m integration packages/tributary-receivers -k http_sync_reply` (Docker). Each test deploys its own
channel. Proven from the sender's side of the socket:

* AA is `200` only after the worker routed the message; the `raw` record carries the reply route and no sender value.
* Reject is `202` with the channel's code and body status `rejected`, dead-lettered; the message is in `raw`.
* No reply by the deadline is `202` `REPLY_TIMEOUT` (message committed); a late reply is dropped.
* No reply partition, and a full pending map, are `503` + `Retry-After` before anything is published.
* A sender that disconnects frees its slot; its message is still processed.
* No committed path ever answers a status that clients or proxies retry on their own.

Synthetic data only.
"""

from __future__ import annotations

import http.client
import json
import socket
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka.admin import NewTopic
from tributary.runtime.topics import dlq_topic, events_topic, out_topic, raw_topic, reply_topic

from tests.support.kafka_helpers import create_topics_with_retry, drain_topic, header_map
from tests.support.mllp_receiver import ensure_channels_topic, publish_channel_spec, start_receiver
from tests.support.processes import ManagedProcess
from tests.support.receiver_specs import http_document

from .sync_reply_rig import (
    ENV,
    REJECTING_STEP,
    await_partitions_held,
    listening_port,
    raw_ids,
    raw_records,
    read_metrics,
    start_worker,
    wait_for,
)

pytestmark = pytest.mark.integration

DESTINATION = "stub"
PAYLOAD = json.dumps({"resourceType": "Patient", "id": "SYNTH-HTTP-SYNC"}).encode()
IDEM_KEY = "SYNTH-IDEM-KEY-5b27"


@pytest.fixture
def procs() -> list[ManagedProcess]:
    started: list[ManagedProcess] = []
    yield started  # type: ignore[misc]
    for proc in started:
        proc.terminate_and_wait()


def deploy(
    kafka: str,
    channel: str,
    *,
    reply: dict[str, Any],
    reply_partitions: int = 3,
    steps: Sequence[dict[str, Any]] | None = None,
) -> None:
    ensure_channels_topic(kafka, ENV)
    create_topics_with_retry(
        kafka,
        [
            NewTopic(raw_topic(ENV, channel), 1, 1),
            NewTopic(out_topic(ENV, channel, DESTINATION), 1, 1),
            NewTopic(dlq_topic(ENV, channel), 1, 1),
            NewTopic(events_topic(ENV), 1, 1),
            NewTopic(reply_topic(ENV, channel), reply_partitions, 1),
        ],
    )
    document = http_document(channel, reply=reply)
    if steps is not None:
        document["spec"]["pipeline"]["steps"] = list(steps)
    publish_channel_spec(kafka, ENV, channel, document)


def start_http(
    kafka: str, procs: list[ManagedProcess], channel: str, instance: str, *, metrics: Path | None = None
) -> ManagedProcess:
    extra = ["--metrics-file", str(metrics)] if metrics is not None else []
    proc = start_receiver(
        "tributary.receivers.http", channel=channel, env=ENV, bootstrap=kafka, instance=instance, extra_args=extra
    )
    procs.append(proc)
    proc.wait_for_event("listening", timeout_s=60)
    await_partitions_held(proc)
    return proc


def post(port: int, *, timeout_s: float = 60.0) -> tuple[int, dict[str, str], dict[str, Any]]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout_s)
    try:
        conn.request(
            "POST",
            "/ingest",
            body=PAYLOAD,
            headers={"Content-Type": "application/fhir+json", "Idempotency-Key": IDEM_KEY},
        )
        response = conn.getresponse()
        headers = {k.lower(): v for k, v in response.getheaders()}
        return response.status, headers, dict(json.loads(response.read()))
    finally:
        conn.close()


def assert_committed(status: int) -> None:
    """The binding constraint (README 3.2.6): a committed outcome is never a status clients retry on their own."""
    assert status in (200, 202), f"committed path answered {status}"


def test_http_sync_reply_aa_is_200_after_routing_and_the_raw_record_has_the_route_and_no_sender_value(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "hsr-aa"
    deploy(kafka, channel, reply={"timeoutMs": 20000})
    receiver = start_http(kafka, procs, channel, f"{channel}-rx")
    procs.append(start_worker(kafka, channel, f"{channel}-wk"))
    port = listening_port(receiver)

    answers = [post(port) for _ in range(10)]
    assert [a[0] for a in answers] == [200] * 10
    msg_ids = {a[2]["tb-msg-id"] for a in answers}
    assert len(msg_ids) == 10
    for status, headers, doc in answers:
        assert_committed(status)
        assert doc["status"] == "accepted" and "code" not in doc and "tb-reply-code" not in headers
        assert headers["tb-msg-id"] == doc["tb-msg-id"] and headers["cache-control"] == "no-store"
        assert headers["content-type"] == "application/json"
    records = raw_records(kafka, channel, expected=10)
    assert {header_map(r)["tb-msg-id"] for r in records} == msg_ids, "every 200 is a committed message"
    for record in records:
        headers = header_map(record)
        assert {"tb-correlation-id", "tb-reply-partition", "tb-reply-deadline"} <= set(headers)
        assert not any(IDEM_KEY in value for value in headers.values()), "Idempotency-Key is never forwarded"
    receiver.collect_pending()
    logged = b"".join(receiver.raw)
    assert logged and IDEM_KEY.encode() not in logged and b"SYNTH-HTTP-SYNC" not in logged


def test_http_sync_reply_reject_is_202_rejected_with_the_code_and_dead_lettered(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "hsr-reject"
    deploy(kafka, channel, reply={"timeoutMs": 20000}, steps=[REJECTING_STEP])
    receiver = start_http(kafka, procs, channel, f"{channel}-rx")
    procs.append(start_worker(kafka, channel, f"{channel}-wk"))

    status, headers, doc = post(listening_port(receiver))
    assert_committed(status)
    assert status == 202 and doc["status"] == "rejected"
    assert headers["tb-reply-code"] == doc["code"] and doc["code"].startswith("VALIDATE_")
    assert doc["tb-msg-id"] in raw_ids(kafka, channel, expected=1), "202 means committed"
    dead = drain_topic(kafka, dlq_topic(ENV, channel), partitions=1, expected=1, timeout_s=30)
    assert [header_map(r)["tb-msg-id"] for r in dead] == [doc["tb-msg-id"]]


def test_http_sync_reply_timeout_is_202_reply_timeout_and_a_late_reply_is_dropped(
    kafka: str, procs: list[ManagedProcess], tmp_path: Path
) -> None:
    channel = "hsr-timeout"
    deploy(kafka, channel, reply={"timeoutMs": 1000})
    metrics = tmp_path / "rx.json"
    receiver = start_http(kafka, procs, channel, f"{channel}-rx", metrics=metrics)

    started = time.monotonic()
    status, headers, doc = post(listening_port(receiver))  # no worker is running
    elapsed = time.monotonic() - started
    assert_committed(status)
    assert (status, doc["status"], headers["tb-reply-code"]) == (202, "unknown", "REPLY_TIMEOUT")
    assert 0.9 <= elapsed < 3.0, f"timeoutMs counted from the raw callback ({elapsed:.2f}s)"
    assert doc["tb-msg-id"] in raw_ids(kafka, channel, expected=1), "202 means committed"

    procs.append(start_worker(kafka, channel, f"{channel}-wk"))  # the worker now routes the committed message
    wait_for(
        lambda: read_metrics(metrics).get("replies_unmatched", 0) >= 1,
        timeout_s=60,
        what="the late reply to reach the receiver and be dropped",
    )
    snapshot = read_metrics(metrics)
    assert snapshot["reply_timeouts"] == 1 and snapshot["reply_pending"] == 0


def test_http_sync_reply_a_process_without_a_reply_partition_is_503_before_publishing(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "hsr-nopart"
    deploy(kafka, channel, reply={"timeoutMs": 5000}, reply_partitions=1)
    first = start_http(kafka, procs, channel, f"{channel}-rx1")  # takes the only reply partition
    second = start_receiver(
        "tributary.receivers.http", channel=channel, env=ENV, bootstrap=kafka, instance=f"{channel}-rx2"
    )
    procs.append(second)
    second.wait_for_event("listening", timeout_s=60)
    del first

    status, headers, doc = post(listening_port(second))
    assert status == 503 and int(headers["retry-after"]) >= 1
    assert (doc["status"], doc["code"], headers["tb-reply-code"]) == (
        "not_committed",
        "REPLY_UNAVAILABLE",
        "REPLY_UNAVAILABLE",
    )
    assert "tb-msg-id" not in doc, "nothing was published, so there is no message to name"
    assert raw_ids(kafka, channel, expected=0) == set(), "the raw topic is unchanged"


def test_http_sync_reply_a_full_pending_map_is_503_reply_busy_and_the_first_request_is_unaffected(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "hsr-busy"
    deploy(kafka, channel, reply={"timeoutMs": 4000, "maxPending": 1})
    receiver = start_http(kafka, procs, channel, f"{channel}-rx")  # no worker: the first request stays pending
    port = listening_port(receiver)

    first: list[tuple[int, dict[str, str], dict[str, Any]]] = []
    thread = threading.Thread(target=lambda: first.append(post(port)))
    thread.start()
    wait_for(lambda: len(raw_ids(kafka, channel, expected=0)) == 1, timeout_s=30, what="the first request in raw")

    status, headers, doc = post(port)
    assert status == 503 and int(headers["retry-after"]) >= 1
    assert (doc["status"], doc["code"]) == ("not_committed", "REPLY_BUSY")
    assert len(raw_ids(kafka, channel, expected=0)) == 1, "the refused request was not published"

    thread.join(timeout=30)
    assert first, "the first request was answered"
    first_status, first_headers, _ = first[0]
    assert_committed(first_status)
    assert (first_status, first_headers["tb-reply-code"]) == (202, "REPLY_TIMEOUT")


def test_http_sync_reply_a_disconnecting_sender_frees_its_slot_and_its_message_is_still_processed(
    kafka: str, procs: list[ManagedProcess]
) -> None:
    channel = "hsr-gone"
    deploy(kafka, channel, reply={"timeoutMs": 1000, "maxPending": 1})
    receiver = start_http(kafka, procs, channel, f"{channel}-rx")  # no worker
    port = listening_port(receiver)

    sock = socket.create_connection(("127.0.0.1", port), timeout=10)
    head = (
        f"POST /ingest HTTP/1.1\r\nHost: x\r\nContent-Type: application/fhir+json\r\nContent-Length: {len(PAYLOAD)}\r\n\r\n"
    ).encode()
    sock.sendall(head + PAYLOAD)
    wait_for(lambda: len(raw_ids(kafka, channel, expected=0)) == 1, timeout_s=30, what="the request in raw")
    sock.close()  # the sender gives up while its entry is pending (maxPending is 1)

    # The slot is free again at once: the next request is admitted (it times out as AE, it is not REPLY_BUSY).
    status, headers, doc = post(port)
    assert status == 202 and headers["tb-reply-code"] == "REPLY_TIMEOUT", doc
    assert len(raw_ids(kafka, channel, expected=0)) == 2, "the abandoned message was still processed"
