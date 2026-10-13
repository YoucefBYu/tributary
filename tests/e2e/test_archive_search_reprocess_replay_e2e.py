"""W3-9b: the archive path end to end, with every process real.

A synthetic HL7 message goes in over MLLP (a real receiver), through a real pipeline worker, and its events and
content are archived into ClickHouse by the real events archiver and the real content archiver. The real API
process then answers from that archive, authenticated with a signed bearer token:

* **Search** finds the message by `tb-msg-id` (the id the receiver put on `raw`), and the message detail shows its
  stages (`transformed`, `routed`: the receiver emits no `received` event today, so that stage exists only as stored
  content, from `raw`).
* **Content view** (content storage on for the channel) returns the stored bytes (compared by SHA-256, never by
  text) and emits a `message.content.view` audit record on `_tb.<env>.audit` that names the actor, the roles and the
  message and carries no payload bytes. Note: no process yet moves `_tb.<env>.audit` into ClickHouse `audit_log`
  (the contract names the events archiver for that, W3-30 part 2), so the audit assertion is on the topic.
* **Reprocess** re-ingests the message under a new `tb-msg-id` whose `tb-parent-id` is the original, and the
  worker processes it (seen on the SSE stream and in the detail).
* **Replay** over a time window re-emits exactly the records inside the window, refuses an `ordering: per-key`
  channel without `allow_reorder`, publishes nothing when refused or bounded, is idempotent on `replay_id`, and
  preserves each key's order among the replayed records and keeps each key on one partition.

The SSE stream (`GET /v1/environments/{env}/events`) is how the new messages are observed. There is **no cancel
operation** in `openapi/v0` (the replay record is `running`, `completed` or `failed`), so cancellation is not
tested; the bounds that do exist (`max_records`, the ordering refusal) are.

Synthetic data only: MSH-3 `TRIBUTARY-SYNTH`, ids and counts in every assertion message, never payload text.
The tests share one stack and run in file order (reprocess and replay add records to `raw`).
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import socket
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka.admin import NewTopic
from testcontainers.core.container import DockerContainer
from tributary.runtime.topics import channels_topic, dlq_topic, events_topic, out_topic, platform_topic, raw_topic
from tributary.sdk.ids import uuid7

from tests.support.archive_e2e import ArchiveStack, Sse, wait_until
from tests.support.kafka_helpers import await_broker, create_topics_with_retry, drain_topic, free_port, header_map

pytestmark = pytest.mark.integration

ENV = "w9b"
CHANNEL = "w9b-adt"
PARTITIONS = 3
KEYS = ["SYNTH-P0", "SYNTH-P1", "SYNTH-P2"]
PER_KEY = 3
SB, EB, CR = b"\x0b", b"\x1c", b"\x0d"
RAW = raw_topic(ENV, CHANNEL)
AUDIT = platform_topic(ENV, "audit")
BASE = f"/v1/environments/{ENV}/channels/{CHANNEL}"


def stamp(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def hl7(control_id: str, key: str) -> bytes:
    return (
        f"MSH|^~\\&|TRIBUTARY-SYNTH|SPIKE|||20260101000000||ADT^A01|{control_id}|P|2.5\r"
        f"PID|1||{key}^^^SPIKE||TESTPATIENT^SYNTHETIC||19700101|U\r"
    ).encode()


def send(port: int, payload: bytes) -> bool:
    with socket.create_connection(("127.0.0.1", port), timeout=30) as sock:
        sock.sendall(SB + payload + EB + CR)
        buf = b""
        while not buf.endswith(EB + CR):
            chunk = sock.recv(4096)
            if not chunk:
                return False
            buf += chunk
    return b"MSA|AA" in buf


def _strings(value: object) -> Iterator[str]:
    """Every string in a decoded JSON value, keys and values, at any depth."""
    if isinstance(value, str):
        yield value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield str(key)
            yield from _strings(item)
    elif isinstance(value, list):
        for item in value:
            yield from _strings(item)


def control_id_of(payload: bytes) -> str:
    return payload.split(b"\r")[0].split(b"|")[9].decode()


@dataclass
class Journey:
    stack: ArchiveStack
    kafka: str
    started_at: datetime
    originals_end: datetime
    sent: dict[str, bytes] = field(default_factory=dict)  # control id -> payload
    ids: dict[str, str] = field(default_factory=dict)  # control id -> tb-msg-id
    key_of: dict[str, str] = field(default_factory=dict)  # tb-msg-id -> key
    seq_of: dict[str, int] = field(default_factory=dict)  # tb-msg-id -> sequence within its key
    reprocessed: str | None = None

    def raw_records(self) -> list:  # type: ignore[type-arg]
        """Every record on `raw`, read to end-of-file on all partitions (never to an expected count)."""
        return drain_topic(self.kafka, RAW, partitions=PARTITIONS, timeout_s=30)

    def settled_raw_records(self) -> list:  # type: ignore[type-arg]
        """As `raw_records`, after a pause, so a late duplicate publish would be counted."""
        time.sleep(1.5)
        return self.raw_records()


@pytest.fixture(scope="module")
def kafka() -> Iterator[str]:
    port = free_port()
    container = (
        DockerContainer("apache/kafka:4.0.0")
        .with_name(f"tb-qa-w39b-kafka-{os.getpid()}")
        .with_env("KAFKA_NODE_ID", "1")
        .with_env("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env("KAFKA_LISTENERS", f"PLAINTEXT://0.0.0.0:{port},CONTROLLER://0.0.0.0:9093")
        .with_env("KAFKA_ADVERTISED_LISTENERS", f"PLAINTEXT://127.0.0.1:{port}")
        .with_env("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .with_env("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .with_env("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .with_env("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .with_env("CLUSTER_ID", "qa-w39b-archive-e2e")
        .with_bind_ports(port, port)
    )
    container.start()
    bootstrap = f"127.0.0.1:{port}"
    try:
        await_broker(bootstrap)
        yield bootstrap
    finally:
        container.stop()


@pytest.fixture(scope="module")
def journey(kafka: str, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Journey]:
    create_topics_with_retry(
        kafka,
        [
            NewTopic(RAW, PARTITIONS, 1),
            NewTopic(out_topic(ENV, CHANNEL, "stub"), PARTITIONS, 1),
            NewTopic(dlq_topic(ENV, CHANNEL), 1, 1),
            NewTopic(events_topic(ENV), 1, 1),
            NewTopic(AUDIT, 1, 1),
            NewTopic(channels_topic(ENV), 1, 1, config={"cleanup.policy": "compact"}),
        ],
    )
    stack = ArchiveStack(tmp_path_factory.mktemp("w9b"), kafka, env=ENV, channel=CHANNEL, run="w9b")
    try:
        stack.start_all()
        started_at = datetime.now(UTC) - timedelta(seconds=2)
        sent: dict[str, bytes] = {}
        key_seq: dict[str, tuple[str, int]] = {}
        for seq in range(PER_KEY):
            for key in KEYS:
                control = f"SYNTH-W9B-{key[-1]}-{seq}"
                payload = hl7(control, key)
                assert send(stack.mllp_port, payload), f"message {control} was not answered AA"
                sent[control] = payload
                key_seq[control] = (key, seq)
        originals_end = datetime.now(UTC)
        total = len(sent)
        records = drain_topic(kafka, RAW, partitions=PARTITIONS, expected=total, timeout_s=60)
        assert len(records) == total
        j = Journey(stack, kafka, started_at, originals_end, sent=sent)
        for record in records:
            control = control_id_of(record.value())
            msg_id = header_map(record)["tb-msg-id"]
            j.ids[control] = msg_id
            j.key_of[msg_id], j.seq_of[msg_id] = key_seq[control]
        # Wait for the archive to hold every message's `transformed` and `routed` events (what the worker emits).
        with stack.client() as http:

            def archived() -> bool:
                page = http.post(f"{BASE}/messages/search", json={"since": stamp(started_at), "limit": 200})
                if page.status_code != 200:
                    return False
                seen = {(i["message_id"], i["stage"]) for i in page.json()["items"]}
                return all((m, s) in seen for m in j.ids.values() for s in ("transformed", "routed"))

            try:
                wait_until(archived, "every message's events in the archive", timeout_s=90, every_s=1.0)
            except AssertionError:
                assert stack.clickhouse is not None
                counts = stack.clickhouse.query(
                    "SELECT stage, count() FROM test.message_events GROUP BY stage FORMAT TSV"
                )
                last = http.post(f"{BASE}/messages/search", json={"since": stamp(started_at), "limit": 200})
                events = [
                    (name, line.get("event"), line.get("code") or line.get("error"))
                    for name, proc in stack.procs.items()
                    for line in proc.collect_pending()
                    if line.get("level") in ("error", "warning")
                ]
                pytest.fail(
                    f"archive incomplete: stages {counts!r}; search {last.status_code}; process errors {events[:8]}"
                )
        assert stack.clickhouse is not None
        wait_until(
            lambda: int(stack.clickhouse.query("SELECT count() FROM test.message_content FINAL")) >= 2 * total,  # type: ignore[union-attr]
            "every message's content in the archive",
            timeout_s=90,
            every_s=1.0,
        )
        time.sleep(1.2)  # anything republished from here on has a record timestamp after `originals_end`
        yield j
    finally:
        stack.teardown()


def test_search_finds_every_message_by_its_tb_msg_id_and_the_detail_shows_its_stages(journey: Journey) -> None:
    ids = set(journey.ids.values())
    with journey.stack.client() as http:
        found: set[str] = set()
        cursor: str | None = None
        for _ in range(10):
            body: dict[str, object] = {"since": stamp(journey.started_at), "stages": ["routed"], "limit": 5}
            if cursor:
                body["cursor"] = cursor
            page = http.post(f"{BASE}/messages/search", json=body)
            assert page.status_code == 200, page.status_code
            data = page.json()
            found |= {item["message_id"] for item in data["items"]}
            cursor = data.get("next_cursor")
            if not cursor:
                break
        assert ids <= found, f"{len(ids - found)} of {len(ids)} ids missing from search"
        one = next(iter(ids))
        detail = http.get(f"{BASE}/messages/{one}")
        assert detail.status_code == 200
        stages = {event["stage"] for event in detail.json()["events"]}
        assert {"transformed", "routed"} <= stages
        assert detail.json()["message_id"] == one
        assert http.get(f"{BASE}/messages/{uuid.uuid4()}").status_code == 404


def test_content_view_returns_the_stored_bytes_and_writes_an_audit_record(journey: Journey) -> None:
    control, message_id = next(iter(journey.ids.items()))
    with journey.stack.client() as http:
        view = http.get(f"{BASE}/messages/{message_id}/content", params={"stage": "received"})
        assert view.status_code == 200, view.status_code
        body = view.json()
        assert body["storage"] == "inline" and body["message_id"] == message_id
        stored = base64.b64decode(body["payload_base64"])
        assert hashlib.sha256(stored).hexdigest() == hashlib.sha256(journey.sent[control]).hexdigest()
        assert body["size_bytes"] == len(journey.sent[control])

    def audit_records() -> list[dict[str, object]]:
        return [
            json.loads(r.value())
            for r in drain_topic(journey.kafka, AUDIT, partitions=1, timeout_s=3)
            if r.value() and b"message.content.view" in r.value()
        ]

    wait_until(lambda: any(r.get("message_id") == message_id for r in audit_records()), "the content-view audit record")
    record = next(r for r in audit_records() if r.get("message_id") == message_id)
    assert record["action"] == "message.content.view" and record["phase"] == "requested"
    assert record["actor"] == "synth-operator-w9b" and "admin" in record["actor_roles"]  # type: ignore[operator]
    assert record["channel"] == CHANNEL and record["stage"] == "received" and record["environment"] == ENV
    tokens = (control, "TESTPATIENT", "SYNTHETIC", journey.key_of[message_id], body["payload_base64"])
    strings = list(_strings(record))
    assert strings, "the audit record has no string values to check"
    leaked = [t for t in tokens for value in strings if t in value]
    assert not leaked, f"the audit record carries {len(leaked)} payload-derived value(s)"
    # The check can fail: the same tokens are found in a record that does carry the payload.
    assert any(t in v for t in tokens for v in _strings({"payload": journey.sent[control].decode()}))


def test_reprocess_reingests_under_a_new_id_linked_to_the_original_and_the_stream_shows_it(journey: Journey) -> None:
    control = "SYNTH-W9B-0-1"
    original = journey.ids[control]
    operation = uuid7()
    with journey.stack.client() as http:
        with Sse(
            journey.stack.base, f"/v1/environments/{ENV}/events?channel={CHANNEL}", journey.stack.tokens.bearer()
        ) as sse:
            refused = http.post(f"{BASE}/messages/{original}/reprocess", json={"operation_id": operation})
            assert refused.status_code == 409 and refused.json()["code"] == "ORDERING_WOULD_BREAK"
            done = http.post(
                f"{BASE}/messages/{original}/reprocess", json={"operation_id": operation, "allow_reorder": True}
            )
            assert done.status_code == 200, done.status_code
            result = done.json()
            new_id = result["new_message_id"]
            assert result["original_message_id"] == original and new_id != original
            journey.reprocessed = new_id
            wait_until(lambda: new_id in sse.message_ids(), "the reprocessed message on the event stream", timeout_s=60)
            event = next(d for e, d in list(sse.events) if e == "message" and d["message_id"] == new_id)
            assert event.get("parent_id") == original
        # The record on raw carries the link in its headers, and the same operation id yields the same new id.
        linked = [r for r in journey.raw_records() if header_map(r)["tb-msg-id"] == new_id]
        assert len(linked) == 1 and header_map(linked[0]).get("tb-parent-id") == original
        key = journey.key_of[original]
        assert linked[0].key() == key.encode()
        again = http.post(
            f"{BASE}/messages/{original}/reprocess", json={"operation_id": operation, "allow_reorder": True}
        )
        assert again.status_code == 200 and again.json()["new_message_id"] == new_id
        detail = http.get(f"{BASE}/messages/{new_id}")
        assert detail.status_code == 200 and detail.json().get("parent_id") == original


def test_replay_over_a_window_reemits_exactly_the_window_and_keeps_each_keys_order(journey: Journey) -> None:
    originals = set(journey.ids.values())
    before_records = journey.raw_records()
    before = {header_map(r)["tb-msg-id"] for r in before_records}
    # The only repeated id is the reprocess retried with the same operation id: it republishes the same id (at-least-once;
    # loader idempotency on tb-msg-id absorbs it, invariant 4), so it is on raw twice.
    ids_on_raw = [header_map(r)["tb-msg-id"] for r in before_records]
    assert len(ids_on_raw) == len(before) + 1 and ids_on_raw.count(journey.reprocessed) == 2
    assert originals <= before and journey.reprocessed in before
    window = {"start": stamp(journey.started_at), "end": stamp(journey.originals_end + timedelta(milliseconds=500))}
    replay_id = uuid7()
    with journey.stack.client() as http:
        # Refused without allow_reorder on a per-key channel: nothing is published.
        refused = http.post(f"{BASE}/replays", json={"replay_id": uuid7(), **window})
        assert refused.status_code == 409 and refused.json()["code"] == "ORDERING_WOULD_BREAK"
        assert len(journey.settled_raw_records()) == len(before_records), "a refused replay published records"
        # Bounded: a span larger than max_records publishes nothing.
        small = http.post(
            f"{BASE}/replays", json={"replay_id": uuid7(), **window, "allow_reorder": True, "max_records": 2}
        )
        assert small.status_code == 202, small.status_code  # accepted, then fails after the audit record (README 3.14)
        replay = small.json()
        wait_until(
            lambda: http.get(f"{BASE}/replays/{replay['replay_id']}").json()["status"] != "running",
            "the bounded replay to finish",
            timeout_s=60,
        )
        final = http.get(f"{BASE}/replays/{replay['replay_id']}").json()
        assert final["status"] == "failed" and final["failure_code"] == "RANGE_TOO_LARGE"
        assert final["republished"] == 0
        assert len(journey.settled_raw_records()) == len(before_records), "a refused replay published records"

        with Sse(
            journey.stack.base, f"/v1/environments/{ENV}/events?channel={CHANNEL}", journey.stack.tokens.bearer()
        ) as sse:
            body = {"replay_id": replay_id, **window, "allow_reorder": True, "max_records": 1000}
            started = http.post(f"{BASE}/replays", json=body)
            assert started.status_code == 202, started.status_code
            wait_until(
                lambda: http.get(f"{BASE}/replays/{replay_id}").json()["status"] != "running",
                "the replay to finish",
                timeout_s=90,
            )
            finished = http.get(f"{BASE}/replays/{replay_id}").json()
            assert finished["status"] == "completed", finished.get("failure_code")
            assert finished["republished"] == len(
                originals
            )  # the window holds the 9 originals, not the reprocessed one
            assert finished["skipped_undecodable"] == 0
            # Idempotent on replay_id: the same body is answered with the existing record, and nothing is published.
            again = http.post(f"{BASE}/replays", json=body)
            assert again.status_code == 200 and again.json()["status"] == "completed"

            # The worker processes the replayed records: the stream shows nine new ids, each linked to an original.
            def replayed_ids() -> set[str]:
                return {
                    str(d["message_id"])
                    for e, d in list(sse.events)
                    if e == "message" and d.get("parent_id") in originals and d["message_id"] != journey.reprocessed
                }

            wait_until(
                lambda: len(replayed_ids()) == len(originals), "nine replayed messages on the stream", timeout_s=90
            )

    after = journey.settled_raw_records()  # after the repeated POST too: a late duplicate would be counted
    assert len(after) == len(before_records) + len(originals), f"{len(after)} records on raw after the replay"
    new = [r for r in after if header_map(r)["tb-msg-id"] not in before]
    assert len(new) == len(originals), f"{len(new)} records were replayed, expected {len(originals)}"
    parents = [header_map(r).get("tb-parent-id") for r in new]
    assert sorted(parents, key=str) == sorted(originals)  # one replayed copy of each original
    # Per key: the replayed copies keep the originals' order and stay on the key's partition.
    by_key: dict[str, list[tuple[int, str]]] = {}
    partitions: dict[str, set[int]] = {}
    for record in sorted(after, key=lambda r: (r.partition(), r.offset())):
        header = header_map(record)
        key = record.key().decode()
        partitions.setdefault(key, set()).add(record.partition())
        parent = header.get("tb-parent-id")
        if header["tb-msg-id"] in before or parent not in originals:
            continue
        by_key.setdefault(key, []).append((record.offset(), parent))
    assert set(by_key) == set(KEYS)
    for key, entries in by_key.items():
        sequences = [journey.seq_of[parent] for _, parent in sorted(entries)]
        assert sequences == sorted(sequences) == list(range(PER_KEY)), f"order changed for a key: {sequences}"
        assert len(partitions[key]) == 1, f"a key spans partitions {sorted(partitions[key])}"
