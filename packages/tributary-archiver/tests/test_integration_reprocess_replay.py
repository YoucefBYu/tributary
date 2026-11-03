"""Reprocess and replay against real Kafka and real ClickHouse (own containers).

`uv run pytest -m integration packages/tributary-archiver/tests/test_integration_reprocess_replay.py`

Synthetic data only. Every payload and key carries a canary; no log line, audit event or exception may.
"""

from __future__ import annotations

import io
import time
import uuid
from datetime import UTC, datetime
from typing import Any

import orjson
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from tributary.archiver.audit import AuditEvent
from tributary.archiver.errors import (
    ArchiverError,
    MessageNotFound,
    PayloadUnavailable,
    ReplayRefused,
    ReprocessRefused,
)
from tributary.archiver.event import parse_event
from tributary.archiver.ordering import ChannelTraits
from tributary.archiver.payload_source import KafkaRawPayloadSource
from tributary.archiver.query import ArchiveQueries
from tributary.archiver.replay import ReplayService
from tributary.archiver.reprocess import ReprocessService
from tributary.archiver.republish import make_publisher, reprocess_id
from tributary.archiver.schema import ensure_schema
from tributary.archiver.sink import ClickhouseSink, ClickhouseTarget, connect
from tributary.archiver.testing import sample_event
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import raw_topic
from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.ids import new_traceparent, uuid7

from tests.support.kafka_helpers import create_topics_with_retry

pytestmark = pytest.mark.integration

ENV = "dev"
CANARY = "CANARY-PHI-4471"
ACTOR = "user:synthetic-operator"


class Env:
    """One test's Kafka and ClickHouse wiring."""

    def __init__(self, bootstrap: str, ch: Any, *, hook: Any = None, ordering: str = "none") -> None:
        self.bootstrap = bootstrap
        self.channel = f"rp-{uuid.uuid4().hex[:10]}"
        self.topic = raw_topic(ENV, self.channel)
        self.settings = KafkaSettings(bootstrap_servers=bootstrap)
        self.defaults = load_topic_defaults()
        self.audit: list[AuditEvent] = []
        self.logs = io.StringIO()
        self.log = JsonLogger("rp-test", stream=self.logs, strict=True)
        name = f"r_{uuid.uuid4().hex[:12]}"

        def target(db: str) -> ClickhouseTarget:
            return ClickhouseTarget(
                host=ch.get_container_host_ip(),
                port=int(ch.get_exposed_port(8123)),
                database=db,
                username=ch.username,
                password=ch.password,
                secure=False,
            )

        admin = connect(target(ch.dbname))
        admin.command(f"CREATE DATABASE {name}")
        admin.close()
        self.client = connect(target(name))
        ensure_schema(self.client, database=name, ttl_days=3650)
        self.sink = ClickhouseSink(self.client)
        self.queries = ArchiveQueries(self.client, database=name)
        self.hook = hook or self.audit.append
        self.ordering = ordering
        make_topics(bootstrap, [(self.topic, 3)])

    def reprocess_service(self) -> ReprocessService:
        return ReprocessService(
            self.queries,
            KafkaRawPayloadSource(self.settings, self.defaults, env=ENV, timeout_s=20),
            make_publisher(self.settings, self.defaults, client_id="rp-test"),
            env=ENV,
            audit=self.hook,
            log=self.log,
            traits=self.traits,
        )

    def traits(self, channel: str) -> ChannelTraits | None:
        return ChannelTraits(self.ordering)  # type: ignore[arg-type]

    def replay_service(self) -> ReplayService:
        return ReplayService(
            self.settings,
            self.defaults,
            make_publisher(self.settings, self.defaults, client_id="rp-test"),
            env=ENV,
            audit=self.hook,
            log=self.log,
            traits=self.traits,
            idle_timeout_s=20,
        )

    def put_events(self, events: list[dict[str, Any]]) -> None:
        self.sink.insert([parse_event(orjson.dumps(e)).values for e in events])

    def event(self, mid: str, stage: str, **over: Any) -> dict[str, Any]:
        stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return sample_event(ts=stamp, channel=self.channel, message_id=mid, stage=stage, **over)

    def raw_event(self, mid: str, partition: int, offset: int, stage: str = "received") -> dict[str, Any]:
        return self.event(mid, stage, topic=self.topic, partition=partition, offset=offset)


def make_topics(bootstrap: str, names: list[tuple[str, int]]) -> None:
    create_topics_with_retry(bootstrap, [NewTopic(n, p, 1) for n, p in names])


def raw_message(channel: str, mid: str, n: int) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: mid,
            h.TB_CHANNEL: channel,
            h.TB_CHANNEL_VERSION: "1",
            h.TB_SOURCE: "mllp-receiver:r-1@127.0.0.1:1",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/hl7-v2+er7",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: new_traceparent(),
            h.TB_REPLY_PARTITION: "2",
            h.TB_REPLY_DEADLINE: "2026-01-01T00:05:00.000Z",
            h.TB_CORRELATION_ID: str(uuid.uuid4()),
        },
        payload=f"MSH|^~\\&|TRIBUTARY-SYNTH|{CANARY}-{n}".encode(),
        key=f"{CANARY}-KEY-{n}".encode(),
    )


def produce(producer: Producer, topic: str, message: Message, *, ts_ms: int | None = None, partition: int = -1) -> None:
    kwargs: dict[str, Any] = {} if ts_ms is None else {"timestamp": ts_ms}
    producer.produce(
        topic,
        value=message.payload,
        key=message.key,
        partition=partition,
        headers=[(k, v.encode()) for k, v in message.headers.items()],
        **kwargs,
    )


def produce_acked(bootstrap: str, topic: str, message: Message, **kw: Any) -> tuple[int, int]:
    got: list[tuple[int, int]] = []
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    producer.produce(
        topic,
        value=message.payload,
        key=message.key,
        headers=[(k, v.encode()) for k, v in message.headers.items()],
        on_delivery=lambda err, m: got.append((m.partition(), m.offset())),
        **kw,
    )
    assert producer.flush(30) == 0 and got
    return got[0]


def read_all(bootstrap: str, topic: str) -> list[Any]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"verify-{uuid.uuid4().hex[:6]}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
        }
    )
    try:
        partitions = sorted(consumer.list_topics(topic, timeout=30).topics[topic].partitions)
        consumer.assign([TopicPartition(topic, p, 0) for p in partitions])
        pending, out = set(partitions), []
        deadline = time.monotonic() + 30
        while pending and time.monotonic() < deadline:
            record = consumer.poll(1.0)
            if record is None:
                continue
            if record.error():
                pending.discard(record.partition())
                continue
            out.append(record)
        return out
    finally:
        consumer.close()


def hdrs(record: Any) -> dict[str, str]:
    return {k: bytes(v).decode() for k, v in record.headers() or []}


def highs(bootstrap: str, topic: str) -> list[TopicPartition]:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "wm"})
    try:
        parts = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        return [
            TopicPartition(topic, p, consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1])
            for p in parts
        ]
    finally:
        consumer.close()


def high_total(bootstrap: str, topic: str) -> int:
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "wm"})
    try:
        parts = consumer.list_topics(topic, timeout=30).topics[topic].partitions
        return sum(consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=30)[1] for p in parts)
    finally:
        consumer.close()


def assert_no_phi(env: Env, *exceptions: BaseException) -> None:
    blob = env.logs.getvalue() + repr(env.audit) + "".join(f"{e!r}{e}" for e in exceptions)
    assert CANARY not in blob and "TRIBUTARY-SYNTH" not in blob


def seed_message(env: Env, n: int = 0) -> tuple[str, tuple[int, int]]:
    mid = uuid7()
    where = produce_acked(env.bootstrap, env.topic, raw_message(env.channel, mid, n))
    env.put_events(
        [
            env.raw_event(mid, *where, stage="received"),
            env.raw_event(mid, *where, stage="queued"),
            env.event(mid, "sent", destination="d-one"),
            env.event(mid, "sent", destination="d-two"),
        ]
    )
    return mid, where


# -- reprocess ---------------------------------------------------------------------------------------


def test_reprocess_republishes_the_original_with_parent_id_and_a_deterministic_new_id(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    env = Env(kafka_bootstrap, ch_container)
    mid, _ = seed_message(env)
    service = env.reprocess_service()
    request = uuid7()
    result = service.reprocess(mid, channel=env.channel, request_id=request, actor=ACTOR)

    assert result.new_message_id == reprocess_id(mid, request) and result.new_message_id != mid
    records = read_all(kafka_bootstrap, env.topic)
    assert len(records) == 2
    original = next(r for r in records if hdrs(r)[h.TB_MSG_ID] == mid)
    child = next(r for r in records if hdrs(r)[h.TB_MSG_ID] == result.new_message_id)
    assert hdrs(child)[h.TB_PARENT_ID] == mid
    assert h.TB_PARENT_ID not in hdrs(original)
    assert child.value() == original.value() and child.key() == original.key()
    assert h.TB_REPLY_PARTITION not in hdrs(child) and h.TB_REPLY_DEADLINE not in hdrs(child)
    assert hdrs(child)[h.TB_ATTEMPT] == "0" and (child.partition(), child.offset()) == (
        result.partition,
        result.offset,
    )
    assert [(a.phase, a.actor, a.message_id) for a in env.audit] == [
        ("requested", ACTOR, mid),
        ("completed", ACTOR, mid),
    ]

    # Same request id again (a retry after an unknown outcome): the SAME tb-msg-id, so loaders absorb it.
    again = service.reprocess(mid, channel=env.channel, request_id=request, actor=ACTOR)
    assert again.new_message_id == result.new_message_id
    # A different request id is a different, deliberate reprocess.
    other = service.reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert other.new_message_id != result.new_message_id
    assert_no_phi(env)


def test_reprocess_refuses_without_coordinates_and_for_children_and_unknown_ids(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    env = Env(kafka_bootstrap, ch_container)
    service = env.reprocess_service()
    before = high_total(kafka_bootstrap, env.topic)
    no_coords, child, unknown = uuid7(), uuid7(), uuid7()
    where = produce_acked(kafka_bootstrap, env.topic, raw_message(env.channel, child, 1))
    before += 1
    env.put_events(
        [
            env.event(no_coords, "received"),
            env.raw_event(child, *where, stage="received"),
            env.event(child, "queued", parent_id=str(uuid7())),
        ]
    )
    caught: list[BaseException] = []
    for mid, expected in ((no_coords, "NO_PAYLOAD_COORDINATES"), (child, "CHILD_MESSAGE")):
        with pytest.raises(ReprocessRefused) as raised:
            service.reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
        assert raised.value.code == expected
        caught.append(raised.value)
    with pytest.raises(MessageNotFound):
        service.reprocess(unknown, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    with pytest.raises(ReprocessRefused) as raised:
        service.reprocess("not-a-uuid", channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.code == "INVALID_REQUEST"
    assert high_total(kafka_bootstrap, env.topic) == before, "a refusal publishes nothing"
    assert env.audit == [], "a refusal before content access is not a content access"
    assert_no_phi(env, *caught)


def test_reprocess_of_a_destination_scoped_dlq_message_is_refused_and_publishes_nothing(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    """Two-destination control: d-one delivered, d-two dead-lettered. A `raw` re-injection would re-deliver
    to BOTH, so it is refused (ADR-003 follow-up 10) until destination-scoped re-entry exists."""
    env = Env(kafka_bootstrap, ch_container)
    mid, _ = seed_message(env)
    env.put_events([env.event(mid, "dlq", destination="d-two", status="error", error_code="HTTP_503")])
    before = high_total(kafka_bootstrap, env.topic)
    with pytest.raises(ReprocessRefused) as raised:
        env.reprocess_service().reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.code == "DESTINATION_SCOPED"
    assert high_total(kafka_bootstrap, env.topic) == before
    # control: a dlq record with NO destination (a step failure) is reprocessable
    plain, _ = seed_message(env, 1)
    env.put_events([env.event(plain, "dlq", status="error", error_code="TRANSFORM_ERROR")])
    assert env.reprocess_service().reprocess(plain, channel=env.channel, request_id=uuid7(), actor=ACTOR).new_message_id
    assert_no_phi(env, raised.value)


def test_reprocess_audit_fails_closed(kafka_bootstrap: str, ch_container: Any) -> None:
    def refuse(event: AuditEvent) -> None:
        raise RuntimeError("audit sink down")

    env = Env(kafka_bootstrap, ch_container, hook=refuse)
    mid, _ = seed_message(env)
    before = high_total(kafka_bootstrap, env.topic)
    with pytest.raises(RuntimeError, match="audit sink down"):
        env.reprocess_service().reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert high_total(kafka_bootstrap, env.topic) == before, "no payload read or published without an audit record"


def test_reprocess_payload_unavailable_cases(kafka_bootstrap: str, ch_container: Any) -> None:
    env = Env(kafka_bootstrap, ch_container)
    mid, (partition, offset) = seed_message(env)
    other, _ = seed_message(env, 1)
    service = env.reprocess_service()
    caught: list[BaseException] = []

    # the events point at a record that is a different message
    wrong = uuid7()
    env.put_events([env.raw_event(wrong, partition, offset)])
    with pytest.raises(PayloadUnavailable) as raised:
        service.reprocess(wrong, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.code == "RECORD_MISMATCH"
    caught.append(raised.value)
    # an offset past the end of the log
    beyond = uuid7()
    env.put_events([env.raw_event(beyond, partition, offset + 1000)])
    with pytest.raises(PayloadUnavailable) as raised:
        service.reprocess(beyond, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.code == "OFFSET_NOT_FOUND"
    caught.append(raised.value)
    # retention lapsed: the log start moves past the recorded offset
    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    for future in admin.delete_records(highs(kafka_bootstrap, env.topic)).values():
        future.result(timeout=30)
    with pytest.raises(PayloadUnavailable) as raised:
        service.reprocess(other, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.code == "RETENTION_LAPSED"
    caught.append(raised.value)
    del mid
    assert_no_phi(env, *caught)


# -- replay ------------------------------------------------------------------------------------------


def transactional(bootstrap: str) -> Producer:
    producer = Producer({"bootstrap.servers": bootstrap, "transactional.id": f"tx-{uuid.uuid4().hex[:8]}"})
    producer.init_transactions(30)
    return producer


def test_replay_republishes_exactly_the_range_across_real_control_records(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    """One partition, three committed transactions: before / in range / after. Each commit writes a control
    record, so a marker sits at the start of the range and one at its very end. An aborted transaction inside
    the span, and a plain record carrying no Tributary identity, must not be republished either."""
    env = Env(kafka_bootstrap, ch_container)
    ids: dict[str, list[str]] = {"before": [], "range": [], "after": []}
    producer = transactional(kafka_bootstrap)
    count = 0

    def now_ms() -> int:
        return int(time.time() * 1000)

    def batch(name: str, size: int, *, commit: bool = True) -> None:
        nonlocal count
        producer.begin_transaction()
        for _ in range(size):
            mid = uuid7()
            if commit:
                ids[name].append(mid)
            count += 1
            produce(producer, env.topic, raw_message(env.channel, mid, count), ts_ms=now_ms(), partition=0)
        if commit:
            producer.commit_transaction(30)
        else:
            producer.abort_transaction(30)

    # Control records carry the broker's wall-clock time, so the data uses real times too (a backdated
    # CreateTime would put a marker "after" records written before it). Gaps keep the three groups apart.
    batch("before", 3)
    time.sleep(1.5)
    start_ms = now_ms()  # the "before" commit marker is OLDER than start: a control record outside the range
    producer.begin_transaction()
    produce(producer, env.topic, Message(headers={"x": "y"}, payload=b"no-identity"), ts_ms=now_ms(), partition=0)
    producer.commit_transaction(30)
    batch("range", 4)
    batch("range", 2, commit=False)  # aborted, inside the span: invisible to read_committed
    end_ms = now_ms() + 1  # the last commit marker sits at end - 1: a control record AT the range boundary
    time.sleep(1.5)
    batch("after", 3)  # its first record is at the end offset, and its marker is outside the range

    before_total = len(read_all(kafka_bootstrap, env.topic))
    replay_id = uuid7()
    result = env.replay_service().replay(
        env.channel,
        start=datetime.fromtimestamp(start_ms / 1000, UTC),
        end=datetime.fromtimestamp(end_ms / 1000, UTC),
        replay_id=replay_id,
        actor=ACTOR,
    )
    assert result.republished == 4 and result.skipped_undecodable == 1

    records = read_all(kafka_bootstrap, env.topic)
    children = [r for r in records if h.TB_PARENT_ID in hdrs(r)]
    assert len(records) == before_total + 4 and len(children) == 4
    assert sorted(hdrs(r)[h.TB_PARENT_ID] for r in children) == sorted(ids["range"])
    assert {hdrs(r)[h.TB_MSG_ID] for r in children} == {reprocess_id(m, replay_id) for m in ids["range"]}
    assert not {hdrs(r)[h.TB_PARENT_ID] for r in children} & set(ids["before"] + ids["after"])
    assert [(a.phase, a.count) for a in env.audit] == [("requested", None), ("completed", 4)]

    # Re-running with the same replay id regenerates the same ids (loader idempotency absorbs them).
    env.replay_service().replay(
        env.channel,
        start=datetime.fromtimestamp(start_ms / 1000, UTC),
        end=datetime.fromtimestamp(end_ms / 1000, UTC),
        replay_id=replay_id,
        actor=ACTOR,
    )
    second = [r for r in read_all(kafka_bootstrap, env.topic) if h.TB_PARENT_ID in hdrs(r)]
    # Same replay_id twice: every new tb-msg-id appears exactly twice, always with the same tb-parent-id.
    pairs = sorted((hdrs(r)[h.TB_MSG_ID], hdrs(r)[h.TB_PARENT_ID]) for r in second)
    first_pairs = sorted((hdrs(r)[h.TB_MSG_ID], hdrs(r)[h.TB_PARENT_ID]) for r in children)
    assert len(second) == 8 and pairs == sorted(first_pairs * 2)
    # the audit trail records the request, its range and its target
    requested = env.audit[0]
    assert (requested.target_topic, requested.channel, requested.allow_reorder) == (env.topic, env.channel, False)
    assert requested.range_start is not None and requested.range_end is not None and requested.ts is not None
    assert_no_phi(env)


def test_replay_over_partitions_is_half_open_and_skips_skewed_timestamps(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    env = Env(kafka_bootstrap, ch_container)
    base = int(time.time() * 1000) - 1_800_000
    producer = Producer({"bootstrap.servers": kafka_bootstrap, "acks": "all", "enable.idempotence": True})
    ids: list[str] = []
    skewed = uuid7()  # sits inside the offset span of partition 0 but carries a timestamp before the range
    for i in range(30):  # ts = base + i s, partition i % 3
        if i == 15:
            produce(producer, env.topic, raw_message(env.channel, skewed, 99), ts_ms=base, partition=0)
        mid = uuid7()
        ids.append(mid)
        produce(producer, env.topic, raw_message(env.channel, mid, i), ts_ms=base + i * 1000, partition=i % 3)
    assert producer.flush(30) == 0

    start, end = (
        datetime.fromtimestamp((base + 10_000) / 1000, UTC),
        datetime.fromtimestamp((base + 20_000) / 1000, UTC),
    )
    replay_id = uuid7()
    service = env.replay_service()

    with pytest.raises(ReplayRefused) as too_big:
        service.replay(env.channel, start=start, end=end, replay_id=replay_id, actor=ACTOR, max_records=3)
    assert too_big.value.code == "RANGE_TOO_LARGE"
    total = high_total(kafka_bootstrap, env.topic)
    with pytest.raises(ReplayRefused) as bad:
        service.replay(env.channel, start=end, end=start, replay_id=replay_id, actor=ACTOR)
    assert bad.value.code == "INVALID_REQUEST"
    with pytest.raises(ReplayRefused) as missing:
        service.replay("no-such-channel", start=start, end=end, replay_id=replay_id, actor=ACTOR)
    assert missing.value.code == "TOPIC_NOT_FOUND"
    assert high_total(kafka_bootstrap, env.topic) == total, "refusals publish nothing"

    result = service.replay(env.channel, start=start, end=end, replay_id=replay_id, actor=ACTOR)
    assert result.republished == 10, "[start, end): i = 10..19, not 20 (== end) and not 9 (< start)"
    assert high_total(kafka_bootstrap, env.topic) == total + 10, "the target is the source channel's own raw"
    copies = [r for r in read_all(kafka_bootstrap, env.topic) if h.TB_PARENT_ID in hdrs(r)]
    assert sorted(hdrs(r)[h.TB_PARENT_ID] for r in copies) == sorted(ids[10:20])
    assert all(hdrs(r)[h.TB_CHANNEL] == env.channel for r in copies)
    assert all(CANARY.encode() in r.value() for r in copies)  # payload copied byte for byte
    assert skewed not in {hdrs(r)[h.TB_PARENT_ID] for r in copies}
    assert result.skipped_out_of_range == 1
    assert_no_phi(env, too_big.value, bad.value, missing.value)


def test_replay_audit_fails_closed(kafka_bootstrap: str, ch_container: Any) -> None:
    def refuse(event: AuditEvent) -> None:
        raise RuntimeError("audit sink down")

    env = Env(kafka_bootstrap, ch_container, hook=refuse)
    produce_acked(kafka_bootstrap, env.topic, raw_message(env.channel, uuid7(), 0))
    total = high_total(kafka_bootstrap, env.topic)
    now = datetime.now(UTC)
    with pytest.raises(RuntimeError, match="audit sink down"):
        env.replay_service().replay(
            env.channel, start=datetime.fromtimestamp(0, UTC), end=now, replay_id=uuid7(), actor=ACTOR
        )
    assert high_total(kafka_bootstrap, env.topic) == total
    assert isinstance(ArchiverError("x"), Exception)


# -- ordering (invariant 5), allow_reorder, failure audit ------------------------------------------------


def test_replay_into_a_per_key_channel_needs_allow_reorder_and_records_it(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    env = Env(kafka_bootstrap, ch_container, ordering="per-key")
    mid = uuid7()
    produce_acked(kafka_bootstrap, env.topic, raw_message(env.channel, mid, 0))
    before = high_total(kafka_bootstrap, env.topic)
    window = {
        "start": datetime.fromtimestamp(0, UTC),
        "end": datetime.now(UTC),
        "replay_id": uuid7(),
        "actor": ACTOR,
    }
    with pytest.raises(ReplayRefused) as refused:
        env.replay_service().replay(env.channel, **window)
    assert refused.value.code == "ORDERING_WOULD_BREAK"
    assert high_total(kafka_bootstrap, env.topic) == before and env.audit == []

    result = env.replay_service().replay(env.channel, allow_reorder=True, **window)
    assert result.republished == 1
    assert [(a.phase, a.allow_reorder) for a in env.audit] == [("requested", True), ("completed", True)]
    assert high_total(kafka_bootstrap, env.topic) == before + 1


def test_reprocess_of_a_per_key_channel_needs_allow_reorder_and_unknown_channels_are_refused(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    env = Env(kafka_bootstrap, ch_container, ordering="per-key")
    mid, _ = seed_message(env)
    service = env.reprocess_service()
    before = high_total(kafka_bootstrap, env.topic)
    with pytest.raises(ReprocessRefused) as refused:
        service.reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert refused.value.code == "ORDERING_WOULD_BREAK" and env.audit == []
    assert high_total(kafka_bootstrap, env.topic) == before
    service.reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR, allow_reorder=True)
    assert env.audit[0].allow_reorder is True and env.audit[0].phase == "requested"

    env.traits = lambda channel: None  # type: ignore[method-assign,assignment]
    with pytest.raises(ReprocessRefused) as unknown:
        env.reprocess_service().reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert unknown.value.code == "ORDERING_WOULD_BREAK", "unknown channel traits fail closed"


def test_replay_has_no_target_channel_option(kafka_bootstrap: str, ch_container: Any) -> None:
    env = Env(kafka_bootstrap, ch_container)
    with pytest.raises(TypeError):
        env.replay_service().replay(  # type: ignore[call-arg]
            env.channel,
            start=datetime.fromtimestamp(0, UTC),
            end=datetime.now(UTC),
            replay_id=uuid7(),
            actor=ACTOR,
            target_channel="elsewhere",
        )


def test_reprocess_requires_a_channel_and_a_timeline_needs_one_or_a_v7_id(
    kafka_bootstrap: str, ch_container: Any
) -> None:
    env = Env(kafka_bootstrap, ch_container)
    mid, _ = seed_message(env)
    with pytest.raises(TypeError):
        env.reprocess_service().reprocess(mid, request_id=uuid7(), actor=ACTOR)  # type: ignore[call-arg]
    from tributary.archiver.errors import QueryError

    with pytest.raises(QueryError) as raised:
        env.queries.timeline(uuid.uuid4())  # no channel and no v7 time: a full-table read
    assert raised.value.code == "QUERY_INVALID"
    assert len(env.queries.timeline(mid)) == 4  # a v7 id bounds the read without a channel


def test_reprocess_facts_are_not_capped_by_the_timeline_row_limit(kafka_bootstrap: str, ch_container: Any) -> None:
    """More than 1,000 events for one message: the destination-scoped `dlq` event is the LAST one. A capped
    timeline would miss it; the dedicated aggregate query does not."""
    env = Env(kafka_bootstrap, ch_container)
    mid, _ = seed_message(env)
    env.put_events([env.event(mid, "sent", destination="d-two", attempt=i) for i in range(1, 1100)])
    env.put_events([env.event(mid, "dlq", destination="d-two", status="error", error_code="HTTP_503")])
    assert len(env.queries.timeline(mid, channel=env.channel)) == 1000  # the cap
    with pytest.raises(ReprocessRefused) as raised:
        env.reprocess_service().reprocess(mid, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.code == "DESTINATION_SCOPED"


def test_a_failed_reprocess_audits_the_failure_with_a_partial_count(kafka_bootstrap: str, ch_container: Any) -> None:
    env = Env(kafka_bootstrap, ch_container)
    _, (partition, offset) = seed_message(env)
    other = uuid7()
    env.put_events([env.raw_event(other, partition, offset)])  # points at someone else's record
    with pytest.raises(PayloadUnavailable) as raised:
        env.reprocess_service().reprocess(other, channel=env.channel, request_id=uuid7(), actor=ACTOR)
    assert raised.value.republished == 0
    assert [(a.phase, a.count) for a in env.audit] == [("requested", None), ("failed", 0)]
    assert "message_reprocess_failed" in env.logs.getvalue()
    assert_no_phi(env, raised.value)
