"""`KafkaSourceReceiver` against a fake consumer and scripted publisher: no broker. The source offset
is committed only after the raw produce is acknowledged (invariant 1); a refused record halts its
partition and nothing at or past it is committed (ADR-007 D16); an unavailable `raw` cluster pauses
and re-reads instead of committing. Payloads are synthetic."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from dataclasses import replace
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition
from tributary.receivers.kafka_source.config import KafkaSourceSettings, SourceSecurity
from tributary.receivers.kafka_source.receiver import KafkaSourceReceiver
from tributary.sdk import Message
from tributary.sdk.keys import parse_key_expression

TOPIC = "lab.results.v1"
PAYLOAD = json.dumps({"patient": "SYNTH-1", "note": "TRIBUTARY-SYNTH"}).encode()


class FakeMsg:
    def __init__(
        self,
        partition: int,
        offset: int,
        value: bytes | None = PAYLOAD,
        key: bytes | None = b"k1",
        headers: list[tuple[str, bytes | None]] | None = None,
        error: Any = None,
    ) -> None:
        self._p, self._o, self._v, self._k, self._h, self._e = partition, offset, value, key, headers, error

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def value(self) -> bytes | None:
        return self._v

    def key(self) -> bytes | None:
        return self._k

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return self._h

    def error(self) -> Any:
        return self._e


class FakeConsumer:
    def __init__(self, partitions: tuple[int, ...] = (0, 1)) -> None:
        self.queue: list[FakeMsg] = []
        self.commits: list[dict[int, int]] = []
        self.paused: list[list[int]] = []
        self.resumed: list[list[int]] = []
        self.seeks: list[tuple[int, int]] = []
        self.partitions = partitions
        self.closed = False
        self.callbacks: dict[str, Any] = {}
        self.fail_commit = False

    def subscribe(self, topics: list[str], **callbacks: Any) -> None:
        assert topics == [TOPIC]
        self.callbacks = callbacks

    def poll(self, timeout: float) -> FakeMsg | None:
        return self.queue.pop(0) if self.queue else None

    def commit(self, *, offsets: list[TopicPartition], asynchronous: bool) -> Any:
        assert asynchronous is False  # the offset must be durable before we rely on it
        if self.fail_commit:
            raise KafkaException(KafkaError(KafkaError._TRANSPORT))
        self.commits.append({tp.partition: tp.offset for tp in offsets})

    def pause(self, partitions: list[TopicPartition]) -> None:
        self.paused.append([tp.partition for tp in partitions])

    def resume(self, partitions: list[TopicPartition]) -> None:
        self.resumed.append([tp.partition for tp in partitions])

    def seek(self, partition: TopicPartition) -> None:
        self.seeks.append((partition.partition, partition.offset))

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(TOPIC, p) for p in self.partitions]

    def close(self) -> None:
        self.closed = True


async def eventually(predicate: Callable[[], bool], timeout_s: float = 5.0) -> None:
    """Poll instead of sleeping a guessed time: returns as soon as `predicate` holds, fails at the deadline."""
    deadline = asyncio.get_running_loop().time() + timeout_s
    while not predicate():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not reached in time")
        await asyncio.sleep(0.01)


class Publisher:
    def __init__(self, outcomes: list[bool] | None = None, consumer: FakeConsumer | None = None) -> None:
        self.calls: list[Message] = []
        self.outcomes = outcomes
        self.consumer = consumer
        self.commits_seen_at_publish: list[int] = []

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        if self.consumer is not None:
            self.commits_seen_at_publish.append(len(self.consumer.commits))
        if self.outcomes:
            return self.outcomes.pop(0)
        return True


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def settings(**kw: object) -> KafkaSourceSettings:
    base = KafkaSourceSettings(
        channel="lab-bridge",
        channel_version=2,
        content_type="application/json",
        source_bootstrap=("kafka.synthetic.example:9093",),
        security=SourceSecurity("SSL"),
        topic=TOPIC,
        start_from="earliest",
        nack_on=frozenset(),
        key_expression=None,
        max_message_bytes=1_000_000,
        ack_timeout_s=5.0,
        env="dev",
        commit_every=3,
        commit_flush_interval_s=10.0,
    )
    if kw.get("key_expression") is not None:
        kw.setdefault("ordering", "per-key")
    return replace(base, **kw)  # type: ignore[arg-type]


def drive(rx: KafkaSourceReceiver, n: int) -> None:
    async def go() -> None:
        for _ in range(n):
            await rx.step()

    asyncio.run(go())


def make(consumer: FakeConsumer, pub: Publisher, clock: Clock | None = None, **kw: object) -> KafkaSourceReceiver:
    return KafkaSourceReceiver(settings(**kw), pub, consumer, clock=clock or Clock())


def test_offset_is_committed_only_after_the_raw_produce_is_acknowledged() -> None:
    c = FakeConsumer()
    pub = Publisher(consumer=c)
    c.queue = [FakeMsg(0, i) for i in range(3)]
    rx = make(c, pub)
    drive(rx, 3)
    # commit_every=3: the commit happens after the third acked publish, never before any of them.
    assert pub.commits_seen_at_publish == [0, 0, 0]
    assert c.commits == [{0: 3}]  # the offset AFTER the last acked record


def test_a_refused_produce_commits_nothing_pauses_and_seeks_back_to_the_record() -> None:
    c = FakeConsumer()
    pub = Publisher([True, False])
    c.queue = [FakeMsg(0, 10), FakeMsg(0, 11)]
    rx = make(c, pub, commit_every=1)
    drive(rx, 2)
    assert c.commits == [{0: 11}]  # only the acked record's offset, never 12
    assert c.paused == [[0, 1]] and c.seeks == [(0, 11)]
    assert rx.metrics.acked == 1 and rx.metrics.nacked_unavailable == 1


def test_unavailable_raw_backs_off_exponentially_then_resumes_and_republishes() -> None:
    c, clock = FakeConsumer(), Clock()
    pub = Publisher([False, False, True])
    c.queue = [FakeMsg(0, 5)]
    rx = make(c, pub, clock, commit_every=1)
    drive(rx, 1)
    assert c.resumed == []
    drive(rx, 1)  # still inside the backoff window: nothing resumes
    assert c.resumed == []
    clock.now += 1.01
    c.queue = [FakeMsg(0, 5)]  # the re-read after resume
    drive(rx, 1)
    assert c.resumed == [[0, 1]]
    assert rx._backoff_s == 4.0  # 1 -> 2 (first failure), 2 -> 4 (second)
    clock.now += 2.01
    c.queue = [FakeMsg(0, 5)]
    drive(rx, 1)
    assert c.commits == [{0: 6}] and rx._backoff_s == 1.0  # success resets it


def test_publisher_exception_is_a_refusal_never_a_commit() -> None:
    class Boom:
        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            raise RuntimeError("SYNTH text")

    c = FakeConsumer()
    c.queue = [FakeMsg(0, 1)]
    rx = KafkaSourceReceiver(settings(commit_every=1), Boom(), c, clock=Clock())
    drive(rx, 1)
    assert c.commits == [] and c.seeks == [(0, 1)]


@pytest.mark.parametrize(
    "msg_kwargs, cfg_kwargs, reason",
    [
        ({"value": b"x" * 101}, {"max_message_bytes": 100}, "oversize"),
        ({"value": None}, {}, "tombstone"),
        ({"value": b""}, {}, "empty"),
        ({"value": b'{"broken":'}, {"nack_on": frozenset({"parse-error"})}, "parse-error"),
        ({"key": None}, {"key_expression": parse_key_expression("record:key")}, "KEY_RECORD_KEY_ABSENT"),
    ],
)
def test_a_refused_record_halts_its_partition_and_nothing_past_it_is_committed(
    msg_kwargs: dict[str, Any], cfg_kwargs: dict[str, Any], reason: str, caplog: pytest.LogCaptureFixture
) -> None:
    c = FakeConsumer()
    pub = Publisher()
    c.queue = [FakeMsg(0, 7), FakeMsg(0, 8, **msg_kwargs), FakeMsg(0, 9), FakeMsg(1, 3)]
    rx = make(c, pub, commit_every=100, **cfg_kwargs)
    with caplog.at_level(logging.DEBUG):
        drive(rx, 4)
    # offset 7 was acked and is committed when the halt happens; 8 and 9 never are.
    assert c.commits[0] == {0: 8}
    assert all(commit.get(0, 0) <= 8 for commit in c.commits)
    assert c.paused == [[0]] and rx.halted == {0: 8}
    assert len(pub.calls) == 2  # record 7 and partition 1's record; 8 and the leftover 9 were not published
    halted = [json.loads(r.getMessage()) for r in caplog.records if "halted" in r.getMessage()]
    assert halted == [
        {
            "event": "kafka_source_partition_halted",
            "channel": "lab-bridge",
            "topic": TOPIC,
            "partition": 0,
            "offset": 8,
            "reason": reason,
        }
    ]
    assert rx.metrics.partitions_halted == 1
    # The other partition keeps flowing and is committed independently of the halted one.
    rx._commit()
    assert c.commits[-1] == {1: 4}


def test_a_halted_partition_is_not_resumed_after_a_backoff() -> None:
    c, clock = FakeConsumer(), Clock()
    pub = Publisher([False, True])
    c.queue = [FakeMsg(0, 1, value=None), FakeMsg(1, 1)]
    rx = make(c, pub, clock, commit_every=1)
    drive(rx, 2)
    clock.now += 2
    drive(rx, 1)
    assert c.resumed == [[1]]  # partition 0 stays halted


def test_a_refused_record_never_reaches_raw_even_when_it_is_the_first_record() -> None:
    c = FakeConsumer()
    pub = Publisher()
    c.queue = [FakeMsg(0, 0, value=b"")]
    rx = make(c, pub)
    drive(rx, 1)
    assert pub.calls == [] and c.commits == [] and rx.halted == {0: 0}


def test_inbound_headers_are_never_copied_and_the_record_key_is_used_only_by_record_key() -> None:
    c = FakeConsumer()
    pub = Publisher()
    evil = [("tb-msg-id", b"attacker"), ("tb-channel", b"other"), ("X-Patient", b"SYNTH9")]
    c.queue = [FakeMsg(0, 1, headers=evil), FakeMsg(0, 2, headers=evil)]
    rx = make(c, pub)
    drive(rx, 1)
    m = pub.calls[0]
    assert m.headers["tb-msg-id"] != "attacker" and m.headers["tb-channel"] == "lab-bridge"
    assert "x-patient" not in {k.lower() for k in m.headers} and m.key is None  # ordering: none
    rx2 = make(c, pub, key_expression=parse_key_expression("record:key"))
    drive(rx2, 1)
    assert pub.calls[1].key == b"k1"


def test_header_key_scheme_matches_kafka_headers_exactly() -> None:
    c = FakeConsumer()
    pub = Publisher()
    c.queue = [
        FakeMsg(0, 1, headers=[("X-Patient", b"SYNTH9")]),
        FakeMsg(0, 2, headers=[("x-patient", b"SYNTH9")]),  # kafka headers match exactly: unresolved
    ]
    rx = make(c, pub, key_expression=parse_key_expression("header:X-Patient"))
    drive(rx, 2)
    assert [m.key for m in pub.calls] == [b"SYNTH9"] and rx.halted == {0: 2}


def test_envelope_is_minted_fresh_with_the_configured_content_type() -> None:
    c = FakeConsumer()
    pub = Publisher()
    c.queue = [FakeMsg(0, 1)]
    drive(make(c, pub, content_type="application/json"), 1)
    m = pub.calls[0]
    assert m.headers["tb-content-type"] == "application/json" and m.headers["tb-attempt"] == "0"
    assert m.headers["tb-source"] == "kafka:kafka-0@kafka.synthetic.example:9093"


def test_time_based_flush_bounds_the_replay_window_when_traffic_is_slow() -> None:
    c, clock = FakeConsumer(), Clock()
    pub = Publisher()
    c.queue = [FakeMsg(0, 1)]
    rx = make(c, pub, clock, commit_every=100, commit_flush_interval_s=5.0)
    drive(rx, 1)
    assert c.commits == []
    clock.now += 5.1
    drive(rx, 1)  # an idle poll flushes
    assert c.commits == [{0: 2}]


def test_commit_failure_keeps_offsets_pending_and_never_raises() -> None:
    c = FakeConsumer()
    pub = Publisher()
    c.queue = [FakeMsg(0, 1)]
    c.fail_commit = True
    rx = make(c, pub, commit_every=1)
    drive(rx, 1)
    assert rx.metrics.commit_failures == 1 and c.commits == []
    c.fail_commit = False
    rx._commit()
    assert c.commits == [{0: 2}]


def test_revoke_commits_acked_offsets_then_forgets_the_partition() -> None:
    c = FakeConsumer()
    pub = Publisher()
    c.queue = [FakeMsg(0, 1), FakeMsg(1, 5)]
    rx = make(c, pub, commit_every=100)
    rx.start()
    drive(rx, 2)
    c.callbacks["on_revoke"](None, [TopicPartition(TOPIC, 0)])
    assert c.commits == [{0: 2, 1: 6}]
    c.callbacks["on_lost"](None, [TopicPartition(TOPIC, 1)])
    assert rx._pending == {}


def test_lost_partitions_are_not_committed() -> None:
    c = FakeConsumer()
    c.queue = [FakeMsg(0, 1)]
    rx = make(c, Publisher(), commit_every=100)
    rx.start()
    drive(rx, 1)
    c.callbacks["on_lost"](None, [TopicPartition(TOPIC, 0)])
    rx._commit()
    assert c.commits == []


def test_poll_errors_are_counted_eof_is_ignored_fatal_raises() -> None:
    from tributary.receivers.errors import ReceiverError

    class Err:
        def __init__(self, code: int, fatal: bool = False) -> None:
            self._c, self._f = code, fatal

        def code(self) -> int:
            return self._c

        def fatal(self) -> bool:
            return self._f

        def name(self) -> str:
            return "X"

    c = FakeConsumer()
    rx = make(c, Publisher())
    c.queue = [FakeMsg(0, 0, error=Err(-191)), FakeMsg(0, 0, error=Err(-195))]
    drive(rx, 2)
    assert rx.metrics.poll_errors == 1
    c.queue = [FakeMsg(0, 0, error=Err(-1, fatal=True))]
    with pytest.raises(ReceiverError):
        drive(rx, 1)


def test_run_closes_after_committing_pending_offsets() -> None:
    c = FakeConsumer()
    c.queue = [FakeMsg(0, 1)]
    rx = make(c, Publisher(), commit_every=100)

    async def go() -> None:
        stop = asyncio.Event()
        task = asyncio.create_task(rx.run(stop))
        await eventually(lambda: rx.metrics.acked == 1)
        stop.set()
        await asyncio.wait_for(task, 3)

    asyncio.run(go())
    assert c.commits == [{0: 2}] and c.closed


def test_payload_key_and_headers_never_reach_logs_or_metrics(caplog: pytest.LogCaptureFixture) -> None:
    c = FakeConsumer()
    c.queue = [FakeMsg(0, 1, key=b"KEYSYNTH", headers=[("h", b"HDRSYNTH")]), FakeMsg(0, 2, value=b"")]
    rx = make(c, Publisher(), commit_every=100)
    with caplog.at_level(logging.DEBUG):
        drive(rx, 2)
    blob = caplog.text + json.dumps(rx.metrics.snapshot())
    for forbidden in ("KEYSYNTH", "HDRSYNTH", "SYNTH-1", "TRIBUTARY-SYNTH"):
        assert forbidden not in blob


def test_a_permanently_unproducible_record_halts_its_partition_instead_of_backing_off() -> None:
    from tributary.receivers.errors import PermanentPublishError

    class Invalid:
        async def publish(self, message: Message, *, timeout_s: float) -> bool:
            raise PermanentPublishError("x")

    c = FakeConsumer()
    c.queue = [FakeMsg(0, 4)]
    rx = KafkaSourceReceiver(settings(commit_every=1), Invalid(), c, clock=Clock())
    drive(rx, 1)
    assert rx.halted == {0: 4} and c.seeks == [] and c.commits == [] and rx.metrics.rejected_envelope == 1
