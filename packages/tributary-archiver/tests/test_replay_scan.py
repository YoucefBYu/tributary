"""`ReplayService` against a fake consumer: the scan loop's edge cases without a broker.

Real-broker behaviour (transaction markers, offsetsForTimes) is in test_integration_reprocess_replay.py.
"""

from __future__ import annotations

import io
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from tributary.archiver.audit import AuditEvent
from tributary.archiver.errors import PublishError, ReplayRefused
from tributary.archiver.ordering import ChannelTraits
from tributary.archiver.replay import HARD_MAX_RECORDS, ReplayService
from tributary.archiver.republish import RawPublisher, reprocess_id
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.sdk import headers as h
from tributary.sdk.ids import new_traceparent, uuid7

CH = "adt"
TOPIC = f"dev.{CH}.raw"
ACTOR = "user:synthetic"
START_MS, END_MS = 1_000_000, 2_000_000
START, END = datetime.fromtimestamp(START_MS / 1000, UTC), datetime.fromtimestamp(END_MS / 1000, UTC)
EOF_ERROR = SimpleNamespace(code=lambda: KafkaError._PARTITION_EOF)


def rec(partition: int, offset: int, ts: int, mid: str | None = None, *, channel: str = CH) -> Any:
    headers = (
        []
        if mid is None
        else [
            (h.TB_MSG_ID, mid.encode()),
            (h.TB_CHANNEL, channel.encode()),
            (h.TB_CHANNEL_VERSION, b"1"),
            (h.TB_SOURCE, b"r:1@127.0.0.1:1"),
            (h.TB_RECEIVED_AT, b"2026-01-01T00:00:00.000Z"),
            (h.TB_CONTENT_TYPE, b"text/plain"),
            (h.TB_ATTEMPT, b"0"),
            (h.TRACEPARENT, new_traceparent().encode()),
        ]
    )
    return SimpleNamespace(
        error=lambda: None,
        partition=lambda: partition,
        offset=lambda: offset,
        timestamp=lambda: (1, ts),
        headers=lambda: headers,
        value=lambda: b"SYNTH",
        key=lambda: b"K",
    )


def eof(partition: int) -> Any:
    return SimpleNamespace(error=lambda: EOF_ERROR, partition=lambda: partition)


class FakeConsumer:
    """`script` is what `poll` returns in order; `None` is an empty poll. After the script, polls return None."""

    def __init__(self, script: list[Any], *, plan: dict[int, tuple[int, int]], raise_on: str | None = None) -> None:
        self.script = list(script)
        self.plan = plan
        self.paused: list[int] = []
        self.raise_on = raise_on
        self.closed = False

    def list_topics(self, topic: str, timeout: float) -> Any:
        parts = dict.fromkeys(self.plan) or {0: None}
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=parts)})

    def offsets_for_times(self, tps: list[Any], timeout: float) -> list[Any]:
        index = 0 if tps and tps[0].offset == START_MS else 1
        return [SimpleNamespace(partition=t.partition, offset=self.plan.get(t.partition, (0, 0))[index]) for t in tps]

    def get_watermark_offsets(self, tp: Any, timeout: float) -> tuple[int, int]:
        return 0, 1000

    def assign(self, tps: list[Any]) -> None:
        if self.raise_on == "assign":
            raise KafkaException(KafkaError(KafkaError._STATE))

    def pause(self, tps: list[Any]) -> None:
        self.paused.extend(t.partition for t in tps)
        if self.raise_on == "pause":
            raise KafkaException(KafkaError(KafkaError._STATE))

    def poll(self, timeout: float) -> Any:
        if self.raise_on == "poll":
            raise KafkaException(KafkaError(KafkaError._STATE))
        return self.script.pop(0) if self.script else None

    def close(self) -> None:
        self.closed = True


class FakeProducer:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.sent: list[Any] = []
        self.pending: list[Any] = []
        self.fail_after = fail_after

    def produce(self, topic: str, *, value: Any, key: Any, headers: Any, on_delivery: Any) -> None:
        self.sent.append(dict(headers))
        self.pending.append(on_delivery)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        msg = SimpleNamespace(partition=lambda: 0, offset=lambda: 1)
        for i, callback in enumerate(self.pending):
            failed = self.fail_after is not None and len(self.sent) - len(self.pending) + i >= self.fail_after
            callback("ERR" if failed else None, msg)
        self.pending = []
        return 0


class Rig:
    def __init__(self, consumer: FakeConsumer, *, producer: FakeProducer | None = None, ordering: str = "none") -> None:
        self.consumer = consumer
        self.producer = producer or FakeProducer()
        self.audit: list[AuditEvent] = []
        self.logs = io.StringIO()
        self.now = 0.0
        self.service = ReplayService(
            KafkaSettings(bootstrap_servers="localhost:9092"),
            load_topic_defaults(),
            RawPublisher(self.producer),
            env="dev",
            audit=self.audit.append,
            log=JsonLogger("t", stream=self.logs, strict=True),
            traits=lambda channel: ChannelTraits(ordering),  # type: ignore[arg-type]
            idle_timeout_s=5.0,
            consumer_factory=lambda config: consumer,
            clock=self.tick,
        )

    def tick(self) -> float:
        self.now += 1.0  # every clock read is one second: poll loops with no data reach their timeouts fast
        return self.now

    def run(self, replay_id: str | None = None, **kw: Any) -> Any:
        return self.service.replay(CH, start=START, end=END, replay_id=replay_id or uuid7(), actor=ACTOR, **kw)


def test_scan_ends_a_partition_on_eof_and_republishes_only_in_range_records() -> None:
    a, b, c = uuid7(), uuid7(), uuid7()
    consumer = FakeConsumer(
        [rec(0, 5, 1_500_000, a), rec(0, 6, 1_600_000, b), rec(0, 7, 900_000, c), eof(0)], plan={0: (5, 9)}
    )
    rig = Rig(consumer)
    result = rig.run()
    assert (result.republished, result.skipped_out_of_range) == (2, 1)
    assert [p[h.TB_PARENT_ID] for p in rig.producer.sent] == [a.encode(), b.encode()]
    assert consumer.paused == [0] and consumer.closed


def test_trailing_marker_gap_ends_on_the_next_record_at_or_after_the_end_offset() -> None:
    """Offsets 7 and 8 are control records (never returned). The next visible record is 9 == end: it is NOT
    republished and the partition finishes without waiting for 7 or 8."""
    a, late = uuid7(), uuid7()
    consumer = FakeConsumer([rec(0, 5, 1_500_000, a), rec(0, 9, 1_700_000, late)], plan={0: (5, 9)})
    rig = Rig(consumer)
    result = rig.run()
    assert result.republished == 1 and [p[h.TB_PARENT_ID] for p in rig.producer.sent] == [a.encode()]


def test_the_record_before_the_end_offset_finishes_the_partition_without_an_eof() -> None:
    a = uuid7()
    consumer = FakeConsumer([rec(0, 8, 1_500_000, a)], plan={0: (5, 9)})
    result = Rig(consumer).run()
    assert result.republished == 1 and consumer.paused == [0]


def test_an_empty_partition_is_not_assigned_and_nothing_is_published() -> None:
    consumer = FakeConsumer([], plan={0: (0, 0)})
    rig = Rig(consumer)
    result = rig.run()
    assert (result.partitions_scanned, result.republished, result.span) == (0, 0, 0)
    assert rig.producer.sent == [] and [e.phase for e in rig.audit] == ["requested", "completed"]


def test_a_stalled_read_is_source_timeout_and_audits_the_failure_with_the_partial_count() -> None:
    a = uuid7()
    consumer = FakeConsumer([rec(0, 5, 1_500_000, a)], plan={0: (5, 9)})  # then silence
    rig = Rig(consumer)
    with pytest.raises(ReplayRefused) as raised:
        rig.run()
    assert raised.value.code == "SOURCE_TIMEOUT" and raised.value.republished == 1
    assert [(e.phase, e.count) for e in rig.audit] == [("requested", None), ("failed", 1)]
    assert "channel_replay_failed" in rig.logs.getvalue() and consumer.closed
    assert len(rig.producer.sent) == 1, "what was queued before the failure is flushed, not dropped"


def test_range_too_large_publishes_nothing_and_audits_a_failure_with_zero() -> None:
    consumer = FakeConsumer([], plan={0: (0, 50)})
    rig = Rig(consumer)
    with pytest.raises(ReplayRefused) as raised:
        rig.run(max_records=10)
    assert raised.value.code == "RANGE_TOO_LARGE" and raised.value.republished == 0
    assert rig.producer.sent == [] and rig.audit[-1].phase == "failed" and rig.audit[-1].count == 0


def test_the_overall_deadline_stops_a_scan_that_keeps_receiving_data() -> None:
    script = [rec(0, 5 + i, 1_500_000, uuid7()) for i in range(3)]
    rig = Rig(FakeConsumer(script, plan={0: (5, 500)}))
    with pytest.raises(ReplayRefused) as raised:
        rig.run(deadline_s=3.0)
    assert raised.value.code == "DEADLINE_EXCEEDED"


@pytest.mark.parametrize("where", ["assign", "poll", "pause"])
def test_kafka_exceptions_are_wrapped_in_a_coded_error(where: str) -> None:
    script = [rec(0, 8, 1_500_000, uuid7())]  # offset 8 is stop - 1: the scan calls pause
    rig = Rig(FakeConsumer(script, plan={0: (5, 9)}, raise_on=where))
    with pytest.raises(ReplayRefused) as raised:
        rig.run()
    assert raised.value.code == "SOURCE_ERROR" and raised.value.__cause__ is None


def test_a_failed_delivery_is_reported_with_the_acknowledged_count() -> None:
    script = [rec(0, 5 + i, 1_500_000 + i, uuid7()) for i in range(3)] + [eof(0)]
    rig = Rig(FakeConsumer(script, plan={0: (5, 20)}), producer=FakeProducer(fail_after=2))
    with pytest.raises(PublishError) as raised:
        rig.run()
    assert raised.value.code == "PRODUCE_FAILED" and raised.value.republished == 2
    assert [(e.phase, e.count) for e in rig.audit][-1] == ("failed", 2)


def test_a_record_without_a_timestamp_or_identity_is_skipped_and_counted() -> None:
    no_ts = rec(0, 5, 0, uuid7())
    no_ts.timestamp = lambda: (0, -1)  # TIMESTAMP_NOT_AVAILABLE
    consumer = FakeConsumer(
        [no_ts, rec(0, 6, 1_500_000, None), rec(0, 7, 1_500_000, uuid7(), channel="other"), eof(0)], plan={0: (5, 9)}
    )
    result = Rig(consumer).run()
    assert (result.republished, result.skipped_no_timestamp, result.skipped_undecodable) == (0, 1, 2)


def test_the_same_replay_id_twice_yields_identical_new_ids_and_parent_ids() -> None:
    ids = [uuid7(), uuid7()]
    replay_id = uuid7()
    runs = []
    for _ in range(2):
        script = [rec(0, 5, 1_500_000, ids[0]), rec(0, 6, 1_600_000, ids[1]), eof(0)]
        rig = Rig(FakeConsumer(script, plan={0: (5, 9)}))
        rig.run(replay_id)
        runs.append([(p[h.TB_MSG_ID], p[h.TB_PARENT_ID]) for p in rig.producer.sent])
    assert runs[0] == runs[1]
    assert runs[0] == [(reprocess_id(i, replay_id).encode(), i.encode()) for i in ids]


def test_replay_refuses_per_key_without_allow_reorder_and_records_the_flag_when_given() -> None:
    rig = Rig(FakeConsumer([eof(0)], plan={0: (5, 9)}), ordering="per-key")
    with pytest.raises(ReplayRefused) as raised:
        rig.run()
    assert raised.value.code == "ORDERING_WOULD_BREAK" and rig.audit == []
    rig.run(allow_reorder=True)
    requested = rig.audit[0]
    assert requested.allow_reorder and requested.target_topic == TOPIC
    assert (requested.range_start, requested.range_end) == (START, END)


def test_limits_are_hard() -> None:
    rig = Rig(FakeConsumer([], plan={0: (0, 0)}))
    for kw in ({"max_records": HARD_MAX_RECORDS + 1}, {"max_records": 0}, {"deadline_s": 0}, {"deadline_s": 10**6}):
        with pytest.raises(ReplayRefused) as raised:
            rig.run(**kw)
        assert raised.value.code == "INVALID_REQUEST"
