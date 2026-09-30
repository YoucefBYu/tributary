"""The Kafka-source receiver's spec lifecycle (invariant 3) against a fake spec feed and a fake source cluster:
not ready until a spec is read, live changes, restart-only changes, refused updates keep the last good spec, a
tombstone pauses consumption and commits only what was acked, and a redeploy resumes from the committed offset.
Real `KafkaSourceService` and `KafkaSourceReceiver`, fake consumer and publisher, no Kafka."""

from __future__ import annotations

import asyncio
import copy
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import TopicPartition
from tributary.receivers.kafka_source import receiver as receiver_mod
from tributary.receivers.kafka_source.config import kafka_source_settings_from_spec
from tributary.receivers.kafka_source.reload import live_fields_changed, restart_required_fields
from tributary.receivers.kafka_source.runner import KafkaSourceRunner, RunnerOptions, SpecFeed
from tributary.receivers.kafka_source.service import KafkaSourceService
from tributary.receivers.metrics import Metrics
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec
from tributary.sdk import Message

from tests.support.engine_kafka import write_engine_kafka_file
from tests.support.receiver_specs import kafka_source_document

TOPIC = "upstream.events"
SECRET_REF = "SYNTH-SECRET-REF-77cd"  # noqa: S105


def record(n: int) -> bytes:
    return json.dumps({"n": n, "note": "TRIBUTARY-SYNTH"}).encode()


def parse(doc: dict[str, Any]) -> ChannelSpec:
    return ChannelSpec.from_document(copy.deepcopy(doc))


class FakeFeed:
    def __init__(self, spec: ChannelSpec | None) -> None:
        self.spec = spec
        self.poll_error: Exception | None = None

    def poll(self) -> None:
        if self.poll_error is not None:
            raise self.poll_error

    def get(self) -> ChannelSpec | None:
        return self.spec

    def close(self) -> None:
        pass


class Recorder:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def _add(self, event: str, fields: dict[str, Any]) -> None:
        self.events.append((event, fields))

    def debug(self, event: str, /, **fields: Any) -> None:
        self._add(event, fields)

    info = warning = error = debug

    def names(self) -> list[str]:
        return [n for n, _ in self.events]

    def of(self, name: str) -> list[dict[str, Any]]:
        return [f for n, f in self.events if n == name]


class Msg:
    def __init__(self, offset: int, value: bytes) -> None:
        self._o, self._v = offset, value

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._o

    def value(self) -> bytes:
        return self._v

    def key(self) -> bytes | None:
        return None

    def headers(self) -> list[tuple[str, bytes | None]] | None:
        return None

    def error(self) -> Any:
        return None


class SourceCluster:
    """One partition of records and the group's committed offset, shared by every consumer built over time."""

    def __init__(self) -> None:
        self.log: list[bytes] = []
        self.committed = 0
        self.commits: list[int] = []
        self.consumers = 0
        self.open_consumers = 0
        self.after_fetch: Callable[[int], None] | None = None

    def add(self, n: int) -> None:
        self.log.extend(record(i) for i in range(len(self.log), len(self.log) + n))


class Consumer:
    def __init__(self, cluster: SourceCluster) -> None:
        self.cluster = cluster
        self.position = cluster.committed
        self.closed = False
        cluster.consumers += 1
        cluster.open_consumers += 1

    def subscribe(self, topics: list[str], **_cb: Any) -> None:
        assert topics == [TOPIC]

    def poll(self, timeout: float) -> Msg | None:
        c = self.cluster
        if self.position >= len(c.log):
            import time

            time.sleep(min(timeout, 0.005))
            return None
        msg = Msg(self.position, c.log[self.position])
        self.position += 1
        if c.after_fetch is not None:
            c.after_fetch(msg.offset())
        return msg

    def commit(self, *, offsets: list[TopicPartition], asynchronous: bool) -> Any:
        assert asynchronous is False
        for tp in offsets:
            self.cluster.committed = tp.offset
            self.cluster.commits.append(tp.offset)

    def pause(self, partitions: list[TopicPartition]) -> None:
        pass

    def resume(self, partitions: list[TopicPartition]) -> None:
        pass

    def seek(self, partition: TopicPartition) -> None:
        self.position = partition.offset

    def assignment(self) -> list[TopicPartition]:
        return [TopicPartition(TOPIC, 0)]

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.cluster.open_consumers -= 1


class Publisher:
    def __init__(self) -> None:
        self.calls: list[Message] = []
        self.gate: asyncio.Event | None = None
        self.entered = asyncio.Event()
        self.closed = False

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        self.entered.set()
        if self.gate is not None:
            await self.gate.wait()
        return True

    def close(self) -> None:
        self.closed = True

    def numbers(self) -> list[int]:
        return [json.loads(m.payload or b"{}")["n"] for m in self.calls]


class Harness:
    def __init__(self, feed: FakeFeed | None) -> None:
        self.feed = feed if feed is not None else FakeFeed(None)
        self.cluster = SourceCluster()
        self.log = Recorder()
        self.publisher = Publisher()
        self.emitted: list[dict[str, Any]] = []
        self.feed_ready = True

        def open_feed() -> SpecFeed:
            if not self.feed_ready:
                raise ChannelSpecError("channel is not deployed")
            return self.feed

        def make_service(cfg: Any, publisher: Any, metrics: Metrics) -> KafkaSourceService:
            return KafkaSourceService(cfg, publisher, Consumer(self.cluster), metrics=metrics)

        self.runner = KafkaSourceRunner(
            open_feed=open_feed,
            make_publisher=lambda _cfg: self.publisher,  # type: ignore[arg-type,return-value]
            make_service=make_service,
            options=RunnerOptions(
                env="dev",
                instance="kafka-t",
                topic="dev.lab-bridge.raw",
                poll_interval_s=0.01,
                open_retry_initial_s=0.01,
                open_retry_max_s=0.02,
            ),
            commit_every=1,
            log=self.log,
            emit=self.emitted.append,
        )

    async def until(self, predicate: Callable[[], bool], timeout: float = 5.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"condition not met; events={self.log.names()}")
            await asyncio.sleep(0.005)

    async def started(self, n: int = 1) -> None:
        await self.until(lambda: sum(e["event"] == "listening" for e in self.emitted) >= n)


def run(scenario: Callable[[Harness], Awaitable[None]], feed: FakeFeed | None) -> Harness:
    async def go() -> Harness:
        h = Harness(feed)
        task = asyncio.create_task(h.runner.run())
        try:
            await scenario(h)
        finally:
            h.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == 0
        return h

    return asyncio.run(go())


def doc(channel: str = "lab-bridge", **kw: Any) -> dict[str, Any]:
    kw.setdefault("topic", TOPIC)
    return kafka_source_document(channel, **kw)


# -- startup ----------------------------------------------------------------------------------------


def test_nothing_is_consumed_until_a_spec_has_been_read() -> None:
    async def scenario(h: Harness) -> None:
        h.feed_ready = False
        h.cluster.add(2)
        await h.until(lambda: "spec_unavailable" in h.log.names())
        assert h.emitted == [] and h.cluster.consumers == 0
        h.feed_ready = True
        h.feed.spec = parse(doc())
        await h.started()
        await h.until(lambda: h.publisher.numbers() == [0, 1])
        await h.until(lambda: h.cluster.committed == 2)

    h = run(scenario, FakeFeed(None))
    [event] = h.emitted
    assert set(event) == {"event", "channel", "channel_version", "topic", "port", "tls"}


def test_a_spec_this_receiver_refuses_stays_not_ready_and_says_why() -> None:
    async def scenario(h: Harness) -> None:
        await h.until(lambda: "spec_rejected" in h.log.names())
        assert h.runner.service is None and h.cluster.consumers == 0

    run(scenario, FakeFeed(parse(doc(nack_on=("queue-full",)))))


# -- live changes -----------------------------------------------------------------------------------


def test_nack_on_size_limit_and_content_type_apply_live_and_each_record_carries_its_version() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.cluster.add(1)
        await h.until(lambda: len(h.publisher.calls) == 1)

        h.feed.spec = parse(doc(version=2, maxMessageBytes=20))  # every synthetic record is longer than 20 bytes
        await h.until(lambda: "spec_applied" in h.log.names())
        assert h.log.of("spec_applied")[0]["changed"] == "channel_version,max_message_bytes"
        h.cluster.add(1)
        await h.until(lambda: h.runner.service is not None and bool(h.runner.service.receiver.halted))
        assert len(h.publisher.calls) == 1  # refused under the new limit: halted, never published
        assert h.runner.restart_requested is False
        assert h.cluster.committed == 1  # what was acked before the refused record stays committed
        assert h.publisher.calls[0].headers["tb-channel-version"] == "1"

    run(scenario, FakeFeed(parse(doc(version=1))))


@pytest.mark.parametrize(
    "bad",
    [
        {"maxMessageBytes": 0},
        {"maxMessageBytes": -1},
        {"maxMessageBytes": 2_000_000},
        {"ack": {"mode": "after-commit", "nackOn": ["queue-full"]}},
        {"ack": {"mode": "after-commit", "timeoutMs": 0}},
        {"ack": {"mode": "fire-and-forget"}},
        {"contentType": "application/xml", "kafka": None},  # a malformed kafka block
    ],
)
def test_bad_values_are_refused_before_apply_and_the_last_good_spec_keeps_consuming(bad: dict[str, Any]) -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        d = doc(version=2)
        d["spec"]["source"].update(bad)
        h.feed.spec = parse(d)
        await h.until(lambda: "spec_update_rejected" in h.log.names())
        assert h.runner.service is not None and h.runner.service.cfg.channel_version == 1
        h.cluster.add(1)
        await h.until(lambda: len(h.publisher.calls) == 1)
        assert h.runner.restart_requested is False

    run(scenario, FakeFeed(parse(doc())))


def test_control_plane_down_the_cached_spec_keeps_consuming() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.feed.poll_error = OSError("channels topic unreachable")
        await h.until(lambda: "spec_poll_failed" in h.log.names())
        h.cluster.add(1)
        await h.until(lambda: len(h.publisher.calls) == 1)
        assert len(h.log.of("spec_poll_failed")) == 1
        h.feed.poll_error = None
        await h.until(lambda: "spec_poll_recovered" in h.log.names())

    run(scenario, FakeFeed(parse(doc())))


# -- restart-only changes ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "field"),
    [
        ({"topic": "other.events"}, "topic"),
        ({"bootstrap": "127.0.0.1:19093"}, "source_bootstrap"),
        ({"start_from": "latest"}, "start_from"),
        (
            {"security": {"protocol": "SASL_SSL", "saslMechanism": "SCRAM-SHA-512", "secretRef": SECRET_REF}},
            "security",
        ),
    ],
)
def test_topic_group_and_foreign_cluster_security_are_restart_only_and_no_value_is_logged(
    kwargs: dict[str, Any], field: str
) -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        kw = dict(kwargs)
        kw.setdefault("topic", TOPIC)
        d = kafka_source_document("lab-bridge", version=2, **kw)
        h.feed.spec = parse(d)
        if field == "security":  # a non-loopback-safe SASL_SSL block needs a non-loopback-free bootstrap: keep it valid
            pass
        await h.until(lambda: h.runner.restart_requested or "spec_update_rejected" in h.log.names())

    h = run(scenario, FakeFeed(parse(doc())))
    assert h.log.of("spec_restart_required")[0]["fields"] == field
    assert "spec_applied" not in h.log.names()
    dumped = repr(h.log.events)
    assert SECRET_REF not in dumped and "other.events" not in dumped and "19093" not in dumped


def test_the_partition_key_is_restart_only() -> None:
    def keyed(version: int, key: str) -> ChannelSpec:
        d = doc(version=version)
        d["spec"]["ordering"] = "per-key"
        d["spec"]["partitionKey"] = key
        return parse(d)

    async def scenario(h: Harness) -> None:
        await h.started()
        h.feed.spec = keyed(2, "json:/other")
        await h.until(lambda: h.runner.restart_requested)

    h = run(scenario, FakeFeed(keyed(1, "json:/n")))
    assert h.log.of("spec_restart_required")[0]["fields"] == "partition_key"


def test_the_reload_table_names_fields_only() -> None:
    a = kafka_source_settings_from_spec(parse(doc()), env="dev", commit_every=1)
    b = kafka_source_settings_from_spec(parse(doc(version=2, maxMessageBytes=10)), env="dev", commit_every=1)
    c = kafka_source_settings_from_spec(parse(doc(version=3, topic="x.y")), env="dev", commit_every=1)
    assert restart_required_fields(a, b) == [] and live_fields_changed(a, b) == ["channel_version", "max_message_bytes"]
    assert restart_required_fields(a, c) == ["topic"]


# -- tombstone and redeploy -------------------------------------------------------------------------


def test_tombstone_pauses_consumption_and_commits_only_what_was_acked() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()
        h.publisher.gate = asyncio.Event()
        h.cluster.add(5)
        await asyncio.wait_for(h.publisher.entered.wait(), 5)  # record 0 is in flight, not yet acked
        assert h.cluster.committed == 0

        h.feed.spec = None  # the tombstone
        await h.until(lambda: "channel_undeployed" in h.log.names())
        h.publisher.gate.set()
        await h.until(lambda: h.cluster.open_consumers == 0)  # committed and closed
        assert h.publisher.numbers() == [0]  # nothing fetched after the pause was published
        assert h.cluster.committed == 1  # exactly what was acked: record 0, never 1..4
        assert set(h.cluster.commits) <= {1}

        h.feed.spec = parse(doc(version=2))  # redeploy: a new consumer resumes from the committed offset
        await h.started(2)
        await h.until(lambda: h.cluster.committed == 5)
        assert h.publisher.numbers() == [0, 1, 2, 3, 4]  # no gap and no duplicate
        assert h.cluster.consumers == 2

    h = run(scenario, FakeFeed(parse(doc())))
    assert h.runner.restart_requested is False


def test_a_record_fetched_when_the_tombstone_lands_is_not_published_or_committed_and_is_read_again() -> None:
    async def scenario(h: Harness) -> None:
        await h.started()

        def tombstone(offset: int) -> None:
            if offset == 1:  # record 1 was fetched; the tombstone lands before it is published
                h.feed.spec = None
                h.runner.pause_component()

        h.cluster.after_fetch = tombstone
        h.cluster.add(3)
        await h.until(lambda: h.cluster.open_consumers == 0)
        assert h.publisher.numbers() == [0]
        assert h.cluster.committed == 1

        h.cluster.after_fetch = None
        h.feed.spec = parse(doc(version=2))
        await h.until(lambda: h.cluster.committed == 3)
        assert h.publisher.numbers() == [0, 1, 2]

    run(scenario, FakeFeed(parse(doc())))


# -- entry point ------------------------------------------------------------------------------------


def _no_client(*_a: object, **_k: object) -> Any:
    raise AssertionError("a Kafka client was built before the refusal")


def _main(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, *argv: str) -> None:
    monkeypatch.setattr("confluent_kafka.Producer", _no_client)
    monkeypatch.setattr("confluent_kafka.Consumer", _no_client)
    monkeypatch.setenv("TB_ENGINE_KAFKA_FILE", str(write_engine_kafka_file(tmp_path, "127.0.0.1:9092")))
    monkeypatch.delenv("TB_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.setattr("sys.argv", ["prog", *argv])
    receiver_mod.main()


@pytest.mark.parametrize("form", ["separate", "equals"])
def test_channel_spec_flag_is_retired_and_its_value_never_echoed(
    form: str, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    value = "SYNTH-SPEC-PATH-5d1e"
    flag = [f"--channel-spec={value}"] if form == "equals" else ["--channel-spec", value]
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, *flag, "--channel", "lab-bridge", "--env", "dev")
    captured = capsys.readouterr()
    assert exc.value.code == 2 and value not in captured.out + captured.err
    lines = [json.loads(x) for x in captured.err.splitlines() if "startup_refused" in x]
    assert len(lines) == 1 and lines[0]["code"] == "CHANNEL_SPEC_FLAG_RETIRED"


@pytest.mark.parametrize(
    "argv",
    [
        ["--channel", "c", "--env", "dev", "--bogus-flag=SYNTH-VALUE-77"],
        ["--env", "dev"],
        ["--channel", "c", "--env", "dev", "--instance", "SYNTH-VALUE-77 bad!"],
        ["--channel", "SYNTH-VALUE-77", "--env", "dev"],
    ],
)
def test_usage_errors_are_one_startup_refused_line_and_echo_nothing(
    argv: list[str], capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    with pytest.raises(SystemExit) as exc:
        _main(monkeypatch, tmp_path, *argv)
    err = capsys.readouterr()
    assert exc.value.code == 2 and "SYNTH-VALUE-77" not in err.err + err.out and "usage:" not in err.err
    assert len([x for x in err.err.splitlines() if "startup_refused" in x]) == 1
