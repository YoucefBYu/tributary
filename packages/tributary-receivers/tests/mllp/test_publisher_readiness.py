"""`RawPublisher` readiness state (`contracts/observability/v0` 2.3 `raw_producer`, 2.4 warm-up, R1, R2):
not warmed until one metadata round trip names a leader for every partition, `all_brokers_down` from
`error_cb` clearing on a delivery report or a metadata round trip, and `fatal` calling the handler once.
A fake producer, no broker."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest
from tributary.receivers.mllp import kafka_publisher
from tributary.receivers.mllp.kafka_publisher import RawPublisher
from tributary.runtime.clients import KafkaSettings

TOPIC = "dev.c.raw"


class FakeError:
    def __init__(self, name: str, *, fatal: bool = False) -> None:
        self._name, self._fatal = name, fatal

    def name(self) -> str:
        return self._name

    def fatal(self) -> bool:
        return self._fatal


def topic_metadata(*leaders: int, error: object | None = None) -> Any:
    partitions = {i: SimpleNamespace(leader=leader, error=None) for i, leader in enumerate(leaders)}
    return SimpleNamespace(topics={TOPIC: SimpleNamespace(partitions=partitions, error=error)})


class FakeProducer:
    last: FakeProducer | None = None

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.metadata: Any = topic_metadata(0, 0)
        self.fail_metadata = False
        self.metadata_calls = 0
        self.produced: list[dict[str, Any]] = []
        type(self).last = self

    def poll(self, timeout: float) -> int:
        time.sleep(0.001)
        return 0

    def flush(self, timeout: float) -> int:
        return 0

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        self.metadata_calls += 1
        if self.fail_metadata:
            raise RuntimeError("no brokers")
        return self.metadata

    def produce(self, topic: str, **kw: Any) -> None:
        self.produced.append(kw)


@pytest.fixture
def pub(monkeypatch: pytest.MonkeyPatch) -> RawPublisher:
    monkeypatch.setattr("confluent_kafka.Producer", FakeProducer)
    monkeypatch.setattr(kafka_publisher, "PROBE_INTERVAL_S", 0.0)
    return RawPublisher(KafkaSettings("localhost:9092"), TOPIC)


def producer() -> FakeProducer:
    assert FakeProducer.last is not None
    return FakeProducer.last


def test_not_warmed_until_a_metadata_round_trip_names_every_leader(pub: RawPublisher) -> None:
    assert pub.readiness_reason() == "not_warmed" and not pub.warmed
    producer().fail_metadata = True
    assert pub.warm_up(0.1) is False and pub.readiness_reason() == "not_warmed"
    producer().fail_metadata = False
    producer().metadata = topic_metadata(0, -1)  # a partition without a leader
    assert pub.warm_up(0.1) is False
    producer().metadata = topic_metadata(error=FakeError("UNKNOWN_TOPIC_OR_PART"))  # no partitions, topic error
    assert pub.warm_up(0.1) is False and not pub.warmed
    producer().metadata = SimpleNamespace(topics={})
    assert pub.warm_up(0.1) is False
    producer().metadata = topic_metadata(0, 1, 2)
    assert pub.warm_up(0.1) is True and pub.warmed and pub.readiness_reason() is None


def test_error_cb_reports_all_brokers_down_and_other_errors_change_nothing(pub: RawPublisher) -> None:
    pub.warm_up(0.1)
    cb = producer().config["error_cb"]
    cb(FakeError("_TRANSPORT"))
    assert pub.readiness_reason() is None
    cb(FakeError("_ALL_BROKERS_DOWN"))
    assert pub.readiness_reason() == "all_brokers_down"


def test_all_brokers_down_clears_on_a_metadata_round_trip_off_the_poll_thread(pub: RawPublisher) -> None:
    async def go() -> None:
        pub.start(asyncio.get_running_loop())
        try:
            pub.warm_up(0.1)
            producer().fail_metadata = True
            producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
            await asyncio.sleep(0.15)
            assert pub.readiness_reason() == "all_brokers_down", "a failed probe does not clear it"
            calls = producer().metadata_calls
            producer().fail_metadata = False
            for _ in range(200):
                if pub.readiness_reason() is None:
                    break
                await asyncio.sleep(0.01)
            assert pub.readiness_reason() is None and producer().metadata_calls > calls
        finally:
            pub.close()

    asyncio.run(go())


def test_all_brokers_down_clears_on_a_successful_delivery_report(pub: RawPublisher) -> None:
    from tributary.sdk import Message

    pub.warm_up(0.1)
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))

    async def go() -> None:
        pub.start(asyncio.get_running_loop())
        msg = Message(headers=_headers(), payload=b"MSH|synthetic", key=None)
        task = asyncio.ensure_future(pub.publish(msg, timeout_s=2.0))
        await asyncio.sleep(0.05)
        producer().produced[0]["on_delivery"](None, None)  # the broker acknowledged
        assert await task is True
        pub.close()

    asyncio.run(go())
    assert pub.readiness_reason() is None


def _headers() -> dict[str, str]:
    from tributary.receivers.mllp.kafka_publisher import build_raw_envelope

    return dict(build_raw_envelope(channel="c", channel_version=1, source="mllp-0", payload=b"x", key=None).headers)


def test_fatal_is_reported_once_to_the_handler(pub: RawPublisher) -> None:
    pub.warm_up(0.1)
    calls: list[int] = []
    pub.fatal_handler = lambda: calls.append(1)
    cb = producer().config["error_cb"]
    cb(FakeError("_FATAL"))
    cb(FakeError("ANYTHING", fatal=True))
    assert pub.readiness_reason() == "fatal" and calls == [1]


# -- the recovery probe's lifecycle (observability/v0 R1) --------------------------------------------------


def wait_until(predicate: Any, what: str, timeout_s: float = 3.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        assert time.monotonic() < deadline, what
        time.sleep(0.005)


def test_at_most_one_probe_is_in_flight_and_they_are_rate_limited(
    pub: RawPublisher, monkeypatch: pytest.MonkeyPatch
) -> None:
    import threading

    now = [1000.0]
    monkeypatch.setattr(kafka_publisher, "time", SimpleNamespace(monotonic=lambda: now[0]))  # a fake clock
    monkeypatch.setattr(kafka_publisher, "PROBE_INTERVAL_S", 5.0)
    pub.warm_up(0.1)
    calls_before = producer().metadata_calls
    release = threading.Event()
    original = producer().list_topics

    def blocking(topic: str | None = None, timeout: float = -1) -> Any:
        release.wait(5)
        return original(topic, timeout)

    producer().list_topics = blocking  # type: ignore[method-assign]
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
    for _ in range(20):
        pub._maybe_probe()  # as the poll thread does on every loop
    threads = [t for t in threading.enumerate() if t.name == "raw-publisher-probe"]
    assert len(threads) == 1, "one probe in flight, however often it is asked for"
    release.set()
    wait_until(lambda: pub.readiness_reason() is None, "the probe to clear the flag")
    assert producer().metadata_calls == calls_before + 1
    # Rate limit: down again at once, the next probe waits for the interval.
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
    pub._maybe_probe()
    assert producer().metadata_calls == calls_before + 1 and pub.readiness_reason() == "all_brokers_down"
    now[0] += 5.1
    pub._maybe_probe()
    wait_until(lambda: pub.readiness_reason() is None, "the second probe after the interval")
    assert producer().metadata_calls == calls_before + 2


def test_a_probe_that_began_before_a_newer_all_brokers_down_cannot_clear_it(pub: RawPublisher) -> None:
    import threading

    pub.warm_up(0.1)
    started, release = threading.Event(), threading.Event()
    original = producer().list_topics

    def slow(topic: str | None = None, timeout: float = -1) -> Any:
        started.set()
        release.wait(5)
        return original(topic, timeout)

    producer().list_topics = slow  # type: ignore[method-assign]
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
    pub._maybe_probe()
    assert started.wait(3)
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))  # a fresh outage after the probe began
    release.set()
    wait_until(lambda: not pub._probe_in_flight, "the stale probe to finish")
    assert pub.readiness_reason() == "all_brokers_down", "its success says nothing about the newer outage"


def test_a_stale_warm_up_cannot_clear_a_newer_outage(pub: RawPublisher) -> None:
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
    original = producer().list_topics

    def down_during(topic: str | None = None, timeout: float = -1) -> Any:
        producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
        return original(topic, timeout)

    producer().list_topics = down_during  # type: ignore[method-assign]
    assert pub.warm_up(0.1) is True
    assert pub.readiness_reason() == "all_brokers_down"


def test_a_closed_publisher_makes_no_probe_call(pub: RawPublisher) -> None:
    pub.warm_up(0.1)
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
    calls = producer().metadata_calls
    pub.close()
    pub._probe()
    assert producer().metadata_calls == calls


def test_close_joins_a_probe_in_flight(pub: RawPublisher) -> None:
    pub.warm_up(0.1)
    original = producer().list_topics

    def slowish(topic: str | None = None, timeout: float = -1) -> Any:
        time.sleep(0.3)
        return original(topic, timeout)

    producer().list_topics = slowish  # type: ignore[method-assign]
    producer().config["error_cb"](FakeError("_ALL_BROKERS_DOWN"))
    pub._maybe_probe()
    pub.close()
    assert not pub._probe_in_flight, "close waited for the probe instead of leaving it on a closing producer"
