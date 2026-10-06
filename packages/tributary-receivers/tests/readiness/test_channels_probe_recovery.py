"""W3-18: the deferred channels probe, with a real `ChannelCache` and its real probe thread, asserted through
readiness transitions (`contracts/observability/v0` R3).

Only the Kafka consumer is a double: `list_topics` is a switch the test turns (healthy, failing, stuck for a
while on the probe thread, then failing). Everything else is production code: `ChannelCache`'s daemon probe
thread (`tb-channels-probe`), `LiveSpecFeed`, the lifecycle and the ops listener.

* **Before the first spec** a stuck or failing reader makes the receiver refuse: `/readyz` is 503 `spec:
  no_spec`, nothing binds, nothing is ACKed; when the reader recovers it applies the spec and becomes ready.
* **After the first spec** the same fault never drops readiness (invariant 3): `/readyz` stays 200 and adds
  `spec_source_unproven` to `degraded` while the probe is failing, and clears it when the probe recovers. The
  event loop that serves `/readyz` is never held up by the stuck probe thread.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from types import SimpleNamespace
from typing import Any

import pytest
from tributary.receivers.lifecycle import LiveSpecFeed
from tributary.runtime.channels import ChannelCache, LiveSpecSource

from tests.support.receiver_specs import http_document

from .support import get
from .test_lifecycle_readiness import Recorder, Rig, reasons

CHANNEL = "fhir-in"
TOPIC = "_tb.dev.channels"


class Record:
    def __init__(self, *, key: bytes, value: bytes | None, error: Any = None, offset: int = 0) -> None:
        self._key, self._value, self._error, self._offset = key, value, error, offset

    def key(self) -> bytes:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class Eof:
    def code(self) -> int:
        return -191

    def name(self) -> str:
        return "_PARTITION_EOF"


class SwitchConsumer:
    """`list_topics` is `healthy`, `failing` (raises at once) or `stuck` (blocks on the calling thread for
    `stuck_s`, then raises, as librdkafka does at its timeout)."""

    def __init__(self, spec_document: dict[str, Any]) -> None:
        self.mode = "healthy"
        self.stuck_s = 0.4
        self.records: list[Any] = [
            Record(key=CHANNEL.encode(), value=json.dumps(spec_document).encode()),
            Record(key=b"", value=None, error=Eof()),
        ]
        self.probe_threads: set[str] = set()
        self.stuck_started = threading.Event()
        self.closed = False

    def assign(self, partitions: list[Any]) -> None:
        pass

    def poll(self, timeout: float) -> Any:
        return self.records.pop(0) if self.records else None

    def list_topics(self, topic: str | None = None, timeout: float = -1) -> Any:
        self.probe_threads.add(threading.current_thread().name)
        if self.mode == "failing":
            raise RuntimeError("brokers unreachable")
        if self.mode == "stuck":
            self.stuck_started.set()
            time.sleep(self.stuck_s)
            raise RuntimeError("timed out")
        detail = SimpleNamespace(leader=1, error=None)
        return SimpleNamespace(topics={TOPIC: SimpleNamespace(partitions={0: detail}, error=None)})

    def close(self) -> None:
        self.closed = True


class Sampler:
    """Polls `/readyz` every few milliseconds on the event loop and keeps `(status, degraded, latency_s)`."""

    def __init__(self, port: int) -> None:
        self.port = port
        self.samples: list[tuple[int, tuple[str, ...], float]] = []
        self._task: asyncio.Task[None] | None = None

    async def _run(self) -> None:
        while True:
            started = time.monotonic()
            status, doc = await get(self.port, "/readyz")
            assert doc is not None
            self.samples.append((status, tuple(doc.get("degraded", ())), time.monotonic() - started))
            await asyncio.sleep(0.01)

    def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        assert self._task is not None
        self._task.cancel()
        await asyncio.gather(self._task, return_exceptions=True)


def rig_with_real_cache(consumer: SwitchConsumer, log: Recorder | None = None) -> Rig:
    rig = Rig(None)
    if log is not None:
        rig.log = log

    def open_feed() -> LiveSpecFeed:
        rig.opened += 1
        cache = ChannelCache(consumer, TOPIC, log=rig.log, probe_interval_s=0.02, probe_timeout_s=1.0)
        cache.start()
        cache.catch_up(timeout_s=5)
        source = LiveSpecSource(cache, CHANNEL)
        source.current()
        return LiveSpecFeed(source, CHANNEL)

    rig.runner._open_feed = open_feed  # type: ignore[assignment]
    rig.runner._log = rig.log
    return rig


def test_a_failing_reader_refuses_before_the_first_spec_and_never_drops_readiness_after_it() -> None:
    consumer = SwitchConsumer(http_document(CHANNEL, version=1))

    async def go() -> None:
        consumer.mode = "failing"
        rig = rig_with_real_cache(consumer)
        task = asyncio.create_task(rig.runner.run())
        try:
            # -- before the first spec: refuse, not ready, nothing binds ---------------------------------
            port = await rig.ops_port()
            await rig.until(lambda: rig.opened >= 2)  # the fault fired and the open is being retried
            status, doc = await get(port, "/readyz")
            assert doc is not None and status == 503 and reasons(doc)["spec"] == "no_spec"
            assert rig.runner.server is None and rig.emitted == []
            # `list_topics` raising out of `start()` is `spec_source_failed`; a topic that cannot be shown is
            # `spec_unavailable`. Either way the receiver refuses and says so.
            assert {"spec_unavailable", "spec_source_failed"} & set(rig.log.names())

            # -- recovery: the reader works, the spec applies, the receiver is ready ----------------------
            consumer.mode = "healthy"
            await rig.await_status(200)
            assert rig.emitted and rig.runner.server is not None

            # -- after the first spec: the same fault degrades, never drops --------------------------------
            sampler = Sampler(port)
            sampler.start()
            consumer.mode = "stuck"
            await asyncio.get_running_loop().run_in_executor(None, consumer.stuck_started.wait, 5)
            assert any(t.name == "tb-channels-probe" and t.is_alive() for t in threading.enumerate()), (
                "the stuck probe runs on a real thread of its own"
            )
            await rig.until(lambda: "spec_poll_failed" in rig.log.names(), timeout=10)  # the probe failed: unproven
            await asyncio.sleep(0.15)
            consumer.mode = "healthy"
            await rig.until(lambda: "spec_poll_recovered" in rig.log.names(), timeout=10)
            await asyncio.sleep(0.15)
            await sampler.stop()

            statuses = {status for status, _d, _t in sampler.samples}
            assert statuses == {200}, f"readiness dropped during a channels outage: {sorted(statuses)}"
            degraded = [d for _s, d, _t in sampler.samples]
            first = degraded.index(("spec_source_unproven",))
            assert () in degraded[:first] and () in degraded[first:], "it appeared, and it cleared"
            assert max(latency for _s, _d, latency in sampler.samples) < 0.5, (
                "the stuck probe thread never held up the loop that serves /readyz"
            )
            assert "tb-channels-probe" in consumer.probe_threads
            status, doc = await get(port, "/readyz")
            assert doc is not None and status == 200 and "degraded" not in doc
        finally:
            rig.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == 0

    asyncio.run(go())


def test_the_control_a_probe_that_never_fails_never_reports_the_degradation() -> None:
    """The fault must be what produced the degradation: with a healthy probe throughout, none appears."""
    consumer = SwitchConsumer(http_document(CHANNEL, version=1))

    async def go() -> None:
        rig = rig_with_real_cache(consumer)
        task = asyncio.create_task(rig.runner.run())
        try:
            await rig.await_status(200)
            sampler = Sampler(await rig.ops_port())
            sampler.start()
            await asyncio.sleep(0.5)
            await sampler.stop()
            assert sampler.samples and all(d == () for _s, d, _t in sampler.samples)
            assert "tb-channels-probe" in consumer.probe_threads, "the probe did run, on its own thread"
        finally:
            rig.runner.request_stop()
            await asyncio.wait_for(task, 15)

    asyncio.run(go())


@pytest.mark.parametrize("mode", ["failing", "stuck"])
def test_a_reader_that_never_comes_up_keeps_the_receiver_refusing(mode: str) -> None:
    consumer = SwitchConsumer(http_document(CHANNEL, version=1))
    consumer.mode = mode
    consumer.stuck_s = 0.05

    async def go() -> None:
        rig = rig_with_real_cache(consumer)
        task = asyncio.create_task(rig.runner.run())
        try:
            port = await rig.ops_port()
            await rig.until(lambda: rig.opened >= 3)
            for _ in range(10):
                status, doc = await get(port, "/readyz")
                assert doc is not None and status == 503 and reasons(doc)["spec"] == "no_spec"
                await asyncio.sleep(0.02)
            assert rig.emitted == [] and rig.runner.server is None, "nothing was bound or accepted"
        finally:
            rig.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == 0

    asyncio.run(go())
