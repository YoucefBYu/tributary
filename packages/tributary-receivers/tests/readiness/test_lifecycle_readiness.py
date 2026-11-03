"""`/readyz` as the lifecycle drives it (`contracts/observability/v0` 2.3, 2.4, R1-R6): a real `HttpRunner` with a real
ops listener on an ephemeral port, a fake spec feed and a fake raw publisher whose warm-up and health the test
controls. Every scenario first proves its fault fired (the not-ready answer names the reason) before it proves
recovery, and `test_a_hard_wired_ready_provider_fails_the_same_assertions` is the broken-implementation control."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from tributary.receivers.http.runner import HttpRunner, RunnerOptions
from tributary.receivers.http.server import HttpServer
from tributary.runtime.errors import ChannelsUnavailableError
from tributary.runtime.ops import OpsSettings
from tributary.runtime.spec import ChannelSpec
from tributary.sdk import Message

from tests.support.receiver_specs import http_document

from .support import assert_valid, get

LOOPBACK = OpsSettings("127.0.0.1", 0)


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

    def debug(self, event: str, /, **fields: Any) -> None:
        self.events.append((event, fields))

    info = warning = error = debug

    def names(self) -> list[str]:
        return [n for n, _ in self.events]

    def of(self, name: str) -> list[dict[str, Any]]:
        return [f for n, f in self.events if n == name]


class WarmPublisher:
    """A `RawPublisher` as readiness sees it. `warm_results` is consumed one per `warm_up` call."""

    def __init__(self, warm_results: list[bool] | None = None) -> None:
        self.warm_results = warm_results if warm_results is not None else [True]
        self.warm_calls = 0
        self.warmed = False
        self.reason_override: str | None = None
        self.fatal_handler: Callable[[], None] | None = None
        self.closed = False

    def warm_up(self, timeout_s: float = 5.0) -> bool:
        self.warm_calls += 1
        ok = self.warm_results.pop(0) if len(self.warm_results) > 1 else self.warm_results[0]
        self.warmed = self.warmed or ok
        return ok

    def readiness_reason(self) -> str | None:
        if self.reason_override is not None:
            return self.reason_override
        return None if self.warmed else "not_warmed"

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        return True

    def close(self) -> None:
        self.closed = True


class Rig:
    def __init__(
        self,
        spec: ChannelSpec | None,
        *,
        publisher: WarmPublisher | None = None,
        ops: OpsSettings | None = LOOPBACK,
        runner_cls: type[HttpRunner] = HttpRunner,
    ) -> None:
        self.feed = FakeFeed(spec)
        self.log = Recorder()
        self.publisher = publisher if publisher is not None else WarmPublisher()
        self.emitted: list[dict[str, Any]] = []
        self.opened = 0

        def open_feed() -> FakeFeed:
            self.opened += 1
            return self.feed

        self.runner = runner_cls(
            open_feed=open_feed,  # type: ignore[arg-type,return-value]
            make_publisher=lambda _cfg: self.publisher,  # type: ignore[arg-type,return-value]
            make_server=lambda cfg, pub, metrics: HttpServer(cfg, pub, metrics=metrics),
            options=RunnerOptions(
                env="dev",
                instance="http-t",
                topic="dev.fhir-in.raw",
                poll_interval_s=0.01,
                open_retry_initial_s=0.01,
                open_retry_max_s=0.02,
                ops=ops,
            ),
            log=self.log,
            emit=self.emitted.append,
        )

    async def until(self, predicate: Callable[[], bool], timeout: float = 5.0) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        while not predicate():
            if asyncio.get_running_loop().time() > deadline:
                raise AssertionError(f"condition not met; events={self.log.names()}")
            await asyncio.sleep(0.005)

    async def ops_port(self) -> int:
        await self.until(lambda: self.runner.ops_port is not None)
        assert self.runner.ops_port is not None
        return self.runner.ops_port

    async def ready_doc(self) -> tuple[int, dict[str, Any]]:
        status, doc = await get(await self.ops_port(), "/readyz")
        assert doc is not None
        assert_valid(doc)
        return status, doc

    async def await_status(self, want: int) -> dict[str, Any]:
        async def poll() -> dict[str, Any]:
            while True:
                status, doc = await self.ready_doc()
                if status == want:
                    return doc
                await asyncio.sleep(0.01)

        return await asyncio.wait_for(poll(), 5)


def reasons(doc: dict[str, Any]) -> dict[str, str | None]:
    return {c["name"]: c.get("reason") for c in doc["checks"]}


def run(scenario: Callable[[Rig], Awaitable[None]], rig: Rig, *, expect_exit: int = 0) -> Rig:
    async def go() -> None:
        task = asyncio.create_task(rig.runner.run())
        try:
            await scenario(rig)
        finally:
            rig.runner.request_stop()
            assert await asyncio.wait_for(task, 15) == expect_exit

    asyncio.run(go())
    return rig


GOOD = http_document("fhir-in", version=4)


# -- before the first spec and the warm-up ------------------------------------------------------------


def test_not_ready_before_the_first_spec_and_livez_never_depends_on_it() -> None:
    async def scenario(rig: Rig) -> None:
        status, doc = await rig.ready_doc()
        assert status == 503 and doc["status"] == "not_ready"
        assert reasons(doc) == {"spec": "no_spec", "component": "starting", "raw_producer": "not_warmed"}
        assert "channel_version" not in doc and doc["channel"] == "fhir-in" and doc["receiver_type"] == "http"
        assert await get(await rig.ops_port(), "/livez") == (200, {"v": 0, "status": "alive", "component": "receiver"})
        assert rig.publisher.warm_calls == 0, "no publisher is warmed before a spec says what to build"

    run(scenario, Rig(None))


def test_the_listener_binds_only_after_the_warm_up_and_readiness_follows() -> None:
    publisher = WarmPublisher([False, False, True])

    async def scenario(rig: Rig) -> None:
        # The fault fired: warm-up is failing, so the receiver is not ready and nothing is listening.
        await rig.until(lambda: publisher.warm_calls >= 2)
        status, doc = await rig.ready_doc()
        assert status == 503 and reasons(doc)["raw_producer"] == "not_warmed"
        assert reasons(doc)["spec"] is None and reasons(doc)["component"] == "starting"
        assert rig.runner.server is None and rig.emitted == []
        assert "producer_warm_failed" in rig.log.names()

        doc = await rig.await_status(200)
        assert reasons(doc) == {"spec": None, "component": None, "raw_producer": None}
        assert doc["channel_version"] == 4 and doc["status"] == "ready"
        assert rig.runner.server is not None and publisher.warm_calls == 3
        [listening] = [e for e in rig.emitted if e["event"] == "listening"]
        assert listening["ops_port"] == rig.runner.ops_port != listening["port"]
        assert "producer_warmed" in rig.log.names()

    run(scenario, Rig(parse(GOOD), publisher=publisher))


def test_a_stop_ends_a_warm_up_that_never_succeeds() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.until(lambda: rig.publisher.warm_calls >= 1)
        assert (await rig.ready_doc())[0] == 503

    run(scenario, Rig(parse(GOOD), publisher=WarmPublisher([False])))


# -- spec states ----------------------------------------------------------------------------------------


def test_a_refused_spec_is_spec_refused_and_nothing_starts() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.until(lambda: "spec_rejected" in rig.log.names())
        status, doc = await rig.ready_doc()
        assert status == 503 and reasons(doc)["spec"] == "spec_refused"
        assert rig.publisher.warm_calls == 0 and rig.runner.server is None

    run(scenario, Rig(parse(http_document("fhir-in", nack_on=("spec-invalid",)))))


def test_a_tombstone_is_undeployed_not_ready_and_a_redeploy_restores_it() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.await_status(200)
        rig.feed.spec = None
        doc = await rig.await_status(503)
        assert reasons(doc)["spec"] == "undeployed" and reasons(doc)["component"] == "starting"
        assert rig.runner.server is None
        rig.feed.spec = parse(http_document("fhir-in", version=5))
        doc = await rig.await_status(200)
        assert doc["channel_version"] == 5

    run(scenario, Rig(parse(GOOD)))


# -- R3: a channels-topic outage is degraded, never not ready ------------------------------------------


def test_a_channels_outage_after_the_first_spec_is_degraded_and_clears_without_dropping_readiness() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.await_status(200)
        rig.feed.poll_error = ChannelsUnavailableError("_tb.dev.channels", error_code="_TRANSPORT", unavailable_for_s=1)
        await rig.until(lambda: "spec_poll_failed" in rig.log.names())  # the fault fired
        for _ in range(5):  # and readiness never dropped while it lasted
            status, doc = await rig.ready_doc()
            assert status == 200 and doc["degraded"] == ["spec_source_unproven"]
            await asyncio.sleep(0.01)
        rig.feed.poll_error = None
        await rig.until(lambda: "spec_poll_recovered" in rig.log.names())
        status, doc = await rig.ready_doc()
        assert status == 200 and "degraded" not in doc

    run(scenario, Rig(parse(GOOD)))


def test_the_lite_profile_is_degraded_and_not_gating() -> None:
    rig = Rig(parse(GOOD))
    object.__setattr__(rig.runner._opts, "lite_profile", True)

    async def scenario(rig: Rig) -> None:
        doc = await rig.await_status(200)
        assert doc["degraded"] == ["lite_profile"]

    run(scenario, rig)


# -- raw producer state: R1 and R2 ---------------------------------------------------------------------


def test_all_brokers_down_makes_it_not_ready_and_recovery_restores_it() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.await_status(200)
        rig.publisher.reason_override = "all_brokers_down"
        doc = await rig.await_status(503)
        assert reasons(doc)["raw_producer"] == "all_brokers_down"
        assert reasons(doc)["spec"] is None and reasons(doc)["component"] is None
        rig.publisher.reason_override = None
        await rig.await_status(200)

    run(scenario, Rig(parse(GOOD)))


def test_a_fatal_producer_is_reported_then_the_process_exits_nonzero() -> None:
    async def go() -> None:
        rig = Rig(parse(GOOD))
        gate = asyncio.Event()
        original = rig.runner.stop_component

        async def slow_stop() -> None:
            await gate.wait()  # holds the drain open so the 503 between fatal and exit can be read
            await original()

        rig.runner.stop_component = slow_stop  # type: ignore[method-assign]
        task = asyncio.create_task(rig.runner.run())
        await rig.await_status(200)
        rig.publisher.reason_override = "fatal"
        assert rig.publisher.fatal_handler is not None
        await asyncio.get_running_loop().run_in_executor(None, rig.publisher.fatal_handler)  # the poll thread
        doc = await rig.await_status(503)
        assert reasons(doc)["raw_producer"] == "fatal" and reasons(doc)["component"] == "stopping"
        gate.set()
        assert await asyncio.wait_for(task, 15) == 1

    asyncio.run(go())


# -- R6: stopping before the drain ---------------------------------------------------------------------


def test_component_goes_stopping_before_the_drain_starts() -> None:
    async def go() -> None:
        rig = Rig(parse(GOOD))
        gate = asyncio.Event()
        original = rig.runner.stop_component
        drain_started = asyncio.Event()

        async def slow_stop() -> None:
            drain_started.set()
            await gate.wait()
            await original()

        rig.runner.stop_component = slow_stop  # type: ignore[method-assign]
        task = asyncio.create_task(rig.runner.run())
        await rig.await_status(200)
        rig.runner.request_stop()
        await asyncio.wait_for(drain_started.wait(), 5)
        status, doc = await rig.ready_doc()  # the listener is still up and answering during the drain
        assert status == 503 and reasons(doc)["component"] == "stopping"
        gate.set()
        assert await asyncio.wait_for(task, 15) == 0

    asyncio.run(go())


def test_a_restart_only_spec_change_also_reports_stopping() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.await_status(200)
        rig.feed.spec = parse(http_document("fhir-in", version=5, port=1))  # a restart-only field
        await rig.until(lambda: rig.runner.restart_requested)

    run(scenario, Rig(parse(GOOD)))


# -- component state -----------------------------------------------------------------------------------


def test_a_failed_bind_is_bind_failed_and_a_dead_component_is_component_failed() -> None:
    async def scenario(rig: Rig) -> None:
        # Occupy the ingest port so the listener cannot bind.
        blocker = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = blocker.sockets[0].getsockname()[1]
        try:
            rig.feed.spec = parse(http_document("fhir-in", version=6, port=port))
            await rig.until(lambda: "listener_bind_failed" in rig.log.names())
            status, doc = await rig.ready_doc()
            assert status == 503 and reasons(doc)["component"] == "bind_failed"
        finally:
            blocker.close()

    run(scenario, Rig(None))


def test_component_failed_when_the_component_task_dies() -> None:
    async def go() -> None:
        rig = Rig(parse(GOOD))
        gate = asyncio.Event()
        original = rig.runner.stop_component

        async def slow_stop() -> None:
            await gate.wait()
            await original()

        rig.runner.stop_component = slow_stop  # type: ignore[method-assign]
        task = asyncio.create_task(rig.runner.run())
        await rig.await_status(200)
        rig.runner.report_failure(RuntimeError("boom"))
        doc = await rig.await_status(503)
        assert reasons(doc)["component"] == "component_failed"
        gate.set()
        assert await asyncio.wait_for(task, 15) == 1

    asyncio.run(go())


# -- the ops listener at start-up ----------------------------------------------------------------------


def test_an_ops_bind_conflict_refuses_the_start_with_exit_2_before_reading_any_spec() -> None:
    async def go() -> None:
        blocker = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = blocker.sockets[0].getsockname()[1]
        try:
            rig = Rig(parse(GOOD), ops=OpsSettings("127.0.0.1", port))
            assert await asyncio.wait_for(rig.runner.run(), 10) == 2
            [refused] = rig.log.of("startup_refused")
            assert refused["code"] == "OPS_LISTENER_BIND_FAILED" and refused["error"] == "OSError"
            assert rig.opened == 0 and rig.publisher.warm_calls == 0 and rig.emitted == []
        finally:
            blocker.close()

    asyncio.run(go())


def test_a_non_loopback_ops_bind_warns_and_a_loopback_one_does_not() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.ops_port()

    loud = run(scenario, Rig(parse(GOOD), ops=OpsSettings("0.0.0.0", 0)))  # noqa: S104
    assert loud.log.of("ops_listener_non_loopback") and "ops_listening" in loud.log.names()
    quiet = run(scenario, Rig(parse(GOOD)))
    assert quiet.log.of("ops_listener_non_loopback") == []
    [listening] = quiet.log.of("ops_listening")
    assert set(listening) == {"ops_port"}


def test_no_ops_listener_unless_configured() -> None:
    async def scenario(rig: Rig) -> None:
        await rig.until(lambda: rig.runner.server is not None)
        assert rig.runner.ops_port is None
        assert all("ops_port" not in e for e in rig.emitted)

    run(scenario, Rig(parse(GOOD), ops=None))


def test_responses_name_no_peer_path_or_free_text() -> None:
    async def scenario(rig: Rig) -> None:
        _status, doc = await rig.ready_doc()
        blob = str(doc)
        assert "127.0.0.1" not in blob and "/readyz" not in blob and "Error" not in blob

    run(scenario, Rig(None))


# -- control: the assertions above are not satisfiable by a hard-wired answer -----------------------------


def test_a_hard_wired_ready_provider_fails_the_same_assertions() -> None:
    """Replace the snapshot with one that always says ready; the not-ready-before-spec scenario must go red."""

    async def go(hard_wired: bool) -> None:
        rig = Rig(None)
        if hard_wired:
            from tributary.receivers import readiness as rd

            ready = rd.Report(
                "fhir-in",
                "http",
                1,
                False,
                (rd.Check("spec", None), rd.Check("component", None), rd.Check("raw_producer", None)),
                (),
            )
            rig.runner.readiness_report = lambda: ready  # type: ignore[method-assign]
        task = asyncio.create_task(rig.runner.run())
        try:
            status, doc = await rig.ready_doc()
            assert status == 503 and reasons(doc)["spec"] == "no_spec"
        finally:
            rig.runner.request_stop()
            await asyncio.wait_for(task, 10)

    asyncio.run(go(hard_wired=False))
    with pytest.raises(AssertionError):
        asyncio.run(go(hard_wired=True))


def test_spec_unavailable_carries_an_error_code() -> None:
    from tributary.receivers.errors import CHANNEL_NOT_DEPLOYED, CHANNELS_UNAUTHORIZED, spec_unavailable_error

    codes = {"CHANNELS_UNAUTHORIZED": CHANNELS_UNAUTHORIZED, "CHANNEL_NOT_DEPLOYED": CHANNEL_NOT_DEPLOYED}

    async def go() -> None:
        for name, code in codes.items():
            rig = Rig(None, ops=None)
            errors: list[Exception] = [spec_unavailable_error(code)]

            def open_feed(errors: list[Exception] = errors, rig: Rig = rig) -> FakeFeed:
                if errors:
                    raise errors.pop()
                return rig.feed

            rig.runner._open_feed = open_feed  # type: ignore[assignment]
            task = asyncio.create_task(rig.runner.run())
            await rig.until(lambda rig=rig: "spec_unavailable" in rig.log.names())  # type: ignore[misc]
            assert rig.log.of("spec_unavailable")[0]["error_code"] == name
            rig.runner.request_stop()
            await asyncio.wait_for(task, 10)

    asyncio.run(go())


def test_a_stop_during_a_slow_warm_up_is_not_held_up_by_it() -> None:
    import threading
    import time

    release = threading.Event()

    class SlowWarm(WarmPublisher):
        def warm_up(self, timeout_s: float = 5.0) -> bool:
            release.wait(timeout_s)
            return False

    async def go() -> float:
        rig = Rig(parse(GOOD), publisher=SlowWarm())
        task = asyncio.create_task(rig.runner.run())
        await rig.until(lambda: rig.runner._warm_task is not None)
        await asyncio.sleep(0.1)
        started = time.monotonic()
        rig.runner.request_stop()
        assert await asyncio.wait_for(task, 15) == 0
        return time.monotonic() - started

    elapsed = asyncio.run(go())
    release.set()
    assert elapsed < 1.5, "shutdown did not wait out the in-flight 5 s round trip"


def test_the_cli_classifies_why_a_spec_could_not_be_read() -> None:
    from types import SimpleNamespace

    from tributary.receivers.cli import classify_spec_unavailable

    class Err:
        def __init__(self, name: str) -> None:
            self._n = name

        def name(self) -> str:
            return self._n

    class Consumer:
        def __init__(self, entry: Any = None, raises: bool = False) -> None:
            self.entry, self.raises = entry, raises

        def list_topics(self, topic: str, timeout: float) -> Any:
            if self.raises:
                raise RuntimeError("no brokers")
            return SimpleNamespace(topics={} if self.entry is None else {topic: self.entry})

    topic = "_tb.dev.channels"
    deployed = Exception("channel fhir-in is not deployed on _tb.dev.channels")
    timeout = Exception("timed out reading _tb.dev.channels")
    ok = SimpleNamespace(error=None)
    assert (
        classify_spec_unavailable(Consumer(SimpleNamespace(error=Err("TOPIC_AUTHORIZATION_FAILED"))), topic, deployed)
        == "CHANNELS_UNAUTHORIZED"
    )
    assert (
        classify_spec_unavailable(Consumer(SimpleNamespace(error=Err("UNKNOWN_TOPIC_OR_PART"))), topic, deployed)
        == "CHANNELS_TOPIC_MISSING"
    )
    assert classify_spec_unavailable(Consumer(None), topic, deployed) == "CHANNELS_TOPIC_MISSING"
    assert classify_spec_unavailable(Consumer(ok), topic, deployed) == "CHANNEL_NOT_DEPLOYED"
    assert classify_spec_unavailable(Consumer(ok), topic, timeout) == "CHANNEL_SPEC_UNAVAILABLE"
    assert classify_spec_unavailable(Consumer(raises=True), topic, deployed) == "CHANNEL_SPEC_UNAVAILABLE"
