"""What W3-25 added to the moved ops listener: `/metrics`, a liveness callable, `/readyz` only where contracted,
the component in the `/livez` body, and `OpsThread` / `start_ops_or_exit` for a synchronous process."""

from __future__ import annotations

import asyncio
import io
import json
import socket
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any

import pytest
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.consumer import ConsumerLoop, LoopSettings
from tributary.runtime.events import RecordingEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.ops import (
    METRICS_CONTENT_TYPE,
    OpsServer,
    OpsSettings,
    OpsThread,
    start_ops_or_exit,
)
from tributary.runtime.spec import RetryPolicy
from tributary.runtime.worker import PipelineWorker, WorkerSettings

from .fakes import FakeConsumer
from .log_line_schema import assert_lines_conform
from .ops_support import Report, get, raw_request

BODY = b"# HELP tb_build_info x\n# TYPE tb_build_info gauge\ntb_build_info 1\n"


def serve(server: OpsServer, scenario: Callable[[int], Awaitable[None]]) -> None:
    async def go() -> None:
        port = await server.start()
        try:
            await scenario(port)
        finally:
            await server.close()

    asyncio.run(go())


def worker_server(**kwargs: Any) -> OpsServer:
    return OpsServer(OpsSettings("127.0.0.1", 0), component="worker", **kwargs)


def test_metrics_is_served_as_prometheus_text() -> None:
    async def scenario(port: int) -> None:
        status, headers, body = await raw_request(port, b"GET /metrics HTTP/1.1\r\n\r\n")
        assert status == 200 and body == BODY
        assert headers["content-type"] == METRICS_CONTENT_TYPE
        assert headers["cache-control"] == "no-store"

    serve(worker_server(metrics=lambda: BODY), scenario)


def test_a_failing_metrics_render_is_503_with_an_empty_body() -> None:
    def boom() -> bytes:
        raise RuntimeError("SYNTH-secret-text")

    async def scenario(port: int) -> None:
        status, _headers, body = await raw_request(port, b"GET /metrics HTTP/1.1\r\n\r\n")
        assert (status, body) == (503, b"")

    serve(worker_server(metrics=boom), scenario)


def test_a_worker_has_no_readyz_and_no_metrics_without_a_renderer() -> None:
    """`observability/v0` 1: no worker, loader or archiver `/readyz` in v0. Absent paths are a plain 404."""

    async def scenario(port: int) -> None:
        for path in (b"/readyz", b"/metrics"):
            status, _h, body = await raw_request(port, b"GET " + path + b" HTTP/1.1\r\n\r\n")
            assert (status, body) == (404, b"")

    serve(worker_server(), scenario)


def test_livez_names_the_component() -> None:
    async def scenario(port: int) -> None:
        assert await get(port, "/livez") == (200, {"v": 0, "status": "alive", "component": "worker"})

    serve(worker_server(), scenario)


@pytest.mark.parametrize("answer", [False, "raise"])
def test_a_stale_heartbeat_fails_livez_with_an_empty_503(answer: object) -> None:
    def liveness() -> bool:
        if answer == "raise":
            raise RuntimeError("SYNTH-secret-text")
        return False

    async def scenario(port: int) -> None:
        status, _h, body = await raw_request(port, b"GET /livez HTTP/1.1\r\n\r\n")
        assert (status, body) == (503, b"")

    serve(worker_server(liveness=liveness), scenario)


def test_readiness_is_served_when_given_receivers_keep_their_readyz() -> None:
    async def scenario(port: int) -> None:
        status, doc = await get(port, "/readyz")
        assert status == 200 and doc is not None and doc["status"] == "ready"

    serve(OpsServer(OpsSettings("127.0.0.1", 0), lambda: Report(), component="receiver"), scenario)


def test_an_unknown_component_is_refused() -> None:
    with pytest.raises(ValueError):
        OpsServer(OpsSettings("127.0.0.1", 0), component="SYNTH")


def blocking_get(port: int, path: str) -> tuple[int, bytes]:
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(f"GET {path} HTTP/1.1\r\n\r\n".encode())
        data = b""
        while chunk := sock.recv(65536):
            data += chunk
    head, _, body = data.partition(b"\r\n\r\n")
    return int(head.split(b" ")[1]), body


def test_ops_thread_serves_from_a_background_thread_and_closes() -> None:
    thread = OpsThread(worker_server(metrics=lambda: BODY))
    port = thread.start()
    try:
        assert blocking_get(port, "/livez")[0] == 200
        assert blocking_get(port, "/metrics") == (200, BODY)
    finally:
        thread.close()
    with pytest.raises(OSError):
        socket.create_connection(("127.0.0.1", port), timeout=1)
    thread.close()  # idempotent


def test_ops_thread_raises_the_bind_error_in_the_caller() -> None:
    with socket.socket() as taken:
        taken.bind(("127.0.0.1", 0))
        taken.listen(1)
        port = taken.getsockname()[1]
        thread = OpsThread(OpsServer(OpsSettings("127.0.0.1", port), component="worker"))
        with pytest.raises(OSError):
            thread.start()
        thread.close()


def test_start_ops_or_exit_logs_the_port_and_refuses_a_conflict_with_exit_2() -> None:
    stream = io.StringIO()
    log = JsonLogger("worker", stream=stream)
    thread = start_ops_or_exit(worker_server(), log)
    try:
        port = thread.server.port
        with pytest.raises(SystemExit) as exited:
            start_ops_or_exit(OpsServer(OpsSettings("127.0.0.1", port), component="worker"), log)
        assert exited.value.code == 2
    finally:
        thread.close()
    lines = assert_lines_conform(stream.getvalue())
    assert [line["event"] for line in lines] == ["ops_listening", "startup_refused"]
    assert lines[0]["ops_port"] == port
    assert lines[1]["code"] == "OPS_LISTENER_BIND_FAILED" and lines[1]["errno"] > 0
    assert json.dumps(lines).count("127.0.0.1") == 0, "no address in the log lines"


# -- the worker's liveness heartbeat (security review MEDIUM, W3-25) ---------------------------------


class StallingConsumer(FakeConsumer):
    """The first `poll` blocks until released: a worker wedged in its first iteration."""

    def __init__(self) -> None:
        super().__init__()
        self.entered = threading.Event()
        self.release = threading.Event()

    def poll(self, timeout: float) -> Any:
        if not self.release.is_set():
            self.entered.set()
            self.release.wait(30)
        return super().poll(0.0)


def stalled_worker() -> tuple[PipelineWorker, StallingConsumer]:
    worker = PipelineWorker(
        WorkerSettings(env="dev", channel="adt-passthrough", kafka=KafkaSettings("127.0.0.1:9092"), instance_id="w-0"),
        log=JsonLogger("worker", stream=io.StringIO()),
    )
    consumer = StallingConsumer()
    worker.loop = ConsumerLoop(
        consumer,
        handler=object(),  # type: ignore[arg-type]  # never reached: the poll returns nothing
        topics=["dev.adt-passthrough.raw"],
        retry=RetryPolicy(policy="blocking", max_attempts=3, on_exhaust="halt-partition"),
        settings=LoopSettings(commit_every=1, poll_timeout_s=0.0),
        emitter=RecordingEmitter(),
        log=JsonLogger("worker", stream=io.StringIO()),
        channel="adt-passthrough",
        channel_version=1,
        instance="worker:w-0@test",
    )
    worker.stale_after_s = 0.3
    return worker, consumer


def test_a_worker_stuck_in_its_first_iteration_fails_livez_then_recovers() -> None:
    worker, consumer = stalled_worker()
    loop = worker.loop
    assert loop is not None
    ops = OpsThread(worker_server(liveness=worker.alive))
    port = ops.start()
    runner = threading.Thread(target=loop.run, kwargs={"max_iterations": 3}, daemon=True)
    try:
        assert blocking_get(port, "/livez")[0] == 200, "not started yet: build() is bounded"
        runner.start()
        assert consumer.entered.wait(5), "the fault fired: the first poll is blocked"
        assert blocking_get(port, "/livez")[0] == 200, "within stale_after_s"
        time.sleep(worker.stale_after_s + 0.2)
        assert loop.last_tick is not None, "the heartbeat was stamped before the first poll"
        assert blocking_get(port, "/livez") == (503, b""), "a wedged first iteration is not alive"
        consumer.release.set()
        runner.join(5)
        assert not runner.is_alive()
        assert blocking_get(port, "/livez")[0] == 200, "iterations completed: alive again"
    finally:
        consumer.release.set()
        ops.close()
