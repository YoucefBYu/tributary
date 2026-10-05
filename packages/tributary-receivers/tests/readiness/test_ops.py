"""The ops listener (`contracts/observability/v0` 2.1, O1-O6): settings resolution, the minimal HTTP surface and
its limits. A fixed `Report` stands in for the lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers import readiness as rd
from tributary.receivers.ops import (
    DEFAULT_BIND,
    DEFAULT_PORT,
    MAX_CONNECTIONS,
    OpsConfigError,
    OpsServer,
    OpsSettings,
    resolve_ops_settings,
)

from .support import assert_valid, get, raw_request


def report(*, ready: bool = True) -> rd.Report:
    checks = (
        rd.Check("spec", None),
        rd.Check("component", None),
        rd.Check("raw_producer", None if ready else rd.NOT_WARMED),
    )
    return rd.Report("lab-in", "mllp", 3, False, checks, ())


def test_defaults_are_loopback_9180() -> None:
    s = resolve_ops_settings(bind=None, port=None, environ={})
    assert (s.bind, s.port) == (DEFAULT_BIND, DEFAULT_PORT) == ("127.0.0.1", 9180)
    assert not s.explicit_non_loopback


def test_flag_wins_over_environment_which_wins_over_default() -> None:
    env = {"TB_OPS_BIND": "10.1.2.3", "TB_OPS_PORT": "9300"}
    assert resolve_ops_settings(bind=None, port=None, environ=env) == OpsSettings("10.1.2.3", 9300)
    assert resolve_ops_settings(bind="127.0.0.1", port="0", environ=env) == OpsSettings("127.0.0.1", 0)


@pytest.mark.parametrize("bind", ["0.0.0.0", "10.0.0.7", "::"])  # noqa: S104
def test_a_non_loopback_bind_is_flagged_so_startup_can_warn(bind: str) -> None:
    assert resolve_ops_settings(bind=bind, port=None, environ={}).explicit_non_loopback


def test_localhost_is_mapped_to_127_0_0_1_so_nothing_is_resolved() -> None:
    s = resolve_ops_settings(bind="localhost", port=None, environ={})
    assert s.bind == "127.0.0.1" and not s.explicit_non_loopback


@pytest.mark.parametrize("bind,port", [("example.org", None), ("", "x"), (None, "70000"), (None, "-1"), (None, "abc")])
def test_bad_bind_or_port_is_refused_without_echoing_the_value(bind: str | None, port: str | None) -> None:
    with pytest.raises(OpsConfigError) as exc:
        resolve_ops_settings(bind=bind or ("example.org" if port is None else None), port=port, environ={})
    assert "example" not in str(exc.value) and "70000" not in str(exc.value)


def serve(scenario, ready: bool = True):  # type: ignore[no-untyped-def]
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), lambda: report(ready=ready))
        port = await server.start()
        try:
            await scenario(port)
        finally:
            await server.close()

    asyncio.run(go())


def test_livez_is_200_and_independent_of_readiness() -> None:
    async def scenario(port: int) -> None:
        assert await get(port, "/livez") == (200, {"v": 0, "status": "alive", "component": "receiver"})

    serve(scenario, ready=False)


def test_readyz_is_200_ready_and_503_not_ready_and_validates_against_the_schema() -> None:
    async def ready(port: int) -> None:
        status, doc = await get(port, "/readyz")
        assert status == 200 and doc is not None and doc["status"] == "ready"
        assert_valid(doc)

    async def not_ready(port: int) -> None:
        status, doc = await get(port, "/readyz")
        assert status == 503 and doc is not None and doc["status"] == "not_ready"
        assert {"name": "raw_producer", "ok": False, "reason": "not_warmed"} in doc["checks"]
        assert_valid(doc)

    serve(ready)
    serve(not_ready, ready=False)


def test_a_broken_snapshot_answers_503_with_an_empty_body_not_a_500() -> None:
    async def go() -> None:
        def boom() -> rd.Report:
            raise RuntimeError("SYNTH-secret-text")

        server = OpsServer(OpsSettings("127.0.0.1", 0), boom)
        port = await server.start()
        try:
            status, _h, body = await raw_request(port, b"GET /readyz HTTP/1.1\r\n\r\n")
            assert status == 503 and body == b""
        finally:
            await server.close()

    asyncio.run(go())


def test_only_get_other_methods_405_unknown_path_404_both_with_empty_bodies_and_nothing_is_echoed() -> None:
    async def scenario(port: int) -> None:
        for method in ("POST", "PUT", "DELETE", "HEAD", "PATCH"):
            status, headers, body = await raw_request(port, f"{method} /readyz HTTP/1.1\r\n\r\n".encode())
            assert (status, body) == (405, b"") and headers["allow"] == "GET"
        status, headers, body = await raw_request(
            port, b"GET /secret-path-SYNTH?patient=SYNTHQ HTTP/1.1\r\nX-Leak: SYNTHHEADER\r\n\r\n"
        )
        assert (status, body) == (404, b"")
        wire = repr(headers)
        assert "SYNTH" not in wire and "127.0.0.1" not in wire

    serve(scenario)


def test_the_query_string_is_ignored() -> None:
    async def scenario(port: int) -> None:
        status, doc = await get(port, "/readyz?verbose=1&x=SYNTH")
        assert status == 200 and doc is not None
        assert "SYNTH" not in str(doc)

    serve(scenario)


def test_a_request_body_is_never_read() -> None:
    async def scenario(port: int) -> None:
        status, _h, body = await raw_request(port, b"GET /livez HTTP/1.1\r\nContent-Length: 5\r\n\r\nSYNTH")
        assert status == 200 and b"SYNTH" not in body

    serve(scenario)


def test_malformed_and_oversized_requests_are_refused_and_do_not_wedge_the_listener() -> None:
    async def scenario(port: int) -> None:
        assert (await raw_request(port, b"NOT A REQUEST\r\n\r\n"))[0] == 400
        big = b"GET /livez HTTP/1.1\r\nX: " + b"a" * 20000 + b"\r\n\r\n"
        status, _h, body = await raw_request(port, big)
        assert status == 431 and body == b""
        assert (await get(port, "/livez"))[0] == 200

    serve(scenario)


def test_a_slow_sender_is_dropped_after_the_read_timeout() -> None:
    async def scenario(port: int) -> None:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(b"GET /livez HTTP/1.1\r\nX-Slow: ")  # never finishes the head
        await w.drain()
        started = asyncio.get_running_loop().time()
        assert await asyncio.wait_for(r.read(100), 5) == b""  # closed by the server
        assert asyncio.get_running_loop().time() - started < 3
        w.close()
        assert (await get(port, "/livez"))[0] == 200

    serve(scenario)


def test_eight_idle_connections_do_not_block_a_probe_the_oldest_is_evicted() -> None:
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        try:
            held = [await asyncio.open_connection("127.0.0.1", port) for _ in range(MAX_CONNECTIONS)]
            for _ in range(100):
                if server.open_connections == MAX_CONNECTIONS:
                    break
                await asyncio.sleep(0.01)
            assert server.open_connections == MAX_CONNECTIONS  # the fault fired: every slot is taken by an idle peer
            started = asyncio.get_running_loop().time()
            assert (await get(port, "/readyz"))[0] == 200, "a probe is served at once"
            assert asyncio.get_running_loop().time() - started < 0.5
            assert server.open_connections <= MAX_CONNECTIONS
            # The oldest idle connection was the one closed; the newest is still open.
            first = held[0][0]
            with contextlib.suppress(ConnectionResetError):
                assert await asyncio.wait_for(first.read(10), 2) == b""
            held[-1][1].write(b"GET /livez HTTP/1.1\r\n\r\n")
            await held[-1][1].drain()
            assert b"200 OK" in await asyncio.wait_for(held[-1][0].read(200), 2)
            for _r, w in held:
                w.close()
        finally:
            await server.close()

    asyncio.run(go())


def test_a_flood_of_idle_connections_never_locks_out_a_probe() -> None:
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        try:
            flood = [await asyncio.open_connection("127.0.0.1", port) for _ in range(40)]
            for _ in range(5):
                assert (await get(port, "/livez"))[0] == 200
            assert server.open_connections <= MAX_CONNECTIONS
            for _r, w in flood:
                w.close()
        finally:
            await server.close()

    asyncio.run(go())


def test_accepting_pauses_only_when_every_slot_is_past_its_head() -> None:
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        writers: Any = [object() for _ in range(MAX_CONNECTIONS)]
        server._active = dict.fromkeys(writers, False)  # all eight writing a response: nothing is evictable
        server._update_room()
        assert not server._room.is_set()
        server._active[writers[3]] = True  # one still reading its head can be evicted
        server._update_room()
        assert server._room.is_set()
        server._active.pop(writers[3])
        server._active.pop(writers[4])
        server._update_room()
        assert server._room.is_set()

    asyncio.run(go())


def test_a_bind_conflict_raises_oserror() -> None:
    async def go() -> None:
        first = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await first.start()
        try:
            with pytest.raises(OSError):
                await OpsServer(OpsSettings("127.0.0.1", port), report).start()
        finally:
            await first.close()

    asyncio.run(go())


def test_the_ops_module_has_no_kafka_client() -> None:
    import tributary.receivers.ops as ops

    source = Path(ops.__file__).read_text(encoding="utf-8")
    assert "confluent_kafka" not in source and "list_topics" not in source
