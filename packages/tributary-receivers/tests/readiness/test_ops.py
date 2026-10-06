"""The ops listener (`contracts/observability/v0` 2.1, O1-O6): settings resolution, the minimal HTTP surface and
its limits. A fixed `Report` stands in for the lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import errno
import socket
import time
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers import readiness as rd
from tributary.receivers.ops import (
    DEFAULT_BIND,
    DEFAULT_PORT,
    MAX_CONNECTIONS,
    MAX_PER_PEER,
    MIN_EVICT_AGE_S,
    OpsConfigError,
    OpsServer,
    OpsSettings,
    _Conn,
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


async def open_from(source: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    sock = socket.socket()
    sock.setblocking(False)
    sock.bind((source, 0))
    await asyncio.get_running_loop().sock_connect(sock, ("127.0.0.1", port))
    return await asyncio.open_connection(sock=sock)


def test_eight_idle_connections_do_not_block_a_probe_the_oldest_is_evicted() -> None:
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        try:
            held = [await open_from("127.0.0.1" if i % 2 == 0 else "127.0.0.2", port) for i in range(MAX_CONNECTIONS)]
            for _ in range(100):
                if server.open_connections == MAX_CONNECTIONS:
                    break
                await asyncio.sleep(0.01)
            assert server.open_connections == MAX_CONNECTIONS  # the fault fired: every slot is taken by an idle peer
            await asyncio.sleep(MIN_EVICT_AGE_S + 0.05)  # old enough to be evictable
            started = asyncio.get_running_loop().time()
            assert await probe_from("127.0.0.3", port, 0.0) == 200, "a probe is served at once"
            assert asyncio.get_running_loop().time() - started < 0.5
            assert server.open_connections <= MAX_CONNECTIONS
            # The oldest idle connection was the one closed; the newest is still open.
            with contextlib.suppress(ConnectionResetError):
                assert await asyncio.wait_for(held[0][0].read(10), 2) == b""
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
            await asyncio.sleep(MIN_EVICT_AGE_S + 0.1)
            for _ in range(5):  # even from the flooder's own address: its idle connections are replaced
                assert (await get(port, "/livez"))[0] == 200
            assert server.open_connections <= MAX_PER_PEER
            for _r, w in flood:
                w.close()
        finally:
            await server.close()

    asyncio.run(go())


def conn_state(peer: str, age: float, *, reading: bool = True, got_bytes: bool = False) -> _Conn:
    return _Conn(peer=peer, born=time.monotonic() - age, reading=reading, got_bytes=got_bytes)


def test_victim_rules_age_bytes_peer_cap_and_the_heaviest_peer() -> None:
    server = OpsServer(OpsSettings("127.0.0.1", 0), report)
    w: Any = [object() for _ in range(12)]
    # Young, mid-trickle (under 200 ms) and past-head connections are protected.
    server._active = {
        w[0]: conn_state("a", 0.001),
        w[1]: conn_state("a", 0.1, got_bytes=True),
        w[2]: conn_state("b", 5.0, reading=False),
    }
    assert server._victim("c") is None
    # Old and silent: evictable; the peer holding the most slots is taken from first, oldest of its connections.
    server._active = {
        w[0]: conn_state("probe", 5.0),
        w[1]: conn_state("flood", 4.0),
        w[2]: conn_state("flood", 3.0),
        w[3]: conn_state("flood", 0.001),  # young: protected, but still counts as a slot the flooder holds
    }
    assert server._victim("other") is w[1]
    del server._active[w[1]]
    assert server._victim("other") is w[2]
    # A trickler (bytes sent, head unfinished) older than 200 ms is evictable by a different address only.
    server._active = {w[0]: conn_state("slow", 1.0, got_bytes=True), w[1]: conn_state("x", 1.0, reading=False)}
    assert server._victim("probe") is w[0]
    assert server._victim("slow") is None, "below the peer cap, an address never evicts its own trickler"
    # At the per-peer cap a peer may replace only its own connections, even if another peer's are older.
    server._active = {w[i]: conn_state("flood", 1.0 + i, got_bytes=True) for i in range(MAX_PER_PEER)}
    server._active[w[10]] = conn_state("other", 9.0)
    assert server._victim("flood") is w[0]
    server._active = {w[i]: conn_state("flood", 0.001) for i in range(MAX_PER_PEER)}
    server._active[w[10]] = conn_state("other", 9.0)
    assert server._victim("flood") is None, "every one of its own is young: the newcomer is refused, not another peer"


def test_one_address_holds_at_most_four_slots() -> None:
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        try:
            conns = [await asyncio.open_connection("127.0.0.1", port) for _ in range(12)]
            await asyncio.sleep(0.3)
            assert server.open_connections == MAX_PER_PEER
            for _r, w in conns:
                w.close()
        finally:
            await server.close()

    asyncio.run(go())


def probe_from(source: str, port: int, gap_s: float) -> Any:
    async def probe() -> int:
        sock = socket.socket()
        sock.setblocking(False)
        sock.bind((source, 0))
        loop = asyncio.get_running_loop()
        await loop.sock_connect(sock, ("127.0.0.1", port))
        await asyncio.sleep(gap_s)  # a real client takes a moment to send its request
        await loop.sock_sendall(sock, b"GET /livez HTTP/1.1\r\n\r\n")
        try:
            data = await asyncio.wait_for(loop.sock_recv(sock, 200), 3)
        except (TimeoutError, ConnectionError):
            return 0
        finally:
            sock.close()
        return int(data.split(b" ")[1]) if data else 0

    return probe()


def test_a_flooder_from_one_address_does_not_stop_probes_from_another() -> None:
    async def go() -> list[int]:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        stop = False

        async def flood() -> None:
            held: list[asyncio.StreamWriter] = []
            while not stop:
                try:
                    _r, w = await asyncio.open_connection("127.0.0.1", port)
                except OSError:
                    continue
                held.append(w)  # never sends a byte
                if len(held) > 40:
                    held.pop(0).close()
                await asyncio.sleep(0)
            for w in held:
                w.close()

        flooders = [asyncio.create_task(flood()) for _ in range(4)]  # thousands of connections a second
        try:
            await asyncio.sleep(0.3)
            results = []
            for _ in range(8):
                results.append(await probe_from("127.0.0.2", port, 0.01))
                await asyncio.sleep(0.02)
            return results
        finally:
            stop = True
            await asyncio.gather(*flooders)
            await server.close()

    results = asyncio.run(go())
    assert results == [200] * 8, results


def test_forty_tricklers_from_one_address_do_not_starve_probes_from_another() -> None:
    """Each sends a byte and stalls (the head never finishes), reconnecting when dropped."""

    async def go() -> list[int]:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        stop = False

        async def trickler() -> None:
            while not stop:
                try:
                    r, w = await asyncio.open_connection("127.0.0.1", port)
                except OSError:
                    await asyncio.sleep(0.01)
                    continue
                with contextlib.suppress(OSError):
                    w.write(b"G")
                    await w.drain()
                with contextlib.suppress(OSError, asyncio.CancelledError):
                    await r.read(1)  # stalls until the server drops it, then reconnects at once
                w.close()

        tasks = [asyncio.create_task(trickler()) for _ in range(40)]
        try:
            await asyncio.sleep(0.5)
            results = []
            for _ in range(20):
                results.append(await probe_from("127.0.0.2", port, 0.01))
                await asyncio.sleep(0.05)
            return results
        finally:
            stop = True
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await server.close()

    results = asyncio.run(asyncio.wait_for(go(), 60))
    assert results == [200] * 20, results


class FlakyLoop:
    """Wraps the running loop's `sock_accept`: raises the scripted errors first, then really accepts."""

    def __init__(self, loop: asyncio.AbstractEventLoop, script: list[BaseException]) -> None:
        self.loop, self.script, self.calls = loop, script, 0
        self.real = loop.sock_accept

    async def sock_accept(self, sock: socket.socket) -> Any:
        self.calls += 1
        if self.script:
            raise self.script.pop(0)
        return await self.real(sock)


class Log:
    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, Any]]] = []

    def error(self, event: str, /, **fields: Any) -> None:
        self.events.append((event, fields))

    debug = info = warning = error


def test_a_connection_aborted_before_accept_is_retried_at_once() -> None:
    async def go() -> float:
        loop = asyncio.get_running_loop()
        flaky = FlakyLoop(loop, [OSError(errno.ECONNABORTED, "x"), OSError(errno.EPROTO, "x")] * 5)
        loop.sock_accept = flaky.sock_accept  # type: ignore[method-assign]
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        try:
            started = loop.time()
            assert (await get(port, "/livez"))[0] == 200
            assert flaky.calls >= 11
            return loop.time() - started
        finally:
            await server.close()

    assert asyncio.run(go()) < 0.4, "ten aborted accepts did not cost ten back-offs"


def test_resource_exhaustion_backs_off_then_recovers() -> None:
    async def go() -> float:
        loop = asyncio.get_running_loop()
        script = [
            OSError(errno.EMFILE, "x"),
            OSError(errno.ENFILE, "x"),
            OSError(errno.ENOBUFS, "x"),
            OSError(errno.ENOMEM, "x"),
        ]
        flaky = FlakyLoop(loop, script)
        loop.sock_accept = flaky.sock_accept  # type: ignore[method-assign]
        log = Log()
        server = OpsServer(OpsSettings("127.0.0.1", 0), report, log=log)
        port = await server.start()
        try:
            started = loop.time()
            assert (await get(port, "/livez"))[0] == 200
            assert log.events == [], "an exhausted accept is not the loop's death"
            return loop.time() - started
        finally:
            await server.close()

    assert asyncio.run(go()) >= 0.15, "each of the four errors backed off (4 x 50 ms)"


def test_an_unexpected_accept_error_ends_the_loop_and_logs_only_the_class() -> None:
    async def go() -> None:
        loop = asyncio.get_running_loop()
        flaky = FlakyLoop(loop, [OSError(errno.EBADF, "SYNTH-secret-text")])
        loop.sock_accept = flaky.sock_accept  # type: ignore[method-assign]
        log = Log()
        server = OpsServer(OpsSettings("127.0.0.1", 0), report, log=log)
        await server.start()
        try:
            for _ in range(100):
                if log.events:
                    break
                await asyncio.sleep(0.01)
            assert log.events == [("ops_accept_loop_died", {"error": "OSError"})]
            assert "SYNTH" not in repr(log.events)
        finally:
            await server.close()

    asyncio.run(go())


def test_an_accepted_socket_is_closed_when_setting_it_up_fails() -> None:
    async def go() -> None:
        loop = asyncio.get_running_loop()
        log = Log()
        server = OpsServer(OpsSettings("127.0.0.1", 0), report, log=log)

        async def broken(*_a: Any, **_k: Any) -> Any:
            raise RuntimeError("SYNTH-secret-text")

        loop.connect_accepted_socket = broken  # type: ignore[method-assign]
        port = await server.start()
        try:
            r, w = await asyncio.open_connection("127.0.0.1", port)
            with contextlib.suppress(ConnectionResetError):
                assert await asyncio.wait_for(r.read(10), 3) == b"", "the accepted socket was closed, not leaked"
            for _ in range(100):
                if log.events:
                    break
                await asyncio.sleep(0.01)
            assert log.events == [("ops_accept_loop_died", {"error": "RuntimeError"})]
            w.close()
        finally:
            await server.close()

    asyncio.run(go())


def test_close_with_live_connections_closes_them_and_the_listener() -> None:
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), report)
        port = await server.start()
        idle = [await asyncio.open_connection("127.0.0.1", port) for _ in range(3)]
        partial_r, partial_w = await asyncio.open_connection("127.0.0.1", port)
        partial_w.write(b"GET /li")
        await partial_w.drain()
        for _ in range(100):
            if server.open_connections == 4:
                break
            await asyncio.sleep(0.01)
        await asyncio.wait_for(server.close(), 5)
        assert server.open_connections == 0
        for r, _w in [*idle, (partial_r, partial_w)]:
            with contextlib.suppress(ConnectionResetError):
                assert await asyncio.wait_for(r.read(10), 3) == b""
        with pytest.raises(OSError):
            await asyncio.open_connection("127.0.0.1", port)
        await server.close()  # idempotent

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


def test_a_socket_lost_while_being_set_up_is_closed_and_the_loop_carries_on() -> None:
    async def go() -> None:
        loop = asyncio.get_running_loop()
        real = loop.connect_accepted_socket
        failures = [OSError(errno.ECONNRESET, "x"), OSError(errno.EPIPE, "x")]

        async def flaky(factory: Any, sock: Any) -> Any:
            if failures:
                raise failures.pop(0)
            return await real(factory, sock)

        loop.connect_accepted_socket = flaky  # type: ignore[method-assign]
        log = Log()
        server = OpsServer(OpsSettings("127.0.0.1", 0), report, log=log)
        port = await server.start()
        try:
            for _ in range(2):  # the two lost connections are closed
                r, w = await asyncio.open_connection("127.0.0.1", port)
                with contextlib.suppress(ConnectionResetError):
                    assert await asyncio.wait_for(r.read(10), 3) == b""
                w.close()
            assert (await get(port, "/livez"))[0] == 200, "the loop survived"
            assert log.events == []
        finally:
            await server.close()

    asyncio.run(go())


def test_a_dead_accept_loop_closes_the_listener_so_probes_are_refused() -> None:
    async def go() -> None:
        loop = asyncio.get_running_loop()
        flaky = FlakyLoop(loop, [OSError(errno.EBADF, "x")])
        loop.sock_accept = flaky.sock_accept  # type: ignore[method-assign]
        log = Log()
        server = OpsServer(OpsSettings("127.0.0.1", 0), report, log=log)
        port = await server.start()
        try:
            for _ in range(100):
                if log.events:
                    break
                await asyncio.sleep(0.01)
            assert log.events
            with pytest.raises(OSError):
                await asyncio.open_connection("127.0.0.1", port)
        finally:
            await server.close()

    asyncio.run(go())
