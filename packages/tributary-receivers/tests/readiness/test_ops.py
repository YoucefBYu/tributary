"""The ops listener (`contracts/observability/v0` 2.1, O1-O6): settings resolution, the minimal HTTP surface and
its limits. A fixed `Report` stands in for the lifecycle."""

from __future__ import annotations

import asyncio

from tributary.receivers import readiness as rd
from tributary.runtime.ops import OpsServer, OpsSettings

from .support import assert_valid, get, raw_request


def report(*, ready: bool = True) -> rd.Report:
    checks = (
        rd.Check("spec", None),
        rd.Check("component", None),
        rd.Check("raw_producer", None if ready else rd.NOT_WARMED),
    )
    return rd.Report("lab-in", "mllp", 3, False, checks, ())


def serve(scenario, ready: bool = True):  # type: ignore[no-untyped-def]
    async def go() -> None:
        server = OpsServer(OpsSettings("127.0.0.1", 0), lambda: report(ready=ready), component="receiver")
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

        server = OpsServer(OpsSettings("127.0.0.1", 0), boom, component="receiver")
        port = await server.start()
        try:
            status, _h, body = await raw_request(port, b"GET /readyz HTTP/1.1\r\n\r\n")
            assert status == 503 and body == b""
        finally:
            await server.close()

    asyncio.run(go())
