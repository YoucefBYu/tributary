"""The loaders' `/livez` and `/metrics` (W3-26): names from `observability/v0`, deliveries counted per reason and
code, a redelivery counted again, no `/readyz`, liveness following the loop's heartbeat."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest
from tributary.loaders.common import LoaderMetrics
from tributary.loaders.ops import LoaderOps, delivery_labels
from tributary.runtime.ops import OpsServer, OpsSettings

CONTRACT = Path(__file__).resolve().parents[3] / "contracts" / "observability" / "v0" / "metrics.json"


def build(metrics: LoaderMetrics, *, age: float | None = None, available: bool = True) -> LoaderOps:
    ops = LoaderOps.create(channel="lab-in", destination="stub", destination_type="http", strict=True)
    ops.bind(
        snapshot=metrics.snapshot,
        spec_version=lambda: 4,
        spec_available=lambda: available,
        loop_age_s=lambda: age,
        stale_after_s=10.0,
    )
    return ops


def test_deliveries_by_outcome_and_code_and_a_redelivery_counts_again() -> None:
    m = LoaderMetrics()
    ops = build(m)
    scope = 'channel="lab-in",destination="stub",destination_type="http"'
    m.record("success")
    assert f'tb_deliveries_total{{{scope},outcome="success",code="NONE"}} 1' in ops.render().decode()
    m.record("success")  # the same record delivered again after an uncommitted batch
    m.record("HTTP_503")
    m.record("circuit-open")
    m.record("duplicate")
    text = ops.render().decode()
    assert f'tb_deliveries_total{{{scope},outcome="success",code="NONE"}} 3' in text
    assert f'tb_deliveries_total{{{scope},outcome="failure",code="HTTP_503"}} 1' in text
    assert f'tb_deliveries_total{{{scope},outcome="failure",code="CIRCUIT_OPEN"}} 1' in text
    assert 'tb_spec_version{component="loader",channel="lab-in"} 4' in text


def test_the_circuit_gauge_and_transitions_follow_opens_and_closes() -> None:
    m = LoaderMetrics()
    ops = build(m)
    m.circuit_opens = 1
    text = ops.render().decode()
    assert 'tb_circuit_open{channel="lab-in",destination="stub"} 1' in text
    assert 'tb_circuit_transitions_total{channel="lab-in",destination="stub",to="open"} 1' in text
    m.circuit_closes = 1
    assert 'tb_circuit_open{channel="lab-in",destination="stub"} 0' in ops.render().decode()


@pytest.mark.parametrize(
    ("reason", "expected"),
    [
        ("success", ("success", "NONE")),
        ("duplicate", ("success", "NONE")),
        ("PAYLOAD_SHAPE_INVALID", ("failure", "PAYLOAD_SHAPE_INVALID")),
        ("tls-verify-failed", ("failure", "TLS_VERIFY_FAILED")),
        ("HTTP_503", ("failure", "HTTP_503")),
        ("not a code!", ("failure", "UNKNOWN")),
    ],
)
def test_delivery_labels(reason: str, expected: tuple[str, str]) -> None:
    assert delivery_labels(reason) == expected


def test_a_scrape_serves_registered_names_only_and_livez_follows_the_heartbeat() -> None:
    registered = {m["name"] for m in json.loads(CONTRACT.read_text(encoding="utf-8"))["metrics"]}
    live = build(LoaderMetrics(), age=1.0)
    wedged = build(LoaderMetrics(), age=60.0)

    async def get(port: int, path: str) -> tuple[int, bytes]:
        r, w = await asyncio.open_connection("127.0.0.1", port)
        w.write(f"GET {path} HTTP/1.1\r\n\r\n".encode())
        await w.drain()
        data = await asyncio.wait_for(r.read(65536), 5)
        w.close()
        head, _, body = data.partition(b"\r\n\r\n")
        return int(head.split(b" ")[1]), body

    async def go() -> None:
        servers = [
            OpsServer(OpsSettings("127.0.0.1", 0), component="loader", metrics=o.render, liveness=o.alive)
            for o in (live, wedged)
        ]
        ports = [await s.start() for s in servers]
        try:
            assert (await get(ports[0], "/livez"))[0] == 200
            assert (await get(ports[1], "/livez"))[0] == 503
            assert (await get(ports[0], "/readyz")) == (404, b"")
            status, body = await get(ports[0], "/metrics")
            assert status == 200
            names = {line.split("{")[0].split(" ")[0] for line in body.decode().splitlines() if line and line[0] != "#"}
            assert names and names <= registered
        finally:
            for s in servers:
                await s.close()

    asyncio.run(go())
