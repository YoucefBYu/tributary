"""Helpers for the readiness integration tests: ask a spawned receiver's ops listener, and wait on its answer."""

from __future__ import annotations

import http.client
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from tests.support.processes import ManagedProcess

from ..readiness.support import assert_valid

CONTRACT = Path(__file__).resolve().parents[4] / "contracts" / "observability" / "v0" / "metrics.json"


def ops_port(proc: ManagedProcess, *, timeout_s: float = 60.0) -> int:
    """The ephemeral ops port, from the first line a receiver logs once its ops listener is bound (O2). Call it
    before waiting for any other event: `wait_for_event` discards what it reads past."""
    return int(proc.wait_for_event("ops_listening", timeout_s=timeout_s)["ops_port"])


def get(port: int, path: str, *, method: str = "GET") -> tuple[int, dict[str, Any] | None]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request(method, path)
        response = conn.getresponse()
        body = response.read()
        return response.status, (json.loads(body) if body else None)
    finally:
        conn.close()


def readyz(port: int) -> tuple[int, dict[str, Any]]:
    status, doc = get(port, "/readyz")
    assert doc is not None
    assert_valid(doc)
    return status, doc


def reasons(doc: dict[str, Any]) -> dict[str, str | None]:
    return {c["name"]: c.get("reason") for c in doc["checks"]}


def await_ready(
    port: int, predicate: Callable[[int, dict[str, Any]], bool], what: str, *, timeout_s: float = 60.0
) -> dict[str, Any]:
    """Poll `/readyz` until `predicate(status, document)`; every answer on the way is schema-valid."""
    deadline = time.monotonic() + timeout_s
    last: tuple[int, dict[str, Any]] | None = None
    while time.monotonic() < deadline:
        last = readyz(port)
        if predicate(*last):
            return last[1]
        time.sleep(0.1)
    raise AssertionError(f"timed out waiting for {what}; last answer {last}")


def scrape(port: int) -> dict[str, float]:
    """`GET /metrics` as `{series: value}` (the series key keeps its label text). Every metric name is one the
    contract registers (`tb_*`), and nothing from a default process collector is served."""
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    try:
        conn.request("GET", "/metrics")
        response = conn.getresponse()
        text = response.read().decode()
        assert response.status == 200
        assert response.getheader("Content-Type", "").startswith("text/plain; version=0.0.4")
    finally:
        conn.close()
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line and not line.startswith("#"):
            series, _, value = line.rpartition(" ")
            out[series] = float(value)
    names = {series.split("{")[0] for series in out}
    registered = {m["name"] for m in json.loads(CONTRACT.read_text(encoding="utf-8"))["metrics"]}
    base = {n.removesuffix("_bucket").removesuffix("_sum").removesuffix("_count") for n in names}
    assert base <= registered, sorted(base - registered)
    return out
