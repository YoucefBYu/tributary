"""Helpers for the readiness integration tests: ask a spawned receiver's ops listener, and wait on its answer."""

from __future__ import annotations

import http.client
import json
import time
from collections.abc import Callable
from typing import Any

from tests.support.processes import ManagedProcess

from ..readiness.support import assert_valid


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
