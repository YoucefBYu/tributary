"""Shared by the readiness tests: the contract's schema, and a plain HTTP GET over a raw socket."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import jsonschema

SCHEMA_PATH = Path(__file__).resolve().parents[4] / "contracts" / "observability" / "v0" / "readiness.schema.json"
SCHEMA = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def assert_valid(document: Any) -> None:
    jsonschema.Draft202012Validator(SCHEMA).validate(document)


async def raw_request(port: int, request: bytes, *, host: str = "127.0.0.1") -> tuple[int, dict[str, str], bytes]:
    r, w = await asyncio.open_connection(host, port)
    try:
        w.write(request)
        await w.drain()
        data = await asyncio.wait_for(r.read(65536), 5)
    finally:
        w.close()
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    headers = {k.lower(): v for k, v in (line.split(": ", 1) for line in lines[1:])}
    return int(lines[0].split(" ")[1]), headers, body


async def get(port: int, path: str) -> tuple[int, dict[str, Any] | None]:
    status, _headers, body = await raw_request(port, f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    return status, (json.loads(body) if body else None)
