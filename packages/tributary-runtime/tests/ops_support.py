"""Shared by the ops listener tests: a stand-in readiness report, the contract's readiness schema, a raw-socket GET,
and a Prometheus text-format parser strict enough to catch a malformed exposition."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import orjson

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
SCHEMA = json.loads((CONTRACTS / "observability" / "v0" / "readiness.schema.json").read_text(encoding="utf-8"))
METRICS_JSON = CONTRACTS / "observability" / "v0" / "metrics.json"


@dataclass(frozen=True, slots=True)
class Report:
    """A receiver-shaped readiness report, as `tributary.receivers.readiness.Report` serialises it."""

    ready: bool = True

    @property
    def http_status(self) -> int:
        return 200 if self.ready else 503

    def body(self) -> bytes:
        raw_producer: dict[str, object] = (
            {"name": "raw_producer", "ok": True}
            if self.ready
            else {"name": "raw_producer", "ok": False, "reason": "not_warmed"}
        )
        return orjson.dumps(
            {
                "v": 0,
                "component": "receiver",
                "status": "ready" if self.ready else "not_ready",
                "channel": "lab-in",
                "receiver_type": "mllp",
                "channel_version": 3,
                "checks": [{"name": "spec", "ok": True}, {"name": "component", "ok": True}, raw_producer],
            }
        )


def assert_valid(document: Any) -> None:
    jsonschema.Draft202012Validator(SCHEMA).validate(document)


async def raw_request(port: int, request: bytes, *, host: str = "127.0.0.1") -> tuple[int, dict[str, str], bytes]:
    r, w = await asyncio.open_connection(host, port)
    try:
        w.write(request)
        await w.drain()
        data = b""
        while True:  # a /metrics body can span several reads; the server closes the connection when done
            chunk = await asyncio.wait_for(r.read(65536), 5)
            if not chunk:
                break
            data += chunk
    finally:
        w.close()
    head, _, body = data.partition(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    headers = {k.lower(): v for k, v in (line.split(": ", 1) for line in lines[1:])}
    return int(lines[0].split(" ")[1]), headers, body


async def get(port: int, path: str) -> tuple[int, dict[str, Any] | None]:
    status, _headers, body = await raw_request(port, f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
    return status, (json.loads(body) if body else None)


# -- Prometheus text format 0.0.4 -------------------------------------------------------------------

_SAMPLE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(.*)\})? (\S+)$")
_PAIR = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"(,|$)')


@dataclass(frozen=True, slots=True)
class Sample:
    name: str
    labels: dict[str, str]
    value: float


@dataclass(slots=True)
class Exposition:
    types: dict[str, str]
    helps: dict[str, str]
    samples: list[Sample]

    def family_of(self, sample_name: str) -> str:
        for suffix in ("_bucket", "_sum", "_count"):
            base = sample_name.removesuffix(suffix)
            if sample_name.endswith(suffix) and self.types.get(base) == "histogram":
                return base
        return sample_name

    def value(self, name: str, **labels: str) -> float | None:
        for sample in self.samples:
            if sample.name == name and sample.labels == labels:
                return sample.value
        return None

    def series(self, name: str) -> list[Sample]:
        return [s for s in self.samples if s.name == name]


def parse_exposition(text: str) -> Exposition:
    """Parse, and fail on anything a Prometheus scrape would refuse: a sample before its TYPE, a bad line, a
    duplicate series, an unparsable value."""
    types: dict[str, str] = {}
    helps: dict[str, str] = {}
    samples: list[Sample] = []
    seen: set[tuple[str, tuple[tuple[str, str], ...]]] = set()
    assert text.endswith("\n"), "the exposition ends with a newline"
    for line in text.splitlines():
        if not line:
            continue
        if line.startswith("# HELP "):
            name, _, help_text = line[7:].partition(" ")
            helps[name] = help_text
            continue
        if line.startswith("# TYPE "):
            name, _, kind = line[7:].partition(" ")
            assert kind in {"counter", "gauge", "histogram"}, line
            assert name not in types, f"TYPE repeated for {name}"
            types[name] = kind
            continue
        match = _SAMPLE.fullmatch(line)
        assert match is not None, f"not a sample line: {line!r}"
        name, _braces, inner, raw_value = match.groups()
        labels: dict[str, str] = {}
        if inner:
            consumed = 0
            for pair in _PAIR.finditer(inner):
                assert pair.start() == consumed, f"bad label syntax: {line!r}"
                labels[pair.group(1)] = pair.group(2).replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")
                consumed = pair.end()
            assert consumed == len(inner), f"bad label syntax: {line!r}"
        value = float(raw_value.replace("+Inf", "inf").replace("-Inf", "-inf"))
        key = (name, tuple(sorted(labels.items())))
        assert key not in seen, f"duplicate series {line!r}"
        seen.add(key)
        sample = Sample(name, labels, value)
        exposition = Exposition(types, helps, samples)
        assert exposition.family_of(name) in types, f"sample before its TYPE: {line!r}"
        samples.append(sample)
    return Exposition(types, helps, samples)
