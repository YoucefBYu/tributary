"""Benchmark harness skeleton: timing, percentiles, environment capture, JSON results."""

from __future__ import annotations

import json
import platform
import statistics
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path


def environment() -> dict[str, object]:
    import os

    return {
        "python": sys.version.split()[0],
        "implementation": platform.python_implementation(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "cpu_count": os.cpu_count(),
    }


def percentile(sorted_vals: Sequence[float], q: float) -> float:
    if not sorted_vals:
        raise ValueError("empty")
    k = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[k]


@dataclass
class Result:
    name: str
    n: int
    total_s: float
    msgs_per_s: float
    p50_us: float
    p99_us: float
    mean_us: float
    extra: dict[str, object] = field(default_factory=dict)


def time_each(name: str, fn: Callable[[bytes], object], corpus: Sequence[bytes], warmup: int = 200) -> Result:
    """Run fn over every message, timing each call individually (single core)."""
    for m in corpus[:warmup]:
        fn(m)
    lat: list[float] = []
    pc = time.perf_counter_ns
    t0 = pc()
    for m in corpus:
        s = pc()
        fn(m)
        lat.append((pc() - s) / 1000)
    total = (pc() - t0) / 1e9
    lat.sort()
    return Result(
        name,
        len(corpus),
        total,
        len(corpus) / total,
        percentile(lat, 0.5),
        percentile(lat, 0.99),
        statistics.fmean(lat),
    )


def write_results(path: Path, results: Sequence[Result], meta: dict[str, object] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = {"environment": environment(), "meta": meta or {}, "results": [asdict(r) for r in results]}
    path.write_text(json.dumps(doc, indent=2))
