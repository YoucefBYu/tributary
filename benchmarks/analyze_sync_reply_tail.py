"""Correlate the W3-24 sync-reply tail with host state. Reads the files `tests/integration/test_sync_reply_tail.py`
writes (`sync-reply-tail-<epoch>.json`) and prints, per file: the distribution, the stalls (seconds in which more
than one sender saw a request over 100 ms), and the host iowait / busy / steal around them against the run's
median. Control ids and timestamps only.

    uv run python benchmarks/analyze_sync_reply_tail.py <dir-or-file> ...
"""

from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path


def analyse(path: Path) -> None:
    data = json.loads(path.read_text(encoding="utf-8"))
    requests, host = data["requests"], data["host_samples"]
    summary = data["summary"]
    print(
        f"== {path.name}: n={summary['n']} p50={summary['p50_ms']} p99={summary['p99_ms']} max={summary['max_ms']} "
        f">100ms={summary['over_100ms']} >200ms={summary['over_200ms']}"
    )
    by_second: dict[int, list[float]] = defaultdict(list)
    for r in requests:
        by_second[int(r["start"])].append(r["ms"])
    host_by_second: dict[int, list[dict[str, float]]] = defaultdict(list)
    for s in host:
        host_by_second[int(s["t"])].append(s)
    stalls = sorted(sec for sec, values in by_second.items() if sum(1 for v in values if v > 100) >= 2)
    io_median = statistics.median(s["iowait_pct"] for s in host)
    print(
        f"   host iowait median {io_median:.1f}%, busy median {statistics.median(s['busy_pct'] for s in host):.1f}%, "
        f"steal max {max(s['steal_pct'] for s in host):.1f}%; stall seconds (>=2 requests over 100 ms): {len(stalls)}"
    )
    for sec in stalls:
        near = [s for k in (sec - 1, sec, sec + 1) for s in host_by_second.get(k, [])]
        print(
            f"   stall at +{sec - int(min(r['start'] for r in requests))}s: worst {max(by_second[sec]):.0f} ms, "
            f"{sum(1 for v in by_second[sec] if v > 100)} requests >100 ms, host iowait max "
            f"{max(s['iowait_pct'] for s in near):.0f}%, busy max {max(s['busy_pct'] for s in near):.0f}%, "
            f"load1 {near[-1]['load1']:.1f}"
        )
    quiet = [
        v
        for sec, vs in by_second.items()
        for v in vs
        if max((s["iowait_pct"] for k in (sec - 1, sec, sec + 1) for s in host_by_second.get(k, [])), default=0) < 20
    ]
    busy_io = [
        v
        for sec, vs in by_second.items()
        for v in vs
        if max((s["iowait_pct"] for k in (sec - 1, sec, sec + 1) for s in host_by_second.get(k, [])), default=0) >= 20
    ]
    for label, values in (("iowait < 20% around the request", quiet), ("iowait >= 20% around the request", busy_io)):
        if values:
            ordered = sorted(values)
            print(
                f"   {label}: n={len(ordered)} p50={ordered[len(ordered) // 2]:.1f} p99={ordered[int(len(ordered) * 0.99)]:.1f} "
                f"max={ordered[-1]:.0f} over200={sum(1 for v in ordered if v > 200)}"
            )


def main(argv: list[str]) -> int:
    files: list[Path] = []
    for arg in argv:
        p = Path(arg)
        files += sorted(p.glob("sync-reply-tail-*.json")) if p.is_dir() else [p]
    for f in files:
        analyse(f)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
