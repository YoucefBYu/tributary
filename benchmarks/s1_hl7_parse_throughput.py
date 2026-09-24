"""S1 for Phase 1, against the real runtime: HL7 v2 parse throughput per core using
`tributary.hl7v2.Hl7Message`, the package that landed under W2-1 -- not the Phase 0 spike's stand-in
parsers (`spikes/S1-hl7-throughput/bench.py`, three third-party candidates compared against a "lazy"
segment-split reference; that reference is now this real package).

**What this measures, and what it does not**: "parse" runs `Hl7Message.parse(raw)` alone (structural
work: segment boundaries, delimiters, MSH-1/2 -- everything else is lazy by design, W2-1's own
docstring). "parse+extract" additionally touches the fields a mapping step would actually read (PID,
PV1, OBX), which is what makes the parser's laziness pay off or not. Phase 0's threshold was "parse plus
**transform**" (200 msg/s/core): no HL7-to-FHIR mapping step exists yet (W2-3, not landed as of this
run), so this script cannot measure transform and does not claim to -- "parse+extract" is the closest
honest substitute until W2-3 lands, and is reported as such, not relabelled as the full threshold.

Multi-process throughput (`--procs`) uses the exact fork/synchronise pattern
`spikes/S1-hl7-throughput/bench.py::run_multi` used, for direct comparability: same corpus generator
(`benchmarks.gen`), same environment/percentile helpers (`benchmarks.harness`), same "start every worker,
release them together, sum completions over wall time" methodology.

    uv run python -m benchmarks.s1_hl7_parse_throughput [--count 10000] [--repeats 3] [--procs 1,2,4,8]

Every number is written to `benchmarks/results/s1-hl7v2.json`, including `environment()` (Python, OS,
CPU) and every dependency version through `importlib.metadata` -- Phase 0's requirement 9 ("versions
never recorded") applied here from the start. Per PHASE-0-DECISION.md section 2: report the environment
next to every number and do not use a Windows/Docker-Desktop run as a capacity-planning input; this
script is the Linux re-run's S1 half (human decision #7, `docs/agent-team/STATUS.md`) when run from CI
(`.github/workflows/bench-linux.yml`), and is also runnable locally for a same-host comparison.
"""

from __future__ import annotations

import argparse
import importlib.metadata as md
import json
import multiprocessing as mp
import statistics
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tributary.hl7v2 import Hl7Message  # noqa: E402

from benchmarks.gen import generate  # noqa: E402
from benchmarks.harness import environment, percentile  # noqa: E402


def _extract(msg: Hl7Message) -> dict[str, Any]:
    """The fields a real ADT/ORU mapping step would read (mirrors the Phase 0 spike's `x_lazy`), using
    the package's own accessor API (`msg.get("SEG[occurrence].field...")`, `paths.py`) rather than
    reaching into internals."""
    obs = [
        (msg.get(f"OBX[{i}].3.1") or "", msg.get(f"OBX[{i}].5") or "", msg.get(f"OBX[{i}].6") or "")
        for i in range(1, msg.segment_count("OBX") + 1)
    ]
    return {
        "mrn": (msg.get("PID.3.1") or "").split("^")[0],
        "family": msg.get("PID.5.1") or "",
        "given": msg.get("PID.5.2") or "",
        "dob": msg.get("PID.7") or "",
        "sex": msg.get("PID.8") or "",
        "class": msg.get("PV1.2") or "",
        "visit": msg.get("PV1.19") or "",
        "obs": obs,
    }


def p_parse(raw: bytes) -> Hl7Message:
    return Hl7Message.parse(raw)


def p_parse_extract(raw: bytes) -> dict[str, Any]:
    return _extract(Hl7Message.parse(raw))


MODES = {"parse": p_parse, "parse+extract": p_parse_extract}


def run_once(mode: str, corpus: list[bytes]) -> tuple[float, list[float]]:
    fn = MODES[mode]
    for m in corpus[:100]:
        fn(m)  # warm-up: first-call import/caching effects excluded from the timed pass
    lat: list[float] = []
    pc = time.perf_counter_ns
    t0 = pc()
    for m in corpus:
        s = pc()
        fn(m)
        lat.append((pc() - s) / 1000)
    return (pc() - t0) / 1e9, lat


def _worker(mode: str, corpus: list[bytes], ready: Any, go: Any, q: Any) -> None:
    fn = MODES[mode]
    for m in corpus[:100]:
        fn(m)
    ready.put(1)
    go.wait()
    t0 = time.perf_counter()
    for m in corpus:
        fn(m)
    q.put(time.perf_counter() - t0)


def run_multi(mode: str, corpus: list[bytes], nproc: int) -> float:
    ctx = mp.get_context("spawn")
    go, q, ready = ctx.Event(), ctx.Queue(), ctx.Queue()
    procs = [ctx.Process(target=_worker, args=(mode, corpus, ready, go, q)) for _ in range(nproc)]
    for p in procs:
        p.start()
    for _ in procs:
        ready.get()
    t0 = time.perf_counter()
    go.set()
    for _ in procs:
        q.get()
    wall = time.perf_counter() - t0
    for p in procs:
        p.join()
    return nproc * len(corpus) / wall


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--count", type=int, default=10000)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--procs", default="1,2,4,8")
    ap.add_argument("--skip-multi", action="store_true")
    ap.add_argument("--out", type=Path, default=ROOT / "benchmarks" / "results" / "s1-hl7v2.json")
    a = ap.parse_args(argv)

    corpus = [m.encode("ascii") for m in generate(a.count, seed=1)]

    out: dict[str, Any] = {
        "spike": "S1 (Phase 1 re-run against the real tributary-hl7v2 package)",
        "environment": environment(),
        "versions": {"tributary-hl7v2": md.version("tributary-hl7v2")},
        "count": a.count,
        "repeats": a.repeats,
        "note": (
            "parse+extract is not 'parse plus transform': no HL7-to-FHIR mapping step exists yet "
            "(W2-3). Do not compare 'parse+extract' msg/s directly against the 200 msg/s/core "
            "threshold as if it included a transform -- it does not."
        ),
        "single": [],
        "multi": [],
    }
    for mode in MODES:
        runs = [run_once(mode, corpus) for _ in range(a.repeats)]
        rates = [len(corpus) / t for t, _ in runs]
        med = statistics.median(rates)
        lat = sorted(runs[rates.index(med)][1])
        rec = {
            "name": mode,
            "msgs_per_s": med,
            "p50_us": percentile(lat, 0.5),
            "p99_us": percentile(lat, 0.99),
            "rates": rates,
        }
        out["single"].append(rec)
        print(
            f"{rec['name']:16s} {med:9.0f} msg/s  p50 {rec['p50_us']:8.1f}us  p99 {rec['p99_us']:8.1f}us  "
            f"runs {[round(x) for x in rates]}",
            flush=True,
        )

    if not a.skip_multi:
        for mode in MODES:
            for n in (int(x) for x in a.procs.split(",")):
                rate = run_multi(mode, corpus, n)
                out["multi"].append({"mode": mode, "procs": n, "msgs_per_s_total": rate, "per_proc": rate / n})
                print(f"multi {mode:16s} n={n}: {rate:9.0f} msg/s total, {rate / n:8.0f}/proc", flush=True)

    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(out, indent=2, default=str))
    print(f"wrote {a.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
