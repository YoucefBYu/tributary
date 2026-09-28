"""S1 for Phase 1, against the real runtime: HL7 v2 parse throughput per core using
`tributary.hl7v2.Hl7Message`, the package that landed under W2-1 -- not the Phase 0 spike's stand-in
parsers (`spikes/S1-hl7-throughput/bench.py`, three third-party candidates compared against a "lazy"
segment-split reference; that reference is now this real package).

**What this measures**: "parse" runs `Hl7Message.parse(raw)` alone (structural work; everything else is
lazy by design). "parse+extract" additionally touches the fields a mapping would read. "parse+map" is the
Phase 0 threshold's "parse plus transform": the real W2-3 steps (`tributary.content_hl7_fhir`:
`adt_to_fhir`, `oru_r01_to_fhir`) run through the SDK, `parse` step then mapping step, producing a FHIR R4
Bundle as JSON bytes, exactly as `packages/tributary-content-hl7-fhir/tests/conftest.py::map_fixture` does.
"harness-only" is the `make_message` + `fake_fhir_context` cost that parse+map also pays (the golden path
builds both per message); subtract it to estimate the mapping alone.

`--corpus fixtures` (used for parse+map) cycles the golden inputs, `tests/fixtures/hl7/` (5 ADT + 1 ORU,
the six that have golden Bundles). `--corpus synth` is the `benchmarks.gen` mix, which has ORM and other
types the mapping does not cover, so the map modes only run on `fixtures`.

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

from tributary.content_hl7_fhir.adt import adt_to_fhir  # noqa: E402
from tributary.content_hl7_fhir.oru import oru_r01_to_fhir  # noqa: E402
from tributary.fhir.fakes import fake_fhir_context  # noqa: E402
from tributary.hl7v2 import Hl7Message  # noqa: E402
from tributary.hl7v2.step import parse as parse_step  # noqa: E402
from tributary.sdk.testing import make_message, run_step  # noqa: E402

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


FIXTURE_DIR = ROOT / "tests" / "fixtures" / "hl7"
GOLDEN_INPUTS = (
    "adt_a01_admit.hl7",
    "adt_a01_admit_with_visit_number.hl7",
    "adt_a01_two_nk1.hl7",
    "adt_a03_discharge.hl7",
    "adt_a08_update_no_pv1.hl7",
    "oru_r01_repeating_obx.hl7",
)
_MSG_ID = "0199036e-7a3e-7f00-8c21-abc123456789"


def p_harness_only(raw: bytes) -> object:
    fake_fhir_context()
    return make_message(raw, content_type="application/hl7-v2+er7", msg_id=_MSG_ID)


def p_parse_map(raw: bytes) -> object:
    ctx = fake_fhir_context()
    message = make_message(raw, content_type="application/hl7-v2+er7", msg_id=_MSG_ID)
    parsed = run_step(parse_step, message, ctx=ctx)
    mapping = oru_r01_to_fhir if b"ORU^R01" in raw[:200] else adt_to_fhir
    return run_step(mapping, parsed, ctx=ctx)


MODES = {"parse": p_parse, "parse+extract": p_parse_extract}
MAP_MODES = {"harness-only": p_harness_only, "parse+map": p_parse_map}
ALL_MODES = {**MODES, **MAP_MODES}


def run_once(mode: str, corpus: list[bytes]) -> tuple[float, list[float]]:
    fn = ALL_MODES[mode]
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
    fn = ALL_MODES[mode]
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
    ap.add_argument("--corpus", choices=("synth", "fixtures"), default="synth")
    ap.add_argument("--skip-multi", action="store_true")
    ap.add_argument("--out", type=Path, default=ROOT / "benchmarks" / "results" / "s1-hl7v2.json")
    a = ap.parse_args(argv)

    if a.corpus == "fixtures":
        base = [(FIXTURE_DIR / n).read_bytes() for n in GOLDEN_INPUTS]
        corpus = [base[i % len(base)] for i in range(a.count)]
        modes = ALL_MODES
    else:
        corpus = [m.encode("ascii") for m in generate(a.count, seed=1)]
        modes = MODES

    out: dict[str, Any] = {
        "spike": "S1 (Phase 1 re-run against the real tributary-hl7v2 package)",
        "environment": environment(),
        "versions": {
            n: md.version(n)
            for n in ("tributary-hl7v2", "tributary-content-hl7-fhir", "tributary-fhir", "fhir.resources", "pydantic")
        },
        "count": a.count,
        "repeats": a.repeats,
        "corpus": a.corpus,
        "note": "parse+map is the real W2-3 mapping; parse+extract is not a transform (see module docstring).",
        "single": [],
        "multi": [],
    }
    for mode in modes:
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
        for mode in modes:
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
