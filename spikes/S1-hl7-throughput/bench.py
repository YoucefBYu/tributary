"""S1: HL7 parse + transform throughput. Throwaway spike code.

Run (repo root):
  uv run --no-project --python 3.12 --with hl7apy --with hl7 --with pydantic --with orjson \
      python spikes/S1-hl7-throughput/bench.py [--count 10000] [--repeats 3] [--procs 1,2,4,8]
Raw results: benchmarks/results/s1.json
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import statistics
import sys
import time
import tracemalloc
from collections.abc import Callable
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

import orjson  # noqa: E402
from pydantic import BaseModel  # noqa: E402

from benchmarks.gen import generate  # noqa: E402
from benchmarks.harness import environment, percentile  # noqa: E402


# ---------- parsers: bytes -> parsed object ----------
def p_lazy(raw: bytes) -> list[bytes]:
    return raw.split(b"\r")  # segments only; fields are split on access


def p_hl7(raw: bytes) -> Any:
    import hl7

    return hl7.parse(raw.decode("ascii"))


def p_hl7apy(raw: bytes) -> Any:
    from hl7apy.consts import VALIDATION_LEVEL
    from hl7apy.parser import parse_message

    return parse_message(raw.decode("ascii"), validation_level=VALIDATION_LEVEL.TOLERANT, find_groups=False)


# ---------- field access: parsed -> flat dict of what the transform needs ----------
def x_lazy(segs: list[bytes]) -> dict[str, Any]:
    pid = next(s for s in segs if s.startswith(b"PID|")).split(b"|")
    pv1 = next((s for s in segs if s.startswith(b"PV1|")), b"").split(b"|")
    fam, _, giv = pid[5].decode().partition("^")
    obx = [s.split(b"|") for s in segs if s.startswith(b"OBX|")]
    return {
        "mrn": pid[3].decode().split("^")[0],
        "family": fam,
        "given": giv.split("^")[0],
        "dob": pid[7].decode(),
        "sex": pid[8].decode(),
        "class": pv1[2].decode() if len(pv1) > 2 else "",
        "visit": pv1[19].decode() if len(pv1) > 19 else "",
        "obs": [(o[3].decode().split("^")[0], o[5].decode(), o[6].decode()) for o in obx],
    }


def x_hl7(m: Any) -> dict[str, Any]:
    pid = m.segment("PID")
    try:
        pv1 = m.segment("PV1")
        cls, visit = str(pv1[2]), str(pv1[19])
    except KeyError:
        cls = visit = ""
    obs = [(str(s[3][0][0]), str(s[5]), str(s[6])) for s in m if str(s[0]) == "OBX"]
    return {
        "mrn": str(pid[3][0][0]),
        "family": str(pid[5][0][0]),
        "given": str(pid[5][0][1]),
        "dob": str(pid[7]),
        "sex": str(pid[8]),
        "class": cls,
        "visit": visit,
        "obs": obs,
    }


def x_hl7apy(m: Any) -> dict[str, Any]:
    pid = m.pid
    pv1 = getattr(m, "pv1", None)
    obs = [(s.obx_3.ce_1.value, s.obx_5.value, s.obx_6.value) for s in m.children if s.name == "OBX"]
    return {
        "mrn": pid.pid_3.cx_1.value,
        "family": pid.pid_5.xpn_1.value,
        "given": pid.pid_5.xpn_2.value,
        "dob": pid.pid_7.value,
        "sex": pid.pid_8.value,
        "class": pv1.pv1_2.value if pv1 is not None else "",
        "visit": pv1.pv1_19.value if pv1 is not None else "",
        "obs": obs,
    }


# ---------- transform: dict -> FHIR-ish Bundle via pydantic + orjson ----------
class HumanName(BaseModel):
    family: str
    given: list[str]


class Identifier(BaseModel):
    system: str
    value: str


class Patient(BaseModel):
    resourceType: str = "Patient"
    identifier: list[Identifier]
    name: list[HumanName]
    birthDate: str
    gender: str


class Encounter(BaseModel):
    resourceType: str = "Encounter"
    status: str
    class_: str
    identifier: list[Identifier]


class Observation(BaseModel):
    resourceType: str = "Observation"
    status: str = "final"
    code: str
    value: str
    unit: str


class Entry(BaseModel):
    resource: Patient | Encounter | Observation


class Bundle(BaseModel):
    resourceType: str = "Bundle"
    type: str = "transaction"
    entry: list[Entry]


_G = {"M": "male", "F": "female", "U": "unknown"}


def to_bundle(d: dict[str, Any]) -> bytes:
    dob = str(d["dob"])
    entries = [
        Entry(
            resource=Patient(
                identifier=[Identifier(system="urn:synth:mrn", value=str(d["mrn"]))],
                name=[HumanName(family=str(d["family"]), given=[str(d["given"])])],
                birthDate=f"{dob[:4]}-{dob[4:6]}-{dob[6:8]}",
                gender=_G.get(str(d["sex"]), "unknown"),
            )
        )
    ]
    if d["visit"] or d["class"]:
        entries.append(
            Entry(
                resource=Encounter(
                    status="in-progress",
                    class_=str(d["class"]),
                    identifier=[Identifier(system="urn:synth:visit", value=str(d["visit"]))],
                )
            )
        )
    for code, val, unit in d["obs"]:
        entries.append(Entry(resource=Observation(code=code, value=val, unit=unit)))
    return orjson.dumps(Bundle(entry=entries).model_dump(mode="json"))


PARSERS: dict[str, tuple[Callable[[bytes], Any], Callable[[Any], dict[str, Any]]]] = {
    "lazy": (p_lazy, x_lazy),
    "python-hl7": (p_hl7, x_hl7),
    "hl7apy": (p_hl7apy, x_hl7apy),
}
MODES = ("parse", "parse+extract", "parse+extract+fhir")


def make_fn(parser: str, mode: str) -> Callable[[bytes], Any]:
    p, x = PARSERS[parser]
    if mode == "parse":
        return p
    if mode == "parse+extract":
        return lambda raw: x(p(raw))
    return lambda raw: to_bundle(x(p(raw)))


def run_once(parser: str, mode: str, corpus: list[bytes]) -> tuple[float, list[float]]:
    fn = make_fn(parser, mode)
    for m in corpus[:100]:
        fn(m)
    lat: list[float] = []
    pc = time.perf_counter_ns
    t0 = pc()
    for m in corpus:
        s = pc()
        fn(m)
        lat.append((pc() - s) / 1000)
    return (pc() - t0) / 1e9, lat


def _worker(parser: str, mode: str, corpus: list[bytes], ready: Any, go: Any, q: Any) -> None:
    fn = make_fn(parser, mode)
    for m in corpus[:100]:
        fn(m)
    ready.put(1)
    go.wait()
    t0 = time.perf_counter()
    for m in corpus:
        fn(m)
    q.put(time.perf_counter() - t0)


def run_multi(parser: str, mode: str, corpus: list[bytes], nproc: int) -> float:
    ctx = mp.get_context("spawn")
    go, q, ready = ctx.Event(), ctx.Queue(), ctx.Queue()
    ps = [ctx.Process(target=_worker, args=(parser, mode, corpus, ready, go, q)) for _ in range(nproc)]
    for p in ps:
        p.start()
    for _ in ps:
        ready.get()
    t0 = time.perf_counter()
    go.set()
    for _ in ps:
        q.get()
    wall = time.perf_counter() - t0
    for p in ps:
        p.join()
    return nproc * len(corpus) / wall


def memory(parser: str, corpus: list[bytes], n: int = 500) -> dict[str, float]:
    p, _ = PARSERS[parser]
    sub = corpus[:n]
    p(sub[0])  # import/warm
    tracemalloc.start()
    keep = [p(m) for m in sub]
    cur, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    del keep
    raw = sum(map(len, sub))
    return {"retained_bytes_per_msg": cur / n, "peak_bytes_per_msg": peak / n, "expansion_vs_raw": cur / raw}


def verify(corpus: list[bytes]) -> int:
    """Check every parser yields a byte-identical Bundle to the lazy reference. Returns mismatch count."""
    bad = {k: 0 for k in PARSERS if k != "lazy"}
    for raw in corpus:
        ref = to_bundle(x_lazy(p_lazy(raw)))
        for k in bad:
            p, x = PARSERS[k]
            if to_bundle(x(p(raw))) != ref:
                bad[k] += 1
    print(f"verify: {len(corpus)} messages, mismatches {bad}")
    return sum(bad.values())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=10000)
    ap.add_argument("--repeats", type=int, default=3)
    ap.add_argument("--procs", default="1,2,4,8")
    ap.add_argument("--skip-multi", action="store_true")
    ap.add_argument("--verify", action="store_true", help="cross-check Bundles across parsers on the first 1000 messages and exit")
    a = ap.parse_args()
    corpus = [m.encode("ascii") for m in generate(a.count, 1)]
    if a.verify:
        sys.exit(1 if verify(corpus[:1000]) else 0)
    import importlib.metadata as md

    out: dict[str, Any] = {
        "environment": environment(),
        "versions": {k: md.version(k) for k in ("hl7apy", "hl7", "pydantic", "orjson")},
        "count": a.count,
        "repeats": a.repeats,
        "single": [],
        "multi": [],
        "memory": {},
    }
    caps = {"hl7apy": 1000}  # ~12 ms/msg; a 10k pass would take minutes per run
    for parser in PARSERS:
        out["memory"][parser] = memory(parser, corpus)
        pc_ = corpus[: caps.get(parser, len(corpus))]
        for mode in MODES:
            runs = [run_once(parser, mode, pc_) for _ in range(a.repeats)]
            rates = [len(pc_) / t for t, _ in runs]
            med = statistics.median(rates)
            lat = sorted(runs[rates.index(med)][1])
            rec = {
                "name": f"{parser}/{mode}",
                "msgs_per_s": med,
                "p50_us": percentile(lat, 0.5),
                "p99_us": percentile(lat, 0.99),
                "rates": rates,
            }
            out["single"].append(rec)
            print(f"{rec['name']:32s} {med:9.0f} msg/s  p50 {rec['p50_us']:8.1f}us  p99 {rec['p99_us']:8.1f}us  runs {[round(x) for x in rates]}", flush=True)
    if not a.skip_multi:
        for parser in PARSERS:
            for n in [int(x) for x in a.procs.split(",")]:
                rate = run_multi(parser, "parse+extract+fhir", corpus[: caps.get(parser, len(corpus))], n)
                out["multi"].append({"parser": parser, "procs": n, "msgs_per_s_total": rate, "per_proc": rate / n})
                print(f"multi {parser:12s} n={n}: {rate:9.0f} msg/s total, {rate / n:8.0f}/proc", flush=True)
    (ROOT / "benchmarks/results").mkdir(parents=True, exist_ok=True)
    (ROOT / "benchmarks/results/s1.json").write_text(json.dumps(out, indent=2, default=str))
    print(json.dumps(out["memory"], indent=1))


if __name__ == "__main__":
    main()
