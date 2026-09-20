"""S3 scenario runner (throwaway, host side). Worker is the stand-in echo_worker.py.

Scenarios: latency (async vs sync-broadcast vs sync-targeted), timeout, memory, restart.
Usage: python s3run.py latency|timeout|memory|restart
Host/container clock skew (~-0.45 s, measured in S2) is corrected with a fixed constant.
"""

from __future__ import annotations

import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
S2 = HERE.parent / "S2-mllp-kafka"
sys.path.insert(0, str(HERE.parent / "common" / "src"))
from tbcommon.admin import TopicSpec, ensure_topics  # noqa: E402

NET = "tributary-spikes_default"
INTERNAL = "kafka1:9092,kafka2:9092,kafka3:9092"
HOST_BOOT = "localhost:19092,localhost:29092,localhost:39092"
CORPUS = HERE.parents[1] / "benchmarks" / "results" / "corpus-1-10000.mllp"
RUNS = S2 / "runs"
SKEW = -0.45


def sh(*a: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(a, capture_output=True, text=True, check=check)


def wait_log(name: str, needle: str, n: int = 100) -> None:
    for _ in range(n):
        if needle in sh("docker", "logs", name, check=False).stdout:
            return
        time.sleep(0.2)
    raise RuntimeError(f"{name}: no {needle}")


def topics(tag: str) -> tuple[str, str]:
    raw, rep = f"spike.s3.{tag}.raw", f"spike.s3.{tag}.reply"
    ensure_topics([TopicSpec(raw, 12, 3), TopicSpec(rep, 12, 3)])
    return raw, rep


def start_worker(raw: str, rep: str, group: str, name: str = "tb-s3-worker") -> None:
    sh("docker", "rm", "-f", name, check=False)
    sh("docker", "run", "-d", "--name", name, "--network", NET, "tb-s3", "echo_worker.py",
       "--bootstrap", INTERNAL, "--raw", raw, "--reply", rep, "--group", group)
    wait_log(name, "worker-up")
    time.sleep(8)  # group join + assignment


def start_server(kind: str, raw: str, rep: str, mode: str = "broadcast", timeout: float = 10.0,
                 max_pending: int = 20000, metrics: str = "m.json") -> None:
    sh("docker", "rm", "-f", "tb-s3-srv", check=False)
    common = ["docker", "run", "-d", "--name", "tb-s3-srv", "--hostname", "mllp", "--network", NET,
              "-v", f"{RUNS}:/runs"]
    if kind == "async":
        sh(*common, "tb-s3", "server.py", "--bootstrap", INTERNAL, "--topic", raw, "--linger-ms", "2")
    else:
        sh(*common, "tb-s3", "sync_server.py", "--bootstrap", INTERNAL, "--topic", raw,
           "--reply-topic", rep, "--reply-mode", mode, "--reply-timeout", str(timeout),
           "--max-pending", str(max_pending), "--metrics-file", f"/runs/{metrics}")
    wait_log("tb-s3-srv", "listening")


def loadgen(run_id: str, rate: int, conns: int, seconds: int, drain: int = 15, bg: bool = False):
    cmd = ["docker", "run", "--rm", "--name", "tb-s3-lg", "--network", NET, "-v", f"{RUNS}:/runs",
           "-v", f"{CORPUS}:/corpus.mllp:ro", "tb-s3", "loadgen.py", "--host", "mllp", "--rate", str(rate),
           "--conns", str(conns), "--seconds", str(seconds), "--drain-s", str(drain), "--corpus",
           "/corpus.mllp", "--run-id", run_id, "--events", "--out", f"/runs/{run_id}.json"]
    if bg:
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, text=True)
    return json.loads(sh(*cmd).stdout.strip().splitlines()[-1])


def cleanup() -> None:
    for n in ("tb-s3-srv", "tb-s3-worker", "tb-s3-worker2", "tb-s3-lg"):
        sh("docker", "rm", "-f", n, check=False)


def metrics(name: str) -> dict:
    try:
        return json.load(open(RUNS / name))
    except (OSError, ValueError):
        return {}


def emit(rec: dict) -> None:
    print(json.dumps(rec), flush=True)
    with open(RUNS / "s3.jsonl", "a") as f:
        f.write(json.dumps(rec) + "\n")


def latency() -> None:
    for rate in (100, 1000):
        for kind, mode in (("async", ""), ("sync", "broadcast"), ("sync", "targeted")):
            tag = f"lat{rate}{kind[0]}{mode[:1]}{int(time.time()) % 10000}"
            raw, rep = topics(tag)
            cleanup()
            if kind == "sync":
                start_worker(raw, rep, f"g-{tag}")
            start_server(kind, raw, rep, mode or "broadcast", metrics=f"{tag}-m.json")
            r = loadgen(tag, rate, 10, 30)
            time.sleep(1.5)
            emit({"scenario": "latency", "kind": kind, "mode": mode, "rate": rate, "worker": "stand-in",
                  **{k: r[k] for k in ("sent", "acked", "nack", "unanswered", "steady")},
                  "server": metrics(f"{tag}-m.json")})
            cleanup()


def timeout() -> None:
    tag = f"to{int(time.time()) % 10000}"
    raw, rep = topics(tag)
    cleanup()
    start_server("sync", raw, rep, timeout=3.0, metrics=f"{tag}-m.json")  # no worker
    r = loadgen(tag, 50, 10, 10, drain=10)
    ev = json.load(open(RUNS / f"{tag}.json"))["events"]
    lat = sorted((e[2] - e[1]) * 1000 for e in ev)
    codes: dict[str, int] = {}
    for e in ev:
        codes[e[3]] = codes.get(e[3], 0) + 1
    m_before = metrics(f"{tag}-m.json")
    # worker arrives late: replies for timed-out requests must be dropped harmlessly
    start_worker(raw, rep, f"g-{tag}")
    time.sleep(6)
    m_after = metrics(f"{tag}-m.json")
    emit({"scenario": "timeout", "reply_timeout_s": 3.0, "sent": r["sent"], "codes": codes,
          "lat_ms_min": round(lat[0], 1), "lat_ms_p50": round(lat[len(lat) // 2], 1),
          "lat_ms_max": round(lat[-1], 1), "unanswered": r["unanswered"],
          "server_before_worker": m_before, "server_after_worker": m_after})
    cleanup()


def memory() -> None:
    tag = f"mem{int(time.time()) % 10000}"
    raw, rep = topics(tag)
    cleanup()
    start_server("sync", raw, rep, timeout=60.0, max_pending=20000, metrics=f"{tag}-m.json")
    time.sleep(3)
    idle = metrics(f"{tag}-m.json")
    lg = loadgen(tag, 2000, 50, 5, drain=1, bg=True)
    samples = []
    for _ in range(9):
        time.sleep(1)
        m = metrics(f"{tag}-m.json")
        samples.append({k: m.get(k) for k in ("pending", "pending_hwm", "rss_kb", "commit_fail", "busy")})
    lg.communicate()
    emit({"scenario": "memory", "idle": {k: idle.get(k) for k in ("pending", "rss_kb")},
          "samples_1s": samples})
    cleanup()


def restart() -> None:
    tag = f"rs{int(time.time()) % 10000}"
    raw, rep = topics(tag)
    cleanup()
    start_worker(raw, rep, f"g-{tag}")
    start_server("sync", raw, rep, timeout=5.0, metrics=f"{tag}-m.json")
    lg = loadgen(tag, 200, 10, 45, drain=15, bg=True)
    time.sleep(12)
    t_kill = time.time()
    sh("docker", "kill", "tb-s3-worker")
    time.sleep(6)
    t_up = time.time()
    sh("docker", "run", "-d", "--name", "tb-s3-worker2", "--network", NET, "tb-s3", "echo_worker.py",
       "--bootstrap", INTERNAL, "--raw", raw, "--reply", rep, "--group", f"g-{tag}")
    lg.communicate()
    time.sleep(2)
    r = json.load(open(RUNS / f"{tag}.json"))
    ev = r["events"]
    t0 = min(e[1] for e in ev) - SKEW
    codes: dict[str, int] = {}
    per_s: dict[int, list[bool]] = {}
    for e in ev:
        codes[e[3]] = codes.get(e[3], 0) + 1
        per_s.setdefault(int(e[1] - SKEW - t0), []).append(e[3] == "AA")
    timeline = {s: f"{sum(v)}/{len(v)}" for s, v in sorted(per_s.items()) if s >= 8}
    v = sh(sys.executable, str(S2 / "verify_loss.py"), "--bootstrap", HOST_BOOT, "--topic", raw,
           "--id-from", "msh10", str(RUNS / f"{tag}.json"), check=False)
    emit({"scenario": "restart", "kill_rel_s": round(t_kill - t0, 1), "restart_rel_s": round(t_up - t0, 1),
          "sent": r["sent"], "codes": codes, "unanswered": r["unanswered"],
          "aa_over_sent_by_second": timeline, "server": metrics(f"{tag}-m.json"),
          "verify": json.loads(v.stdout.strip().splitlines()[-1]) if v.stdout.strip() else v.stderr[-200:]})
    cleanup()


if __name__ == "__main__":
    {"latency": latency, "timeout": timeout, "memory": memory, "restart": restart}[sys.argv[1]]()
