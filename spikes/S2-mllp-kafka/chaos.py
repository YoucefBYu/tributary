"""S2 broker-failure harness (throwaway, host side).

Scenario leader-kill (x N trials): load runs against a fresh topic; at t=KILL_AT the leader of
a rotating partition is hard-killed (docker kill); it is restarted DOWN_S later. Checks per trial:
  * zero acknowledged-message loss (every AA'd control id is in the topic; verify_loss.py)
  * ACK/AR counts and worst ACK latency around the failover (availability cost, reported)
Scenario outage (x M trials): two of three brokers are killed so ISR < min.insync.replicas=2 and
acks=all cannot succeed. Checks: no message SENT after the kill completed is ACKed (AA) until the
brokers are restarted, and loss is still zero. Host<->container clock skew is measured and
reported; a fixed GRACE_S absorbs it.

Usage: python chaos.py leader-kill --trials 20 | outage --trials 5
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "common" / "src"))
from tbcommon import faults  # noqa: E402
from tbcommon.admin import TopicSpec, ensure_topics, partition_leaders  # noqa: E402

NET = "tributary-spikes_default"
INTERNAL = "kafka1:9092,kafka2:9092,kafka3:9092"
HOST_BOOT = "localhost:19092,localhost:29092,localhost:39092"
CORPUS = HERE.parents[1] / "benchmarks" / "results" / "corpus-1-10000.mllp"
GRACE_S = 0.5


def sh(*a: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(a, capture_output=True, text=True, check=check)


def clock_skew_s() -> float:
    """container wall clock minus host wall clock (s), median of 5."""
    d = []
    for _ in range(5):
        h0 = time.time()
        c = float(sh("docker", "run", "--rm", "alpine", "date", "+%s.%N").stdout)
        d.append(c - (h0 + time.time()) / 2)
    return sorted(d)[2]


def trial(kind: str, i: int, rate: int, conns: int, seconds: int, kill_at: float, down_s: float) -> dict:
    run_id = f"{kind[:2]}{i}x{int(time.time()) % 100000}"
    topic = f"spike.s2.{run_id}"
    ensure_topics([TopicSpec(topic, 12, 3)])
    sh("docker", "rm", "-f", "tb-s2-srv", check=False)
    sh("docker", "run", "-d", "--name", "tb-s2-srv", "--hostname", "mllp", "--network", NET,
       "tb-s2", "server.py", "--bootstrap", INTERNAL, "--topic", topic, "--linger-ms", "2")
    for _ in range(50):
        if "listening" in sh("docker", "logs", "tb-s2-srv").stdout:
            break
        time.sleep(0.2)
    (HERE / "runs").mkdir(exist_ok=True)
    lg = subprocess.Popen(
        ["docker", "run", "--rm", "--name", "tb-s2-lg", "--network", NET,
         "-v", f"{HERE / 'runs'}:/runs", "-v", f"{CORPUS}:/corpus.mllp:ro", "tb-s2", "loadgen.py",
         "--host", "mllp", "--rate", str(rate), "--conns", str(conns), "--seconds", str(seconds),
         "--drain-s", "20", "--corpus", "/corpus.mllp", "--run-id", run_id, "--events",
         "--out", f"/runs/{run_id}.json"],
        stdout=subprocess.PIPE, text=True)
    time.sleep(kill_at)
    victims: list[str] = []
    if kind == "leader-kill":
        leaders = partition_leaders(topic)
        victims = [faults.service_for_node_id(leaders[i % len(leaders)])]
    else:
        victims = ["kafka1", "kafka2"]
    t_kill0 = time.time()
    for v in victims:
        faults.kill_broker(v)
    t_kill = time.time()
    time.sleep(down_s)
    t_up0 = time.time()
    for v in victims:
        faults.start_broker(v)
    out = lg.communicate()[0].strip().splitlines()[-1]
    summ = json.loads(out)
    faults.wait_healthy()
    sh("docker", "rm", "-f", "tb-s2-srv", check=False)
    run = json.load(open(HERE / "runs" / f"{run_id}.json"))
    skew = clock_skew_s()
    ev = run["events"]
    # AA'd messages sent after the kill finished and acked before the restart began
    bad = [e[0] for e in ev if e[3] == "AA" and e[1] - skew > t_kill + GRACE_S and e[2] - skew < t_up0]
    lat_max = max((e[2] - e[1] for e in ev if e[3] == "AA"), default=0) * 1000
    v = sh(sys.executable, str(HERE / "verify_loss.py"), "--bootstrap", HOST_BOOT, "--topic", topic,
           str(HERE / "runs" / f"{run_id}.json"), check=False)
    verify = json.loads(v.stdout.strip().splitlines()[-1]) if v.stdout.strip() else {"error": v.stderr[-300:]}
    return {"kind": kind, "trial": i, "run_id": run_id, "victims": victims, "sent": summ["sent"],
            "acked": summ["acked"], "nack": summ["nack"], "unanswered": summ["unanswered"],
            "max_ack_latency_ms": round(lat_max, 1), "kill_wall_s": round(t_kill - t_kill0, 2),
            "aa_while_unavailable": len(bad), "clock_skew_s": round(skew, 4), "verify": verify}


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("kind", choices=["leader-kill", "outage"])
    p.add_argument("--trials", type=int, default=20)
    p.add_argument("--rate", type=int, default=500)
    p.add_argument("--conns", type=int, default=10)
    p.add_argument("--seconds", type=int, default=40)
    p.add_argument("--kill-at", type=float, default=10)
    p.add_argument("--down-s", type=float, default=12)
    a = p.parse_args()
    for i in range(a.trials):
        r = trial(a.kind, i, a.rate, a.conns, a.seconds, a.kill_at, a.down_s)
        print(json.dumps(r), flush=True)
        with open(HERE / "runs" / f"{a.kind}.jsonl", "a") as f:
            f.write(json.dumps(r) + "\n")


if __name__ == "__main__":
    main()
