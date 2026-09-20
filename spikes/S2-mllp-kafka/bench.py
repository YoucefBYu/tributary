"""S2 matrix runner (throwaway, host side). One fresh topic + server container per cell;
loadgen runs in a Linux container on the compose network. Then verify_loss on the host.

Usage: python bench.py --rates 100,1000,5000 --conns 1,10,100 --tls both --seconds 30
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
NET = "tributary-spikes_default"
INTERNAL = "kafka1:9092,kafka2:9092,kafka3:9092"
HOST_BOOT = "localhost:19092,localhost:29092,localhost:39092"
CORPUS = ROOT / "benchmarks" / "results" / "corpus-1-10000.mllp"


def sh(*a: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(a, capture_output=True, text=True, check=check)


def cell(rate: int, conns: int, tls: bool, seconds: int, linger: int) -> dict:
    run_id = f"r{rate}c{conns}{'t' if tls else 'p'}{int(time.time()) % 100000}"
    topic = f"spike.s2.{run_id}"
    sys.path.insert(0, str(HERE.parent / "common" / "src"))
    from tbcommon.admin import TopicSpec, ensure_topics

    ensure_topics([TopicSpec(topic, 12, 3)])
    sh("docker", "rm", "-f", "tb-s2-srv", check=False)
    srv = ["docker", "run", "-d", "--name", "tb-s2-srv", "--hostname", "mllp", "--network", NET,
           "-v", f"{HERE / 'certs'}:/certs:ro", "tb-s2", "server.py", "--host", "0.0.0.0", "--bootstrap", INTERNAL,
           "--topic", topic, "--linger-ms", str(linger)]
    if tls:
        srv += ["--tls-cert", "/certs/cert.pem", "--tls-key", "/certs/key.pem"]
    sh(*srv)
    for _ in range(50):
        if "listening" in sh("docker", "logs", "tb-s2-srv").stdout:
            break
        time.sleep(0.2)
    (HERE / "runs").mkdir(exist_ok=True)
    lg = ["docker", "run", "--rm", "--network", NET, "-v", f"{HERE / 'runs'}:/runs",
          "-v", f"{CORPUS}:/corpus.mllp:ro", "-v", f"{HERE / 'certs'}:/certs:ro", "tb-s2",
          "loadgen.py", "--host", "mllp", "--rate", str(rate), "--conns", str(conns),
          "--seconds", str(seconds), "--corpus", "/corpus.mllp", "--run-id", run_id,
          "--out", f"/runs/{run_id}.json"]
    if tls:
        lg += ["--tls", "--cafile", "/certs/cert.pem", "--server-hostname", "mllp"]
    out = sh(*lg).stdout.strip().splitlines()[-1]
    res = json.loads(out)
    srv_logs = sh("docker", "logs", "tb-s2-srv").stdout
    sh("docker", "rm", "-f", "tb-s2-srv", check=False)
    v = sh(sys.executable, str(HERE / "verify_loss.py"), "--bootstrap", HOST_BOOT, "--topic",
           topic, str(HERE / "runs" / f"{run_id}.json"), check=False)
    res["verify"] = json.loads(v.stdout.strip().splitlines()[-1]) if v.stdout.strip() else v.stderr[-300:]
    res["run_id"], res["linger_ms"] = run_id, linger
    res["server_log_lines"] = len(srv_logs.splitlines())
    return res


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--rates", default="1000")
    p.add_argument("--conns", default="10")
    p.add_argument("--tls", default="off", choices=["off", "on", "both"])
    p.add_argument("--seconds", type=int, default=30)
    p.add_argument("--linger", type=int, default=2)
    p.add_argument("--out", default="runs/matrix.jsonl")
    a = p.parse_args()
    modes = {"off": [False], "on": [True], "both": [False, True]}[a.tls]
    for tls in modes:
        for c in map(int, a.conns.split(",")):
            for r in map(int, a.rates.split(",")):
                res = cell(r, c, tls, a.seconds, a.linger)
                print(json.dumps({k: v for k, v in res.items()}), flush=True)
                with open(HERE / a.out, "a") as f:
                    f.write(json.dumps(res) + "\n")


if __name__ == "__main__":
    main()
