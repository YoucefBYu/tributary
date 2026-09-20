"""S4: topic-count stress on the shared 3-broker KRaft cluster. Throwaway spike code.

Run from repo root (needs confluent-kafka via the spikes/common project):
  uv run --project spikes/common python spikes/S4-topic-scale/s4.py topics --tiers 100,250,500,750,1000
  uv run --project spikes/common python spikes/S4-topic-scale/s4.py groups --channels 500 --consumers 100
  uv run --project spikes/common python spikes/S4-topic-scale/s4.py regex-group --channels 500
  uv run --project spikes/common python spikes/S4-topic-scale/s4.py shared --channels 1000 --messages 100000
  uv run --project spikes/common python spikes/S4-topic-scale/s4.py cleanup

Topics are `s4.c<NNNN>.{raw,out,retry,dlq}`, 12 partitions, RF=3, min.insync=2.
Results are appended as JSON lines to benchmarks/results/s4.jsonl. Every record carries
the broker heap that TB_KAFKA_HEAP had when the compose file was started (pass --heap
so it is written to the record; the script cannot see it otherwise).

Do NOT run while another spike is timing on the cluster.
"""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer

from tbcommon import admin as tbadmin
from tbcommon import clients, config

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "benchmarks/results/s4.jsonl"
PREFIX = "s4"
SUFFIXES = ("raw", "out", "retry", "dlq")
PARTITIONS = 12
BROKERS = ("tb-kafka1", "tb-kafka2", "tb-kafka3")


def topic_names(channel: int) -> list[str]:
    return [f"{PREFIX}.c{channel:04d}.{s}" for s in SUFFIXES]


def record(kind: str, heap: str, **data: Any) -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    rec = {"kind": kind, "heap": heap, "ts": time.time(), **data}
    with OUT.open("a") as f:
        f.write(json.dumps(rec, default=str) + "\n")
    print(json.dumps(rec, default=str), flush=True)


def docker_stats() -> dict[str, Any]:
    """Container memory and CPU per broker, plus JVM RSS and open fds when available."""
    out: dict[str, Any] = {}
    try:
        raw = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}", *BROKERS],
            capture_output=True, text=True, timeout=60, check=True,
        ).stdout
        for line in raw.splitlines():
            d = json.loads(line)
            out[d["Name"]] = {"mem": d["MemUsage"], "mem_pct": d["MemPerc"], "cpu": d["CPUPerc"]}
    except Exception as exc:  # noqa: BLE001 - measurement aid only
        out["error"] = repr(exc)
    for b in BROKERS:
        try:
            r = subprocess.run(
                ["docker", "exec", b, "sh", "-c",
                 "p=$(pgrep -f kafka.Kafka | head -1); echo fds=$(ls /proc/$p/fd | wc -l); grep -E 'VmRSS|Threads' /proc/$p/status"],
                capture_output=True, text=True, timeout=30, env={**__import__("os").environ, "MSYS_NO_PATHCONV": "1"},
            )
            out.setdefault(b, {})["proc"] = " ".join(r.stdout.split())
        except Exception as exc:  # noqa: BLE001
            out.setdefault(b, {})["proc_error"] = repr(exc)
    return out


def metadata_latency(samples: int = 5) -> dict[str, float]:
    """Full list_topics latency (what every client pays on startup) and topic/partition counts."""
    lat: list[float] = []
    md = None
    for _ in range(samples):
        t0 = time.perf_counter()
        md = tbadmin.admin().list_topics(timeout=120.0)
        lat.append(time.perf_counter() - t0)
    assert md is not None
    parts = sum(len(t.partitions) for t in md.topics.values())
    return {"list_topics_median_s": statistics.median(lat), "list_topics_max_s": max(lat),
            "topics": len(md.topics), "partitions": parts}


def probe_produce(heap: str, tag: str, n: int = 300) -> dict[str, float]:
    """ACK latency probe on a small dedicated topic while the cluster carries the stress load."""
    name = f"{PREFIX}.probe"
    tbadmin.ensure_topics([tbadmin.TopicSpec(name, partitions=3)])
    p = clients.make_producer()
    lat: list[float] = []

    def cb(err: Any, msg: Any, t0: float) -> None:
        if err is None:
            lat.append((time.perf_counter() - t0) * 1000)

    for i in range(n):
        t0 = time.perf_counter()
        p.produce(name, value=b"synthetic-%d" % i, key=b"k%d" % (i % 10), on_delivery=lambda e, m, t0=t0: cb(e, m, t0))
        p.poll(0)
        time.sleep(0.005)  # about 150-200 msg/s, well below capacity
    p.flush(60)
    lat.sort()
    if not lat:
        return {"acks": 0}
    return {"acks": len(lat), "ack_p50_ms": lat[len(lat) // 2], "ack_p99_ms": lat[int(len(lat) * 0.99) - 1]}


def cmd_topics(a: argparse.Namespace) -> None:
    """Create channels cumulatively up to each tier; measure each batch and the state at each tier."""
    tbadmin.wait_for_cluster()
    record("baseline", a.heap, **metadata_latency(), stats=docker_stats())
    created = 0
    for tier in [int(x) for x in a.tiers.split(",")]:
        t_tier = time.perf_counter()
        batch_times: list[dict[str, float]] = []
        failed: str | None = None
        while created < tier:
            upto = min(tier, created + a.batch)
            specs = [tbadmin.TopicSpec(n, partitions=PARTITIONS) for c in range(created, upto) for n in topic_names(c)]
            t0 = time.perf_counter()
            try:
                tbadmin.ensure_topics(specs, timeout=a.timeout)
            except Exception as exc:  # noqa: BLE001 - the degradation point is the result
                failed = f"channels {created}-{upto}: {exc!r}"
                batch_times.append({"channels": upto - created, "seconds": time.perf_counter() - t0, "failed": 1})
                print("FAILED", failed, flush=True)
                break
            batch_times.append({"channels": upto - created, "seconds": time.perf_counter() - t0})
            created = upto
        record("tier", a.heap, tier=tier, channels_created=created, topics=created * 4, partitions=created * 4 * PARTITIONS,
               create_seconds=time.perf_counter() - t_tier, batches=batch_times, failed=failed,
               metadata=metadata_latency(), stats=docker_stats(), probe=probe_produce(a.heap, f"tier{tier}"))
        if failed:
            break


def _pump(consumers: list[Consumer], stop: threading.Event) -> None:
    while not stop.is_set():
        for c in consumers:
            c.poll(0.05)


def cmd_groups(a: argparse.Namespace) -> None:
    """One consumer group per channel (the ADR model): start `consumers` groups, time until all hold a full assignment."""
    n = a.consumers
    assigned: dict[int, int] = {}
    consumers: list[Consumer] = []
    t0 = time.perf_counter()

    def make(i: int) -> Consumer:
        def on_assign(c: Consumer, parts: list[Any]) -> None:
            c.incremental_assign(parts)
            assigned[i] = assigned.get(i, 0) + len(parts)

        c = Consumer(clients.consumer_config(f"s4-g{i:04d}", **{"group.instance.id": f"s4-g{i:04d}-0"}))
        c.subscribe([f"{PREFIX}.c{i:04d}.raw"], on_assign=on_assign)
        return c

    stop = threading.Event()
    threads: list[threading.Thread] = []
    for i in range(n):
        consumers.append(make(i))
    shards = [consumers[k::a.threads] for k in range(a.threads)]
    for s in shards:
        t = threading.Thread(target=_pump, args=(s, stop), daemon=True)
        t.start()
        threads.append(t)
    times: dict[int, float] = {}
    deadline = time.perf_counter() + a.timeout
    while time.perf_counter() < deadline and len(times) < n:
        for i, k in list(assigned.items()):
            if k >= PARTITIONS and i not in times:
                times[i] = time.perf_counter() - t0
        time.sleep(0.2)
    ts = sorted(times.values())
    res = {"groups": n, "ready": len(ts), "all_ready_s": ts[-1] if len(ts) == n else None,
           "p50_s": ts[len(ts) // 2] if ts else None, "p99_s": ts[int(len(ts) * 0.99) - 1] if ts else None}
    # steady-state cost: memory and CPU with all consumers idle-polling
    time.sleep(15)
    record("groups", a.heap, channels=a.channels, **res, stats=docker_stats())
    stop.set()
    for c in consumers:
        c.close()


def cmd_regex_group(a: argparse.Namespace) -> None:
    """One group subscribed by pattern to all raw topics; time the initial join and rebalances as members come and go."""
    total = a.channels * PARTITIONS
    state: dict[str, Any] = {}
    lock = threading.Lock()
    members: list[Consumer] = []
    held: dict[int, int] = {}

    def spawn(i: int) -> Consumer:
        def on_assign(c: Consumer, parts: list[Any]) -> None:
            c.incremental_assign(parts)
            with lock:
                held[i] = held.get(i, 0) + len(parts)

        def on_revoke(c: Consumer, parts: list[Any]) -> None:
            c.incremental_unassign(parts)
            with lock:
                held[i] = held.get(i, 0) - len(parts)

        c = Consumer(clients.consumer_config("s4-regex", **{"group.instance.id": f"s4-regex-{i}"}))
        c.subscribe([f"^{PREFIX}\\.c\\d+\\.raw$"], on_assign=on_assign, on_revoke=on_revoke)
        return c

    stop = threading.Event()

    def pump() -> None:
        while not stop.is_set():
            for c in list(members):
                c.poll(0.05)

    threading.Thread(target=pump, daemon=True).start()

    def wait_stable(label: str) -> float:
        t0 = time.perf_counter()
        while time.perf_counter() - t0 < a.timeout:
            with lock:
                ok = sum(held.get(i, 0) for i in range(len(members))) == total and all(held.get(i, 0) > 0 for i in range(len(members)))
            if ok:
                time.sleep(2)  # confirm stable
                with lock:
                    if sum(held.get(i, 0) for i in range(len(members))) == total:
                        return time.perf_counter() - t0
            time.sleep(0.25)
        state[label + "_timeout"] = True
        return float("nan")

    members.append(spawn(0))
    state["join_1_member_s"] = wait_stable("join1") - 2
    for k in (1, 2):
        members.append(spawn(k))
        state[f"add_member_{k + 1}_s"] = wait_stable(f"add{k + 1}") - 2
    victim = members.pop()
    with lock:
        held.pop(len(members), None)
    victim.close()
    state["remove_member_s"] = wait_stable("remove") - 2
    record("regex-group", a.heap, channels=a.channels, partitions=total, **state, stats=docker_stats())
    stop.set()
    for c in members:
        c.close()


def cmd_shared(a: argparse.Namespace) -> None:
    """Topic-sharing fallback: 4 shared topics (raw/out/retry/dlq), channel id in key and header tb-channel.

    Measures produce throughput, consumer dispatch-by-header throughput, per-key ordering, and partition skew.
    """
    specs = [tbadmin.TopicSpec(f"{PREFIX}.shared.{s}", partitions=PARTITIONS * 4) for s in SUFFIXES]
    tbadmin.ensure_topics(specs)
    topic = f"{PREFIX}.shared.raw"
    p = Producer({**clients.producer_config(), "linger.ms": 5, "batch.num.messages": 1000})
    seq: dict[int, int] = {}
    t0 = time.perf_counter()
    for i in range(a.messages):
        ch = i % a.channels
        seq[ch] = seq.get(ch, 0) + 1
        p.produce(topic, value=b"synthetic-payload-%d" % i, key=b"c%04d" % ch,
                  headers=[("tb-channel", b"c%04d" % ch), ("tb-seq", str(seq[ch]).encode())])
        if i % 1000 == 0:
            p.poll(0)
    p.flush(120)
    prod_s = time.perf_counter() - t0
    c = Consumer(clients.consumer_config(f"s4-shared-{int(time.time())}"))
    c.subscribe([topic])
    got = 0
    last: dict[str, int] = {}
    order_violations = 0
    per_part: dict[int, int] = {}
    t1 = time.perf_counter()
    idle = 0
    while got < a.messages and idle < 30:
        m = c.poll(1.0)
        if m is None:
            idle += 1
            continue
        if m.error():
            raise KafkaException(m.error())
        idle = 0
        h = dict(m.headers() or [])
        ch = h["tb-channel"].decode()
        s = int(h["tb-seq"])
        if s <= last.get(ch, 0):
            order_violations += 1
        last[ch] = s
        per_part[m.partition()] = per_part.get(m.partition(), 0) + 1
        got += 1
    cons_s = time.perf_counter() - t1
    c.close()
    counts = sorted(per_part.values())
    record("shared", a.heap, channels=a.channels, messages=a.messages, consumed=got, produce_msgs_per_s=a.messages / prod_s,
           consume_msgs_per_s=got / cons_s, order_violations=order_violations, partitions_used=len(per_part),
           partition_min=counts[0] if counts else 0, partition_max=counts[-1] if counts else 0, stats=docker_stats())


def cmd_cleanup(a: argparse.Namespace) -> None:
    md = tbadmin.admin().list_topics(timeout=120.0)
    names = [n for n in md.topics if n.startswith(PREFIX + ".")]
    t0 = time.perf_counter()
    for i in range(0, len(names), 100):
        tbadmin.delete_topics(names[i:i + 100], timeout=a.timeout)
    record("cleanup", a.heap, deleted=len(names), seconds=time.perf_counter() - t0, stats=docker_stats())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--heap", default="unknown", help="broker heap the cluster was started with, e.g. '-Xmx3G'")
    ap.add_argument("--timeout", type=float, default=300)
    sub = ap.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("topics")
    s.add_argument("--tiers", default="100,250,500,750,1000")
    s.add_argument("--batch", type=int, default=25, help="channels per create_topics call")
    s.set_defaults(fn=cmd_topics)
    s = sub.add_parser("groups")
    s.add_argument("--channels", type=int, default=500)
    s.add_argument("--consumers", type=int, default=100)
    s.add_argument("--threads", type=int, default=8)
    s.set_defaults(fn=cmd_groups)
    s = sub.add_parser("regex-group")
    s.add_argument("--channels", type=int, default=500)
    s.set_defaults(fn=cmd_regex_group)
    s = sub.add_parser("shared")
    s.add_argument("--channels", type=int, default=1000)
    s.add_argument("--messages", type=int, default=100000)
    s.set_defaults(fn=cmd_shared)
    s = sub.add_parser("cleanup")
    s.set_defaults(fn=cmd_cleanup)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    sys.exit(main())
