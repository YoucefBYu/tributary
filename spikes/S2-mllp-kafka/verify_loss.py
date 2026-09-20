"""S2 loss check (throwaway): every control id ACKed (AA) in run.json must exist in the
topic. Exit code 1 on any acknowledged-message loss. Prints ids only (no content).

Usage: python verify_loss.py --bootstrap localhost:9092 --topic spike.s2.raw run.json [...]
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from confluent_kafka import Consumer, TopicPartition


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="spike.s2.raw")
    p.add_argument("--id-from", choices=["key", "msh10"], default="key",
                   help="msh10: control id parsed from the message value (S3 keys records by corr id)")
    p.add_argument("runs", nargs="+")
    a = p.parse_args()
    acked: set[str] = set()
    for f in a.runs:
        acked |= set(json.load(open(f))["acked_ids"])
    c = Consumer({"bootstrap.servers": a.bootstrap, "group.id": f"verify-{time.time_ns()}",
                  "enable.auto.commit": False, "auto.offset.reset": "earliest",
                  "isolation.level": "read_committed"})
    md = c.list_topics(a.topic, timeout=10).topics[a.topic]
    tps, ends = [], {}
    for pid in md.partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(a.topic, pid), timeout=10)
        ends[pid] = hi
        tps.append(TopicPartition(a.topic, pid, lo))
    c.assign(tps)
    seen: set[str] = set()
    pos = {pid: -1 for pid in ends}
    idle_until = time.time() + 30
    while any(pos[p_] < ends[p_] - 1 for p_ in ends if ends[p_] > 0) and time.time() < idle_until:
        m = c.poll(1.0)
        if m is None or m.error():
            continue
        pos[m.partition()] = m.offset()
        if a.id_from == "key":
            seen.add(m.key().decode())
        else:
            seen.add(m.value().split(bytes([13]), 1)[0].split(b"|")[9].decode())
    lost = acked - seen
    print(json.dumps({"acked": len(acked), "in_topic": len(seen & acked),
                      "duplicates_or_unacked_extra": len(seen - acked), "lost": len(lost),
                      "lost_sample": sorted(lost)[:10]}))
    return 1 if lost else 0


if __name__ == "__main__":
    sys.exit(main())
