"""The transactional Kafka-to-Kafka stage as its own OS process.

Same reason as `s5.worker`: the interesting failure is a kill in the middle of
an open transaction. In-process that is unreachable -- ``finally`` would abort
it cleanly. Killed, the transaction is left open on the broker, and the next
process with the same ``transactional.id`` has to fence the zombie and abort it
during ``init_transactions``. That is the only way to test that the output topic
still contains each message exactly once.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import threading

import orjson

from .txn_stage import TransactionalStage, TxnConfig


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="s5.txn_worker")
    p.add_argument("--in-topic", required=True)
    p.add_argument("--out-topic", required=True)
    p.add_argument("--group", required=True)
    p.add_argument("--instance-id", default=None)
    p.add_argument(
        "--transactional-id",
        required=True,
        help="ONE per worker, stable across restarts (KIP-447 fences by group metadata)",
    )
    p.add_argument("--events-topic", default=None)
    p.add_argument("--batch-size", type=int, default=50)
    p.add_argument("--batch-max-wait-s", type=float, default=0.25)
    p.add_argument("--idle-exit-s", type=float, default=10.0)
    p.add_argument("--max-runtime-s", type=float, default=180.0)
    p.add_argument("--stats", default=None, help="written on clean exit only")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = TxnConfig(
        in_topic=args.in_topic,
        out_topic=args.out_topic,
        group=args.group,
        instance_id=args.instance_id,
        transactional_id=args.transactional_id,
        batch_size=args.batch_size,
        batch_max_wait_s=args.batch_max_wait_s,
        idle_exit_s=args.idle_exit_s,
    )
    stage = TransactionalStage(cfg, events_topic=args.events_topic)
    stop = threading.Event()
    timer = threading.Timer(args.max_runtime_s, stop.set)
    timer.daemon = True
    timer.start()
    try:
        stats = stage.run(stop)
    finally:
        timer.cancel()

    if args.stats:
        with open(args.stats, "wb") as fh:
            fh.write(orjson.dumps({"pid": os.getpid(), "stats": stats.as_dict()}))
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
