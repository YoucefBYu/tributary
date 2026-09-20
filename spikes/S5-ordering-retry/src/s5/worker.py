"""One ordered-loader worker, as its own OS process.

Why a separate process and not a thread: the fault we most need to inject is
``kill -9``. A thread cannot be killed without unwinding, and unwinding is
exactly what a real crash does not do -- it is the difference between "the
consumer committed on the way out" and "everything since the last commit is
replayed". So each worker is a process, the runner kills it with
``Popen.kill()`` (``TerminateProcess`` on Windows, ``SIGKILL`` elsewhere: no
atexit, no ``consumer.close()``, no LeaveGroup), and the group has to notice
through the session timeout like it would in production.

This is also the multi-process worker model from ADR-001 7.2 in miniature: N
independent processes in one consumer group, no shared state, each owning whole
partitions.

Everything the runner needs back is on disk, because a killed process cannot
report anything:

* ``--log`` receives one JSONL line per accepted delivery, fsynced (`s5.sink`);
* ``--stats`` is written only on a *clean* exit, so its absence is itself the
  signal that this worker was killed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import threading
from pathlib import Path

import orjson

from .backoff import Backoff, Jitter
from .blocking_loader import BlockingLoader, LoaderConfig, OnExhaust
from .sink import FailurePlan, FileSink


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="s5.worker")
    p.add_argument("--topic", required=True)
    p.add_argument("--dlq-topic", required=True)
    p.add_argument("--group", required=True)
    p.add_argument("--instance-id", default=None, help="group.instance.id (static membership)")
    p.add_argument("--events-topic", default=None)
    p.add_argument("--log", required=True, type=Path, help="delivery log (JSONL)")
    p.add_argument("--stats", default=None, type=Path, help="written on clean exit only")
    p.add_argument("--destination", default="fhir")
    # failure injection in the *sink*, i.e. the downstream system
    p.add_argument("--transient-rate", type=float, default=0.0)
    p.add_argument(
        "--poison-file",
        default=None,
        type=Path,
        help='JSON list of [key, seq] pairs the sink rejects permanently',
    )
    p.add_argument(
        "--outage",
        action="append",
        default=[],
        metavar="START:END",
        help="seconds since worker start where every send fails (repeatable)",
    )
    # retry policy
    p.add_argument("--max-attempts", type=int, default=8)
    p.add_argument("--on-exhaust", choices=[e.value for e in OnExhaust], default=OnExhaust.DLQ_AND_CONTINUE.value)
    p.add_argument("--backoff-base", type=float, default=0.05)
    p.add_argument("--backoff-cap", type=float, default=1.0)
    p.add_argument("--jitter", choices=[j.value for j in Jitter], default=Jitter.FULL.value)
    p.add_argument("--commit-every", type=int, default=1)
    p.add_argument("--commit-interval-s", type=float, default=1.0)
    # lifecycle
    p.add_argument("--idle-exit-s", type=float, default=10.0)
    p.add_argument(
        "--max-runtime-s",
        type=float,
        default=180.0,
        help="hard stop; the run is a measurement, not a service",
    )
    p.add_argument("--session-timeout-ms", type=int, default=None)
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--fsync", action="store_true", default=True)
    p.add_argument("--no-fsync", dest="fsync", action="store_false")
    return p.parse_args(argv)


def _poison(path: Path | None) -> frozenset[tuple[str, int]]:
    if path is None:
        return frozenset()
    pairs = json.loads(path.read_text(encoding="utf-8"))
    return frozenset((str(k), int(s)) for k, s in pairs)


def _outages(specs: list[str]) -> tuple[tuple[float, float], ...]:
    out: list[tuple[float, float]] = []
    for spec in specs:
        start, _, end = spec.partition(":")
        out.append((float(start), float(end)))
    return tuple(out)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        stream=sys.stderr,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    instance = args.instance_id or f"w{os.getpid()}"

    plan = FailurePlan(
        poison=_poison(args.poison_file),
        outages=_outages(args.outage),
        transient_rate=args.transient_rate,
        seed=args.seed,
    )
    sink = FileSink(path=args.log, plan=plan, fsync=args.fsync)
    cfg = LoaderConfig(
        topic=args.topic,
        dlq_topic=args.dlq_topic,
        group=args.group,
        instance_id=args.instance_id,
        destination=args.destination,
        max_attempts=args.max_attempts,
        on_exhaust=OnExhaust(args.on_exhaust),
        backoff=Backoff(base=args.backoff_base, cap=args.backoff_cap, jitter=Jitter(args.jitter)),
        commit_every=args.commit_every,
        commit_interval_s=args.commit_interval_s,
        idle_exit_s=args.idle_exit_s,
        seed=args.seed,
    )
    overrides: dict[str, object] = {}
    if args.session_timeout_ms is not None:
        overrides["session.timeout.ms"] = args.session_timeout_ms

    loader = BlockingLoader(
        cfg,
        sink,
        events_topic=args.events_topic,
        worker=instance,
        consumer_overrides=overrides,
    )

    stop = threading.Event()
    timer = threading.Timer(args.max_runtime_s, stop.set)
    timer.daemon = True
    timer.start()
    try:
        stats = loader.run(stop)
    finally:
        timer.cancel()

    if args.stats is not None:
        payload = {
            "instance": instance,
            "pid": os.getpid(),
            "stats": stats.as_dict(),
            "sink": {
                "accepted": sink.accepted,
                "redelivered": sink.redelivered,
                "rejected": sink.rejected,
            },
        }
        args.stats.write_bytes(orjson.dumps(payload))
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry point
    raise SystemExit(main())
