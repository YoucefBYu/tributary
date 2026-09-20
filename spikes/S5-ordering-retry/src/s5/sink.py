"""A fake downstream system with injectable failures.

The "external system" is an append-only JSONL file, fsynced per accepted
record. That matters: the whole point of S5 is to prove the offset commit
happens strictly *after* the downstream write is durable, and a buffered file
would let a `kill -9` erase evidence that the write happened.

Only ids and sequence numbers are written. No payload bytes ever reach disk
here (invariant 6).
"""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

import orjson


class SinkRejected(Exception):
    """Downstream refused the message. Retryable unless ``permanent``."""

    def __init__(self, reason: str, *, permanent: bool = False) -> None:
        super().__init__(reason)
        self.reason = reason
        self.permanent = permanent


@dataclass(frozen=True, slots=True)
class FailurePlan:
    """Deterministic-by-seed failure injection."""

    #: (key, seq) pairs that always fail permanently -> must reach the DLQ.
    poison: frozenset[tuple[str, int]] = frozenset()
    #: Wall-clock windows, seconds since plan start, where every send fails.
    outages: tuple[tuple[float, float], ...] = ()
    #: Independent probability that any given send fails transiently.
    transient_rate: float = 0.0
    seed: int = 1

    def rng(self) -> random.Random:
        return random.Random(self.seed)


@dataclass
class FileSink:
    """Idempotent-by-``tb-msg-id`` sink writing to an append-only log."""

    path: Path
    plan: FailurePlan = field(default_factory=FailurePlan)
    fsync: bool = True
    started_at: float = field(default_factory=time.monotonic)
    _rng: random.Random = field(init=False)
    _seen: set[str] = field(default_factory=set, init=False)
    accepted: int = field(default=0, init=False)
    redelivered: int = field(default=0, init=False)
    rejected: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        self._rng = self.plan.rng()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def _should_fail(self, key: str, seq: int) -> SinkRejected | None:
        if (key, seq) in self.plan.poison:
            return SinkRejected("poison-message", permanent=True)
        elapsed = time.monotonic() - self.started_at
        for start, end in self.plan.outages:
            if start <= elapsed < end:
                return SinkRejected(f"destination-outage[{start}:{end}]")
        if self.plan.transient_rate and self._rng.random() < self.plan.transient_rate:
            return SinkRejected("transient-5xx")
        return None

    def deliver(
        self,
        *,
        msg_id: str,
        key: str,
        seq: int,
        attempt: int,
        worker: str,
        topic: str = "",
        partition: int = -1,
        offset: int = -1,
    ) -> bool:
        """Write downstream. Returns True if this was a first delivery.

        Raises :class:`SinkRejected` on failure. Never partially writes.

        ``topic``/``partition``/``offset`` are the *source* coordinates. They
        are recorded so the analysis can check the thing S5 exists to check:
        that no offset was committed before this line hit the disk.
        """
        failure = self._should_fail(key, seq)
        if failure is not None:
            self.rejected += 1
            raise failure

        first = msg_id not in self._seen
        if not first:
            # Idempotency by tb-msg-id (invariant 4). Still logged, so the
            # report can state a real duplicate-delivery rate.
            self.redelivered += 1
        self._seen.add(msg_id)
        self.accepted += 1

        record = {
            "ts": time.time(),
            "msg_id": msg_id,
            "key": key,
            "seq": seq,
            "attempt": attempt,
            "worker": worker,
            "first": first,
            "topic": topic,
            "partition": partition,
            "offset": offset,
        }
        self._fh.write(orjson.dumps(record).decode() + "\n")
        self._fh.flush()
        if self.fsync:
            os.fsync(self._fh.fileno())
        return first

    def close(self) -> None:
        try:
            self._fh.flush()
            if self.fsync:
                os.fsync(self._fh.fileno())
        finally:
            self._fh.close()


def read_delivery_log(path: Path) -> list[dict[str, object]]:
    """Read every worker's delivery log, ordered by write time.

    A partial final line is possible if a worker was killed mid-write; it is
    dropped, which is the conservative choice (it counts as a non-delivery).
    """
    records: list[dict[str, object]] = []
    for file in sorted(path.parent.glob(path.name)) if "*" in path.name else [path]:
        if not file.exists():
            continue
        with file.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(orjson.loads(line))
                except orjson.JSONDecodeError:
                    continue
    records.sort(key=lambda r: float(r["ts"]))  # type: ignore[arg-type]
    return records
