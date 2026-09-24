"""A discrete-event model of the three retry-delay mechanisms.

The cluster cannot wait out production tiers: a 15-minute tier needs a run of an hour or more per
data point. The model can, and it isolates the one quantity that separates the mechanisms -
**lateness**, the time between `tb-retry-not-before` and the attempt that honours it - from
everything else (broker latency, poll scheduling, fetch waits), which the cluster run measures
separately. `runner.py` feeds the model the exact first-attempt trace of a cluster run and compares.

What the model deliberately leaves out: a retry consumer's own service time (zero here), fetch
latency after a resume, and crashes. The first two are what the cluster measures; crashes are what
the kill scenario measures. The model does compute **replay exposure** for the `buffer` mechanism:
the number of already-processed retry records sitting above the commit watermark at a random
instant, which is how many would be replayed if the loader died then.
"""

from __future__ import annotations

import heapq
import itertools
import random
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Literal

from .plan import FailurePlan, Tiers

Mechanism = Literal["pause", "buffer", "tiers"]
MECHANISMS: tuple[Mechanism, ...] = ("pause", "buffer", "tiers")


@dataclass(frozen=True, slots=True)
class Arrival:
    """A record reaching `out.<dest>`: its first delivery attempt happens at `t`."""

    t: float
    msg_id: str
    key: int
    seq: int


@dataclass(frozen=True, slots=True)
class Attempt:
    msg_id: str
    key: int
    seq: int
    number: int  # 1 = first delivery from out, 2 = first retry, ...
    at: float
    not_before: float
    ok: bool

    @property
    def lateness(self) -> float:
        return self.at - self.not_before


@dataclass(slots=True)
class _Entry:
    msg_id: str
    key: int
    seq: int
    retry_number: int
    not_before: float
    processed: bool = False


@dataclass(slots=True)
class _Log:
    """One retry partition: an append-only log plus its consumer's position."""

    entries: list[_Entry] = field(default_factory=list)
    read: int = 0  # next offset the consumer has not yet read
    waiting: bool = False  # pause/tiers: a consider event is scheduled
    buffered: int = 0  # buffer: read but not yet processed
    low: int = 0  # the commit watermark: every offset below it is processed
    processed: int = 0  # processed entries in total, so exposure is processed - low


@dataclass(slots=True)
class ModelResult:
    mechanism: Mechanism
    attempts: list[Attempt]
    dlq: list[str]
    max_buffered: int
    exposure_samples: list[int]

    def retries(self) -> list[Attempt]:
        return [attempt for attempt in self.attempts if attempt.number > 1]

    def lateness_by_retry(self) -> dict[int, list[float]]:
        grouped: dict[int, list[float]] = defaultdict(list)
        for attempt in self.retries():
            grouped[attempt.number - 1].append(attempt.lateness)
        return dict(grouped)

    def succeeded(self) -> dict[str, Attempt]:
        return {attempt.msg_id: attempt for attempt in self.attempts if attempt.ok}

    def inverted(self) -> int:
        """Messages first delivered after a higher-sequence message of the same key."""
        return count_inversions((a.key, a.seq, a.at) for a in self.attempts if a.ok)


def count_inversions(successes: Iterable[tuple[int, int, float]]) -> int:
    """(key, seq, delivered_at) -> how many were delivered after a later message of their key."""
    by_key: dict[int, list[tuple[float, int]]] = defaultdict(list)
    for key, seq, at in successes:
        by_key[key].append((at, seq))
    inverted = 0
    for deliveries in by_key.values():
        highest = -1
        for _at, seq in sorted(deliveries):
            if seq < highest:
                inverted += 1
            highest = max(highest, seq)
    return inverted


def simulate(
    arrivals: Iterable[Arrival],
    plan: FailurePlan,
    tiers: Tiers,
    mechanism: Mechanism,
    *,
    partitions: int = 3,
    max_attempts: int = 5,
    buffer_limit: int | None = None,
    sample_every: float = 1.0,
) -> ModelResult:
    """Run one mechanism over a trace. Deterministic: same inputs, same result."""
    if mechanism not in MECHANISMS:
        raise ValueError(f"unknown mechanism {mechanism}")
    limit = buffer_limit if buffer_limit is not None else 1 << 62
    topics = len(tiers.delays) if mechanism == "tiers" else 1
    logs = {(topic, part): _Log() for topic in range(topics) for part in range(partitions)}
    order = itertools.count()
    events: list[tuple[float, int, str, object]] = []
    attempts: list[Attempt] = []
    dlq: list[str] = []
    max_buffered = 0
    exposure: list[int] = []

    def push(at: float, kind: str, payload: object) -> None:
        heapq.heappush(events, (at, next(order), kind, payload))

    for arrival in arrivals:
        push(arrival.t, "first", arrival)

    def process(now: float, entry: _Entry | None, arrival: Arrival | None) -> None:
        if entry is not None:
            msg_id, key, seq, number, not_before = (
                entry.msg_id,
                entry.key,
                entry.seq,
                entry.retry_number + 1,
                entry.not_before,
            )
        else:
            assert arrival is not None
            msg_id, key, seq, number, not_before = arrival.msg_id, arrival.key, arrival.seq, 1, arrival.t
        failed = plan.fails(msg_id, number, now)
        attempts.append(Attempt(msg_id, key, seq, number, now, not_before, not failed))
        if not failed:
            return
        if number >= max_attempts:
            dlq.append(msg_id)
            return
        retry_number = number  # the retry this failure schedules
        topic = tiers.tier(retry_number) if mechanism == "tiers" else 0
        target = logs[(topic, key % partitions)]
        target.entries.append(_Entry(msg_id, key, seq, retry_number, now + tiers.delay(retry_number)))
        on_append(now, (topic, key % partitions), target)

    def on_append(now: float, where: tuple[int, int], log: _Log) -> None:
        if mechanism == "buffer":
            fill_buffer(now, where, log)
        elif not log.waiting:
            log.waiting = True
            push(now, "consider", where)

    def fill_buffer(now: float, where: tuple[int, int], log: _Log) -> None:
        nonlocal max_buffered
        while log.read < len(log.entries) and log.buffered < limit:
            entry = log.entries[log.read]
            log.read += 1
            log.buffered += 1
            push(max(now, entry.not_before), "due", (where, entry))
        max_buffered = max(max_buffered, sum(item.buffered for item in logs.values()))

    next_sample = events[0][0] if events else 0.0  # traces may use epoch time
    while events:
        now, _, kind, payload = heapq.heappop(events)
        while now >= next_sample:
            exposure.append(_exposure(logs) if mechanism == "buffer" else 0)
            next_sample += sample_every
        if kind == "first":
            process(now, None, payload)  # type: ignore[arg-type]
        elif kind == "consider":
            where = payload
            log = logs[where]  # type: ignore[index]
            log.waiting = False
            if log.read >= len(log.entries):
                continue
            head = log.entries[log.read]
            if head.not_before > now:
                log.waiting = True
                push(head.not_before, "consider", where)
                continue
            log.read += 1
            head.processed = True
            log.processed += 1
            process(now, head, None)
            log.waiting = True
            push(now, "consider", where)
        else:  # due
            where, entry = payload  # type: ignore[misc]
            log = logs[where]
            log.buffered -= 1
            entry.processed = True
            log.processed += 1
            process(now, entry, None)
            fill_buffer(now, where, log)

    return ModelResult(mechanism, attempts, dlq, max_buffered, exposure)


def _exposure(logs: dict[tuple[int, int], _Log]) -> int:
    """Processed records above each partition's watermark: the replay cost of dying right now."""
    total = 0
    for log in logs.values():
        while log.low < log.read and log.entries[log.low].processed:
            log.low += 1
        total += log.processed - log.low
    return total


def steady_arrivals(*, rate: float, duration: float, keys: int, seed: int = 1) -> list[Arrival]:
    """`rate` records per second for `duration` seconds over `keys` keys, per-key sequence numbers."""
    rng = random.Random(seed)
    per_key: dict[int, int] = defaultdict(int)
    arrivals: list[Arrival] = []
    count = int(rate * duration)
    for index in range(count):
        key = rng.randrange(keys)
        arrivals.append(Arrival(index / rate, f"m{seed}-{index}", key, per_key[key]))
        per_key[key] += 1
    return arrivals


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return float("nan")
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, round(fraction * (len(ordered) - 1))))
    return ordered[index]
