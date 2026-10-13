"""Dedicated thread pools and admission gates for the archive work (W3-6).

The event loop's default executor also serves the audit producer (`KafkaAuditSender`) and the channels
publisher. Archive reads, reprocess, replays and stream polls block on Kafka or ClickHouse and, for the library
services, on the audit hook itself (a replay thread waits for the audit record the loop produces on the default
executor). Sharing one pool would let a burst of archive work starve the audit produce its own callers wait for.

So each kind of work has its own bounded pool, and an `Admission` gate in front of it: a pool queues without
limit, a gate does not. A request that finds its gate full is refused at once (503 `ARCHIVE_UNAVAILABLE`), before
any audit record is written for it. Content views have a pool and a gate of their own, so a flood of searches,
details or stats cannot make an operator's content view wait after its audit record exists.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Any

from .problems import ApiProblem


class Admission:
    """At most `capacity` units of work in flight (running or queued). Thread-safe: slots are given back from the
    worker threads."""

    def __init__(self, capacity: int) -> None:
        self._free = capacity
        self._lock = threading.Lock()

    def try_enter(self) -> Callable[[], None] | None:
        with self._lock:
            if self._free <= 0:
                return None
            self._free -= 1
        released = False

        def leave() -> None:
            nonlocal released
            with self._lock:
                if not released:
                    released = True
                    self._free += 1

        return leave

    def enter(self) -> Callable[[], None]:
        """Like `try_enter`, but a full gate is 503 `ARCHIVE_UNAVAILABLE`."""
        leave = self.try_enter()
        if leave is None:
            raise ApiProblem("ARCHIVE_UNAVAILABLE")
        return leave


def submit_gated[T](pool: Executor, leave: Callable[[], None], fn: Callable[..., T], *args: Any) -> asyncio.Future[T]:
    """Run `fn` on `pool`, giving the gate slot back when the JOB ends, whichever way it ends: finished, failed, or
    cancelled while still queued. The callback is on the job itself (`concurrent.futures.Future`), not in the
    worker function: a job cancelled before a thread picks it up never runs the function, so a `finally` inside
    it would never give the slot back. A job already running cannot be cancelled, so its slot stays taken until the
    thread returns, which is the point of the gate. `leave` is idempotent."""
    try:
        job = pool.submit(fn, *args)
    except BaseException:
        leave()
        raise
    job.add_done_callback(lambda _job: leave())
    return asyncio.wrap_future(job)


class Pools:
    def __init__(self, *, replays: int = 4) -> None:
        self.reads = ThreadPoolExecutor(8, "tb-archive-read")
        self.content = ThreadPoolExecutor(4, "tb-archive-content")  # content views only: never behind a flood of reads
        self.streams = ThreadPoolExecutor(8, "tb-stream-poll")
        self.replays = ThreadPoolExecutor(max(1, replays), "tb-replay")  # one thread per allowed concurrent replay
        self.reprocess = ThreadPoolExecutor(1, "tb-reprocess")  # reprocess is serialised in this process
        # In flight = running + queued: twice the workers, so a short burst queues and a flood is refused.
        self.read_gate = Admission(16)
        self.content_gate = Admission(8)
        self.stream_gate = Admission(16)

    def shutdown(self) -> None:
        for pool in (self.reads, self.content, self.streams, self.replays, self.reprocess):
            pool.shutdown(wait=False, cancel_futures=True)
