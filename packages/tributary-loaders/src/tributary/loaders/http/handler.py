"""`HttpRecordHandler`: the `tributary.runtime.consumer.RecordHandler` this loader gives the
runtime's `ConsumerLoop`.

`ConsumerLoop` owns everything W1-6's original `OutConsumer` stand-in used to: commit-after-delivery,
per-partition pause/seek/resume blocking retry with backoff and jitter, `onExhaust` (DLQ or halt).
This class is now the only loader-specific piece — one method, `handle`, that runs one `HttpLoader`
delivery attempt and translates the result into what the loop's `RecordHandler` protocol expects:
return normally for success, raise `DeliveryError(error_code, retryable=...)` for anything else. See
`consumer.py`'s (removed) module docstring in git history for the hand-rolled version this replaces,
and `http/__init__.py` for why: platform-engineer asked not to duplicate the runtime's retry/DLQ
machinery a second time now that it exists.

`HttpLoader.deliver` is a coroutine; `ConsumerLoop.run` is a plain synchronous loop with no event loop
of its own. `handle` drives one persistent event loop with `run_until_complete` per record rather than
spinning one up per call — a fresh `asyncio.run` per record would also mean a fresh `HttpLoader`
executor per record (`loader.py`'s dedicated bounded thread pool), defeating the point of it being a
bound at all. `ConsumerLoop` calls `handle` from exactly one thread, one record at a time (confirmed
with platform-engineer), so the one event loop and the one `HttpLoader` are never touched
concurrently — **this class is not thread-safe, and must not become one by adding a thread pool
around `handle`**; key-level parallelism within one partition is a different, unbuilt design
(ADR-001 7.2), not a thing to approximate here.

`spec_source.current()` is called once per record (the same rule `PipelineHandler` follows) to pick
up a republished channel spec without a restart: only `HttpLoader.config` (URL, method, headers,
auth, idempotency) is refreshed this way. `retry` (attempts, backoff, `onExhaust`) is fixed for the
life of the `ConsumerLoop`, because `ConsumerLoop.__init__` takes one `RetryPolicy` at construction —
a destination that changes its retry policy needs a restart until the runtime hot-reloads that too.
"""

from __future__ import annotations

import asyncio

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.errors import DeliveryError
from tributary.runtime.records import ConsumedRecord

from .config import http_destination_config_from_destination
from .loader import HttpLoader, Reason

ERROR_CODES: dict[Reason, str] = {
    "circuit-open": "CIRCUIT_OPEN",
    "timeout": "TIMEOUT",
    "transport-error": "TRANSPORT_ERROR",
    "http-5xx": "HTTP_5XX",
    "http-429": "HTTP_429",
    "http-4xx": "HTTP_4XX",
}


class HttpRecordHandler:
    def __init__(self, http_loader: HttpLoader, *, spec_source: LiveSpecSource, destination: str) -> None:
        self._http_loader = http_loader
        self._spec_source = spec_source
        self._destination = destination
        self._loop = asyncio.new_event_loop()

    def handle(self, record: ConsumedRecord) -> None:
        spec = self._spec_source.current()
        destination = spec.destination(self._destination)
        self._http_loader.config = http_destination_config_from_destination(spec.name, destination)
        outcome = self._loop.run_until_complete(self._http_loader.deliver(record.message))
        if outcome.success:
            return
        raise DeliveryError(ERROR_CODES[outcome.reason], retryable=outcome.retryable)

    def metrics_snapshot(self) -> dict[str, int]:
        return self._http_loader.metrics.snapshot()

    def close(self) -> None:
        self._http_loader.close()
        self._spec_source.close()
        self._loop.close()
