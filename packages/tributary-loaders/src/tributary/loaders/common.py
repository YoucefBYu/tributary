"""Shared building blocks for the wave-2 destination loaders (postgresql, clickhouse, s3, kafka, mllp,
fhir): a circuit breaker, a generic delivery-outcome/metrics shape, and a no-op logger.

`http/circuit_breaker.py` and `http/metrics.py` predate this module and implement the same ideas; they
are not migrated here to avoid touching working, reviewed wave-1 code for a refactor with no functional
need of its own (a future pass can consolidate both onto this module). Every loader added from W2-6
onward imports from here instead of writing a fourth or fifth copy — `tributary.loaders.secrets`'s own
docstring already flags this as worth sharing "if a third connector needs this"; a sixth destination
type is well past that point.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto

from tributary.sdk.context import Logger

__all__ = ["CircuitBreaker", "DeliveryOutcome", "LoaderMetrics", "NullLogger", "State"]


class State(Enum):
    CLOSED = auto()
    OPEN = auto()
    HALF_OPEN = auto()


class CircuitBreaker:
    """Closed -> Open after `failure_threshold` consecutive failures; Open rejects every call until
    `open_seconds` has elapsed, then allows one probe (Half-Open) whose outcome decides Closed or Open
    again. See `http/circuit_breaker.py` for the fuller rationale this is copied from; behaviour is
    identical on purpose so every destination type's breaker acts the same way."""

    def __init__(
        self, failure_threshold: int, open_seconds: float, *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        self._threshold = failure_threshold
        self._open_seconds = open_seconds
        self._clock = clock
        self._state = State.CLOSED
        self._consecutive_failures = 0
        self._opened_at: float | None = None

    @classmethod
    def disabled(cls) -> CircuitBreaker:
        return cls(failure_threshold=2**31, open_seconds=0.0)

    @property
    def state(self) -> State:
        if (
            self._state is State.OPEN
            and self._opened_at is not None
            and self._clock() - self._opened_at >= self._open_seconds
        ):
            return State.HALF_OPEN
        return self._state

    def allow(self) -> bool:
        return self.state is not State.OPEN

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._state = State.CLOSED
        self._opened_at = None

    def record_failure(self) -> None:
        if self.state is State.HALF_OPEN:
            self._state = State.OPEN
            self._opened_at = self._clock()
            return
        self._consecutive_failures += 1
        if self._consecutive_failures >= self._threshold:
            self._state = State.OPEN
            self._opened_at = self._clock()


@dataclass(frozen=True, slots=True)
class DeliveryOutcome:
    """One delivery attempt's result. `retryable` is stored explicitly rather than derived from
    `reason`, unlike `http.loader.DeliveryOutcome`: each destination type has its own failure
    vocabulary (HTTP status classes, Postgres error classes, S3 error codes, ...) and an explicit flag
    is one shape every one of them can share. `detail`, when set, is a short safe token (a status code,
    a Postgres SQLSTATE, an S3 error code) — never a response body, a query, an object key or anything
    that could carry PHI (invariant 6)."""

    success: bool
    reason: str
    retryable: bool = False
    detail: str | None = None


@dataclass
class LoaderMetrics:
    """In-process delivery-outcome counters. Same v0 scope as `http/metrics.py`: a plain snapshot, not
    a Prometheus/OpenTelemetry exporter. Invariant 6: every counter is keyed by a fixed reason string
    this package defines, never by header, key or payload content."""

    by_reason: dict[str, int] = field(default_factory=dict)
    circuit_opens: int = 0
    circuit_closes: int = 0

    def record(self, reason: str) -> None:
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1

    def snapshot(self) -> dict[str, int]:
        out: dict[str, int] = {f"delivery_{reason}": count for reason, count in self.by_reason.items()}
        out["circuit_opens"] = self.circuit_opens
        out["circuit_closes"] = self.circuit_closes
        return out


class NullLogger:
    """The default when no `log=` is given: tests and library callers that do not care about log
    output get quiet ones. A real `tributary.runtime.logs.JsonLogger` satisfies the same `Logger`
    protocol structurally."""

    def debug(self, event: str, /, **fields: object) -> None:
        pass

    def info(self, event: str, /, **fields: object) -> None:
        pass

    def warning(self, event: str, /, **fields: object) -> None:
        pass

    def error(self, event: str, /, **fields: object) -> None:
        pass


_: Logger = NullLogger()  # structural-typing smoke check, evaluated at import time
