"""In-process delivery-outcome counters for one `HttpLoader`.

Same v0 scope as `tributary.receivers.metrics`: a plain snapshot, not a Prometheus or OpenTelemetry
exporter — no metrics pipeline exists yet in this repo to wire into. `snapshot()` is the seam a real
exporter reads from later.

Invariant 6: no PHI in a metric or a label. Every counter here is keyed by `Reason`, a fixed enum
this package defines; nothing takes a header value, a record key or payload bytes.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class LoaderMetrics:
    #: keyed by `loader.Reason` ("success", "http-5xx", "timeout", ...); `str` here, not that type,
    #: to avoid a circular import (`loader.py` constructs one of these).
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
