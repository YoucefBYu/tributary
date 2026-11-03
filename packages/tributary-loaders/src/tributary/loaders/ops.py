"""The loaders' ops listener: `GET /livez` and `GET /metrics` (`contracts/observability/v0` 2.1, 3.1) on the shared
`tributary.runtime.ops`. A loader contracts no `/readyz` (observability README 1), so that path is a 404.

The loop runs on the main thread and blocks, so the listener runs on its own (`start_ops_or_exit`), and `/livez`
reads the consumer loop's heartbeat: a wedged loop fails it (503) instead of looking alive for ever.

| Metric | Source |
|---|---|
| `tb_build_info{component, version}` | 1, set once |
| `tb_spec_version{component, channel}`, `tb_spec_source_unproven{component, channel}` | the spec cache (a dict read, never a poll: the consumer belongs to the loop's thread) |
| `tb_deliveries_total{channel, destination, destination_type, outcome, code}` | the loader's `LoaderMetrics.by_reason`, one series per reason (`delivery_labels`) |
| `tb_circuit_open{channel, destination}` | 1 while opens exceed closes: every open is followed by exactly one close |
| `tb_circuit_transitions_total{channel, destination, to}` | `circuit_opens` and `circuit_closes` |

A redelivery of a record is another delivery attempt, so it counts: `tb_deliveries_total` is attempts, not unique
messages (at-least-once, invariant 4). Labels are the channel and destination names from the spec, the destination
type and a fixed reason vocabulary; never a message id, URL, key or payload (invariant 6).
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Callable
from typing import Final, Protocol

from tributary.runtime.channels import LiveSpecSource
from tributary.runtime.consumer import ConsumerLoop
from tributary.runtime.errors import RuntimeConfigurationError
from tributary.runtime.metrics import MetricsContract, MetricsRegistry, build_version
from tributary.runtime.ops import OpsServer, OpsThread, ops_settings_or_exit, start_ops_or_exit
from tributary.runtime.startup import refuse_configuration
from tributary.sdk.context import Logger

COMPONENT: Final = "loader"
#: A reason that is a success: the record is at the destination, new (`success`) or already there (`duplicate`).
SUCCESS_REASONS: Final = frozenset({"success", "duplicate"})
#: Loader reasons that differ from their registered code (`envelope/v0` `error-codes.json`) beyond case.
CODE_ALIASES: Final[dict[str, str]] = {}
_CODE: Final = re.compile(r"[A-Z][A-Z0-9_]{0,63}")


def delivery_labels(reason: str) -> tuple[str, str]:
    """`(outcome, code)` for a loader reason: `("success", "NONE")`, or `("failure", <code>)`. A reason that is
    already a code (`PAYLOAD_SHAPE_INVALID`) is kept; `circuit-open` becomes `CIRCUIT_OPEN`, `tls-verify-failed`
    `TLS_VERIFY_FAILED`. A reason that cannot be a code is `UNKNOWN`."""
    if reason in SUCCESS_REASONS:
        return "success", "NONE"
    code = CODE_ALIASES.get(reason) or reason.upper().replace("-", "_")
    return "failure", code if _CODE.fullmatch(code) else "UNKNOWN"


class SnapshotSource(Protocol):
    """Every loader's record handler."""

    def metrics_snapshot(self) -> dict[str, int]: ...


class LoaderOps:
    """Registers every loader metric on construction (a registry that drifted from the contract refuses the start,
    not the first scrape). `bind` connects it to the running loop; before that `/metrics` shows `tb_build_info`
    and zero series, and `/livez` is alive (startup is bounded by the spec catch-up)."""

    def __init__(
        self, registry: MetricsRegistry, *, channel: str, destination: str, destination_type: str, version: str | None
    ) -> None:
        if registry.component != COMPONENT:
            raise ValueError("LoaderOps needs a registry for component 'loader'")
        self.registry = registry
        self._channel = channel
        self._destination = destination
        self._type = destination_type
        self.build_info = registry.gauge("tb_build_info")
        self.spec_version = registry.gauge("tb_spec_version")
        self.spec_unproven = registry.gauge("tb_spec_source_unproven")
        self.deliveries = registry.counter("tb_deliveries_total")
        self.circuit_open = registry.gauge("tb_circuit_open")
        self.transitions = registry.counter("tb_circuit_transitions_total")
        self._snapshot: Callable[[], dict[str, int]] | None = None
        self._version: Callable[[], int | None] = lambda: None
        self._available: Callable[[], bool] = lambda: True
        self._age: Callable[[], float | None] = lambda: None
        self._stale_after_s = 300.0
        self.build_info.set(1, component=COMPONENT, version=version or build_version())
        self.spec_unproven.set(0, component=COMPONENT, channel=channel)
        self.circuit_open.set(0, channel=channel, destination=destination)
        for to in ("open", "closed"):
            self.transitions.set_total(0, channel=channel, destination=destination, to=to)
        registry.collector(self._collect)

    @classmethod
    def create(
        cls,
        *,
        channel: str,
        destination: str,
        destination_type: str,
        contracts_dir: str | None = None,
        strict: bool = False,
        log: Logger | None = None,
    ) -> LoaderOps:
        registry = MetricsRegistry(MetricsContract.load(contracts_dir), component=COMPONENT, strict=strict, log=log)
        return cls(registry, channel=channel, destination=destination, destination_type=destination_type, version=None)

    def bind(
        self,
        *,
        snapshot: Callable[[], dict[str, int]],
        spec_version: Callable[[], int | None],
        spec_available: Callable[[], bool],
        loop_age_s: Callable[[], float | None],
        stale_after_s: float,
    ) -> None:
        self._snapshot = snapshot
        self._version = spec_version
        self._available = spec_available
        self._age = loop_age_s
        self._stale_after_s = stale_after_s

    def bind_runtime(
        self, *, handler: SnapshotSource, spec_source: LiveSpecSource, loop: ConsumerLoop, stale_after_s: float
    ) -> None:
        """`bind` for the real objects of a loader's `build_loop`: its handler (`metrics_snapshot()`), its
        `LiveSpecSource` and its `ConsumerLoop`."""
        channel = self._channel

        def version() -> int | None:
            spec = spec_source.cache.get(channel)
            return None if spec is None else int(spec.version)

        self.bind(
            snapshot=handler.metrics_snapshot,
            spec_version=version,
            spec_available=lambda: spec_source.available,
            loop_age_s=loop.seconds_since_tick,
            stale_after_s=stale_after_s,
        )

    def alive(self) -> bool:
        """`/livez`: the loop has not started yet, or iterated within `stale_after_s`. Never Kafka."""
        age = self._age()
        return age is None or age < self._stale_after_s

    def render(self) -> bytes:
        return self.registry.render()

    def _collect(self) -> None:
        channel, destination = self._channel, self._destination
        version = self._version()
        if version is not None:
            self.spec_version.set(version, component=COMPONENT, channel=channel)
        self.spec_unproven.set(0 if self._available() else 1, component=COMPONENT, channel=channel)
        if self._snapshot is None:
            return
        snapshot = self._snapshot()
        merged: dict[tuple[str, str], int] = {}
        for key, count in snapshot.items():
            if key.startswith("delivery_"):
                labels = delivery_labels(key.removeprefix("delivery_"))
                merged[labels] = merged.get(labels, 0) + count
        for (outcome, code), count in merged.items():
            self.deliveries.set_total(
                count,
                channel=channel,
                destination=destination,
                destination_type=self._type,
                outcome=outcome,
                code=code,
            )
        opens, closes = snapshot.get("circuit_opens", 0), snapshot.get("circuit_closes", 0)
        self.circuit_open.set(1 if opens > closes else 0, channel=channel, destination=destination)
        self.transitions.set_total(opens, channel=channel, destination=destination, to="open")
        self.transitions.set_total(closes, channel=channel, destination=destination, to="closed")


def start_loader_ops(
    args: argparse.Namespace, log: Logger, *, channel: str, destination: str, destination_type: str
) -> tuple[LoaderOps, OpsThread]:
    """Resolve `--ops-bind`/`--ops-port`, build the metrics, bind on a thread, or one `startup_refused` line and
    exit 2 (O4): before any Kafka client exists."""
    settings = ops_settings_or_exit(args, log)
    try:
        ops = LoaderOps.create(
            channel=channel,
            destination=destination,
            destination_type=destination_type,
            contracts_dir=getattr(args, "contracts_dir", None),
            log=log,
        )
    except RuntimeConfigurationError as exc:
        refuse_configuration(log, exc)
    server = OpsServer(settings, component=COMPONENT, metrics=ops.render, liveness=ops.alive, log=log)
    return ops, start_ops_or_exit(server, log)
