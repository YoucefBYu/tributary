"""Metrics bound to `contracts/observability/v0/metrics.json`, exposed in the Prometheus text format (0.0.4).

**The registry file is the contract** (README 3.1). A process can register only a metric that file names,
with the type it declares, and only if the process's component is among its `emitters`. Anything else is
refused at registration (`MetricRegistrationError`), which is at process start, not at the first scrape.

**Labels are a closed list (README 3.2, invariant 6).** A series must carry exactly the metric's label names
(a mismatch is a programming error and always raises `MetricLabelError`). Every value is checked against the
label's `enum`, `pattern` or per-metric `label_values` the first time a series is seen. A value that fails is
never exported: in `strict` mode (tests) it raises; otherwise the observation is dropped and a
`metric_label_refused` line names the metric and the **label name**, once, never the value. A data-plane
process must not stop over a metric, and a value outside the closed list is exactly the thing that must not
reach a scrape. Series per metric are also capped (`max_series`), the same way.

**No client library.** The exposition format is small and stable, and writing it here avoids a dependency, its
global registry (which would export unregistered `process_*` and `python_*` names) and its HTTP server (which
would bypass O5). Ruling H1 permitted `prometheus-client` for exposition only; it was not needed.

Thread-safety: one lock per registry. Observations come from the data-plane thread and `render()` from the ops
listener's thread. Collectors (callbacks that refresh gauges and mirrored counters from in-memory state) run at
the start of `render()`, outside the lock; they must not do I/O (O6).
"""

from __future__ import annotations

import json
import math
import re
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final, Literal

from tributary.sdk.context import Logger

from .config import find_contracts_dir
from .errors import RuntimeConfigurationError, safe_token

CONTRACT_RELATIVE: Final = Path("observability") / "v0" / "metrics.json"
DEFAULT_MAX_SERIES: Final = 10_000

MetricType = Literal["counter", "gauge", "histogram"]
_TYPES: Final = frozenset({"counter", "gauge", "histogram"})


class MetricsContractError(RuntimeConfigurationError):
    """`metrics.json` is missing or malformed."""

    code = "METRICS_CONTRACT_INVALID"


class MetricRegistrationError(RuntimeConfigurationError):
    """A metric the contract does not name, with another type, or not emitted by this component."""

    code = "METRIC_UNREGISTERED"


class MetricLabelError(ValueError):
    """A series with the wrong label names, or (strict mode) a value outside the label's closed list.

    The message names the metric and the label, never the value."""


@dataclass(frozen=True, slots=True)
class LabelRule:
    name: str
    enum: frozenset[str] | None = None
    pattern: re.Pattern[str] | None = None
    per_metric: bool = False


@dataclass(frozen=True, slots=True)
class MetricDef:
    name: str
    type: MetricType
    help: str
    labels: tuple[str, ...]
    emitters: frozenset[str]
    buckets: tuple[float, ...] | None = None
    label_values: Mapping[str, frozenset[str]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class MetricsContract:
    """The parsed registry: metric definitions by name, in file order, and the label rules."""

    metrics: Mapping[str, MetricDef]
    labels: Mapping[str, LabelRule]

    @classmethod
    def load(cls, contracts_dir: str | Path | None = None) -> MetricsContract:
        """Read `observability/v0/metrics.json` from the contracts directory `config.find_contracts_dir` finds
        (argument, then `TB_CONTRACTS_DIR`, then the checkout)."""
        path = find_contracts_dir(contracts_dir) / CONTRACT_RELATIVE
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise MetricsContractError(f"cannot read {CONTRACT_RELATIVE.as_posix()}: {type(exc).__name__}") from None
        return cls.from_document(document)

    @classmethod
    def from_document(cls, document: Any) -> MetricsContract:
        if not isinstance(document, dict):
            raise MetricsContractError("metrics.json is not an object")
        try:
            raw_labels: dict[str, Any] = document["labels"]
            raw_buckets: dict[str, list[float]] = document["buckets"]
            raw_metrics: list[dict[str, Any]] = document["metrics"]
            labels: dict[str, LabelRule] = {}
            for name, rule in raw_labels.items():
                labels[name] = LabelRule(
                    name=name,
                    enum=frozenset(rule["enum"]) if "enum" in rule else None,
                    pattern=re.compile(rule["pattern"]) if "pattern" in rule else None,
                    per_metric=bool(rule.get("per_metric", False)),
                )
            metrics: dict[str, MetricDef] = {}
            for entry in raw_metrics:
                name = entry["name"]
                kind = entry["type"]
                if kind not in _TYPES:
                    raise MetricsContractError(f"metric {safe_token(name)}: unknown type")
                metric_labels = tuple(entry["labels"])
                values = {key: frozenset(vals) for key, vals in entry.get("label_values", {}).items()}
                for label in metric_labels:
                    if label not in labels:
                        raise MetricsContractError(f"metric {safe_token(name)}: label {safe_token(label)} undefined")
                    if labels[label].per_metric and label not in values:
                        raise MetricsContractError(f"metric {safe_token(name)}: {safe_token(label)} has no values")
                buckets: tuple[float, ...] | None = None
                if kind == "histogram":
                    buckets = tuple(float(b) for b in raw_buckets[entry["buckets"]])
                    if list(buckets) != sorted(set(buckets)):
                        raise MetricsContractError(f"metric {safe_token(name)}: buckets not increasing")
                metrics[name] = MetricDef(
                    name=name,
                    type=kind,
                    help=entry["help"],
                    labels=metric_labels,
                    emitters=frozenset(entry["emitters"]),
                    buckets=buckets,
                    label_values=values,
                )
        except (KeyError, TypeError, re.error) as exc:
            raise MetricsContractError(f"metrics.json is malformed ({type(exc).__name__})") from None
        return cls(metrics=metrics, labels=labels)

    def for_component(self, component: str) -> tuple[MetricDef, ...]:
        return tuple(m for m in self.metrics.values() if component in m.emitters)


LabelKey = tuple[str, ...]


class MetricsRegistry:
    """The metrics one process exports. Families are created through `counter`, `gauge` and `histogram`."""

    def __init__(
        self,
        contract: MetricsContract,
        *,
        component: str,
        strict: bool = False,
        log: Logger | None = None,
        max_series: int = DEFAULT_MAX_SERIES,
    ) -> None:
        self.contract = contract
        self.component = component
        self.strict = strict
        self.max_series = max_series
        self._log = log
        self._lock = threading.Lock()
        self._families: dict[str, _Family] = {}
        self._collectors: list[Callable[[], None]] = []
        self._refused: set[tuple[str, str]] = set()
        self._collector_failures: set[str] = set()

    # -- registration ------------------------------------------------------------------------------

    def _register(self, name: str, kind: MetricType) -> MetricDef:
        definition = self.contract.metrics.get(name)
        if definition is None:
            raise MetricRegistrationError(f"metric {safe_token(name)} is not in observability/v0 metrics.json")
        if definition.type != kind:
            raise MetricRegistrationError(f"metric {safe_token(name)} is a {definition.type}, not a {kind}")
        if self.component not in definition.emitters:
            raise MetricRegistrationError(f"metric {safe_token(name)} is not emitted by {safe_token(self.component)}")
        if name in self._families:
            raise MetricRegistrationError(f"metric {safe_token(name)} is already registered")
        return definition

    def counter(self, name: str) -> Counter:
        family = Counter(self, self._register(name, "counter"))
        self._families[name] = family
        return family

    def gauge(self, name: str) -> Gauge:
        family = Gauge(self, self._register(name, "gauge"))
        self._families[name] = family
        return family

    def histogram(self, name: str) -> Histogram:
        family = Histogram(self, self._register(name, "histogram"))
        self._families[name] = family
        return family

    def collector(self, collect: Callable[[], None]) -> None:
        """Run `collect` at the start of every `render()`. It must only read in-memory state."""
        self._collectors.append(collect)

    @property
    def names(self) -> frozenset[str]:
        return frozenset(self._families)

    # -- label checks ------------------------------------------------------------------------------

    def _refuse(self, metric: str, label: str, reason: str) -> None:
        if self.strict:
            raise MetricLabelError(f"metric {safe_token(metric)}: label {safe_token(label)} {reason}")
        if (metric, label) in self._refused:
            return
        self._refused.add((metric, label))
        if self._log is not None:
            self._log.warning("metric_label_refused", metric=safe_token(metric), label=safe_token(label), reason=reason)

    def _value_ok(self, definition: MetricDef, label: str, value: str) -> bool:
        rule = self.contract.labels[label]
        if rule.per_metric:
            return value in definition.label_values[label]
        if rule.enum is not None and value not in rule.enum:
            return False
        return rule.pattern is None or rule.pattern.fullmatch(value) is not None

    # -- exposition --------------------------------------------------------------------------------

    def render(self) -> bytes:
        for collect in list(self._collectors):
            try:
                collect()
            except Exception as exc:  # stale values beat no scrape; say so once per failure class
                name = safe_token(type(exc).__name__)
                if name not in self._collector_failures:
                    self._collector_failures.add(name)
                    if self._log is not None:
                        self._log.warning("metrics_collector_failed", error=name)
        # Copy under the lock, format outside it: the data-plane thread's observations wait only for the copy.
        with self._lock:
            snapshots = [
                (family, family._snapshot())
                for name in self.contract.metrics  # file order: stable output
                if (family := self._families.get(name)) is not None
            ]
        lines: list[str] = []
        for family, items in snapshots:
            family._render(sorted(items, key=lambda item: item[0]), lines)
        lines.append("")
        return "\n".join(lines).encode("utf-8")


class _Family:
    __slots__ = ("_registry", "_series", "definition")

    def __init__(self, registry: MetricsRegistry, definition: MetricDef) -> None:
        self._registry = registry
        self.definition = definition
        self._series: dict[LabelKey, Any] = {}

    def _key(self, labels: Mapping[str, str | None]) -> LabelKey | None:
        """The series key, or `None` when the observation must be dropped (non-strict refusal).

        Called with the registry lock held."""
        definition = self.definition
        if len(labels) != len(definition.labels) or set(labels) != set(definition.labels):
            raise MetricLabelError(f"metric {safe_token(definition.name)}: wrong label names")
        key = tuple("" if labels[n] is None else str(labels[n]) for n in definition.labels)
        if key in self._series:
            return key
        for label, value in zip(definition.labels, key, strict=True):
            if not self._registry._value_ok(definition, label, value):
                self._registry._refuse(definition.name, label, "value outside the closed list")
                return None
        if len(self._series) >= self._registry.max_series:
            self._registry._refuse(
                definition.name, definition.labels[0] if definition.labels else "-", "too many series"
            )
            return None
        return key

    def _header(self, lines: list[str]) -> None:
        lines.append(f"# HELP {self.definition.name} {_escape_help(self.definition.help)}")
        lines.append(f"# TYPE {self.definition.name} {self.definition.type}")

    def _labels_text(self, key: LabelKey, extra: tuple[tuple[str, str], ...] = ()) -> str:
        pairs = [*zip(self.definition.labels, key, strict=True), *extra]
        if not pairs:
            return ""
        return "{" + ",".join(f'{name}="{_escape_value(value)}"' for name, value in pairs) + "}"

    def _snapshot(self) -> list[tuple[LabelKey, Any]]:
        """An immutable copy of every series. Called with the registry lock held."""
        return list(self._series.items())

    def _render(self, items: list[tuple[LabelKey, Any]], lines: list[str]) -> None:
        """Format a snapshot. Called without the lock."""
        raise NotImplementedError


class Counter(_Family):
    __slots__ = ()

    def inc(self, amount: float = 1.0, **labels: str | None) -> None:
        if amount < 0:
            raise ValueError("a counter only goes up")
        with self._registry._lock:
            key = self._key(labels)
            if key is not None:
                self._series[key] = self._series.get(key, 0.0) + amount

    def set_total(self, total: float, **labels: str | None) -> None:
        """Mirror a cumulative count the process already keeps (`LoopStats`). A lower value is ignored: a
        counter never goes down within one process."""
        with self._registry._lock:
            key = self._key(labels)
            if key is not None and total >= self._series.get(key, 0.0):
                self._series[key] = float(total)

    def value(self, **labels: str | None) -> float:
        with self._registry._lock:
            key = tuple("" if labels.get(n) is None else str(labels[n]) for n in self.definition.labels)
            return float(self._series.get(key, 0.0))

    def _render(self, items: list[tuple[LabelKey, Any]], lines: list[str]) -> None:
        self._header(lines)
        for key, value in items:
            lines.append(f"{self.definition.name}{self._labels_text(key)} {_number(value)}")


class Gauge(_Family):
    __slots__ = ()

    def set(self, value: float, **labels: str | None) -> None:
        with self._registry._lock:
            key = self._key(labels)
            if key is not None:
                self._series[key] = float(value)

    def value(self, **labels: str | None) -> float | None:
        with self._registry._lock:
            key = tuple("" if labels.get(n) is None else str(labels[n]) for n in self.definition.labels)
            found = self._series.get(key)
            return None if found is None else float(found)

    def _render(self, items: list[tuple[LabelKey, Any]], lines: list[str]) -> None:
        self._header(lines)
        for key, value in items:
            lines.append(f"{self.definition.name}{self._labels_text(key)} {_number(value)}")


@dataclass(slots=True)
class _HistogramState:
    counts: list[int]
    total: float = 0.0
    count: int = 0


class Histogram(_Family):
    """Buckets are the contract's named set (`latency` or `delivery`), so every emitter buckets alike."""

    __slots__ = ("_bounds",)

    def __init__(self, registry: MetricsRegistry, definition: MetricDef) -> None:
        super().__init__(registry, definition)
        self._bounds: tuple[float, ...] = definition.buckets or ()

    def observe(self, value: float, **labels: str | None) -> None:
        if math.isnan(value):
            return
        with self._registry._lock:
            key = self._key(labels)
            if key is None:
                return
            state = self._series.get(key)
            if state is None:
                state = _HistogramState(counts=[0] * len(self._bounds))
                self._series[key] = state
            for index, bound in enumerate(self._bounds):
                if value <= bound:
                    state.counts[index] += 1
                    break
            state.total += value
            state.count += 1

    def count(self, **labels: str | None) -> int:
        with self._registry._lock:
            key = tuple("" if labels.get(n) is None else str(labels[n]) for n in self.definition.labels)
            state = self._series.get(key)
            return 0 if state is None else int(state.count)

    def _snapshot(self) -> list[tuple[LabelKey, Any]]:
        # The state objects are mutated in place by `observe`: copy their contents, not the references.
        return [(key, (tuple(state.counts), state.total, state.count)) for key, state in self._series.items()]

    def _render(self, items: list[tuple[LabelKey, Any]], lines: list[str]) -> None:
        self._header(lines)
        name = self.definition.name
        for key, (counts, total, count) in items:
            cumulative = 0
            for bound, hits in zip(self._bounds, counts, strict=True):
                cumulative += hits
                lines.append(f"{name}_bucket{self._labels_text(key, (('le', _bound(bound)),))} {cumulative}")
            lines.append(f"{name}_bucket{self._labels_text(key, (('le', '+Inf'),))} {count}")
            lines.append(f"{name}_sum{self._labels_text(key)} {_number(total)}")
            lines.append(f"{name}_count{self._labels_text(key)} {count}")


def _escape_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _escape_help(text: str) -> str:
    return text.replace("\\", "\\\\").replace("\n", "\\n")


def _number(value: float) -> str:
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    if float(value).is_integer() and abs(value) < 2**53:
        return str(int(value))
    return repr(float(value))


def _bound(value: float) -> str:
    return repr(float(value))


def build_version(distribution: str = "tributary-runtime") -> str:
    """The installed distribution's version for `tb_build_info`, or `0.0.0+unknown` (pattern-safe)."""
    from importlib import metadata

    try:
        version = metadata.version(distribution)
    except metadata.PackageNotFoundError:
        return "0.0.0+unknown"
    return version if re.fullmatch(r"[0-9A-Za-z.+-]{1,64}", version) else "0.0.0+unknown"
