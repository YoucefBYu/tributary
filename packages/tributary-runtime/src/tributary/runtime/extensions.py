"""Context extensions: how a step's `ctx` gains a capability the SDK does not define, such as `ctx.fhir`.

The contract is `contracts/context-extension/v0` (set 1.5, ADR-007 D21-D22); this module implements its
loading rules L1-L7. In short: `Context` is frozen for set 1.x, and `tributary-runtime` never imports
`tributary-fhir` (W2-2: optional attachment, not a hard import), so the package owning a capability
registers a `tributary.context` entry point whose value is a zero-argument factory returning
`ContextExtension = Callable[[Context, Message], Context]`:

    [project.entry-points."tributary.context"]
    fhir = "tributary.fhir.runtime:context_extension"

**Only what the spec declares runs (L1, L2).** A channel names its capabilities in
`spec.pipeline.context` (channel-spec README 3.3.4). `ContextResolver.resolve` reads entry-point
*metadata* for the whole group, but calls `EntryPoint.load()` only for a declared name, and only when
exactly one installed entry point carries it, that entry point's distribution (PEP 503 normalised) is
the one `capabilities.CAPABILITIES` pins it to, and the factory's module is one of that distribution's
own installed files (`provenance.py`). Anything else refuses the spec with
`ContextExtensionError`, a `ChannelSpecError`: at startup the worker does not start; on a hot reload the
previous version keeps running, exactly as for an unresolvable `uses` step. An installed registrant that
is not loaded is never imported, and is logged as `context_extension_skipped` with its reason (L5).

**Shadowing (set 1.7, L5 `duplicate`, ADR-007 D24).** Discovery reads every installed distribution,
not `entry_points()`, which keeps only the first installation per distribution name on `sys.path`.
Resolution counts the installation `importlib.metadata` shows (the one `import` loads); each hidden
same-named installation is logged `duplicate` (warning) and is never loaded or refused, the same rule
`steps.py` applies to `uses` steps (`provenance.InstalledEntryPoints`).

**The factory runs at most once per capability per process** and its result is cached; the per-message
callable then runs once per `WorkerContext` that runs a step (`WorkerContext.step_view`).

**The probe (L3)** runs on each extension before its first use, against a pipeline context and a
destination context: attrs, `tb-usr-*` and `vars` must reach the base (the worker reads them there), a
real `view.reply(...)` must land in the base's reply cell on the pipeline context and must raise on the
destination context, and `view.log`, `view.secrets`, `view.vars` and `view.kv` must be the base's own
objects (P1). It checks delegation only; obligations P2-P7 (no own logger, no global state, no I/O,
determinism) are enforced by review and the registry gate, not here. A factory or a probe that raises,
`SystemExit` included, refuses the spec; it never stops the worker.

Nothing here logs a payload or a header value (invariant 6): entry-point, distribution and channel
names, versions and fixed reason codes only. Exception text from plugin code is never repeated.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import EntryPoint

from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.context import Context, Logger, ReplyContext
from tributary.sdk.ids import new_traceparent, uuid7

from .capabilities import CAPABILITIES, ENTRY_POINT_GROUP, normalise_distribution
from .context import ContextExtension, WorkerContext
from .errors import ChannelSpecError, safe_token
from .provenance import InstalledEntryPoints, provenance_problem, scan_entry_points
from .spec import ChannelSpec

__all__ = [
    "DUPLICATE",
    "ENTRY_POINT_GROUP",
    "ContextExtensionError",
    "ContextResolver",
    "ExtensionFactory",
    "LoadedExtension",
    "verify_extension",
]

#: What an entry point in `ENTRY_POINT_GROUP` loads: called once, returns the per-message hook.
ExtensionFactory = Callable[[], ContextExtension]

#: `context_extension_skipped` reasons (L5). The first is routine; the others point at a transitive,
#: typosquatted or shadowed package in the image, so they are warnings. None stops the worker: nothing
#: was imported.
NOT_DECLARED = "not-declared"
UNREGISTERED = "unregistered"
DISTRIBUTION_MISMATCH = "distribution-mismatch"
#: Set 1.7: a later installation of a distribution name `importlib.metadata` already shows. Never loaded.
DUPLICATE = "duplicate"

_PROBE_CHANNEL = "tb-extension-probe"
_PROBE_ATTR = ("tb_probe", "1")
_PROBE_USR = ("tb-probe", "1")
_PROBE_VAR = "tb.extension-probe"
_PROBE_REPLY = b'{"probe": true}'
_PROBE_REPLY_TYPE = "application/json"


class ContextExtensionError(ChannelSpecError):
    """A declared context capability cannot be resolved, loaded or verified. The spec is refused (L2)."""

    code = "CONTEXT_EXTENSION_INVALID"


@dataclass(frozen=True, slots=True)
class LoadedExtension:
    """One verified extension. Every field but `extend` is package metadata, safe to log."""

    name: str
    distribution: str
    version: str
    extend: ContextExtension


class ContextResolver:
    """Resolves a channel version's `pipeline.context` to its extensions. One per worker process.

    `points` replaces entry-point discovery and `registry` the capability registry, for tests only (L7
    is the worker-level override; this is the unit-test seam below it).
    """

    def __init__(
        self,
        *,
        log: Logger,
        points: Callable[[], Iterable[EntryPoint]] | None = None,
        registry: Mapping[str, str] = CAPABILITIES,
    ) -> None:
        self._log = log
        self._points = points if points is not None else lambda: scan_entry_points(ENTRY_POINT_GROUP)
        self._registry = registry
        self._loaded: dict[tuple[str, str, str], LoadedExtension] = {}

    def resolve(self, spec: ChannelSpec) -> tuple[ContextExtension, ...]:
        """The extensions for `spec`, wrapping order by name (L4). Raises `ContextExtensionError`."""
        declared = spec.pipeline.context
        scan = InstalledEntryPoints.split(self._points())
        installed = list(scan.visible)
        skipped = [(point, self._skip_reason(point, declared)) for point in installed]
        skipped.extend((point, DUPLICATE) for point in scan.shadowed)  # hidden: never loaded or refused
        for point, reason in skipped:
            if reason is not None:
                fields = _metadata(point)
                (self._log.info if reason == NOT_DECLARED else self._log.warning)(
                    "context_extension_skipped",
                    extension=point.name,
                    distribution=fields[0],
                    version=fields[1],
                    channel=spec.name,
                    channel_version=spec.version,
                    reason=reason,
                )
        resolved: list[ContextExtension] = []
        for name in sorted(declared):
            extension = self._load(name, installed, spec)
            self._log.info(
                "context_extension_loaded",
                extension=extension.name,
                distribution=extension.distribution,
                version=extension.version,
                channel=spec.name,
                channel_version=spec.version,
            )
            resolved.append(extension.extend)
        return tuple(resolved)

    def _skip_reason(self, point: EntryPoint, declared: tuple[str, ...]) -> str | None:
        expected = self._registry.get(point.name)
        if expected is None:
            return UNREGISTERED
        if provenance_problem(point.dist, point.value, expected) is not None:
            return DISTRIBUTION_MISMATCH
        if point.name not in declared:
            return NOT_DECLARED
        return None  # declared and from the registered distribution: a load candidate

    def _load(self, name: str, installed: list[EntryPoint], spec: ChannelSpec) -> LoadedExtension:
        where = f"channel {safe_token(spec.name)}: context capability {safe_token(name)}"
        expected = self._registry.get(name)
        if expected is None:
            raise ContextExtensionError(f"{where} is not registered (contracts/context-extension/v0)")
        matches = [point for point in installed if point.name == name]
        if not matches:
            raise ContextExtensionError(f"{where} is not installed: no {ENTRY_POINT_GROUP} entry point carries it")
        if len(matches) > 1:
            raise ContextExtensionError(f"{where} is registered by more than one distribution")
        point = matches[0]
        distribution, version = _metadata(point)
        # Name (PEP 508 ASCII), install folder and target module, all from metadata, before any import.
        problem = provenance_problem(point.dist, point.value, expected)
        if problem is not None:
            raise ContextExtensionError(
                f"{where} {problem}: distribution {safe_token(distribution)}, registered to {safe_token(expected)}"
            )
        key = (name, normalise_distribution(distribution), point.value)
        cached = self._loaded.get(key)
        if cached is not None:
            return cached
        extension = LoadedExtension(name=name, distribution=distribution, version=version, extend=_build(point))
        verify_extension(extension)
        self._loaded[key] = extension
        return extension


def _metadata(point: EntryPoint) -> tuple[str, str]:
    """`(distribution name, distribution version)` from the entry point's metadata. No import."""
    dist = point.dist
    if dist is None:
        return "unknown", "unknown"
    return str(dist.name), str(dist.version)


def _build(point: EntryPoint) -> ContextExtension:
    name = safe_token(point.name)
    try:
        factory = point.load()
    except (Exception, SystemExit) as exc:  # import-time code can raise anything, even sys.exit()
        raise ContextExtensionError(f"cannot load context extension {name}: {type(exc).__name__}") from None
    if not callable(factory):
        raise ContextExtensionError(f"context extension {name} is not a callable factory")
    try:
        extend = factory()
    except (Exception, SystemExit) as exc:  # plugin code can call sys.exit(); refuse, never stop the worker
        raise ContextExtensionError(f"context extension {name} failed to start: {type(exc).__name__}") from None
    if not callable(extend):
        raise ContextExtensionError(f"context extension {name}'s factory did not return a callable")
    return extend  # type: ignore[no-any-return]


def verify_extension(extension: LoadedExtension) -> None:
    """The L3 probe, on a pipeline context and a destination context. Raises `ContextExtensionError`."""
    subject = _probe_message()
    for destination in (None, "tb-probe-destination"):
        base = WorkerContext(
            channel=_PROBE_CHANNEL,
            channel_version=1,
            instance="worker:extension-probe@localhost",
            log=_NullLogger(),
            destination=destination,
        )
        try:
            view = extension.extend(base, subject)
            _check(view, base)
        except ContextExtensionError as exc:
            raise ContextExtensionError(f"context extension {safe_token(extension.name)}: {exc}") from None
        except (Exception, SystemExit) as exc:  # as for the factory: a refused spec, not a stopped worker
            raise ContextExtensionError(
                f"context extension {safe_token(extension.name)} failed its probe: {type(exc).__name__}"
            ) from None


def _check(view: object, base: WorkerContext) -> None:
    if not isinstance(view, Context):
        raise ContextExtensionError("returned an object that is not a Context")
    if not isinstance(view, ReplyContext):
        raise ContextExtensionError(
            "dropped `reply`: the worker's context is a ReplyContext, so the wrapper must be one"
        )
    if view.log is not base.log:
        raise ContextExtensionError("does not return its base's own `log`")
    if view.secrets is not base.secrets:
        raise ContextExtensionError("does not return its base's own `secrets`")
    if view.vars is not base.vars:
        raise ContextExtensionError("does not return its base's own `vars`")
    if view.kv is not base.kv:
        raise ContextExtensionError("does not return its base's own `kv`")
    view.set_attr(*_PROBE_ATTR)
    view.set_usr(*_PROBE_USR)
    view.vars[_PROBE_VAR] = True
    if base.attrs().get(_PROBE_ATTR[0]) != _PROBE_ATTR[1]:
        raise ContextExtensionError("does not delegate set_attr to its base")
    if base.usr_headers().get(_PROBE_USR[0]) != _PROBE_USR[1]:
        raise ContextExtensionError("does not delegate set_usr to its base")
    if base.destination is None:
        view.reply(_PROBE_REPLY, content_type=_PROBE_REPLY_TYPE)
        landed = base.reply_body
        if landed is None or landed.body != _PROBE_REPLY:
            raise ContextExtensionError("does not forward `reply` to its base")
        return
    try:
        view.reply(_PROBE_REPLY, content_type=_PROBE_REPLY_TYPE)
    except Exception:
        return  # a destination context refuses `reply` (channel-spec 3.2.6), and so must its wrapper
    raise ContextExtensionError("accepted `reply` on a destination context, which must raise")


def _probe_message() -> Message:
    """A synthetic record for the probe: engine-minted ids and a JSON marker, nothing HL7-shaped."""
    return Message(
        headers={
            h.TB_MSG_ID: uuid7(),
            h.TB_CHANNEL: _PROBE_CHANNEL,
            h.TB_CHANNEL_VERSION: "1",
            h.TB_SOURCE: "worker:extension-probe@localhost",
            h.TB_RECEIVED_AT: h.format_timestamp(datetime.now(UTC)),
            h.TB_CONTENT_TYPE: "application/json",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: new_traceparent(),
        },
        payload=b'{"probe": true}',
    )


class _NullLogger:
    """The probe's `ctx.log`: an extension that logs during the probe logs nowhere."""

    def debug(self, event: str, /, **fields: object) -> None:
        return None

    def info(self, event: str, /, **fields: object) -> None:
        return None

    def warning(self, event: str, /, **fields: object) -> None:
        return None

    def error(self, event: str, /, **fields: object) -> None:
        return None
