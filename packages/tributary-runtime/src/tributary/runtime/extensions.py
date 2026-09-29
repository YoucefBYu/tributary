"""Context extensions: how a step's `ctx` gains a capability the SDK does not define, such as `ctx.fhir`.

ADR-001 6.2 shows a mapping step calling `ctx.fhir.upsert(...)`. The SDK's `Context` deliberately has no
`.fhir` or `.terminology` (they belong to `tributary-fhir`, W2-2), `Context` is frozen for set 1.x
(channel-spec README 3.2.6: a new capability arrives as an additive sub-protocol), and the W2-2 approval
made the runtime's attachment **optional, not a hard import**: `tributary-runtime` does not depend on
`tributary-fhir` and never imports it. So the package that owns a capability registers a Python entry
point, the same plugin mechanism as `tributary.steps` (ADR-001 6.2, "Plugins/extensions: Python entry
points"), and the worker applies whatever is installed:

    [project.entry-points."tributary.context"]
    fhir = "tributary.fhir.runtime:context_extension"

**The entry point names a factory**, called once at worker start with no arguments, returning the
per-message callable `(ctx, message) -> ctx` (`ContextExtension`). The factory is where an extension
does start-up work (a terminology table, say); the callable runs once per `WorkerContext` that runs a
step, before the first step on it, and every step on that context receives the same wrapped object.

**Always attached when installed**, not only for steps that declare a need. No contract lets a step or a
spec declare a capability, and inferring one from a parameter annotation is fragile (a string under
`from __future__ import annotations`). An unused `ctx.fhir` costs one small object per message.

**Checked before the worker consumes anything.** A wrapper that forgot to delegate `set_attr`,
`set_usr`, `vars` or `reply` to its base would lose attrs from events, `tb-usr-*` headers from records,
or a synchronous reply body, silently and for every channel. `load_context_extensions` probes each
extension against a throwaway context and refuses the worker (`ContextExtensionError`) instead. Any
failure to import or build one is a refusal too, never a skipped extension: a channel whose mapping step
then raised `AttributeError` on every message would dead-letter everything with nothing at startup to
say why (that is the bug this module fixes, qa-engineer's W2 exit criterion 1 run).

Steps stay unit-testable without Kafka (invariant 7): a test builds the extended context itself, e.g.
`tributary.fhir.fake_fhir_context()`. Nothing here logs a payload or a header value (invariant 6):
entry-point and distribution names only.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from importlib.metadata import EntryPoint, entry_points

from tributary.sdk import Message
from tributary.sdk import headers as h
from tributary.sdk.context import Context, Logger, ReplyContext
from tributary.sdk.ids import new_traceparent, uuid7

from .context import ContextExtension, WorkerContext
from .errors import RuntimeConfigurationError, safe_token

ENTRY_POINT_GROUP = "tributary.context"

#: What an entry point in `ENTRY_POINT_GROUP` loads: called once at startup, returns the per-message hook.
ExtensionFactory = Callable[[], ContextExtension]

_PROBE_CHANNEL = "tb-extension-probe"
_PROBE_ATTR = ("tb_probe", "1")
_PROBE_USR = ("tb-probe", "1")
_PROBE_VAR = "tb.extension-probe"


class ContextExtensionError(RuntimeConfigurationError):
    """An installed context extension cannot be loaded, or breaks what the worker relies on."""

    code = "CONTEXT_EXTENSION_INVALID"


@dataclass(frozen=True, slots=True)
class LoadedExtension:
    """One verified extension. `name` and `distribution` are package metadata, safe to log."""

    name: str
    distribution: str
    extend: ContextExtension


def load_context_extensions(
    *, log: Logger | None = None, points: Iterable[EntryPoint] | None = None
) -> tuple[LoadedExtension, ...]:
    """Load, build and verify every extension in `ENTRY_POINT_GROUP`, ordered by entry-point name.

    Raises `ContextExtensionError`; the worker then refuses to start. `points` replaces discovery, for
    tests.
    """
    found = sorted(points if points is not None else entry_points(group=ENTRY_POINT_GROUP), key=lambda p: p.name)
    names = [point.name for point in found]
    duplicates = sorted({name for name in names if names.count(name) > 1})
    if duplicates:
        raise ContextExtensionError(
            f"context extension {safe_token(duplicates[0])} is registered by more than one distribution"
        )
    loaded = tuple(_build(point) for point in found)
    for extension in loaded:
        verify_extension(extension)
        if log is not None:
            log.info("context_extension_loaded", extension=extension.name, distribution=extension.distribution)
    return loaded


def _build(point: EntryPoint) -> LoadedExtension:
    name = point.name
    try:
        factory = point.load()
    except (Exception, SystemExit) as exc:  # import-time code can raise anything, even sys.exit()
        raise ContextExtensionError(f"cannot load context extension {safe_token(name)}: {type(exc).__name__}") from None
    if not callable(factory):
        raise ContextExtensionError(f"context extension {safe_token(name)} is not a callable factory")
    try:
        extend = factory()
    except Exception as exc:
        raise ContextExtensionError(
            f"context extension {safe_token(name)} failed to start: {type(exc).__name__}"
        ) from None
    if not callable(extend):
        raise ContextExtensionError(f"context extension {safe_token(name)}'s factory did not return a callable")
    distribution = point.dist.name if point.dist is not None else "unknown"
    return LoadedExtension(name=name, distribution=distribution, extend=extend)


def verify_extension(extension: LoadedExtension) -> None:
    """Probe one extension on a pipeline context and on a destination context. Raises `ContextExtensionError`.

    What is checked is exactly what the worker reads from the `WorkerContext` underneath rather than
    from the step's view: attrs (events), `tb-usr-*` (record headers), `vars` (shared across steps), and
    `reply` (the synchronous answer, channel-spec 3.2.6: `attach()` of a `ReplyContext` stays one).
    """
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
        except ContextExtensionError:
            raise
        except Exception as exc:
            raise ContextExtensionError(
                f"context extension {safe_token(extension.name)} failed its probe: {type(exc).__name__}"
            ) from None


def _check(view: object, base: WorkerContext) -> None:
    if not isinstance(view, Context):
        raise ContextExtensionError("a context extension returned an object that is not a Context")
    if not isinstance(view, ReplyContext):
        raise ContextExtensionError(
            "a context extension dropped `reply`: the worker's context is a ReplyContext, so the wrapper must be one"
        )
    view.set_attr(*_PROBE_ATTR)
    view.set_usr(*_PROBE_USR)
    view.vars[_PROBE_VAR] = True
    if base.attrs().get(_PROBE_ATTR[0]) != _PROBE_ATTR[1]:
        raise ContextExtensionError("a context extension does not delegate set_attr to its base")
    if base.usr_headers().get(_PROBE_USR[0]) != _PROBE_USR[1]:
        raise ContextExtensionError("a context extension does not delegate set_usr to its base")
    if base.vars.get(_PROBE_VAR) is not True:
        raise ContextExtensionError("a context extension does not share its base's vars")


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
