"""Loading a channel's steps from its spec.

`ref` is a dotted path inside the bundle (`adt_flow.steps.passthrough`); `uses` is a step registered
through the `tributary.steps` entry point. The spec carries exactly one of them and never inline code,
because inline source cannot be imported by pytest, reviewed in a diff or type-checked — invariant 7
(channel-spec contract section 7).

Steps are trusted code (ADR-001 section 8). Importing a module executes it, so this is a
remote-code-execution surface by design, and the controls are elsewhere: the bundle is digest-pinned,
and no data-plane component may write to `_tb.<env>.channels` (`contracts/topics/v0` 3.4).

**Any failure while importing is a refused spec, never a crashed worker.** An entry point or a `ref`
module can raise anything at import time (`SyntaxError`, `RuntimeError`, a missing dependency, even
`SystemExit`). `KeyboardInterrupt` is deliberately not caught: it is an operator's stop, not a spec. On a hot
reload that would otherwise escape `handle` on every record; it is a `ChannelSpecError` instead, with
the exception's type name only, and the previous plan keeps running.

**What `ref` may name, until bundle-fetch exists.** A `ref` should resolve inside the bundle that
`pipeline.bundle` pins, but nothing fetches a bundle yet (README 3.3.1), so there is no bundle namespace
to check it against. Two namespaces are refused now because they are certainly not bundle code: the
Python standard library (`ref: os.system` would import and then be refused only by the signature
check, and an import alone can have side effects) and `tributary.*`, whose steps are named with `uses`.
Restricting `ref` to the bundle's own top-level package is proposed to the architect for bundle-fetch.

**Which distribution may answer a `uses` name (set 1.6, README 3.3.5, rules N1-N6).** A `uses` name has
at least three segments, and its first two are its namespace (N1). A `tributary.*` namespace is bound by
the engine registry (`step_namespaces.STEP_NAMESPACES`, N2), any other by the spec's
`pipeline.stepNamespaces` (N3). The name resolves only if exactly one installed `tributary.steps` entry
point carries it and that entry point's distribution, PEP 503 normalised, is the bound one (N4).
Everything before that reads entry-point metadata only: `EntryPoint.load()` is called on the one
verified entry point and on nothing else, so a look-alike package in the image is never imported, and a
refusal is a `ChannelSpecError` like every other step refusal. `StepResolver.bind` logs
`step_entry_point_loaded` and `step_entry_point_skipped` (N5); `points` and `registry` are the unit-test
seam (N6), and nothing outside tests sets them.
"""

from __future__ import annotations

import copy
import importlib
import sys
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib.metadata import EntryPoint, distributions
from types import MappingProxyType

from tributary.sdk import Splitter, Step
from tributary.sdk.context import Logger
from tributary.sdk.errors import StepDefinitionError, safe_token

from .errors import ChannelSpecError
from .executor import ChannelPlan, PipelineExecutor, StepStats
from .provenance import canonical_distribution, install_folder_name, provenance_problem
from .spec import ChannelSpec, Pipeline, StepRef
from .step_namespaces import ENTRY_POINT_GROUP, RESERVED_FIRST_SEGMENT, STEP_NAMESPACES, is_reserved, namespace_of

__all__ = [
    "DISTRIBUTION_MISMATCH",
    "DUPLICATE",
    "ENTRY_POINT_GROUP",
    "UNREGISTERED_NAMESPACE",
    "Node",
    "Resolver",
    "StepResolver",
    "build_plan",
    "load_step",
    "load_steps",
]

Node = Step | Splitter
Resolver = Callable[[StepRef], Node]

#: `step_entry_point_skipped` reasons (N5), all at warning level: each points at a look-alike or a
#: transitive package in the image. None stops the worker by itself, because nothing was imported.
DUPLICATE = "duplicate"
DISTRIBUTION_MISMATCH = "distribution-mismatch"
UNREGISTERED_NAMESPACE = "unregistered-namespace"

_NO_BINDINGS: Mapping[str, str] = MappingProxyType({})


def load_step(reference: StepRef, *, namespaces: Mapping[str, str] | None = None) -> Node:
    """Resolve one step and bind its `with`. Raises `ChannelSpecError`, which stops the channel, not the worker.

    `namespaces` is the spec's `pipeline.stepNamespaces`; absent means engine namespaces only. This is
    the unlogged form for callers without a channel version (the corpus tests, harness resolvers); the
    worker goes through `StepResolver.bind`, which logs N5. Binding happens here, once per channel
    version (README 3.3): an unknown key, a key that is not a Python identifier, a wrongly typed value or
    a missing required key refuses the spec loudly, and the worker keeps running the previous one.
    """
    return StepResolver().load(reference, namespaces=namespaces)


def _all_entry_points() -> list[EntryPoint]:
    """Every `tributary.steps` entry point of every installed distribution, **without** de-duplication.

    `importlib.metadata.entry_points()` keeps only the first distribution per name on `sys.path`, so a
    same-named package earlier on the path would hide the real one without a trace (security review of
    a1219af, Low 2). Reading every distribution lets the resolver see the hidden one and say so (N5).
    """
    return [point for dist in distributions() for point in dist.entry_points.select(group=ENTRY_POINT_GROUP)]


@dataclass(frozen=True, slots=True)
class _Installed:
    """What one metadata scan found: the entry points `importlib.metadata` would show, and the hidden rest.

    `visible` keeps the first installation per distribution name in `sys.path` order, which is what
    `entry_points()` returns and what N4 counts. `shadowed` are entry points of a later installation
    with the same name: never loaded, and logged as `duplicate` when they matter (N5).
    """

    visible: tuple[EntryPoint, ...]
    shadowed: tuple[EntryPoint, ...]

    @classmethod
    def split(cls, points: Iterable[EntryPoint]) -> _Installed:
        first: dict[str, object] = {}
        visible: list[EntryPoint] = []
        shadowed: list[EntryPoint] = []
        for point in points:
            dist = point.dist
            if dist is None:
                visible.append(point)
                continue
            name = install_folder_name(dist) or canonical_distribution(str(dist.name)) or str(dist.name)
            install = getattr(dist, "_path", None) or id(dist)
            if first.setdefault(name, install) == install:
                visible.append(point)
            else:
                shadowed.append(point)
        return cls(visible=tuple(visible), shadowed=tuple(shadowed))


_NOTHING = _Installed(visible=(), shadowed=())


@dataclass(frozen=True, slots=True)
class _Scope:
    """What one resolution logs against: the channel version, and which lines it has already written."""

    channel: str | None
    channel_version: int | None
    logged: set[tuple[str, ...]]


class StepResolver:
    """Resolves `uses` and `ref` steps under rules N1-N6. One per worker process; holds no mutable state.

    `points` replaces entry-point discovery and `registry` the engine registry, for tests only (N6).
    """

    def __init__(
        self,
        *,
        log: Logger | None = None,
        points: Callable[[], Iterable[EntryPoint]] | None = None,
        registry: Mapping[str, str] = STEP_NAMESPACES,
    ) -> None:
        self._log: Logger = log if log is not None else _Quiet()
        self._points = points if points is not None else _all_entry_points
        self._registry = registry

    def bind(self, spec: ChannelSpec) -> Resolver:
        """A resolver for one channel version: the spec's bindings, one metadata scan, N5 logging.

        The image-wide check runs here, once per version: every installed entry point in a reserved
        `tributary.*` namespace that the registry would not load is logged, named by a spec or not.
        """
        installed = _Installed.split(self._points())
        scope = _Scope(channel=spec.name, channel_version=spec.version, logged=set())
        for point in installed.visible:
            reason = self._reserved_failure(point)
            if reason is not None:
                self._skipped(point, reason, scope)
        for point in installed.shadowed:  # a hidden installation of an engine distribution's name
            if point.name.split(".", 1)[0] == RESERVED_FIRST_SEGMENT:
                self._skipped(point, DUPLICATE, scope)
        namespaces = spec.pipeline.step_namespaces

        def resolve(reference: StepRef) -> Node:
            return self._node(reference, namespaces, installed, scope)

        return resolve

    def load(self, reference: StepRef, *, namespaces: Mapping[str, str] | None = None) -> Node:
        """One step, outside any channel version. Discovery runs only for a `uses` step."""
        installed = _Installed.split(self._points()) if reference.uses else _NOTHING
        scope = _Scope(channel=None, channel_version=None, logged=set())
        return self._node(reference, namespaces if namespaces is not None else _NO_BINDINGS, installed, scope)

    # -- one step ------------------------------------------------------------------------------------

    def _node(self, reference: StepRef, namespaces: Mapping[str, str], installed: _Installed, scope: _Scope) -> Node:
        loaded = (
            self._from_entry_point(reference.uses, namespaces, installed, scope)
            if reference.uses
            else _from_dotted_path(reference.ref)
        )
        return _as_node(reference, loaded)

    def _from_entry_point(
        self, name: str | None, namespaces: Mapping[str, str], installed: _Installed, scope: _Scope
    ) -> object:
        if not name:
            raise ChannelSpecError("step has no `uses` value")
        namespace = namespace_of(name)
        if namespace is None:  # N1
            raise ChannelSpecError(
                f"uses {safe_token(name)} has no namespace: a uses name needs at least three segments "
                "(channel-spec README 3.3.5 N1)"
            )
        carriers = [point for point in installed.visible if point.name == name]
        for point in installed.shadowed:  # hidden by an earlier same-named installation: never loaded
            if point.name == name:
                self._skipped(point, DUPLICATE, scope)
        expected = self._registry.get(namespace) if is_reserved(namespace) else namespaces.get(namespace)
        if expected is None:  # N2, N3
            for point in carriers:
                self._skipped(point, UNREGISTERED_NAMESPACE, scope)
            where = "the engine registry" if is_reserved(namespace) else "spec.pipeline.stepNamespaces"
            raise ChannelSpecError(
                f"uses {safe_token(name)}: namespace {safe_token(namespace)} is not bound by {where} "
                "(channel-spec README 3.3.5)"
            )
        if not carriers:
            raise ChannelSpecError(f"no step registered as {safe_token(name)} in {ENTRY_POINT_GROUP}")
        if len(carriers) > 1:  # N4: two distributions, since importlib.metadata de-duplicates one
            for point in carriers:
                self._skipped(point, DUPLICATE, scope)
            raise ChannelSpecError(
                f"uses {safe_token(name)} is registered by more than one entry point; none is loaded"
            )
        point = carriers[0]
        # Name (PEP 508 ASCII), install folder and target module, all from metadata, before any import.
        problem = provenance_problem(point.dist, point.value, expected)
        if problem is not None:
            self._skipped(point, DISTRIBUTION_MISMATCH, scope)
            raise ChannelSpecError(
                f"uses {safe_token(name)} {problem}: distribution {safe_token(_metadata(point)[0])}, "
                f"bound to {safe_token(expected)}"
            )
        try:
            target = point.load()  # the one verified entry point, and the only import (N4)
        except (Exception, SystemExit) as exc:  # import-time code can raise anything, even sys.exit()
            raise ChannelSpecError(f"cannot load step {safe_token(name)}: {type(exc).__name__}") from None
        self._emit("step_entry_point_loaded", point, scope, reason=None, level="info")
        return target

    # -- N5 ------------------------------------------------------------------------------------------

    def _reserved_failure(self, point: EntryPoint) -> str | None:
        """Why the registry would refuse an installed entry point in a reserved namespace, if it would."""
        if point.name.split(".", 1)[0] != RESERVED_FIRST_SEGMENT:
            return None
        namespace = namespace_of(point.name)
        expected = self._registry.get(namespace) if namespace is not None else None
        if expected is None:
            return UNREGISTERED_NAMESPACE
        if provenance_problem(point.dist, point.value, expected) is not None:
            return DISTRIBUTION_MISMATCH
        return None

    def _skipped(self, point: EntryPoint, reason: str, scope: _Scope) -> None:
        self._emit("step_entry_point_skipped", point, scope, reason=reason, level="warning")

    def _emit(self, event: str, point: EntryPoint, scope: _Scope, *, reason: str | None, level: str) -> None:
        distribution, version = _metadata(point)
        key = (event, point.name, distribution, point.value, reason or "")
        if key in scope.logged:
            return  # one line per distinct name per applied version, however often the spec names it
        scope.logged.add(key)
        fields: dict[str, object] = {"step": point.name, "distribution": distribution, "version": version}
        if scope.channel is not None:
            fields.update(channel=scope.channel, channel_version=scope.channel_version)
        if reason is not None:
            fields["reason"] = reason
        (self._log.warning if level == "warning" else self._log.info)(event, **fields)


def _metadata(point: EntryPoint) -> tuple[str, str]:
    """`(distribution name, distribution version)` from the entry point's metadata. No import."""
    dist = point.dist
    if dist is None:
        return "unknown", "unknown"
    return str(dist.name), str(dist.version)


class _Quiet:
    """The logger of an unlogged `StepResolver` (`load_step`)."""

    def debug(self, event: str, /, **fields: object) -> None:
        return None

    def info(self, event: str, /, **fields: object) -> None:
        return None

    def warning(self, event: str, /, **fields: object) -> None:
        return None

    def error(self, event: str, /, **fields: object) -> None:
        return None


def _as_node(reference: StepRef, loaded: object) -> Node:
    """Wrap a loaded callable as a step if it is not one yet, and bind the spec's `with`."""
    node: Node
    if isinstance(loaded, Step | Splitter):
        node = loaded
    elif callable(loaded):
        # A bundle author who forgot @step still gets a working channel; the decorator's checks are
        # applied here instead, so the signature and return contract are enforced either way.
        try:
            node = Step(loaded, name=reference.name)
        except StepDefinitionError as exc:
            raise ChannelSpecError(f"step {safe_token(reference.name)}: {exc}") from None
    else:
        raise ChannelSpecError(f"step {safe_token(reference.name)} does not resolve to a callable")
    try:
        return node.bind(reference.params)
    except StepDefinitionError as exc:
        # The binder's messages carry key names only, never values (they may have been pasted from data).
        raise ChannelSpecError(f"step {safe_token(reference.name)}: {exc}") from None


def _from_dotted_path(path: str | None) -> object:
    if not path or "." not in path:
        raise ChannelSpecError(f"step ref {safe_token(str(path))} must be a dotted path module.attribute")
    module_name, _, attribute = path.rpartition(".")
    top = module_name.split(".", 1)[0]
    if top in sys.stdlib_module_names or top == "tributary":
        raise ChannelSpecError(
            f"step ref {safe_token(path)} names the standard library or the engine; a ref names bundle code, "
            "and an engine step is named with `uses`"
        )
    try:
        module = importlib.import_module(module_name)
    except (Exception, SystemExit) as exc:  # ImportError, anything raised at import, even sys.exit()
        raise ChannelSpecError(f"cannot import {safe_token(module_name)} for step ref: {type(exc).__name__}") from None
    try:
        return getattr(module, attribute)
    except AttributeError as exc:
        raise ChannelSpecError(f"{safe_token(module_name)} has no attribute {safe_token(attribute)}") from exc


def load_steps(pipeline: Pipeline) -> tuple[Node, ...]:
    """Resolve every step in order. All-or-nothing: a channel with one unloadable step does not run."""
    return tuple(load_step(reference, namespaces=pipeline.step_namespaces) for reference in pipeline.steps)


def build_plan(
    spec: ChannelSpec,
    *,
    resolve: Resolver | None = None,
    pipeline_override: tuple[Node, ...] | None = None,
    stats: StepStats | None = None,
) -> ChannelPlan:
    """Resolve and bind every step of a channel version into its executor tree. All-or-nothing.

    Refused here, with a `ChannelSpecError`, so the previous version keeps running:

    * **a splitter on a `per-key` channel** (README 3.3.2 split rule 3). Children inherit the parent's
      key, which for a multi-patient batch is the wrong patient's, and a later message for another
      patient in the batch could overtake them: invariant 5 broken silently;
    * more than one splitter in `pipeline.steps` (fan-out of fan-out is 10^8 children);
    * a splitter in `destinations[].steps`: a destination's copy is one record to one `out.<dest>`.
    """
    stats = stats if stats is not None else StepStats()
    # N3: the default resolver carries this version's `stepNamespaces`; a harness resolver replaces it.
    resolve = resolve if resolve is not None else StepResolver().bind(spec)
    nodes = (
        pipeline_override
        if pipeline_override is not None
        else tuple(_resolved(resolve, ref, spec.name) for ref in spec.pipeline.steps)
    )
    split_at = [index for index, node in enumerate(nodes) if isinstance(node, Splitter)]
    if len(split_at) > 1:
        raise ChannelSpecError(f"channel {safe_token(spec.name)}: at most one splitter per pipeline")
    splitter: Splitter | None = None
    head_nodes: tuple[Node, ...] = nodes
    tail_nodes: tuple[Node, ...] = ()
    if split_at:
        if spec.ordered:
            raise ChannelSpecError(
                f"channel {safe_token(spec.name)}: a splitter is refused on a per-key channel "
                "(README 3.3.2 rule 3): children would inherit one patient's key and could be overtaken. "
                "Split at ingest, where each message is keyed on its own"
            )
        index = split_at[0]
        node = nodes[index]
        if isinstance(node, Splitter):
            splitter = node
        head_nodes, tail_nodes = nodes[:index], nodes[index + 1 :]

    retries = spec.pipeline.step_retries
    destinations: dict[str, PipelineExecutor] = {}
    for destination in spec.destinations:
        steps = tuple(_resolved(resolve, ref, spec.name) for ref in destination.steps)
        if any(isinstance(step, Splitter) for step in steps):
            raise ChannelSpecError(
                f"channel {safe_token(spec.name)}: destination {safe_token(destination.name)} names a splitter; "
                "split in pipeline.steps (README 3.3.2)"
            )
        destinations[destination.name] = PipelineExecutor(
            _steps(steps), step_retries=retries, stats=stats, scope=destination.name
        )
    return ChannelPlan(
        channel=spec.name,
        version=spec.version,
        head=PipelineExecutor(_steps(head_nodes), step_retries=retries, stats=stats),
        splitter=splitter,
        tail=PipelineExecutor(_steps(tail_nodes), step_retries=retries, stats=stats),
        destinations=destinations,
        step_retries=retries,
        stats=stats,
    )


def _resolved(resolve: Resolver, reference: StepRef, channel: str) -> Node:
    """Resolve one step and name it as the spec does.

    The spec's `name` is unique within its list and is what an operator reads in the spec, so timing,
    errors and logs use it rather than the function's name — two uses of one step stay distinguishable.
    """
    try:
        node = resolve(reference)
    except ChannelSpecError:
        raise
    except StepDefinitionError as exc:  # a resolver other than load_step, e.g. in a test harness
        raise ChannelSpecError(f"channel {safe_token(channel)}: step {safe_token(reference.name)}: {exc}") from None
    except Exception as exc:  # anything else a resolver raises refuses the spec rather than the worker
        raise ChannelSpecError(
            f"channel {safe_token(channel)}: step {safe_token(reference.name)}: {type(exc).__name__}"
        ) from None
    if node.name == reference.name:
        return node
    named = copy.copy(node)
    named.name = reference.name
    return named


def _steps(nodes: tuple[Node, ...]) -> tuple[Step, ...]:
    return tuple(node for node in nodes if isinstance(node, Step))
