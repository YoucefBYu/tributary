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
"""

from __future__ import annotations

import copy
import importlib
import sys
from collections.abc import Callable
from importlib.metadata import entry_points

from tributary.sdk import Splitter, Step
from tributary.sdk.errors import StepDefinitionError, safe_token

from .errors import ChannelSpecError
from .executor import ChannelPlan, PipelineExecutor, StepStats
from .spec import ChannelSpec, Pipeline, StepRef

ENTRY_POINT_GROUP = "tributary.steps"

Node = Step | Splitter
Resolver = Callable[[StepRef], Node]


def load_step(reference: StepRef) -> Node:
    """Resolve one step and bind its `with`. Raises `ChannelSpecError`, which stops the channel, not the worker.

    Binding happens here, once per channel version (README 3.3): an unknown key, a key that is not a
    Python identifier, a wrongly typed value or a missing required key refuses the spec loudly, and the
    worker keeps running the previous one.
    """
    loaded = _from_entry_point(reference.uses) if reference.uses else _from_dotted_path(reference.ref)

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


def _from_entry_point(name: str | None) -> object:
    if not name:
        raise ChannelSpecError("step has no `uses` value")
    matches = [point for point in entry_points(group=ENTRY_POINT_GROUP) if point.name == name]
    if not matches:
        raise ChannelSpecError(f"no step registered as {safe_token(name)} in {ENTRY_POINT_GROUP}")
    try:
        return matches[0].load()
    except (Exception, SystemExit) as exc:  # import-time code can raise anything, even sys.exit()
        raise ChannelSpecError(f"cannot load step {safe_token(name)}: {type(exc).__name__}") from None


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
    return tuple(load_step(reference) for reference in pipeline.steps)


def build_plan(
    spec: ChannelSpec,
    *,
    resolve: Resolver = load_step,
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
