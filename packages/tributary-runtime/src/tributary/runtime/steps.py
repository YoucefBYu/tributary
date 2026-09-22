"""Loading a channel's steps from its spec.

`ref` is a dotted path inside the bundle (`adt_flow.steps.passthrough`); `uses` is a step registered
through the `tributary.steps` entry point. The spec carries exactly one of them and never inline code,
because inline source cannot be imported by pytest, reviewed in a diff or type-checked — invariant 7
(channel-spec contract section 7).

Steps are trusted code (ADR-001 section 8). Importing a module executes it, so this is a
remote-code-execution surface by design, and the controls are elsewhere: the bundle is digest-pinned,
and no data-plane component may write to `_tb.<env>.channels` (`contracts/topics/v0` 3.4).
"""

from __future__ import annotations

import importlib
from importlib.metadata import entry_points

from tributary.sdk import Step
from tributary.sdk.errors import safe_token

from .errors import ChannelSpecError
from .spec import Pipeline, StepRef

ENTRY_POINT_GROUP = "tributary.steps"


def load_step(reference: StepRef) -> Step:
    """Resolve one step. Raises `ChannelSpecError`, which stops the channel rather than the worker."""
    loaded = _from_entry_point(reference.uses) if reference.uses else _from_dotted_path(reference.ref)

    if isinstance(loaded, Step):
        return loaded
    if callable(loaded):
        # A bundle author who forgot @step still gets a working channel; the decorator's checks are
        # applied here instead, so the signature and return contract are enforced either way.
        return Step(loaded, name=reference.name)
    raise ChannelSpecError(f"step {safe_token(reference.name)} does not resolve to a callable")


def _from_entry_point(name: str | None) -> object:
    if not name:
        raise ChannelSpecError("step has no `uses` value")
    matches = [point for point in entry_points(group=ENTRY_POINT_GROUP) if point.name == name]
    if not matches:
        raise ChannelSpecError(f"no step registered as {safe_token(name)} in {ENTRY_POINT_GROUP}")
    return matches[0].load()


def _from_dotted_path(path: str | None) -> object:
    if not path or "." not in path:
        raise ChannelSpecError(f"step ref {safe_token(str(path))} must be a dotted path module.attribute")
    module_name, _, attribute = path.rpartition(".")
    try:
        module = importlib.import_module(module_name)
    except ImportError as exc:
        raise ChannelSpecError(f"cannot import {safe_token(module_name)} for step ref: {type(exc).__name__}") from exc
    try:
        return getattr(module, attribute)
    except AttributeError as exc:
        raise ChannelSpecError(f"{safe_token(module_name)} has no attribute {safe_token(attribute)}") from exc


def load_steps(pipeline: Pipeline) -> tuple[Step, ...]:
    """Resolve every step in order. All-or-nothing: a channel with one unloadable step does not run."""
    return tuple(load_step(reference) for reference in pipeline.steps)
