"""The engine's own steps, and the check that the contract's example is actually runnable.

The last test here is the one that matters: `qa-engineer-2` found the walking-skeleton example pointing
at `adt_flow.steps.passthrough`, a module nothing in the repo publishes, so the spec was schema-valid
and unrunnable. Schema conformance cannot catch that — resolving the steps can, which is why this test
loads them through the shipping loader rather than asserting on the JSON.
"""

from __future__ import annotations

import json
import re
from importlib.metadata import entry_points
from pathlib import Path

import pytest
from tributary.runtime.core_steps import passthrough
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import ChannelSpec, StepRef
from tributary.runtime.steps import ENTRY_POINT_GROUP, load_step, load_steps
from tributary.sdk import Message, Step
from tributary.sdk.testing import make_message

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"
PASSTHROUGH = "tributary.core.passthrough"


def test_passthrough_is_registered_under_its_contract_name() -> None:
    """The entry-point NAME is what a spec writes, so the name is the contract, not the module path.

    Asserted through `importlib.metadata` rather than by importing the module: the thing that unblocks
    Compose is the installed entry point, and an import would pass even with the pyproject wiring
    missing.
    """
    registered = {point.name: point.value for point in entry_points(group=ENTRY_POINT_GROUP)}
    assert registered[PASSTHROUGH] == "tributary.runtime.core_steps:passthrough"


def test_passthrough_resolves_through_the_step_loader() -> None:
    loaded = load_step(StepRef(name="passthrough", uses=PASSTHROUGH))
    assert isinstance(loaded, Step)
    assert loaded.takes_ctx is False


def test_passthrough_changes_nothing_at_all() -> None:
    """Identity means identity: same headers, same payload, same key, same object."""
    message = make_message(b"synthetic-payload", key=b"SYNTH-KEY-1")
    result = passthrough(message)

    assert result is message
    assert isinstance(result, Message)
    assert result.headers == message.headers
    assert result.payload == message.payload
    assert result.key == message.key


def test_an_unregistered_uses_name_is_a_channel_error_not_a_crash() -> None:
    """A spec naming a step the engine does not provide stops that channel, loudly, with its name."""
    with pytest.raises(ChannelSpecError, match=re.escape("tributary.core.nonexistent")):
        load_step(StepRef(name="missing", uses="tributary.core.nonexistent"))


def test_a_ref_into_a_bundle_that_is_not_installed_fails_loudly() -> None:
    """Why `ref` is documented rather than schema-rejected (channel-spec 3.3.1): it cannot silently
    half-work. Bundle fetch does not exist in wave 1, so this is the honest state of that path."""
    with pytest.raises(ChannelSpecError, match="adt_flow"):
        load_step(StepRef(name="passthrough", ref="adt_flow.steps.passthrough"))


def test_the_walking_skeleton_example_resolves_to_a_runnable_pipeline() -> None:
    """The regression for `qa-engineer-2`'s finding.

    A spec can be schema-valid and unrunnable — every step name resolvable by the validator and none of
    them importable. This loads the contract example's steps through the shipping loader and then runs
    them, so "the example works" is checked rather than assumed.
    """
    document = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": 1}
    spec = ChannelSpec.from_document(document)

    steps = load_steps(spec.pipeline)

    assert [step.name for step in steps] == [reference.name for reference in spec.pipeline.steps]
    message = make_message(b"synthetic", key=b"SYNTH-KEY-1", channel=spec.name)
    for node in steps:
        assert isinstance(node, Step)
        outcome = node(message)
        assert isinstance(outcome, Message)
        message = outcome


MANIFEST = EXAMPLES.parent / "manifest.json"


def manifest_entries() -> list[tuple[str, bool, tuple[str, ...]]]:
    """`(file, runnable, requires)` per valid example, from the manifest that owns the classification.

    Read rather than duplicated: this test and the contract's README were both asserting which examples
    run today, and an allow-list in a test drifts from the contract that owns the examples — the
    architect would have no reason to remember this file exists when adding a wave-3 example.
    """
    document = json.loads(MANIFEST.read_text(encoding="utf-8"))
    entries = []
    for entry in document["valid"]:
        name = Path(str(entry["file"])).name
        entries.append((name, bool(entry["runnable"]), tuple(entry.get("requires", ()))))
    assert entries, "no valid examples in the manifest: the corpus test would pass vacuously"
    return entries


def resolvable(spec: ChannelSpec) -> tuple[bool, list[str]]:
    """Whether every step in a spec resolves **and binds its `with`** in this distribution.

    Destination steps (README 3.5) count: a runnable example whose destination step does not resolve
    would fail at load exactly like a pipeline step. Binding counts too, because since README 3.3 set 1.1
    a step whose `with` does not bind is refused at load, and "resolves" without it would be a claim
    the worker then contradicts.
    """
    unresolved: list[str] = []
    references = [*spec.pipeline.steps, *(ref for destination in spec.destinations for ref in destination.steps)]
    for reference in references:
        try:
            load_step(reference, namespaces=spec.pipeline.step_namespaces)  # N3: the spec's own bindings
        except ChannelSpecError:
            unresolved.append(reference.uses or reference.ref or reference.name)
    return not unresolved, unresolved


def load_spec(name: str) -> ChannelSpec:
    document = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": 1}
    return ChannelSpec.from_document(document)


def classification_failure(
    name: str,
    *,
    runnable: bool,
    requires: tuple[str, ...],
    everything_resolves: bool,
    unresolved: list[str],
) -> str | None:
    """Apply the manifest's `runnable_note` rule. Returns the failure message, or `None` when consistent.

    A function rather than inline asserts so both halves can be driven with fabricated inputs below.
    Mutating the real manifest to prove the rule fires would mean writing to a file the architect owns,
    which is not mine to touch even temporarily.
    """
    if runnable:
        if not everything_resolves:
            return f"{name} is marked runnable but these steps resolve to nothing: {unresolved}"
        return None
    if not requires:
        return f"{name} is marked not runnable but lists no requires"
    if everything_resolves:
        return (
            f"{name} now resolves completely: promote it to runnable: true in the manifest and drop "
            f"requires {list(requires)}"
        )
    return None


@pytest.mark.parametrize(("name", "runnable", "requires"), manifest_entries())
def test_the_manifest_s_runnable_classification_matches_reality(
    name: str, runnable: bool, requires: tuple[str, ...]
) -> None:
    """Both halves of the manifest's `runnable_note`, applied to the real corpus.

    `runnable: true` with a step that stops resolving is the defect qa-engineer-2 found: schema-valid,
    unrunnable, invisible to a checker that validates documents in isolation.

    `runnable: false` with **everything** resolving is the opposite failure and the one more likely to go
    unnoticed — the exemption has outlived the gap it describes, which is indistinguishable from
    coverage.
    """
    everything_resolves, unresolved = resolvable(load_spec(name))
    failure = classification_failure(
        name,
        runnable=runnable,
        requires=requires,
        everything_resolves=everything_resolves,
        unresolved=unresolved,
    )
    assert failure is None, failure


@pytest.mark.parametrize(
    ("runnable", "requires", "everything_resolves", "expect_failure", "why"),
    [
        (True, (), True, False, "runnable and it resolves: consistent"),
        (True, (), False, True, "runnable but a step resolves to nothing — qa-engineer-2's defect"),
        (False, ("tributary.hl7v2. (W2-1)",), False, False, "illustrative and still unresolvable: consistent"),
        (False, ("tributary.hl7v2. (W2-1)",), True, True, "a stale exemption outliving the gap it describes"),
        (False, (), False, True, "not runnable and no requires: the classification says nothing"),
    ],
)
def test_the_classification_rule_fires_on_both_kinds_of_drift(
    runnable: bool, requires: tuple[str, ...], everything_resolves: bool, expect_failure: bool, why: str
) -> None:
    """The mutation control for the test above, driven with fabricated inputs.

    Without this, the corpus test passes today for the trivial reason that the manifest happens to be
    correct, and nobody would learn whether it can go red at all.
    """
    failure = classification_failure(
        "example.json",
        runnable=runnable,
        requires=requires,
        everything_resolves=everything_resolves,
        unresolved=[] if everything_resolves else ["tributary.hl7v2.parse"],
    )
    assert (failure is not None) is expect_failure, f"{why}: got {failure!r}"


def test_the_corpus_has_at_least_one_runnable_example() -> None:
    """Compose and the e2e demo need one, and "every example is illustrative" would pass the test above
    while leaving nothing that actually runs."""
    assert [name for name, runnable, _requires in manifest_entries() if runnable]
