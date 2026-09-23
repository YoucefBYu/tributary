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
    for step in steps:
        outcome = step(message)
        assert isinstance(outcome, Message)
        message = outcome


#: Step namespaces that no package provides yet, with the wave that will. An example may reference
#: these — demonstrating a channel you will be able to author — but a name outside both this set and the
#: installed entry points is a typo or a reference to something nobody is building.
UNIMPLEMENTED_NAMESPACES = {
    "tributary.hl7v2.": "tributary-hl7v2, W2-1",
    "tributary.fhir.": "tributary-fhir, W2-2",
}


@pytest.mark.parametrize("name", [path.name for path in sorted(EXAMPLES.glob("*.json"))])
def test_every_uses_name_in_the_corpus_either_resolves_or_names_a_known_future_package(name: str) -> None:
    """The generalised form of `qa-engineer-2`'s finding.

    A `uses` name can be schema-valid and resolve to nothing, which is how the walking-skeleton example
    shipped unrunnable. Three other examples reference `tributary.hl7v2.*`, which is legitimate — they
    illustrate channels wave 2 will make authorable — so the assertion is not "everything resolves" but
    "everything resolves **or** names a namespace someone is building". That distinction is what makes
    this test catch a typo (`tributary.core.passthrogh`) instead of just tracking build order.

    `ref` names are skipped: they need bundle fetch, which wave 1 does not have (channel-spec 3.3.1).
    """
    document = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    document["metadata"] = {**document["metadata"], "version": 1}
    spec = ChannelSpec.from_document(document)

    for reference in spec.pipeline.steps:
        if reference.uses is None:
            continue
        pending = [owner for prefix, owner in UNIMPLEMENTED_NAMESPACES.items() if reference.uses.startswith(prefix)]
        if pending:
            with pytest.raises(ChannelSpecError):
                load_step(reference)
            continue
        assert isinstance(load_step(reference), Step), (
            f"{name}: {reference.uses} resolves to nothing and is not a known future namespace"
        )


def test_the_future_namespace_list_stays_honest() -> None:
    """A mutation control on the exemption above: the moment `tributary-hl7v2` ships its entry points,
    this fails and the exemption has to be deleted rather than quietly masking a real gap."""
    registered = {point.name for point in entry_points(group=ENTRY_POINT_GROUP)}
    for prefix, owner in UNIMPLEMENTED_NAMESPACES.items():
        assert not [name for name in registered if name.startswith(prefix)], (
            f"{owner} now registers steps under {prefix}: drop it from UNIMPLEMENTED_NAMESPACES"
        )
