"""The runtime's capability registry against `contracts/context-extension/v0/capabilities.json`, both ways.

`tributary.runtime.capabilities` is a copy so the worker needs no `contracts/` on disk; this test is what
stops the copy drifting. A name in the contract but not the runtime would refuse every spec using it; a
name in the runtime but not the contract would load an unreviewed plugin (context-extension/v0 section 4).
"""

from __future__ import annotations

import json
import re
from pathlib import Path

from tributary.runtime.capabilities import CAPABILITIES, ENTRY_POINT_GROUP, NAME_PATTERN, normalise_distribution

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
REGISTRY = json.loads((CONTRACTS / "context-extension" / "v0" / "capabilities.json").read_text(encoding="utf-8"))
SCHEMA = json.loads((CONTRACTS / "channel-spec" / "v0" / "channel.schema.json").read_text(encoding="utf-8"))


def test_every_contract_capability_is_in_the_runtime_with_its_distribution() -> None:
    contract = {name: entry["distribution"] for name, entry in REGISTRY["capabilities"].items()}
    assert {name: normalise_distribution(dist) for name, dist in contract.items()} == dict(CAPABILITIES)


def test_every_runtime_capability_is_in_the_contract() -> None:
    assert set(CAPABILITIES) <= set(REGISTRY["capabilities"])


def test_the_group_and_name_grammar_match_the_contract() -> None:
    assert REGISTRY["entry_point_group"] == ENTRY_POINT_GROUP
    assert NAME_PATTERN.pattern == REGISTRY["name_pattern"].removeprefix("^").removesuffix("$")
    assert all(re.fullmatch(REGISTRY["name_pattern"].strip("^$"), name) for name in CAPABILITIES)


def test_the_channel_spec_enum_is_the_runtime_registry() -> None:
    """The parser refuses a name the runtime cannot resolve; the schema must refuse exactly the same."""
    enum = SCHEMA["$defs"]["pipeline"]["properties"]["context"]["items"]["enum"]
    assert sorted(enum) == sorted(CAPABILITIES)


def test_pep503_normalisation() -> None:
    assert normalise_distribution("Tributary_FHIR") == "tributary-fhir"
    assert normalise_distribution("tributary.._-fhir") == "tributary-fhir"
