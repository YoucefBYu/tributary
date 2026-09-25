"""`tributary.sdk.codes` against `contracts/envelope/v0/error-codes.json`, in both directions.

Envelope README 2.3.2 rule 3: the contract is the source and the code is checked against it; neither
is generated from the other. So each direction is its own assertion: a code only in `codes.py` is an
engine code nobody reviewed, and a code only in the contract is one a channel's `Reject` could borrow.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.sdk import Reject, RejectCodeError
from tributary.sdk.codes import ENGINE_CODES, RESERVED_PREFIXES, is_engine_code

REGISTRY = Path(__file__).resolve().parents[3] / "contracts" / "envelope" / "v0" / "error-codes.json"


def registry() -> dict[str, object]:
    document = json.loads(REGISTRY.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def contract_codes() -> set[str]:
    document = registry()
    return {entry["code"] for entry in document["codes"]} | {entry["code"] for entry in document["log_only"]}  # type: ignore[attr-defined]


def contract_prefixes() -> set[str]:
    return {entry["prefix"] for entry in registry()["reserved_prefixes"]}  # type: ignore[attr-defined]


def test_every_sdk_engine_code_is_registered_in_the_contract() -> None:
    assert set(ENGINE_CODES) - contract_codes() == set(), "add these to error-codes.json through the architect"


def test_every_registered_code_is_known_to_the_sdk() -> None:
    assert contract_codes() - set(ENGINE_CODES) == set(), "a channel's Reject could borrow these"


def test_reserved_prefixes_match_in_both_directions() -> None:
    assert set(RESERVED_PREFIXES) - contract_prefixes() == set()
    assert contract_prefixes() - set(RESERVED_PREFIXES) == set()
    assert len(RESERVED_PREFIXES) == len(set(RESERVED_PREFIXES))


@pytest.mark.parametrize("prefix", sorted(contract_prefixes()))
def test_a_member_of_every_reserved_family_is_refused_to_a_step(prefix: str) -> None:
    code = f"{prefix}SOMETHING"
    assert is_engine_code(code)
    with pytest.raises(RejectCodeError):
        Reject(code)
