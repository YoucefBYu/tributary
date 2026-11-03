"""ADR-010 section 3: the prefix `tb-` is reserved in the channel position, so no channel can be named
`tb-archiver` (or any `tb-*`). The rule comes from the contract schema, which the validator loads; the
control plane holds no copy of the pattern."""

from __future__ import annotations

import copy
import json
from collections.abc import Callable
from pathlib import Path

import pytest
from api_kit import REPO, Contract, Harness, Idp, bearer, run
from tributary.api import validation

EXAMPLE = REPO / "contracts/channel-spec/v0/examples/valid/walking-skeleton-loopback.json"
Make = Callable[..., Harness]


def doc(name: str) -> dict[str, object]:
    d = copy.deepcopy(json.loads(EXAMPLE.read_text()))
    d["metadata"] = {"name": name}
    return d  # type: ignore[no-any-return]


@pytest.mark.parametrize("name", ["tb-archiver", "tb-x"])
def test_create_and_validate_refuse_a_reserved_name(
    make_harness: Make, idp: Idp, contract: Contract, name: str
) -> None:
    async def go() -> None:
        async with make_harness().running() as c:
            h = bearer(idp.mint(roles=["tb-admin"]))
            created = await c.post("/v1/channels", json={"document": doc(name), "origin": {"kind": "api"}}, headers=h)
            contract.check("POST", "/v1/channels", created)
            assert (created.status_code, created.json()["code"]) == (422, "SPEC_INVALID")
            assert {"json_path": "$.metadata.name", "validator": "pattern"} in created.json()["errors"]
            report = await c.post("/v1/validate", json=doc(name), headers=h)
            assert report.status_code == 200 and report.json()["valid"] is False
            assert {"json_path": "$.metadata.name", "validator": "pattern"} in report.json()["errors"]

    run(go())


def test_the_rule_is_the_contract_schemas_not_a_copy() -> None:
    assert "(?!tb-)" in (validation.find_contracts_dir() / validation.SCHEMA_RELATIVE).read_text()
    assert validation.validate_document(doc("tb-archiver"))
    assert not validation.validate_document(doc("archiver"))
    assert "(?!" not in Path(validation.__file__).read_text()  # no hand-copied pattern
