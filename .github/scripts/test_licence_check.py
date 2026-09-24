"""Unit tests for the licence policy (a check that cannot fail is not a check)."""

import importlib.metadata as md
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import licence_check as lc


class FakeDist(md.Distribution):
    def __init__(self, name: str, **fields: str) -> None:
        lines = [f"Name: {name}", "Version: 1"] + [f"{k}: {v}" for k, v in fields.items()]
        self._text = "\n".join(lines)

    def read_text(self, filename: str) -> str | None:
        return self._text if filename == "METADATA" else None

    def locate_file(self, path: str) -> Path:
        return Path(path)


@pytest.mark.parametrize("expr", ["MIT", "Apache-2.0 OR BSD-2-Clause", "BSD-3-Clause", "ISC", "PSF-2.0"])
def test_allowed(expr: str) -> None:
    assert lc.classify("x", FakeDist("x", **{"License-Expression": expr}))[0] == "ok"


@pytest.mark.parametrize(
    "expr", ["GPL-3.0-only", "AGPL-3.0-or-later", "SSPL-1.0", "BUSL-1.1", "LGPL-2.1", "Proprietary"]
)
def test_denied(expr: str) -> None:
    assert lc.classify("x", FakeDist("x", **{"License-Expression": expr}))[0] == "fail"


def test_and_expression_needs_every_term() -> None:
    assert lc.classify("x", FakeDist("x", **{"License-Expression": "MIT AND GPL-2.0"}))[0] == "fail"


def test_missing_metadata_fails() -> None:
    assert lc.classify("x", FakeDist("x"))[0] == "fail"


def test_mpl_only_for_the_approved_set_in_its_approved_scope() -> None:
    mpl = {"License-Expression": "MPL-2.0"}
    # orjson is approved "runtime": it must be in the runtime closure to pass.
    assert lc.classify("orjson", FakeDist("orjson", **mpl), runtime_names=frozenset({"orjson"}))[0] == "mpl-approved"
    # hypothesis, pathspec, certifi are approved "non-runtime": absent from the runtime closure.
    for name in ("hypothesis", "pathspec", "certifi"):
        assert lc.classify(name, FakeDist(name, **mpl), runtime_names=frozenset())[0] == "mpl-approved"
    assert lc.classify("newthing", FakeDist("newthing", **mpl))[0] == "fail"


def test_mpl_approval_does_not_transfer_across_scope() -> None:
    """The approval answered a specific question (is it in the runtime closure or not); a package
    that moves across that line needs a fresh decision, not automatic coverage by its old approval."""
    mpl = {"License-Expression": "MPL-2.0"}
    # orjson approved "runtime" but not actually in the runtime closure this run: fails.
    status, detail = lc.classify("orjson", FakeDist("orjson", **mpl), runtime_names=frozenset())
    assert status == "fail"
    assert "moved" in detail
    # certifi approved "non-runtime" but now present in the runtime closure: fails.
    status, detail = lc.classify("certifi", FakeDist("certifi", **mpl), runtime_names=frozenset({"certifi"}))
    assert status == "fail"
    assert "moved" in detail


def test_orjson_compound_expression_is_still_mpl_gated() -> None:
    expr = {"License-Expression": "MPL-2.0 AND (Apache-2.0 OR MIT)"}
    assert lc.classify("orjson", FakeDist("orjson", **expr), runtime_names=frozenset({"orjson"}))[0] == "mpl-approved"
    assert lc.classify("other", FakeDist("other", **expr))[0] == "fail"


def test_lgpl_only_for_the_approved_set_in_its_approved_scope() -> None:
    lgpl = {"License-Expression": "LGPL-2.1"}
    # paramiko is approved "runtime": it must be in the runtime closure to pass.
    status, _ = lc.classify("paramiko", FakeDist("paramiko", **lgpl), runtime_names=frozenset({"paramiko"}))
    assert status == "lgpl-approved"
    # A different LGPL package is not on the allow-list: the family is not approved, only the name.
    assert lc.classify("pylibftdi", FakeDist("pylibftdi", **lgpl))[0] == "fail"


def test_lgpl_approval_does_not_transfer_across_scope() -> None:
    """Mirrors the MPL scope check: paramiko approved "runtime" but absent from the runtime closure
    this run fails, because the approval answered a different question than the one being asked now."""
    lgpl = {"License-Expression": "LGPL-2.1"}
    status, detail = lc.classify("paramiko", FakeDist("paramiko", **lgpl), runtime_names=frozenset())
    assert status == "fail"
    assert "moved" in detail


def test_agpl_is_never_covered_by_the_lgpl_allow_list() -> None:
    """LGPL_PATTERN must not swallow AGPL text even under paramiko's own name."""
    agpl = {"License-Expression": "AGPL-3.0-or-later"}
    assert lc.classify("paramiko", FakeDist("paramiko", **agpl), runtime_names=frozenset({"paramiko"}))[0] == "fail"


def test_build_requires_are_collected() -> None:
    assert any(r.startswith("hatchling") for r in lc.build_requires())


def test_runtime_closure_excludes_the_dev_group() -> None:
    """Real resolution against this repo, not a fake: this is the set `classify`'s scope check
    trusts, so a mistake here would silently defeat it rather than fail loudly."""
    names = lc.runtime_closure_names()
    assert "orjson" in names  # tributary-runtime's own dependency: must ship
    for dev_only in ("hypothesis", "pathspec", "certifi", "pytest", "ruff", "mypy", "testcontainers"):
        assert dev_only not in names
