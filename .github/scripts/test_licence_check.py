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


PARAMIKO_RUNTIME = {"runtime_names": frozenset({"paramiko"}), "importer_names": frozenset({"tributary-loaders"})}


@pytest.mark.parametrize(
    "expr",
    [
        "LGPL-2.1-only AND AGPL-3.0-only",
        "LGPL-2.1 AND GPL-3.0-only",
        "LGPL-2.1 AND MPL-2.0",
        "LGPL-2.1 OR SSPL-1.0",
    ],
)
def test_lgpl_compound_expressions_are_rejected_not_short_circuited(expr: str) -> None:
    """Regression for HIGH on 1c01120 (security-reviewer): the old `classify` searched a whole
    descriptor string for "does it contain lgpl anywhere" and approved the *entire* string on that one
    hit, so every one of these four -- an AND-ed denied term, an AND-ed different denied family, an
    AND-ed unapproved MPL, and an OR-ed denied term -- passed as "lgpl-approved" for paramiko even
    though the licence actually offered (or required) something this repo has not cleared. Confirmed
    against the pre-fix code (commit 1c01120) before writing this test: all four returned
    "lgpl-approved" there; this asserts each now fails, so a regression back to the old behaviour turns
    this test red."""
    dist = FakeDist("paramiko", **{"License-Expression": expr})
    assert lc.classify("paramiko", dist, **PARAMIKO_RUNTIME)[0] == "fail"


@pytest.mark.parametrize("name", ["orjson", "certifi"])
def test_mpl_compound_expression_with_an_unapproved_extra_term_is_rejected(name: str) -> None:
    """Regression for the MPL branch the LGPL fix above was modelled on (architect/lead asked to have
    it checked too): "MPL-2.0 AND GPL-3.0" turns out to already fail pre-fix, because DENIED matches
    "GPL-3.0" before the MPL short-circuit is ever reached -- not a real demonstration of the bug.
    "MPL-2.0 AND Proprietary" is: DENIED does not match "Proprietary", so the old code fell straight
    into `"mpl" in x.lower()` on the *whole* string and returned "mpl-approved" for both orjson and
    certifi, without ever checking the AND-ed extra term. Confirmed against the pre-fix code: both
    returned "mpl-approved"; this asserts both now fail, so a regression back to the short-circuit
    turns this test red."""
    dist = FakeDist(name, **{"License-Expression": "MPL-2.0 AND Proprietary"})
    assert lc.classify(name, dist, runtime_names=frozenset({"orjson"}))[0] == "fail"


def test_lgpl_importer_scope_rejects_an_unapproved_importer() -> None:
    """MEDIUM on 1c01120: the approval named tributary-loaders and tributary-receivers specifically,
    not "paramiko anywhere in the workspace". A third member pulling it in must still fail even though
    paramiko's own name and runtime scope are unchanged."""
    lgpl = {"License-Expression": "LGPL-2.1"}
    ok_importers = frozenset({"tributary-loaders", "tributary-receivers"})
    assert (
        lc.classify(
            "paramiko", FakeDist("paramiko", **lgpl), runtime_names=frozenset({"paramiko"}), importer_names=ok_importers
        )[0]
        == "lgpl-approved"
    )
    status, detail = lc.classify(
        "paramiko",
        FakeDist("paramiko", **lgpl),
        runtime_names=frozenset({"paramiko"}),
        importer_names=ok_importers | {"tributary-archiver"},
    )
    assert status == "fail"
    assert "tributary-archiver" in detail


def test_build_requires_are_collected() -> None:
    assert any(r.startswith("hatchling") for r in lc.build_requires())


def test_runtime_closure_excludes_the_dev_group() -> None:
    """Real resolution against this repo, not a fake: this is the set `classify`'s scope check
    trusts, so a mistake here would silently defeat it rather than fail loudly."""
    names = lc.runtime_closure_names()
    assert "orjson" in names  # tributary-runtime's own dependency: must ship
    for dev_only in ("hypothesis", "pathspec", "certifi", "pytest", "ruff", "mypy", "testcontainers"):
        assert dev_only not in names


def test_workspace_members_lists_known_packages() -> None:
    members = lc.workspace_members()
    assert {"tributary-sdk", "tributary-runtime", "tributary-loaders", "tributary-receivers"} <= set(members)


def test_importers_of_is_bounded_by_workspace_members() -> None:
    """Real resolution against this repo: `confluent-kafka` is a direct dependency of at least one
    member, so this is not vacuously true, and every importer found must be an actual workspace
    member -- the set `classify`'s per-importer scope check trusts."""
    importers = lc.importers_of("confluent-kafka")
    assert importers
    assert importers <= set(lc.workspace_members())


def test_resolve_covers_a_member_the_dev_group_does_not_yet_list() -> None:
    """Regression for the gap healthcare-standards-engineer hit landing tributary-fhir: the old
    `resolve()` called `uv export --all-groups` with no `--all-packages`, so a workspace member's own
    runtime dependencies (fhir.resources, fhir-core) were invisible to this gate until the member was
    separately added to the root dev group -- confirmed by running that exact old export against this
    repo before writing this test: no fhir-resources or fhir-core line. `--all-packages` fixes it."""
    reqs = lc._export("--all-groups", "--all-packages")
    names = {lc._norm(r.split("==", 1)[0]) for r in reqs}
    assert "fhir-resources" in names
    assert "fhir-core" in names
