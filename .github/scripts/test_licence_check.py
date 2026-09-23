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


def test_mpl_only_for_the_approved_set() -> None:
    mpl = {"License-Expression": "MPL-2.0"}
    for name in ("orjson", "hypothesis", "pathspec", "certifi"):
        assert lc.classify(name, FakeDist(name, **mpl))[0] == "mpl-approved"
    assert lc.classify("newthing", FakeDist("newthing", **mpl))[0] == "fail"


def test_orjson_compound_expression_is_still_mpl_gated() -> None:
    expr = {"License-Expression": "MPL-2.0 AND (Apache-2.0 OR MIT)"}
    assert lc.classify("orjson", FakeDist("orjson", **expr))[0] == "mpl-approved"
    assert lc.classify("other", FakeDist("other", **expr))[0] == "fail"


def test_build_requires_are_collected() -> None:
    assert any(r.startswith("hatchling") for r in lc.build_requires())
