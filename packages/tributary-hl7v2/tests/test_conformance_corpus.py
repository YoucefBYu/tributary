"""Conformance corpus: every fixture in `tests/fixtures/hl7/` parses, round-trips, and is synthetic.

`tests/fixtures/hl7/` is this package's corpus (`docs/agent-team/OWNERSHIP.md`), covering the shapes
the accessor and delimiter code is meant to handle: standard and custom delimiters, escaped characters,
repeating segments and fields, and an optional segment absent altogether.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from tributary.hl7v2.message import Hl7Message

FIXTURES = Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "hl7"


def _fixture_files() -> list[Path]:
    if not FIXTURES.is_dir():
        return []
    return sorted(FIXTURES.glob("*.hl7"))


@pytest.mark.parametrize("path", _fixture_files(), ids=lambda p: p.name)
def test_fixture_parses_without_error(path: Path) -> None:
    Hl7Message.parse(path.read_bytes())


@pytest.mark.parametrize("path", _fixture_files(), ids=lambda p: p.name)
def test_fixture_round_trips_byte_identically(path: Path) -> None:
    raw = path.read_bytes()
    assert Hl7Message.parse(raw).to_bytes() == raw


@pytest.mark.parametrize("path", _fixture_files(), ids=lambda p: p.name)
def test_fixture_is_synthetic(path: Path) -> None:
    """Belt and braces alongside the `guard-content.sh` pre-write hook: every committed fixture's
    `MSH-3` is `TRIBUTARY-SYNTH`, checked again here so a fixture added by a future non-interactive
    process (not routed through the Write/Edit hook) cannot slip a non-synthetic sample into the corpus.
    """
    msg = Hl7Message.parse(path.read_bytes())
    assert msg["MSH.3"] == "TRIBUTARY-SYNTH"


def test_corpus_is_not_empty() -> None:
    assert len(_fixture_files()) >= 5


def test_corpus_covers_repeating_segments_and_custom_delimiters() -> None:
    names = {p.name for p in _fixture_files()}
    assert "custom_delimiters.hl7" in names
    assert "escaped_characters.hl7" in names
    assert "adt_a01_two_nk1.hl7" in names
    assert "oru_r01_repeating_obx.hl7" in names
    assert "adt_a08_update_no_pv1.hl7" in names
