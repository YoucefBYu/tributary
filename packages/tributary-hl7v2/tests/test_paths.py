"""`paths.parse`: the `SEG[occ].field[rep].component.subcomponent` accessor grammar."""

from __future__ import annotations

import pytest
from tributary.hl7v2.errors import FieldPathError
from tributary.hl7v2.paths import MAX_INDEX, FieldPath, parse


def test_segment_only() -> None:
    assert parse("PID") == FieldPath("PID", 1, None, 1, None, None)


def test_segment_and_field() -> None:
    assert parse("PID.5") == FieldPath("PID", 1, 5, 1, None, None)


def test_field_and_component() -> None:
    assert parse("PID.5.1") == FieldPath("PID", 1, 5, 1, 1, None)


def test_field_component_and_subcomponent() -> None:
    assert parse("PID.5.1.2") == FieldPath("PID", 1, 5, 1, 1, 2)


def test_segment_occurrence() -> None:
    assert parse("NK1[2].3") == FieldPath("NK1", 2, 3, 1, None, None)


def test_field_repetition() -> None:
    assert parse("PID.3[2].1") == FieldPath("PID", 1, 3, 2, 1, None)


def test_segment_name_is_uppercased() -> None:
    assert parse("pid.5").segment == "PID"


@pytest.mark.parametrize(
    "path",
    [
        "",
        "PID.",
        "PID.0",
        "PID.5.0",
        "PID.5[0]",
        "PID[0].5",
        "PID-5-1",
        "1PID.5",
        "PID.5.1.2.3",
        "PID..5",
    ],
)
def test_invalid_paths_raise_typed_error(path: str) -> None:
    with pytest.raises(FieldPathError):
        parse(path)


def test_error_includes_the_path_text_verbatim() -> None:
    # The path is an engine-defined address, not sender data, so it is safe (and useful) to quote back.
    with pytest.raises(FieldPathError) as caught:
        parse("PID.0")
    assert "PID.0" in str(caught.value)


# -- security/DoS regression: every numeric index is bounded ---------------------------------------


def test_max_index_is_accepted() -> None:
    assert parse(f"PID.{MAX_INDEX}").field == MAX_INDEX


@pytest.mark.parametrize(
    "path",
    ["PID[10000].5", "PID.10000", "PID.3[10000]", "PID.5.10000", "PID.5.1.10000", "PID.99999999999"],
)
def test_an_index_over_max_index_is_rejected(path: str) -> None:
    """A code review finding: an unbounded index (say, 999999999) reaches `with_field`'s padding loops
    (`message.py`) and tries to grow a Python list to that length for a single field write."""
    with pytest.raises(FieldPathError):
        parse(path)
