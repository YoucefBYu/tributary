import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
from check_expectations import drifted


def row(ok: bool, measured: bool = True) -> str:
    return json.dumps(
        {"contract": "c", "example": "e", "pinned_pair_matches": ok, "measured": [{"x": 1}] if measured else []}
    )


def test_all_match() -> None:
    assert drifted([row(True), row(True)]) == (2, [])


def test_drift_detected() -> None:
    assert drifted([row(True), row(False)])[1] == ["c: e"]


def test_empty_measured_is_drift() -> None:
    assert drifted([row(True, measured=False)])[1] == ["c: e"]


def test_empty_input() -> None:
    assert drifted([""]) == (0, [])
