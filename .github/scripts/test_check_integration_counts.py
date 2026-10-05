"""Tests for check_integration_counts.py (no Kafka, no Docker)."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import check_integration_counts as c


def _junit(tmp: Path, tests: int, failures: int = 0, errors: int = 0, skipped: int = 0) -> Path:
    path = tmp / "j.xml"
    path.write_text(
        f'<testsuites><testsuite tests="{tests}" failures="{failures}" errors="{errors}" skipped="{skipped}"/>'
        "</testsuites>"
    )
    return path


def _expected(tmp: Path, count: int) -> Path:
    path = tmp / "e.json"
    path.write_text(json.dumps({"count": count}))
    return path


def test_exact_match_passes(tmp_path: Path) -> None:
    assert c.main([str(_junit(tmp_path, 5)), str(_expected(tmp_path, 5))]) == 0


def test_fewer_passed_fails(tmp_path: Path) -> None:
    assert c.main([str(_junit(tmp_path, 4)), str(_expected(tmp_path, 5))]) == 1


def test_skipped_fails_even_when_passed_matches(tmp_path: Path) -> None:
    assert c.main([str(_junit(tmp_path, 6, skipped=1)), str(_expected(tmp_path, 5))]) == 1


def test_failure_fails(tmp_path: Path) -> None:
    assert c.main([str(_junit(tmp_path, 5, failures=1)), str(_expected(tmp_path, 4))]) == 1
