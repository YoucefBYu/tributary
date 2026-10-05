"""Fail unless a `pytest -m integration --junitxml` run passed exactly the expected number of tests.

Reads tests/support/integration_expected.json (W3-21) and the junit file. passed = tests - failures - errors -
skipped, and skipped must be 0 (Docker missing, an xfail or a skip all fail the job). The selector is
`-m integration` in both the collection and the run, so chaos or bench tests that are also marked integration
are counted identically on both sides. Prints counts only.

Usage: check_integration_counts.py JUNIT_XML [EXPECTED_JSON]
"""

from __future__ import annotations

import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

DEFAULT_EXPECTED = Path(__file__).resolve().parents[2] / "tests" / "support" / "integration_expected.json"


def counts(junit: Path) -> dict[str, int]:
    root = ET.parse(junit).getroot()  # noqa: S314 (junit from our own pytest run)
    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    total = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
    for suite in suites:
        for key in total:
            total[key] += int(suite.get(key, "0"))
    total["passed"] = total["tests"] - total["failures"] - total["errors"] - total["skipped"]
    return total


def problems(actual: dict[str, int], expected: int) -> list[str]:
    out: list[str] = []
    if actual["passed"] != expected:
        out.append(f"passed {actual['passed']} != expected {expected}")
    if actual["skipped"] != 0:
        out.append(f"skipped {actual['skipped']} != 0")
    if actual["failures"] or actual["errors"]:
        out.append(f"failures {actual['failures']}, errors {actual['errors']}")
    return out


def main(argv: list[str]) -> int:
    if not 1 <= len(argv) <= 2:
        print(__doc__)
        return 2
    expected_file = Path(argv[1]) if len(argv) == 2 else DEFAULT_EXPECTED
    expected = int(json.loads(expected_file.read_text(encoding="utf-8"))["count"])
    actual = counts(Path(argv[0]))
    print(f"integration: {actual}, expected passed {expected}")
    found = problems(actual, expected)
    for line in found:
        print(f"FAIL {line}")
    return 1 if found else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
