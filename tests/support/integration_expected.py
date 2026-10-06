"""The exact expected `-m integration` collection (W3-21), and the check that pins it.

`tests/support/integration_expected.json` records how many tests `uv run pytest -m integration` collects and
how many per module. CI (W3-14) fails unless the run's `passed` equals `count` and `skipped` is 0, so a branch that
deletes or silently deselects an integration test fails, instead of passing with fewer tests. `count` includes
parametrised cases and the `chaos` tests that are also marked `integration`, because that is what the marker
selects.

Usage (from the repo root):

    uv run python -m tests.support.integration_expected --check    # exit 1 and a per-module diff on drift
    uv run python -m tests.support.integration_expected --write    # regenerate the file

Whoever adds, removes or parametrises an integration test regenerates the file in the same change and messages
`qa-engineer`. Collection needs no Docker (it imports the test modules and runs nothing); a collection error is a
failure, never an empty list.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from collections import Counter
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

EXPECTED_FILE = Path(__file__).with_name("integration_expected.json")
COMMAND = ["uv", "run", "pytest", "-m", "integration", "--collect-only", "-q", "-p", "no:cacheprovider"]


def modules_from_collection(lines: Iterable[str]) -> dict[str, int]:
    """Per-module test counts from `--collect-only -q` output: each `path::name` line is one test."""
    counts: Counter[str] = Counter()
    for line in lines:
        if "::" in line and not line.startswith((" ", "=", "-")):
            counts[line.split("::", 1)[0].strip()] += 1
    return dict(sorted(counts.items()))


def expected_document(modules: Mapping[str, int]) -> dict[str, Any]:
    return {
        "marker": "integration",
        "command": "uv run pytest -m integration",
        "count": sum(modules.values()),
        "modules": dict(sorted(modules.items())),
    }


def diff(expected: Mapping[str, Any], actual_modules: Mapping[str, int]) -> list[str]:
    """Human-readable differences between the recorded document and a collection. Empty means an exact match,
    including the recorded total agreeing with the per-module sum."""
    problems: list[str] = []
    recorded: Mapping[str, int] = expected.get("modules", {})
    if expected.get("count") != sum(recorded.values()):
        problems.append(f"file is inconsistent: count {expected.get('count')} != module sum {sum(recorded.values())}")
    for module in sorted(set(recorded) | set(actual_modules)):
        before, after = recorded.get(module), actual_modules.get(module)
        if before != after:
            problems.append(f"{module}: expected {before}, collected {after}")
    total = sum(actual_modules.values())
    if expected.get("count") != total:
        problems.append(f"total: expected {expected.get('count')}, collected {total}")
    return problems


def collect(cwd: Path) -> dict[str, int]:
    result = subprocess.run(COMMAND, cwd=cwd, capture_output=True, text=True, check=False)  # noqa: S603
    if result.returncode != 0:
        raise SystemExit(
            f"collection failed (exit {result.returncode}):\n{result.stdout[-2000:]}\n{result.stderr[-2000:]}"
        )
    return modules_from_collection(result.stdout.splitlines())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--check", action="store_true")
    mode.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[2]
    actual = collect(root)
    if args.write:
        EXPECTED_FILE.write_text(json.dumps(expected_document(actual), indent=2) + "\n", encoding="utf-8")
        print(f"wrote {EXPECTED_FILE.name}: {sum(actual.values())} tests in {len(actual)} modules")
        return 0
    problems = diff(json.loads(EXPECTED_FILE.read_text(encoding="utf-8")), actual)
    for problem in problems:
        print(problem)
    print("integration collection matches" if not problems else f"{len(problems)} difference(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
