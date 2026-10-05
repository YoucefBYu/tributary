"""Fail if any pinned failure pair in contracts/ has drifted from the measured one.

Reads the JSON lines printed by `contracts/validate.py --emit-expectations` on stdin. That mode
always exits 0 (contracts/ is architect-owned), so the check lives here. Also fails on empty input
so a broken emitter cannot pass silently. Prints contract and example names only.
"""

import json
import sys


def drifted(lines: list[str]) -> tuple[int, list[str]]:
    total = 0
    bad: list[str] = []
    for line in lines:
        if not line.strip():
            continue
        row = json.loads(line)
        total += 1
        if not row.get("pinned_pair_matches") or not row.get("measured"):
            bad.append(f"{row.get('contract')}: {row.get('example')}")
    return total, bad


def main() -> int:
    total, bad = drifted(sys.stdin.read().splitlines())
    if total == 0:
        print("no expectations emitted")
        return 1
    for item in bad:
        print(f"  DRIFT {item}")
    print(f"{total} invalid examples, {len(bad)} drifted")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
