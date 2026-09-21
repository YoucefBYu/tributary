"""Scan git-tracked text files for committed secrets (CLAUDE.md: no secrets in the repo).

Pattern based, dependency free. Deliberately narrow so a hit is worth reading. To allow a known
false positive (for example a documented dummy value), add ``tb-secret-scan: allow`` on the line.
Findings print file:line and the rule name only, never the matched text (invariant 6 spirit:
a scanner must not re-leak what it finds into CI logs).
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RULES = {
    "private-key": re.compile(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |PGP |ENCRYPTED )?PRIVATE KEY"),
    "aws-access-key-id": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    "github-token": re.compile(r"\bgh[pousr]_[A-Za-z0-9]{36,}\b"),
    "slack-token": re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    "generic-api-key": re.compile(r"\bsk-[A-Za-z0-9]{32,}\b"),
    "url-with-password": re.compile(
        r"\b[a-z][a-z0-9+.-]*://[^\s:/@]+:(?!\$|\{|<|%)[^\s@/$]{6,}@(?!\S*(?:example|localhost))"
    ),
    "hard-coded-secret": re.compile(
        r"(?i)\b(?:password|passwd|secret|api[_-]?key|token)\b\s*[:=]\s*[\"'](?!\s*[\"'])(?!\$|\{|<|changeme|dev-only|example|test|dummy|xxx|\*)[^\"']{12,}[\"']"
    ),
}
SKIP_SUFFIXES = {".lock", ".png", ".jpg", ".ico", ".pdf", ".gz", ".zip", ".hl7"}
SKIP_PARTS = {"uv.lock", "pnpm-lock.yaml"}
# Deliberately-invalid contract examples (for instance a claim-check URI with userinfo) that the
# conformance suite proves are rejected.
SKIP_PREFIXES = ("contracts/",)


def tracked_files() -> list[Path]:
    out = subprocess.run(
        ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        check=True,
        capture_output=True,
    ).stdout.decode()
    return [ROOT / p for p in out.split("\0") if p]


def main() -> int:
    hits: list[str] = []
    for path in tracked_files():
        if path.suffix in SKIP_SUFFIXES or path.name in SKIP_PARTS or path.name == "secret_scan.py":
            continue
        rel = path.relative_to(ROOT).as_posix()
        if rel.startswith(SKIP_PREFIXES) and "/examples/invalid/" in rel:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        for n, line in enumerate(text.splitlines(), 1):
            if "tb-secret-scan: allow" in line:
                continue
            for rule, rx in RULES.items():
                if rx.search(line):
                    hits.append(f"{path.relative_to(ROOT).as_posix()}:{n}: {rule}")
    if hits:
        print("SECRET SCAN FAILED (values not shown):", *hits, sep="\n  ", file=sys.stderr)
        return 1
    print("secret scan passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
