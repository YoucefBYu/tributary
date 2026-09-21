"""Fail if the root .gitignore stops ignoring the sensitive paths (Phase 0 High 1).

Uses ``git check-ignore`` so the check tests the effective behaviour, not the file text.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MUST_IGNORE = [
    "samples/mirth/export.xml",
    "samples/mirth/nested/channel.xml",
    ".env",
    ".env.local",
    "secrets/key.pem",
    "deploy/secrets/token",
]
MUST_TRACK = ["samples/mirth/README.md"]


def ignored(path: str) -> bool:
    r = subprocess.run(["git", "check-ignore", "-q", "--no-index", path], cwd=ROOT)
    return r.returncode == 0


def main() -> int:
    bad = [p for p in MUST_IGNORE if not ignored(p)]
    bad += [f"{p} (must NOT be ignored)" for p in MUST_TRACK if ignored(p)]
    if bad:
        print("gitignore guard FAILED, sensitive paths no longer protected:", *bad, sep="\n  ", file=sys.stderr)
        return 1
    print("gitignore guard passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
