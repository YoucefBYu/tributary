"""Regression: a loader's log component is `loader-<type>`, never `loader-<destination>`. The component is capped at
32 characters (`contracts/observability/v0/log-line.schema.json`), so a long destination name made `JsonLogger`
replace every line with `log_refused`. The destination is its own field (`base={"destination": ...}`)."""

from __future__ import annotations

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "src" / "tributary" / "loaders"


def test_no_loader_builds_its_component_from_the_destination_name() -> None:
    offenders = [
        str(path.relative_to(SRC))
        for path in SRC.rglob("*.py")
        if re.search(r"JsonLogger\(\s*f[\"']", path.read_text(encoding="utf-8"))
    ]
    assert offenders == []


def test_every_loader_component_fits_the_schema_limit() -> None:
    components = {
        m
        for path in SRC.rglob("main.py")
        for m in re.findall(r"JsonLogger\(\"(loader-[a-z0-9]+)\"", path.read_text(encoding="utf-8"))
    }
    assert len(components) == 8 and all(re.fullmatch(r"[a-z][a-z0-9_-]{0,31}", c) for c in components)
