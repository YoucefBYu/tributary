"""Channel document validation against `channel-spec/v0/channel.schema.json` (the authority).

Issues are `json_path` plus the failing keyword, never the jsonschema message, because that message embeds
the failing value (`openapi/v0` README 2, invariant 6). Control-plane rules add their own names.
"""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator  # type: ignore[import-untyped]  # no stubs; jsonschema is MIT
from tributary.runtime.config import find_contracts_dir

SCHEMA_RELATIVE = Path("channel-spec") / "v0" / "channel.schema.json"
MAX_ISSUES = 100
_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_KEYWORD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")


@lru_cache(maxsize=1)
def _validator() -> Draft202012Validator:
    path = find_contracts_dir() / SCHEMA_RELATIVE
    return Draft202012Validator(json.loads(path.read_text(encoding="utf-8")))


def json_path(parts: Any) -> str:
    out = "$"
    for part in parts:
        if isinstance(part, int):
            out += f"[{part}]"
        elif _IDENT.match(str(part)):
            out += f".{part}"
        else:
            out += "['" + re.sub(r"[^A-Za-z0-9_.:-]", "?", str(part))[:64] + "']"  # key text is input: sanitised
    return out[:512]


def validate_document(document: dict[str, Any]) -> list[dict[str, str]]:
    """Schema issues, sorted by path, at most 100. Empty means valid."""
    issues = [
        {
            "json_path": json_path(e.absolute_path),
            "validator": str(e.validator) if _KEYWORD.match(str(e.validator)) else "schema",
        }
        for e in _validator().iter_errors(document)
    ]
    issues.sort(key=lambda i: (i["json_path"], i["validator"]))
    return issues[:MAX_ISSUES]


def control_plane_issues(document: dict[str, Any]) -> list[dict[str, str]]:
    """Rules the schema cannot express. `metadata.version` belongs to the server (README 3.2)."""
    metadata = document.get("metadata")
    if isinstance(metadata, dict) and "version" in metadata:
        return [{"json_path": "$.metadata.version", "validator": "version_set_by_client"}]
    return []
