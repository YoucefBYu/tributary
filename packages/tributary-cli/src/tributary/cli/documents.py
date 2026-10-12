"""Reading a channel file. Channel documents are configuration, not patient data, but the CLI still prints
only names, versions, statuses and the contract's `(json_path, validator)` pairs, never document content."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import yaml

from .errors import InputError

MAX_BYTES = 262144  # the API's request cap (openapi/v0 README 3.8)


def load_document(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > MAX_BYTES:
            raise InputError(f"{path.name} is larger than the API accepts ({MAX_BYTES} bytes)")
        raw = path.read_bytes()
    except OSError:
        raise InputError(f"cannot read {path.name}") from None
    try:
        parsed = yaml.safe_load(raw) if path.suffix.lower() in (".yaml", ".yml") else json.loads(raw)
    except (yaml.YAMLError, ValueError):
        raise InputError(
            f"{path.name} is not valid {'YAML' if path.suffix.lower() in ('.yaml', '.yml') else 'JSON'}"
        ) from None
    if not isinstance(parsed, dict):
        raise InputError(f"{path.name} must hold one channel document (a mapping)")
    return parsed


def channel_name(document: dict[str, Any]) -> str:
    name = (document.get("metadata") or {}).get("name") if isinstance(document.get("metadata"), dict) else None
    if not isinstance(name, str) or not name:
        raise InputError("the document has no metadata.name")
    return name


def without_version(document: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(document)
    if isinstance(out.get("metadata"), dict):
        out["metadata"].pop("version", None)
    return out
