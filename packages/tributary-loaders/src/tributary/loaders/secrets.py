"""Resolving a channel spec's `secretRef` to a value on disk.

Duplicated from `tributary-receivers`' `secrets.py` rather than shared, because there is no common
home for it yet (it is not SDK-shaped: the SDK is stdlib-only and never touches a filesystem or a
socket, per its own docstring). Both copies implement the same Kubernetes Secret-volume convention
(`<base_dir>/<ref>/<key>`); if a third connector needs this, it is worth proposing to the architect
as a shared module rather than a third copy.

CLAUDE.md: "Secrets only via secretRef resolved at runtime." Invariant 3: the data plane must start
and run with the control-plane API down, so resolution never calls out to it.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, runtime_checkable

DEFAULT_SECRETS_DIR = "/var/run/secrets/tributary"
ENV_SECRETS_DIR = "TB_SECRETS_DIR"


@runtime_checkable
class SecretResolver(Protocol):
    def resolve_text(self, ref: str, key: str) -> str: ...


class DirSecretResolver:
    """`<base_dir>/<ref>/<key>`, read as UTF-8 text (a bearer token, a `user:pass` basic-auth
    value). Raises `FileNotFoundError` for a missing ref or key: fail closed."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self._base = Path(base_dir if base_dir is not None else os.environ.get(ENV_SECRETS_DIR, DEFAULT_SECRETS_DIR))

    def resolve_text(self, ref: str, key: str) -> str:
        path = (self._base / ref / key).resolve()
        if self._base.resolve() not in path.parents:
            raise FileNotFoundError(f"secretRef {ref!r} resolves outside the secrets directory")
        if not path.is_file():
            raise FileNotFoundError(f"no {key!r} under secretRef {ref!r} in {self._base}")
        return path.read_text(encoding="utf-8").strip()
