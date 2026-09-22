"""Resolving a channel spec's `secretRef` to material on disk.

CLAUDE.md: "Secrets only via secretRef resolved at runtime." A channel spec never carries a secret
value (`contracts/channel-spec/v0` `secretRef` `$comment`: "No secret value ever appears in a spec"),
and invariant 3 says the data plane must start and run with the control-plane API down, so resolution
must not call out to it. `DirSecretResolver` mirrors the Kubernetes convention this deployment already
uses for TLS: a `Secret` mounted as a directory of files named after its keys
(`/var/run/secrets/tributary/<ref>/tls.crt`, `.../tls.key`, `.../ca.crt`). Nothing here reads a secret
value into a Python `str`; each caller (`tls.py`) hands the *path* to `ssl.SSLContext`, which reads the
file itself, so a secret's bytes pass through this process only inside the standard library's TLS
code, never through a log statement, an f-string, or a dict this package builds.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, runtime_checkable

DEFAULT_SECRETS_DIR = "/var/run/secrets/tributary"
ENV_SECRETS_DIR = "TB_SECRETS_DIR"


@runtime_checkable
class SecretResolver(Protocol):
    """Resolves one key of one `secretRef` to a filesystem path."""

    def resolve_file(self, ref: str, key: str) -> Path: ...


class DirSecretResolver:
    """`<base_dir>/<ref>/<key>`. Raises `FileNotFoundError` for a missing ref or key: fail closed,
    the same way a missing required header does, rather than start a listener with no certificate."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self._base = Path(base_dir if base_dir is not None else os.environ.get(ENV_SECRETS_DIR, DEFAULT_SECRETS_DIR))

    def resolve_file(self, ref: str, key: str) -> Path:
        path = (self._base / ref / key).resolve()
        if self._base.resolve() not in path.parents:
            # A ref or key containing ".." must not escape the secrets directory.
            raise FileNotFoundError(f"secretRef {ref!r} resolves outside the secrets directory")
        if not path.is_file():
            raise FileNotFoundError(f"no {key!r} under secretRef {ref!r} in {self._base}")
        return path
