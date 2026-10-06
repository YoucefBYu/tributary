"""Resolving a channel spec's `secretRef` to material on disk.

CLAUDE.md: "Secrets only via secretRef resolved at runtime." A channel spec never carries a secret
value (`contracts/channel-spec/v0` `secretRef` `$comment`: "No secret value ever appears in a spec"),
and invariant 3 says the data plane must start and run with the control-plane API down, so resolution
must not call out to it. `DirSecretResolver` mirrors the Kubernetes convention this deployment already
uses for TLS: a `Secret` mounted as a directory of files named after its keys
(`/var/run/secrets/tributary/<ref>/tls.crt`, `.../tls.key`, `.../ca.crt`). `resolve_file` hands the
*path* to `ssl.SSLContext` (`tls.py`) or `paramiko` (the SFTP source's private key), which read the
file themselves, so those secrets' bytes pass through this process only inside library code that
already handles key material, never through a log statement, an f-string, or a dict this package
builds.

`resolve_text` (added for the `http` source's bearer token and the `sftp`/`kafka` sources' username
and password, W2-7) is the other shape a receiver needs: a value compared in memory (a bearer token
against an `Authorization` header, constant-time) or handed to a client library's own `str` parameter
(`paramiko`'s `password=`, `confluent_kafka`'s `sasl.password`). It mirrors
`tributary.loaders.secrets.DirSecretResolver.resolve_text` exactly; the two packages keep separate
copies for the same reason their `resolve_file` copies already do (no shared non-SDK home yet)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, runtime_checkable

from .errors import SecretUnresolvedError

DEFAULT_SECRETS_DIR = "/var/run/secrets/tributary"
ENV_SECRETS_DIR = "TB_SECRETS_DIR"


@runtime_checkable
class SecretResolver(Protocol):
    """Resolves one key of one `secretRef` to a filesystem path, or to text."""

    def resolve_file(self, ref: str, key: str) -> Path: ...

    def resolve_text(self, ref: str, key: str) -> str: ...


class DirSecretResolver:
    """`<base_dir>/<ref>/<key>`. Raises `SecretUnresolvedError` (a `FileNotFoundError`) for a missing ref or key: fail closed,
    the same way a missing required header does, rather than start a listener with no certificate."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self._base = Path(base_dir if base_dir is not None else os.environ.get(ENV_SECRETS_DIR, DEFAULT_SECRETS_DIR))

    def _resolve(self, ref: str, key: str) -> Path:
        path = (self._base / ref / key).resolve()
        if self._base.resolve() not in path.parents:
            # A ref or key containing ".." must not escape the secrets directory.
            raise SecretUnresolvedError(f"secretRef {ref!r} resolves outside the secrets directory")
        if not path.is_file():
            raise SecretUnresolvedError(f"no {key!r} under secretRef {ref!r} in {self._base}")
        return path

    def resolve_file(self, ref: str, key: str) -> Path:
        """The path itself, unread (a TLS certificate, key, CA bundle, or an SFTP private key)."""
        return self._resolve(ref, key)

    def resolve_text(self, ref: str, key: str) -> str:
        """Read as UTF-8 text (a bearer token, an SFTP/SASL username or password)."""
        return self._resolve(ref, key).read_text(encoding="utf-8").strip()
