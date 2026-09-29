"""Resolving a `secretRef` to local files: `<TB_SECRETS_DIR>/<ref>/<key>` (`engine-kafka/v0` section 2).

The convention every connector already uses, and the Kubernetes one: a `Secret` mounted read-only as a
directory of files named after its keys. Resolution reads the local filesystem only, at start, and
never calls the control-plane API or PostgreSQL (invariant 3).

`resolve_file` returns the path and does not read it: librdkafka and `ssl` read certificates and keys
themselves. `resolve_text` reads a short value (a SCRAM username or password) for a client library's
own `str` parameter. Neither value, nor the secrets directory, appears in any exception message: a
failure names the `secretRef` and the key, which are names from the document, never values.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from pathlib import Path
from typing import Final, Protocol, runtime_checkable

from .errors import RuntimeConfigurationError, safe_token

DEFAULT_SECRETS_DIR: Final = "/var/run/secrets/tributary"
ENV_SECRETS_DIR: Final = "TB_SECRETS_DIR"
#: A text secret is a username or a password. Anything larger is a mistake (a key file named
#: `password`), and reading it whole into memory would only make the mistake bigger.
MAX_TEXT_SECRET_BYTES: Final = 4096


class SecretResolutionError(RuntimeConfigurationError):
    """A `secretRef` key is missing, empty, oversized, or escapes the secrets directory."""


@runtime_checkable
class SecretResolver(Protocol):
    """Resolves one key of one `secretRef` to a filesystem path, or to text."""

    def resolve_file(self, ref: str, key: str) -> Path: ...

    def resolve_text(self, ref: str, key: str) -> str: ...


def _name(ref: str, key: str) -> str:
    return f"{safe_token(ref)}/{safe_token(key)}"


class DirSecretResolver:
    """`<base_dir>/<ref>/<key>`, failing closed with `SecretResolutionError`."""

    __slots__ = ("_base",)

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self._base = Path(base_dir if base_dir is not None else DEFAULT_SECRETS_DIR)

    @classmethod
    def from_environ(cls, environ: Mapping[str, str] | None = None) -> DirSecretResolver:
        """The resolver `TB_SECRETS_DIR` names, or the default mount point."""
        env = os.environ if environ is None else environ
        return cls(env.get(ENV_SECRETS_DIR) or DEFAULT_SECRETS_DIR)

    def __repr__(self) -> str:
        return "DirSecretResolver(<secrets dir>)"

    def _resolve(self, ref: str, key: str) -> Path:
        base = self._base.resolve()
        path = (base / ref / key).resolve()
        if base not in path.parents:
            # `..` in a ref or key, or a symlink pointing out of the mount.
            raise SecretResolutionError(f"secretRef key {_name(ref, key)} resolves outside the secrets directory")
        if not path.is_file():
            raise SecretResolutionError(f"secretRef key {_name(ref, key)} does not exist")
        if path.stat().st_size == 0:
            raise SecretResolutionError(f"secretRef key {_name(ref, key)} is empty")
        return path

    def resolve_file(self, ref: str, key: str) -> Path:
        """The path itself, unread (a certificate, private key or CA bundle). Must be non-empty."""
        return self._resolve(ref, key)

    def resolve_text(self, ref: str, key: str) -> str:
        """The key's content as UTF-8, surrounding whitespace stripped. Must be non-empty."""
        path = self._resolve(ref, key)
        if path.stat().st_size > MAX_TEXT_SECRET_BYTES:
            raise SecretResolutionError(f"secretRef key {_name(ref, key)} is larger than a text secret can be")
        try:
            text = path.read_text(encoding="utf-8").strip()
        except (OSError, UnicodeDecodeError) as exc:
            raise SecretResolutionError(
                f"secretRef key {_name(ref, key)} is not readable UTF-8 ({type(exc).__name__})"
            ) from None
        if not text:
            raise SecretResolutionError(f"secretRef key {_name(ref, key)} is empty")
        return text
