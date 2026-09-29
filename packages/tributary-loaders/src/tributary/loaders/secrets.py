"""Resolving a channel spec's `secretRef` to a value on disk.

Duplicated from `tributary-receivers`' `secrets.py` rather than shared, because there is no common
home for it yet (it is not SDK-shaped: the SDK is stdlib-only and never touches a filesystem or a
socket, per its own docstring). Both copies implement the same Kubernetes Secret-volume convention
(`<base_dir>/<ref>/<key>`); if a third connector needs this, it is worth proposing to the architect
as a shared module rather than a third copy.

CLAUDE.md: "Secrets only via secretRef resolved at runtime." Invariant 3: the data plane must start
and run with the control-plane API down, so resolution never calls out to it.

`resolve_file` (added for W2-6's `postgresql` loader, `tls.secretRef`/`auth.secretRef` with
`client-certificate`) mirrors `tributary.receivers.secrets.SecretResolver.resolve_file` exactly: a
*path*, not the bytes, so a client certificate or CA bundle passes through this process only inside
`ssl.SSLContext`'s own file-reading code, never through an f-string or a dict this package builds.
`resolve_text` (basic/bearer auth tokens, a database password) is the other shape: read once into a
`str` because that is what those call sites need — `urllib`'s headers, `asyncpg`'s `password=`."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol, runtime_checkable

from .errors import LoaderError

DEFAULT_SECRETS_DIR = "/var/run/secrets/tributary"
ENV_SECRETS_DIR = "TB_SECRETS_DIR"


class SecretFormatError(LoaderError):
    """A resolved secret contains a control character (a newline, NUL, ...). Fixed message: the value, and
    the ref and key that produced it, are never in it. Raised at startup, so a mis-provisioned secret
    stops the loader instead of surfacing later as a malformed request header."""


def reject_control_characters(value: str) -> str:
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in value):
        raise SecretFormatError("a resolved secret contains a control character")
    return value


@runtime_checkable
class SecretResolver(Protocol):
    def resolve_text(self, ref: str, key: str) -> str: ...

    def resolve_file(self, ref: str, key: str) -> Path: ...


class DirSecretResolver:
    """`<base_dir>/<ref>/<key>`. Raises `FileNotFoundError` for a missing ref or key: fail closed."""

    def __init__(self, base_dir: str | Path | None = None) -> None:
        self._base = Path(base_dir if base_dir is not None else os.environ.get(ENV_SECRETS_DIR, DEFAULT_SECRETS_DIR))

    def _resolve(self, ref: str, key: str) -> Path:
        path = (self._base / ref / key).resolve()
        if self._base.resolve() not in path.parents:
            raise FileNotFoundError(f"secretRef {ref!r} resolves outside the secrets directory")
        if not path.is_file():
            raise FileNotFoundError(f"no {key!r} under secretRef {ref!r} in {self._base}")
        return path

    def resolve_text(self, ref: str, key: str) -> str:
        """Read as UTF-8 text (a bearer token, a `user:pass` basic-auth value, a database password).
        Surrounding whitespace is stripped; a control character anywhere else is `SecretFormatError`."""
        try:
            text = self._resolve(ref, key).read_text(encoding="utf-8")
        except UnicodeDecodeError:
            raise SecretFormatError("a resolved secret is not valid UTF-8") from None
        return reject_control_characters(text.strip())

    def resolve_file(self, ref: str, key: str) -> Path:
        """The path itself, unread (a TLS certificate, key or CA bundle)."""
        return self._resolve(ref, key)
