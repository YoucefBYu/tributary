"""Resolving a `secretRef` to local files: `<TB_SECRETS_DIR>/<ref>/<key>` (`engine-kafka/v0` section 2).

The convention every connector already uses, and the Kubernetes one: a `Secret` mounted read-only as a
directory of files named after its keys. Resolution reads the local filesystem only, at start, and
never calls the control-plane API or PostgreSQL (invariant 3).

`resolve_file` returns the path and does not read it: librdkafka and `ssl` read certificates and keys
themselves. `resolve_text` reads a short value (a SCRAM username or password) for a client library's
own `str` parameter. Neither value, nor the secrets directory, nor an OS error's text (which is a path)
appears in any exception message: `SecretResolutionError` is built from the `secretRef` name, the key
name and a reason from a fixed table, and has no free-text constructor.
"""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import Final, Protocol, runtime_checkable

from .errors import RuntimeConfigurationError, safe_token

DEFAULT_SECRETS_DIR: Final = "/var/run/secrets/tributary"
ENV_SECRETS_DIR: Final = "TB_SECRETS_DIR"
#: A text secret is a username or a password. Anything larger is a mistake (a key file named
#: `password`), and reading it whole into memory would only make the mistake bigger.
MAX_TEXT_SECRET_BYTES: Final = 4096


#: Why a key did not resolve. The message is built from this table and the two names, never from a
#: path, an OS error text or anything read from the file.
SECRET_FAILURES: Final[Mapping[str, str]] = {
    "outside": "resolves outside the secrets directory",
    "missing": "does not exist",
    "unreadable": "is not readable",
    "empty": "is empty",
    "writable": "is group- or world-writable",
    "oversized": "is larger than a text secret can be",
    "not-utf8": "is not UTF-8 text",
    "control-character": "contains a control character",
}


class SecretResolutionError(RuntimeConfigurationError):
    """A `secretRef` key is missing, unreadable, empty, oversized, malformed, or escapes the directory.

    Constructed from the `secretRef` name, the key name and a `reason` from `SECRET_FAILURES` only, so
    there is no way to put a value, a path or an OS error's text into one. An unknown reason is recorded
    as `unresolvable` rather than trusted.
    """

    code = "SECRET_UNRESOLVED"

    def __init__(self, ref: str, key: str, reason: str) -> None:
        self.reason = reason if reason in SECRET_FAILURES else "unresolvable"
        self.ref = safe_token(ref)
        self.key = safe_token(key)
        super().__init__(f"secretRef key {self.ref}/{self.key} {describe_failure(self.reason)}")


def describe_failure(reason: str) -> str:
    """The fixed sentence for a `SecretResolutionError.reason`."""
    return SECRET_FAILURES.get(reason, "could not be resolved")


def has_control_character(value: str) -> bool:
    """C0 controls and DEL: a NUL truncates a librdkafka string and a CR/LF is a file-format mistake.

    Same rule as the loaders' and receivers' resolvers, so a secret one component accepts is not refused
    by another."""
    return any(ord(c) < 0x20 or ord(c) == 0x7F for c in value)


@runtime_checkable
class SecretResolver(Protocol):
    """Resolves one key of one `secretRef` to a filesystem path, or to text."""

    def resolve_file(self, ref: str, key: str) -> Path: ...

    def resolve_text(self, ref: str, key: str) -> str: ...


class DirSecretResolver:
    """`<base_dir>/<ref>/<key>`, failing closed with `SecretResolutionError`.

    Refused: a key that resolves outside the directory, is missing, unreadable, empty, or group- or
    world-writable (the file the symlink chain ends at; Kubernetes and Compose secret mounts are 0644 or
    stricter and read-only). Text keys are also refused when oversized, not UTF-8, or holding a control
    character.
    """

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
        try:
            base = self._base.resolve()
            path = (base / ref / key).resolve()
            if base not in path.parents:
                # `..` in a ref or key, or a symlink pointing out of the mount.
                raise SecretResolutionError(ref, key, "outside")
            if not path.is_file():
                raise SecretResolutionError(ref, key, "missing")
            status = path.stat()
        except (OSError, RuntimeError, ValueError):
            # A permission error's text is the path; a symlink loop is RuntimeError before 3.13; a NUL in a
            # name is ValueError. None of their messages is repeated.
            raise SecretResolutionError(ref, key, "unreadable") from None
        if status.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            # Secret mounts are read-only; a file another principal can rewrite is not this process's
            # credential any more (a swapped CA bundle is a silent man-in-the-middle).
            raise SecretResolutionError(ref, key, "writable")
        if status.st_size == 0:
            raise SecretResolutionError(ref, key, "empty")
        return path

    def resolve_file(self, ref: str, key: str) -> Path:
        """The path itself, unread (a certificate, private key or CA bundle). Must be non-empty."""
        return self._resolve(ref, key)

    def resolve_text(self, ref: str, key: str) -> str:
        """The key's content as UTF-8, surrounding whitespace stripped. Must be non-empty and free of
        control characters (C0 and DEL) once stripped."""
        path = self._resolve(ref, key)
        try:
            if path.stat().st_size > MAX_TEXT_SECRET_BYTES:
                raise SecretResolutionError(ref, key, "oversized")
            raw = path.read_bytes()
        except OSError:
            raise SecretResolutionError(ref, key, "unreadable") from None
        if len(raw) > MAX_TEXT_SECRET_BYTES:  # grew between the stat and the read
            raise SecretResolutionError(ref, key, "oversized")
        try:
            text = raw.decode("utf-8").strip()
        except UnicodeDecodeError:
            # Its message quotes the offending byte and its position: part of the secret.
            raise SecretResolutionError(ref, key, "not-utf8") from None
        if not text:
            raise SecretResolutionError(ref, key, "empty")
        if has_control_character(text):
            raise SecretResolutionError(ref, key, "control-character")
        return text
