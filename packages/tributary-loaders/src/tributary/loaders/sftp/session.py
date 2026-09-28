"""The SFTP connection for the destination loader: host-key pinning, authentication, and the four remote
operations the loader needs (`size`, `write`, `rename`, `remove`) behind a small `SftpSession` protocol, so
`loader.py` is unit-testable without a server.

## Host keys (README 3.4.4, 10.1, 10.2)

The accepted keys are the spec's OpenSSH `SHA256:` fingerprints. **Nothing is sent to the server until the
key it presented matches a pin**: a `paramiko.Transport` is started (key exchange only), the presented key
is compared, and only then is it authenticated. There is no trust-on-first-use, no `AutoAddPolicy`, no
`WarningPolicy` and no "accept any" path: no `SSHClient` is used at all, so paramiko's permissive
`MissingHostKeyPolicy` default never applies. A server that offers several host-key types is handled by
retrying the handshake with the mismatched type disabled (a fingerprint does not say which type it
belongs to); each retry is still unauthenticated and still satisfied only by a pinned key. All pins are
compared in constant time with no early exit.

## Errors

Typed, with fixed messages: no host, path, file name, key material, credential or paramiko text is ever
put in an exception (invariant 6; a directory listing and a remote file name are PHI surfaces, README
10.1). Every network wait is bounded: TCP connect, SSH banner, key exchange, authentication, every SFTP
channel read and write (`channel.settimeout`), and close (run in a daemon thread and abandoned after
`_CLOSE_TIMEOUT_S`, since `Transport.close` joins its reader thread).
"""

from __future__ import annotations

import base64
import contextlib
import errno
import hashlib
import hmac
import logging
import socket
import threading
from collections.abc import Collection
from typing import Protocol

import paramiko  # type: ignore[import-untyped]  # no stubs; LGPL-2.1, approved for SFTP, unmodified

from ..errors import LoaderError
from ..secrets import SecretResolver
from .config import SftpDestinationConfig

__all__ = [
    "WEAK_ALGORITHMS",
    "AuthFailedError",
    "ConnectionLostError",
    "HostKeyMismatchError",
    "ParamikoSession",
    "RemoteError",
    "SessionError",
    "SessionTimeoutError",
    "SftpSession",
    "connect",
    "fingerprint",
    "verify_host_key",
]

#: Algorithms paramiko still offers by default and this loader never negotiates (receivers review):
#: CBC and 3DES ciphers, SHA-1 and MD5 MACs, SHA-1 and group1 key exchange, and SHA-1 RSA (`ssh-rsa`, for
#: both the server host key and client public-key auth; `rsa-sha2-256/512` stay enabled). A server that
#: offers nothing else is refused at the handshake, before any credential is sent.
WEAK_ALGORITHMS: dict[str, tuple[str, ...]] = {
    "ciphers": ("3des-cbc", "aes128-cbc", "aes192-cbc", "aes256-cbc"),
    "kex": ("diffie-hellman-group1-sha1", "diffie-hellman-group14-sha1", "diffie-hellman-group-exchange-sha1"),
    "macs": ("hmac-md5", "hmac-md5-96", "hmac-sha1", "hmac-sha1-96"),
    "keys": ("ssh-rsa",),
    "pubkeys": ("ssh-rsa",),
}

_MAX_HANDSHAKE_ATTEMPTS = 6
_RSA_ALGORITHMS = ("ssh-rsa", "rsa-sha2-256", "rsa-sha2-512")
_CLOSE_TIMEOUT_S = 5.0


class SessionError(LoaderError):
    """A connection or remote operation failed. The message never names a host, path or file."""


class HostKeyMismatchError(SessionError):
    """The server's host key is not a pinned fingerprint. Nothing was sent to it, credentials included."""


class AuthFailedError(SessionError):
    """The server refused the credentials (or they could not be loaded)."""


class SessionTimeoutError(SessionError):
    """A bounded wait elapsed."""


class ConnectionLostError(SessionError):
    """The connection failed or dropped (refused, reset, EOF, SSH protocol error). Retryable."""


class RemoteError(SessionError):
    """The server answered an SFTP request with a failure. `status` is the SFTP status code where
    paramiko preserves it (2 no such file, 3 permission denied) and 4 (general failure) otherwise."""

    def __init__(self, status: int) -> None:
        super().__init__(f"sftp operation failed (status {status})")
        self.status = status


class SftpSession(Protocol):
    def size(self, path: str) -> int | None:
        """Size in bytes of a remote regular file, or `None` if there is no such file."""
        ...

    def write(self, path: str, data: bytes) -> None: ...

    def rename(self, src: str, dst: str) -> None:
        """A plain SFTP rename: refused by the server if `dst` exists (no overwrite)."""
        ...

    def remove(self, path: str) -> None: ...

    def close(self) -> None: ...


def fingerprint(key_blob: bytes) -> str:
    """The OpenSSH `SHA256:` fingerprint of a public key blob (`ssh-keygen -lf` form, no padding)."""
    return "SHA256:" + base64.b64encode(hashlib.sha256(key_blob).digest()).decode("ascii").rstrip("=")


def verify_host_key(key_blob: bytes, pinned: Collection[str]) -> bool:
    """True iff `key_blob` matches a pin. Every pin is compared (no early exit)."""
    presented = fingerprint(key_blob).encode("ascii")
    results = [hmac.compare_digest(presented, pin.encode("ascii")) for pin in pinned]
    return any(results)


def _map(exc: BaseException) -> SessionError:
    """Paramiko and socket exceptions to typed, text-free errors."""
    if isinstance(exc, TimeoutError):
        return SessionTimeoutError("sftp wait timed out")
    if isinstance(exc, paramiko.AuthenticationException):
        return AuthFailedError("sftp authentication failed")
    if isinstance(exc, EOFError | paramiko.SSHException):
        return ConnectionLostError("sftp connection lost")
    if isinstance(exc, OSError):
        if exc.errno == errno.ENOENT:
            return RemoteError(2)
        if exc.errno == errno.EACCES:
            return RemoteError(3)
        if exc.errno is None:  # paramiko's conversion of any other SFTP status carries no errno
            return RemoteError(4)
        return ConnectionLostError("sftp connection lost")
    return ConnectionLostError("sftp connection lost")


class ParamikoSession:
    def __init__(self, transport: paramiko.Transport, client: paramiko.SFTPClient) -> None:
        self._transport = transport
        self._client = client

    def _fail(self, exc: BaseException) -> SessionError:
        """`_map`, plus one distinction it cannot make alone: paramiko raises a bare `OSError` with no
        errno both for an SFTP status it does not name (a general failure: retryable, `SFTP_4`) and for
        "Socket is closed" on a dead connection (found against a real sshd). A transport that is no
        longer active settles it: that is a lost connection, which a fresh session can fix."""
        mapped = _map(exc)
        if isinstance(mapped, RemoteError) and mapped.status == 4 and not self._transport.is_active():
            return ConnectionLostError("sftp connection lost")
        return mapped

    def size(self, path: str) -> int | None:
        try:
            attrs = self._client.stat(path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                return None
            raise self._fail(exc) from None
        except (EOFError, paramiko.SSHException) as exc:
            raise self._fail(exc) from None
        return int(attrs.st_size or 0)

    def write(self, path: str, data: bytes) -> None:
        try:
            with self._client.open(path, "wb") as remote:  # "wb" truncates a leftover .part from a crash
                remote.write(data)
        except (OSError, EOFError, paramiko.SSHException) as exc:
            raise self._fail(exc) from None

    def rename(self, src: str, dst: str) -> None:
        try:
            self._client.rename(src, dst)
        except (OSError, EOFError, paramiko.SSHException) as exc:
            raise self._fail(exc) from None

    def remove(self, path: str) -> None:
        try:
            self._client.remove(path)
        except (OSError, EOFError, paramiko.SSHException) as exc:
            raise self._fail(exc) from None

    def close(self) -> None:
        """Best effort and bounded: closing a dead or wedged connection must neither raise nor hang."""

        def _close() -> None:
            for closer in (self._client.close, self._transport.close):
                with contextlib.suppress(Exception):
                    closer()

        closer_thread = threading.Thread(target=_close, name="sftp-close", daemon=True)
        closer_thread.start()
        closer_thread.join(_CLOSE_TIMEOUT_S)


class Credentials:
    """Resolved once at startup. `repr=False` semantics by construction: no `__repr__` shows a value."""

    __slots__ = ("_key", "_password", "_username")

    def __init__(self, username: str, password: str | None, key: paramiko.PKey | None) -> None:
        self._username = username
        self._password = password
        self._key = key

    def __repr__(self) -> str:
        return "Credentials(...)"

    def apply(self, transport: paramiko.Transport) -> None:
        if self._key is not None:
            transport.auth_publickey(self._username, self._key)
        else:
            transport.auth_password(self._username, self._password or "")


def resolve_credentials(config: SftpDestinationConfig, resolver: SecretResolver) -> Credentials:
    """`secretRef` keys, the same layout as the SFTP source: `username`, and `password` or `private_key`
    (+ optional `passphrase`). Fails at startup, not on the first message."""
    ref = config.auth_secret_ref
    try:
        username = resolver.resolve_text(ref, "username")
        if config.auth_type == "password":
            return Credentials(username, resolver.resolve_text(ref, "password"), None)
        path = resolver.resolve_file(ref, "private_key")
        try:
            passphrase: str | None = resolver.resolve_text(ref, "passphrase")
        except FileNotFoundError:
            passphrase = None
        return Credentials(username, None, paramiko.PKey.from_path(str(path), passphrase=passphrase))
    except Exception:
        raise AuthFailedError("sftp credentials could not be resolved or loaded") from None


def _handshake(config: SftpDestinationConfig, disabled_keys: list[str]) -> paramiko.Transport:
    sock = socket.create_connection((config.host, config.port), timeout=config.timeout_s)
    try:
        disabled = {name: list(algorithms) for name, algorithms in WEAK_ALGORITHMS.items()}
        disabled["keys"] += disabled_keys  # host-key types already tried and not pinned (see below)
        transport = paramiko.Transport(sock, disabled_algorithms=disabled)
        transport.banner_timeout = config.timeout_s
        transport.handshake_timeout = config.timeout_s
        transport.auth_timeout = config.timeout_s
        transport.start_client(timeout=config.timeout_s)
    except BaseException:
        sock.close()
        raise
    return transport


def _verified_transport(config: SftpDestinationConfig) -> paramiko.Transport:
    """A started, **unauthenticated** transport whose host key matches a pin, or `HostKeyMismatchError`."""
    disabled: list[str] = []
    for _ in range(_MAX_HANDSHAKE_ATTEMPTS):
        try:
            transport = _handshake(config, disabled)
        except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
            if disabled:
                break  # the server has no other host-key type to offer: every key it has was unpinned
            raise _map(exc) from None
        key = transport.get_remote_server_key()
        if verify_host_key(key.asbytes(), config.host_keys):
            return transport
        name = key.get_name()
        transport.close()
        algorithms = list(_RSA_ALGORITHMS) if "rsa" in name else [name]
        fresh = [a for a in algorithms if a not in disabled]
        if not fresh:
            break
        disabled += fresh
    raise HostKeyMismatchError("server host key does not match any pinned fingerprint")


def connect(config: SftpDestinationConfig, credentials: Credentials) -> ParamikoSession:
    """Connect, verify the host key against the pins, only then authenticate."""
    # paramiko logs the negotiated peer software version and authentication outcomes at INFO/DEBUG.
    logging.getLogger("paramiko").setLevel(logging.WARNING)
    transport: paramiko.Transport | None = None
    try:
        transport = _verified_transport(config)
        credentials.apply(transport)
        transport.set_keepalive(15)
        client = paramiko.SFTPClient.from_transport(transport)
        channel = client.get_channel() if client is not None else None
        if client is None or channel is None:
            raise ConnectionLostError("sftp subsystem unavailable")
        channel.settimeout(config.timeout_s)
        return ParamikoSession(transport, client)
    except SessionError:
        if transport is not None:
            transport.close()
        raise
    except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
        if transport is not None:
            transport.close()
        raise _map(exc) from None
    except BaseException:
        if transport is not None:
            transport.close()
        raise
