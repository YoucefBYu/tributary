"""The SFTP connection: host-key pinning, authentication, and the handful of remote operations the
poller needs behind a small `SftpSession` protocol (so `poller.py` is unit-testable without a server).

Host key verification (contract README 3.2.4): the accepted fingerprints are the spec's OpenSSH
`SHA256:` values. There is no trust-on-first-use and no "accept any" path: a `paramiko.Transport`
never authenticates until `verify_host_key` has matched the server's key against the pinned set, so
credentials are never sent to an unverified host. A server that offers several host key types is
handled by retrying the handshake with the mismatched type disabled (a fingerprint does not say which
type it belongs to), each retry still unauthenticated and still only satisfied by a pinned key.

No file name, path, key material or credential appears in an exception message here: the typed errors
say what class of thing failed. Paramiko's own exception text is never propagated.
"""

from __future__ import annotations

import base64
import contextlib
import errno
import hashlib
import hmac
import socket
import stat
from collections.abc import Collection
from dataclasses import dataclass
from typing import Protocol

import paramiko  # type: ignore[import-untyped]  # no stubs; LGPL-2.1, approved, unmodified

from ..errors import ReceiverError
from ..secrets import SecretResolver
from .config import SftpSettings

__all__ = [
    "HostKeyMismatchError",
    "ParamikoSession",
    "RemoteEntry",
    "RemoteExistsError",
    "RemoteNotFoundError",
    "SessionError",
    "SftpSession",
    "connect",
    "fingerprint",
]

_MAX_HANDSHAKE_ATTEMPTS = 6
_READ_CHUNK = 32768
_RSA_ALGORITHMS = ("ssh-rsa", "rsa-sha2-256", "rsa-sha2-512")


class SessionError(ReceiverError):
    """A connection or remote operation failed. The message never names a path or file."""


class HostKeyMismatchError(SessionError):
    """The server's host key is not one of the pinned fingerprints. Nothing was sent to it."""


class RemoteNotFoundError(SessionError):
    pass


class RemoteExistsError(SessionError):
    pass


@dataclass(frozen=True, slots=True)
class RemoteEntry:
    name: str = ""
    size: int = 0
    mtime: float = 0.0
    is_regular: bool = False

    def __repr__(self) -> str:  # a file name is PHI (README 3.2.4): never in a repr, hence never a log line
        return f"RemoteEntry(size={self.size}, regular={self.is_regular})"


class SftpSession(Protocol):
    def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]: ...
    def rename(self, src: str, dst: str) -> None: ...
    def read_bounded(self, path: str, max_bytes: int) -> bytes | None: ...
    def remove(self, path: str) -> None: ...
    def exists(self, path: str) -> bool: ...
    def close(self) -> None: ...


def fingerprint(key_blob: bytes) -> str:
    """The OpenSSH `SHA256:` fingerprint of a public key blob (`ssh-keygen -lf` form, no padding)."""
    return "SHA256:" + base64.b64encode(hashlib.sha256(key_blob).digest()).decode("ascii").rstrip("=")


def verify_host_key(key_blob: bytes, pinned: Collection[str]) -> bool:
    """True iff `key_blob` matches a pinned fingerprint. Every pin is compared (no early exit)."""
    presented = fingerprint(key_blob).encode("ascii")
    results = [hmac.compare_digest(presented, pin.encode("ascii")) for pin in pinned]
    return any(results)


class ParamikoSession:
    def __init__(self, transport: paramiko.Transport, client: paramiko.SFTPClient) -> None:
        self._transport = transport
        self._client = client

    def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
        out: list[RemoteEntry] = []
        try:
            for attrs in self._client.listdir_iter(directory):
                if len(out) >= limit:
                    break  # a hostile or huge listing is cut off, never held in full
                mode = attrs.st_mode
                out.append(
                    RemoteEntry(
                        name=attrs.filename,
                        size=attrs.st_size or 0,
                        mtime=float(attrs.st_mtime or 0),
                        is_regular=mode is not None and stat.S_ISREG(mode),
                    )
                )
        except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
            raise self._map(exc) from None
        return out

    @staticmethod
    def _map(exc: BaseException) -> SessionError:
        if isinstance(exc, OSError) and exc.errno == errno.ENOENT:
            return RemoteNotFoundError("remote path not found")
        if isinstance(exc, OSError) and exc.errno == errno.EEXIST:
            return RemoteExistsError("remote path exists")
        return SessionError(f"sftp operation failed ({type(exc).__name__})")

    def rename(self, src: str, dst: str) -> None:
        """SFTP v3 `rename`: fails, rather than overwrites, if `dst` exists, which is what makes a
        claim atomic between two pollers racing for one file (exactly one rename succeeds)."""
        try:
            self._client.rename(src, dst)
        except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
            if isinstance(exc, OSError) and exc.errno is None and self._client_exists(src) is False:
                raise RemoteNotFoundError("remote path not found") from None
            raise self._map(exc) from None

    def _client_exists(self, path: str) -> bool | None:
        try:
            self._client.stat(path)
        except OSError as exc:
            return False if exc.errno == errno.ENOENT else None
        except (TimeoutError, EOFError, paramiko.SSHException):
            return None
        return True

    def read_bounded(self, path: str, max_bytes: int) -> bytes | None:
        """The file's bytes, or `None` if it is larger than `max_bytes` (at most `max_bytes + 1` are
        ever read into memory, whatever the server's stat said)."""
        chunks: list[bytes] = []
        total = 0
        try:
            with self._client.open(path, "rb") as handle:
                while total <= max_bytes:
                    chunk = handle.read(min(_READ_CHUNK, max_bytes + 1 - total))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
        except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
            raise self._map(exc) from None
        return None if total > max_bytes else b"".join(chunks)

    def remove(self, path: str) -> None:
        try:
            self._client.remove(path)
        except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
            raise self._map(exc) from None

    def exists(self, path: str) -> bool:
        found = self._client_exists(path)
        if found is None:
            raise SessionError("sftp operation failed (stat)")
        return found

    def close(self) -> None:
        for closer in (self._client.close, self._transport.close):
            with contextlib.suppress(Exception):  # best effort: closing a dead connection must not raise
                closer()


def _load_key(resolver: SecretResolver, ref: str) -> paramiko.PKey:
    path = resolver.resolve_file(ref, "private_key")
    try:
        passphrase: str | None = resolver.resolve_text(ref, "passphrase")
    except FileNotFoundError:
        passphrase = None
    try:
        return paramiko.PKey.from_path(str(path), passphrase=passphrase)
    except Exception:
        raise SessionError("private key could not be loaded") from None


def _handshake(cfg: SftpSettings, disabled_keys: list[str]) -> paramiko.Transport:
    sock = socket.create_connection((cfg.host, cfg.port), timeout=cfg.connect_timeout_s)
    try:
        transport = paramiko.Transport(sock, disabled_algorithms={"keys": disabled_keys} if disabled_keys else None)
        transport.banner_timeout = cfg.connect_timeout_s
        transport.handshake_timeout = cfg.connect_timeout_s
        transport.auth_timeout = cfg.connect_timeout_s
        transport.start_client(timeout=cfg.connect_timeout_s)
    except BaseException:
        sock.close()
        raise
    return transport


def _verified_transport(cfg: SftpSettings) -> paramiko.Transport:
    """A started, unauthenticated transport whose host key matches a pin, or `HostKeyMismatchError`."""
    disabled: list[str] = []
    for _ in range(_MAX_HANDSHAKE_ATTEMPTS):
        try:
            transport = _handshake(cfg, disabled)
        except (TimeoutError, OSError, EOFError, paramiko.SSHException):
            if disabled:
                break  # the server has no other host key type to offer: every key it has was unpinned
            raise
        key = transport.get_remote_server_key()
        if verify_host_key(key.asbytes(), cfg.host_keys):
            return transport
        name = key.get_name()
        transport.close()
        algorithms = list(_RSA_ALGORITHMS) if "rsa" in name else [name]
        fresh = [a for a in algorithms if a not in disabled]
        if not fresh:
            break  # the server offered this type again despite it being disabled: no other key to try
        disabled += fresh
    raise HostKeyMismatchError("server host key does not match any pinned fingerprint")


def connect(cfg: SftpSettings, resolver: SecretResolver) -> ParamikoSession:
    """Connect, pin-verify the host key, then authenticate. Raises `HostKeyMismatchError` (nothing
    sent to the host) or `SessionError`."""
    transport: paramiko.Transport | None = None
    try:
        transport = _verified_transport(cfg)
        username = resolver.resolve_text(cfg.auth_secret_ref, "username")
        if cfg.auth_type == "private-key":
            transport.auth_publickey(username, _load_key(resolver, cfg.auth_secret_ref))
        else:
            transport.auth_password(username, resolver.resolve_text(cfg.auth_secret_ref, "password"))
        transport.set_keepalive(15)
        client = paramiko.SFTPClient.from_transport(transport)
        if client is None:
            raise SessionError("sftp subsystem unavailable")
        channel = client.get_channel()
        if channel is None:
            raise SessionError("sftp subsystem unavailable")
        channel.settimeout(cfg.op_timeout_s)
        return ParamikoSession(transport, client)
    except SessionError:
        if transport is not None:
            transport.close()
        raise
    except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
        if transport is not None:
            transport.close()
        raise SessionError(f"sftp connect failed ({type(exc).__name__})") from None
    except BaseException:
        if transport is not None:
            transport.close()
        raise
