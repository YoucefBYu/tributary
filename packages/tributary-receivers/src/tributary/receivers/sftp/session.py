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
import logging
import socket
import stat
from collections.abc import Collection
from dataclasses import dataclass
from typing import Protocol

import paramiko  # type: ignore[import-untyped]  # no stubs; LGPL-2.1, approved, unmodified
from paramiko.sftp import CMD_CLOSE, CMD_OPENDIR, CMD_READDIR  # type: ignore[import-untyped]

from ..errors import ReceiverError
from ..secrets import SecretResolver
from .config import SftpSettings

__all__ = [
    "HostKeyMismatchError",
    "NotRegularFileError",
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
_MIN_ENTRY_BYTES = 12
_EXTRA_SCANNED = 4096  # skipped entries (`.`, `..`, undecodable names) tolerated beyond `limit` in one listing
_RSA_ALGORITHMS = ("ssh-rsa", "rsa-sha2-256", "rsa-sha2-512")


class SessionError(ReceiverError):
    """A connection or remote operation failed. The message never names a path or file."""


class HostKeyMismatchError(SessionError):
    """The server's host key is not one of the pinned fingerprints. Nothing was sent to it."""


class NotRegularFileError(SessionError):
    """The claimed name is not a regular file (a link, a directory or a swapped-in object). Nothing was read."""


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
    #: Directory entries whose names were not valid UTF-8 and were skipped, cumulative for this session.
    undecodable_names: int

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
        self.undecodable_names = 0

    def list_dir(self, directory: str, limit: int) -> list[RemoteEntry]:
        """At most `limit` entries. Names are decoded here rather than by paramiko (whose decode of a
        non-UTF-8 name raises and would abort the whole listing, stopping the poller on one bad file): an
        entry whose name is not valid UTF-8 is skipped and counted in `undecodable_names`."""
        out: list[RemoteEntry] = []
        scanned = 0
        max_scanned = limit + _EXTRA_SCANNED  # entries examined, kept or not: `.`, `..` and bad names count too
        handle = None
        try:
            _, msg = self._client._request(CMD_OPENDIR, directory)
            handle = msg.get_string()
            while len(out) < limit and scanned < max_scanned:
                try:
                    _, msg = self._client._request(CMD_READDIR, handle)
                except EOFError:
                    break
                count = msg.get_int()
                # A server-claimed count is never trusted: an entry needs at least 12 bytes (two empty
                # strings and an attribute flags word), so a count the reply cannot hold is a lie, and looping
                # on it would spin for billions of iterations over an exhausted buffer.
                if count > (len(msg.asbytes()) - msg.packet.tell()) // _MIN_ENTRY_BYTES:
                    raise SessionError("sftp directory listing is malformed")
                for _ in range(count):
                    if len(out) >= limit or scanned >= max_scanned:
                        break  # the limit holds per entry, not per reply
                    scanned += 1
                    raw_name = msg.get_string()
                    msg.get_string()  # longname: unused, and a second place a name could appear
                    attrs = paramiko.SFTPAttributes._from_msg(msg)
                    try:
                        name = raw_name.decode("utf-8")
                    except UnicodeDecodeError:
                        self.undecodable_names += 1
                        continue
                    if name in (".", ".."):
                        continue
                    mode = attrs.st_mode
                    out.append(
                        RemoteEntry(
                            name=name,
                            size=attrs.st_size or 0,
                            mtime=float(attrs.st_mtime or 0),
                            is_regular=mode is not None and stat.S_ISREG(mode),
                        )
                    )
        except (TimeoutError, OSError, EOFError, paramiko.SSHException) as exc:
            raise self._map(exc) from None
        finally:
            if handle is not None:
                with contextlib.suppress(Exception):
                    self._client._request(CMD_CLOSE, handle)
        return out[:limit]

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
        ever read into memory, whatever the server's stat said). Raises `NotRegularFileError` before any
        byte is read unless the file is regular.

        No lstat-then-open-by-name gap: the file is opened first, the open handle is `fstat`ed (that is
        the object we would read), and only then is the name `lstat`ed again and required to be a regular
        file matching the handle's size, mtime and mode. SFTP v3 has no `O_NOFOLLOW` and no inode, so a
        link in the name's place is caught by the second `lstat` (a link is not regular), and a swap in the
        window between `open` and that `lstat` needs a regular file whose size and mtime equal the target's.
        That residual needs write access to the claimed directory and cannot be closed on this protocol."""
        chunks: list[bytes] = []
        total = 0
        try:
            with self._client.open(path, "rb") as handle:
                opened = handle.stat()
                if opened.st_mode is None or not stat.S_ISREG(opened.st_mode):
                    raise NotRegularFileError("not a regular file")
                named = self._client.lstat(path)
                if (
                    named.st_mode is None
                    or not stat.S_ISREG(named.st_mode)
                    or (named.st_mode, named.st_size, named.st_mtime)
                    != (opened.st_mode, opened.st_size, opened.st_mtime)
                ):
                    raise NotRegularFileError("not a regular file")
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


#: Algorithms paramiko still offers that are weak or SHA-1 based; a server that can negotiate nothing else
#: is refused rather than talked down to. What remains: AES-CTR/GCM and ChaCha20 ciphers, curve25519/ECDH/DH
#: group14+-sha2 key exchange, hmac-sha2-* and ETM MACs, ed25519/ECDSA/rsa-sha2 keys.
DISABLED_ALGORITHMS: dict[str, tuple[str, ...]] = {
    "ciphers": ("3des-cbc", "aes128-cbc", "aes192-cbc", "aes256-cbc"),
    "kex": (
        "diffie-hellman-group1-sha1",
        "diffie-hellman-group14-sha1",
        "diffie-hellman-group-exchange-sha1",
        # Group exchange lets the server pick the group and paramiko's client minimum is 1024 bits; group14
        # (2048), group16 (4096), ECDH and curve25519 remain, so nothing needed is lost.
        "diffie-hellman-group-exchange-sha256",
    ),
    "macs": ("hmac-md5", "hmac-md5-96", "hmac-sha1", "hmac-sha1-96"),
    "keys": ("ssh-rsa",),  # server host key signature: RSA with SHA-1 (rsa-sha2-* stays)
    "pubkeys": ("ssh-rsa",),  # our client-auth signature: same
}


def disabled_algorithms(extra_host_key_types: list[str]) -> dict[str, list[str]]:
    merged = {kind: list(names) for kind, names in DISABLED_ALGORITHMS.items()}
    merged["keys"] += [name for name in extra_host_key_types if name not in merged["keys"]]
    return merged


def _handshake(cfg: SftpSettings, disabled_keys: list[str]) -> paramiko.Transport:
    sock = socket.create_connection((cfg.host, cfg.port), timeout=cfg.connect_timeout_s)
    try:
        transport = paramiko.Transport(sock, disabled_algorithms=disabled_algorithms(disabled_keys))
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
    logging.getLogger("paramiko").setLevel(logging.WARNING)  # its INFO/DEBUG lines can carry paths and names
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
