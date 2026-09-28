"""A minimal in-process SFTP server for tests (paramiko's server side, rooted at a local directory).

Test helper only: password auth for one synthetic user, one or more host keys (so host-key pinning and
key-type fallback can be exercised), a real SFTP subsystem over a real TCP loopback socket. It records
whether any authentication attempt reached it, which is how tests prove credentials are not sent to a
host whose key does not match a pin.
"""

from __future__ import annotations

import os
import socket
import threading
from pathlib import Path
from typing import Any

import paramiko  # type: ignore[import-untyped]

USER = "synth-user"
PASSWORD = "synth-pass"  # noqa: S105  # tb-secret-scan: allow


class _Handle(paramiko.SFTPHandle):  # type: ignore[misc]
    def __init__(self, flags: int = 0) -> None:
        super().__init__(flags)


class _Fs(paramiko.SFTPServerInterface):  # type: ignore[misc]
    ROOT = Path("/")

    def _p(self, path: str) -> str:
        return str(self.ROOT / path.lstrip("/"))

    def list_folder(self, path: str) -> Any:
        try:
            out = []
            for name in os.listdir(self._p(path)):
                attr = paramiko.SFTPAttributes.from_stat(os.stat(os.path.join(self._p(path), name)), name)
                out.append(attr)
            return out
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    def stat(self, path: str) -> Any:
        try:
            return paramiko.SFTPAttributes.from_stat(os.stat(self._p(path)))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)

    lstat = stat

    def open(self, path: str, flags: int, attr: Any) -> Any:
        try:
            fd = os.open(self._p(path), flags | getattr(os, "O_BINARY", 0), 0o666)
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        handle = _FileHandle(flags)
        handle.filename = self._p(path)
        handle.readfile = os.fdopen(fd, "rb") if not flags & (os.O_WRONLY | os.O_RDWR) else None
        return handle

    def remove(self, path: str) -> int:
        try:
            os.remove(self._p(path))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        return paramiko.SFTP_OK

    def rename(self, oldpath: str, newpath: str) -> int:
        # SFTP v3 semantics: refuse to overwrite. link+unlink is the atomic no-overwrite rename.
        # OpenSSH's sftp-server does the same, and removes the spare link when the unlink loses a race.
        try:
            os.link(self._p(oldpath), self._p(newpath))
        except OSError as exc:
            return paramiko.SFTPServer.convert_errno(exc.errno)
        try:
            os.unlink(self._p(oldpath))
        except OSError as exc:
            os.unlink(self._p(newpath))
            return paramiko.SFTPServer.convert_errno(exc.errno)
        return paramiko.SFTP_OK

    def canonicalize(self, path: str) -> str:
        return path if path.startswith("/") else "/" + path


class _FileHandle(paramiko.SFTPHandle):  # type: ignore[misc]
    pass


class _Auth(paramiko.ServerInterface):  # type: ignore[misc]
    def __init__(self, owner: FakeSftpServer) -> None:
        self.owner = owner

    def check_auth_password(self, username: str, password: str) -> int:
        self.owner.auth_attempts += 1
        ok = username == USER and password == PASSWORD
        return paramiko.AUTH_SUCCESSFUL if ok else paramiko.AUTH_FAILED

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_channel_request(self, kind: str, chanid: int) -> int:
        return paramiko.OPEN_SUCCEEDED if kind == "session" else paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


class FakeSftpServer:
    """`with FakeSftpServer(root, host_keys) as srv:` then connect to `srv.port`."""

    def __init__(self, root: Path, host_keys: list[paramiko.PKey]) -> None:
        self.root = root
        self.host_keys = host_keys
        self.auth_attempts = 0
        self._sock = socket.socket()
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port: int = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []
        _Fs.ROOT = root

    def __enter__(self) -> FakeSftpServer:
        t = threading.Thread(target=self._accept, daemon=True)
        t.start()
        self._threads.append(t)
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        self._sock.close()

    def _accept(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            t = paramiko.Transport(conn)
            for key in self.host_keys:
                t.add_server_key(key)
            t.set_subsystem_handler("sftp", paramiko.SFTPServer, _Fs)
            try:
                t.start_server(server=_Auth(self))
            except Exception:  # a client that rejects our key hangs up mid-handshake
                t.close()
                continue
            self._threads.append(t)
