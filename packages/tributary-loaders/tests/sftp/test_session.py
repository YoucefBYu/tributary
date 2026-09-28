"""Host-key pinning and timeouts against a real in-process paramiko SSH server (no SFTP subsystem, no
Docker): the properties README 10.2 names. The server records every authentication attempt, so "nothing
is sent to an unpinned host" is measured, not assumed."""

from __future__ import annotations

import contextlib
import socket
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field

import paramiko
import pytest
from tributary.loaders.http.config import CircuitBreakerSpec
from tributary.loaders.sftp.config import SftpDestinationConfig
from tributary.loaders.sftp.session import (
    AuthFailedError,
    ConnectionLostError,
    Credentials,
    HostKeyMismatchError,
    SessionTimeoutError,
    connect,
    fingerprint,
    verify_host_key,
)


class _Server(paramiko.ServerInterface):
    def __init__(self) -> None:
        self.auth_attempts = 0

    def get_allowed_auths(self, username: str) -> str:
        return "password"

    def check_auth_password(self, username: str, password: str) -> int:
        self.auth_attempts += 1
        return paramiko.AUTH_FAILED

    def check_channel_request(self, kind: str, chanid: int) -> int:
        return paramiko.OPEN_FAILED_ADMINISTRATIVELY_PROHIBITED


@dataclass
class Listener:
    port: int
    keys: list[paramiko.PKey]
    servers: list[_Server] = field(default_factory=list)

    @property
    def auth_attempts(self) -> int:
        return sum(s.auth_attempts for s in self.servers)


def _serve(keys: list[paramiko.PKey]) -> Iterator[Listener]:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(8)
    listener = Listener(port=sock.getsockname()[1], keys=keys)
    stop = threading.Event()
    transports: list[paramiko.Transport] = []

    def loop() -> None:
        sock.settimeout(0.2)
        while not stop.is_set():
            try:
                client, _ = sock.accept()
            except (TimeoutError, OSError):
                continue
            transport = paramiko.Transport(client)
            for key in keys:
                transport.add_server_key(key)
            server = _Server()
            listener.servers.append(server)
            transports.append(transport)
            with contextlib.suppress(Exception):  # a client that disconnects mid-handshake is expected here
                transport.start_server(server=server)

    thread = threading.Thread(target=loop, daemon=True)
    thread.start()
    try:
        yield listener
    finally:
        stop.set()
        thread.join(2)
        for transport in transports:
            transport.close()
        sock.close()


@pytest.fixture(scope="module")
def ecdsa_key() -> paramiko.PKey:
    return paramiko.ECDSAKey.generate()


@pytest.fixture(scope="module")
def other_key() -> paramiko.PKey:
    return paramiko.ECDSAKey.generate()


@pytest.fixture
def ecdsa_server(ecdsa_key: paramiko.PKey) -> Iterator[Listener]:
    yield from _serve([ecdsa_key])


def config_for(port: int, pins: list[str], *, timeout_s: float = 3.0) -> SftpDestinationConfig:
    return SftpDestinationConfig(
        channel="c",
        destination="d",
        host="127.0.0.1",
        port=port,
        host_keys=tuple(pins),
        auth_type="password",
        auth_secret_ref="sftp-ref",  # noqa: S106 - a secretRef name, not a secret
        directory="/out",
        file_name_prefix="",
        file_extension="",
        timeout_s=timeout_s,
        circuit_breaker=CircuitBreakerSpec(5, 30.0),
        idempotency_technique="deterministic-name",
        backoff="fixed",
        initial_delay_ms=10,
        max_delay_ms=100,
        max_attempts=3,
        on_exhaust="halt-partition",
    )


def test_fingerprint_matches_the_openssh_form(ecdsa_key: paramiko.PKey) -> None:
    fp = fingerprint(ecdsa_key.asbytes())
    assert fp.startswith("SHA256:") and "=" not in fp and len(fp) == 7 + 43


def test_verify_host_key_compares_every_pin(ecdsa_key: paramiko.PKey) -> None:
    good = fingerprint(ecdsa_key.asbytes())
    assert verify_host_key(ecdsa_key.asbytes(), ["SHA256:" + "A" * 43, good])
    assert not verify_host_key(ecdsa_key.asbytes(), ["SHA256:" + "A" * 43])
    assert not verify_host_key(ecdsa_key.asbytes(), [])


def test_a_pinned_key_lets_the_handshake_reach_authentication(ecdsa_server: Listener, ecdsa_key: paramiko.PKey) -> None:
    config = config_for(ecdsa_server.port, [fingerprint(ecdsa_key.asbytes())])
    with pytest.raises(AuthFailedError):
        connect(config, Credentials("user", "wrong", None))
    assert ecdsa_server.auth_attempts == 1  # the pin matched, so (and only so) credentials were sent


def test_an_unpinned_key_is_refused_before_any_credential_is_sent(
    ecdsa_server: Listener, other_key: paramiko.PKey
) -> None:
    config = config_for(ecdsa_server.port, [fingerprint(other_key.asbytes())])
    with pytest.raises(HostKeyMismatchError):
        connect(config, Credentials("user", "the-password", None))
    assert ecdsa_server.auth_attempts == 0


def test_a_server_offering_two_key_types_is_matched_on_the_pinned_one(ecdsa_key: paramiko.PKey) -> None:
    rsa_key = paramiko.RSAKey.generate(2048)
    for pinned in (rsa_key, ecdsa_key):
        for listener in _serve([ecdsa_key, rsa_key]):
            config = config_for(listener.port, [fingerprint(pinned.asbytes())])
            with pytest.raises(AuthFailedError):
                connect(config, Credentials("user", "wrong", None))
            assert listener.auth_attempts == 1


def test_an_unpinned_multi_key_server_is_refused_without_authentication(
    ecdsa_key: paramiko.PKey, other_key: paramiko.PKey
) -> None:
    rsa_key = paramiko.RSAKey.generate(2048)
    for listener in _serve([ecdsa_key, rsa_key]):
        with pytest.raises(HostKeyMismatchError):
            connect(config_for(listener.port, [fingerprint(other_key.asbytes())]), Credentials("u", "p", None))
        assert listener.auth_attempts == 0


def test_a_refused_connection_is_a_connection_error_not_a_hang() -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    started = time.monotonic()
    with pytest.raises(ConnectionLostError):
        connect(config_for(port, ["SHA256:" + "A" * 43]), Credentials("u", "p", None))
    assert time.monotonic() - started < 3.0


def test_a_server_that_accepts_and_never_speaks_times_out() -> None:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    sock.listen(1)
    port = sock.getsockname()[1]
    held: list[socket.socket] = []
    threading.Thread(target=lambda: held.append(sock.accept()[0]), daemon=True).start()
    started = time.monotonic()
    try:
        with pytest.raises((SessionTimeoutError, ConnectionLostError)) as exc_info:
            connect(config_for(port, ["SHA256:" + "A" * 43], timeout_s=0.5), Credentials("u", "p", None))
    finally:
        for s in held:
            s.close()
        sock.close()
    assert time.monotonic() - started < 5.0
    assert "127.0.0.1" not in str(exc_info.value)


def test_credentials_never_show_in_repr() -> None:
    assert "hunter2" not in repr(Credentials("someone", "hunter2", None))


class _DeadTransport:
    def is_active(self) -> bool:
        return False


class _LiveTransport:
    def is_active(self) -> bool:
        return True


class _FailingClient:
    def stat(self, path: str) -> object:
        raise OSError("Socket is closed")  # what paramiko raises on a dead connection: no errno


def test_a_bare_oserror_on_an_inactive_transport_is_a_lost_connection_not_an_sftp_failure() -> None:
    from tributary.loaders.sftp.session import ParamikoSession, RemoteError

    dead = ParamikoSession(_DeadTransport(), _FailingClient())  # type: ignore[arg-type]
    with pytest.raises(ConnectionLostError):
        dead.size("/x")
    live = ParamikoSession(_LiveTransport(), _FailingClient())  # type: ignore[arg-type]
    with pytest.raises(RemoteError) as exc_info:
        live.size("/x")  # the same exception on a live transport is a genuine server-side failure
    assert exc_info.value.status == 4
