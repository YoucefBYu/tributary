"""`session.connect` and `ParamikoSession` against a real in-process SFTP server over real loopback
sockets: host-key pinning by SHA-256 (no trust on first use), credentials never sent to an unpinned
host, key-type fallback, and the real protocol behaviour of rename/read/remove."""

from __future__ import annotations

import asyncio
import os
from dataclasses import replace
from pathlib import Path

import paramiko  # type: ignore[import-untyped]
import pytest
from tributary.receivers.secrets import DirSecretResolver
from tributary.receivers.sftp.poller import CLAIM_PREFIX, SftpPoller
from tributary.receivers.sftp.session import (
    DISABLED_ALGORITHMS,
    HostKeyMismatchError,
    NotRegularFileError,
    ParamikoSession,
    RemoteExistsError,
    RemoteNotFoundError,
    SessionError,
    connect,
    disabled_algorithms,
    fingerprint,
    verify_host_key,
)
from tributary.sdk import Message

from tests.support.kafka_helpers import free_port

from .fake_server import PASSWORD, USER, FakeSftpServer
from .test_poller import HL7, settings


def pin(key: paramiko.PKey) -> str:
    return fingerprint(key.asbytes())


@pytest.fixture(scope="module")
def ed25519() -> paramiko.PKey:
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import ed25519 as ed

    private = ed.Ed25519PrivateKey.generate()
    pem = private.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.OpenSSH, serialization.NoEncryption()
    )
    import io

    return paramiko.Ed25519Key.from_private_key(io.StringIO(pem.decode()))


@pytest.fixture(scope="module")
def rsa() -> paramiko.PKey:
    return paramiko.RSAKey.generate(2048)


@pytest.fixture
def secrets(tmp_path: Path) -> DirSecretResolver:
    ref = tmp_path / "secrets" / "sftp-cred"
    ref.mkdir(parents=True)
    (ref / "username").write_text(USER + "\n", encoding="utf-8")
    (ref / "password").write_text(PASSWORD + "\n", encoding="utf-8")
    return DirSecretResolver(tmp_path / "secrets")


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "srv" / "in").mkdir(parents=True)
    (tmp_path / "srv" / "rejected").mkdir()
    (tmp_path / "srv" / "done").mkdir()
    return tmp_path / "srv"


def cfg_for(server: FakeSftpServer, pins: list[str], **kw: object):  # type: ignore[no-untyped-def]
    return replace(
        settings(directory="/in", reject_directory="/rejected", archive_directory="/done"),
        host="127.0.0.1",
        port=server.port,
        host_keys=frozenset(pins),
        auth_secret_ref="sftp-cred",  # noqa: S106
        connect_timeout_s=5.0,
        op_timeout_s=5.0,
        **kw,
    )


def test_fingerprint_is_the_openssh_sha256_form(ed25519: paramiko.PKey) -> None:
    fp = pin(ed25519)
    assert fp.startswith("SHA256:") and "=" not in fp and len(fp) == len("SHA256:") + 43
    assert verify_host_key(ed25519.asbytes(), [fp, "SHA256:" + "A" * 43])
    assert not verify_host_key(ed25519.asbytes(), ["SHA256:" + "A" * 43])


def test_connect_with_the_pinned_key_lists_reads_and_removes(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    (root / "in" / "a.hl7").write_bytes(HL7)
    with FakeSftpServer(root, [ed25519]) as srv:
        session = connect(cfg_for(srv, [pin(ed25519)]), secrets)
        try:
            entries = session.list_dir("/in", 100)
            assert [(e.name, e.size, e.is_regular) for e in entries] == [("a.hl7", len(HL7), True)]
            assert session.read_bounded("/in/a.hl7", 1000) == HL7
            assert session.read_bounded("/in/a.hl7", len(HL7) - 1) is None  # larger than the bound
            session.remove("/in/a.hl7")
            assert not session.exists("/in/a.hl7")
        finally:
            session.close()


def test_an_unpinned_host_key_is_refused_before_any_credential_is_sent(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    with FakeSftpServer(root, [ed25519]) as srv:
        with pytest.raises(HostKeyMismatchError):
            connect(cfg_for(srv, ["SHA256:" + "B" * 43]), secrets)
        assert srv.auth_attempts == 0


def test_no_trust_on_first_use_even_when_the_pin_set_is_a_different_key(
    root: Path, ed25519: paramiko.PKey, rsa: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    with FakeSftpServer(root, [ed25519]) as srv:
        with pytest.raises(HostKeyMismatchError):
            connect(cfg_for(srv, [pin(rsa)]), secrets)  # a real key, but not this server's
        assert srv.auth_attempts == 0


def test_pin_for_the_second_host_key_type_is_honoured_by_retrying_the_handshake(
    root: Path, ed25519: paramiko.PKey, rsa: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    with FakeSftpServer(root, [ed25519, rsa]) as srv:
        session = connect(cfg_for(srv, [pin(rsa)]), secrets)  # server may prefer ed25519; we pinned rsa
        session.close()
        assert srv.auth_attempts == 1


def test_wrong_password_is_a_session_error_with_no_secret_text(
    root: Path, ed25519: paramiko.PKey, tmp_path: Path
) -> None:
    ref = tmp_path / "bad" / "sftp-cred"
    ref.mkdir(parents=True)
    (ref / "username").write_text(USER, encoding="utf-8")
    (ref / "password").write_text("definitely-wrong-SYNTHPW", encoding="utf-8")  # tb-secret-scan: allow
    with FakeSftpServer(root, [ed25519]) as srv:
        with pytest.raises(SessionError) as info:
            connect(cfg_for(srv, [pin(ed25519)]), DirSecretResolver(tmp_path / "bad"))
        assert "SYNTHPW" not in str(info.value) and USER not in str(info.value)


def test_connection_refused_is_a_session_error(ed25519: paramiko.PKey, secrets: DirSecretResolver, root: Path) -> None:
    with FakeSftpServer(root, [ed25519]) as srv:
        cfg = cfg_for(srv, [pin(ed25519)])
    cfg = replace(cfg, port=free_port())  # nothing listens here
    with pytest.raises(SessionError):
        connect(cfg, secrets)


def test_rename_is_atomic_no_overwrite_and_maps_errors(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    (root / "in" / "a.hl7").write_bytes(b"1")
    (root / "in" / "b.hl7").write_bytes(b"2")
    with FakeSftpServer(root, [ed25519]) as srv:
        s = connect(cfg_for(srv, [pin(ed25519)]), secrets)
        try:
            with pytest.raises((RemoteExistsError, SessionError)):
                s.rename("/in/a.hl7", "/in/b.hl7")  # target exists: refused, b untouched
            assert (root / "in" / "b.hl7").read_bytes() == b"2"
            with pytest.raises(RemoteNotFoundError):
                s.rename("/in/missing.hl7", "/in/c.hl7")
            s.rename("/in/a.hl7", "/in/c.hl7")
            assert (root / "in" / "c.hl7").read_bytes() == b"1"
        finally:
            s.close()


class Rec:
    def __init__(self, outcome: bool = True) -> None:
        self.calls: list[Message] = []
        self.outcome = outcome

    async def publish(self, message: Message, *, timeout_s: float) -> bool:
        self.calls.append(message)
        return self.outcome


def test_poller_end_to_end_over_real_sftp_moves_only_after_commit(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    (root / "in" / "a.hl7").write_bytes(HL7)
    (root / "in" / "big.hl7").write_bytes(b"x" * 500)
    with FakeSftpServer(root, [ed25519]) as srv:
        cfg = cfg_for(srv, [pin(ed25519)], after_commit="move", max_message_bytes=100)
        cfg = replace(cfg, max_message_bytes=len(HL7) + 5)
        # refuse first: nothing is removed or archived while Kafka is down
        down = Rec(outcome=False)
        p = SftpPoller(cfg, down, lambda: connect(cfg, secrets))
        assert asyncio.run(p.poll_once()) == 0
        assert sorted(x.name for x in (root / "in").iterdir()) == ["a.hl7", "big.hl7"]
        assert list((root / "done").iterdir()) == []
        # Kafka back: a.hl7 archived, big.hl7 rejected (never deleted)
        up = Rec()
        p2 = SftpPoller(cfg, up, lambda: connect(cfg, secrets))
        assert asyncio.run(p2.poll_once()) == 1
        assert [x.name for x in (root / "done").iterdir()] == ["a.hl7"]
        assert [x.name for x in (root / "rejected").iterdir()] == ["big.hl7"]
        assert [x for x in (root / "in").iterdir() if x.name.startswith(CLAIM_PREFIX)] == []
        assert up.calls[0].payload == HL7


def test_two_pollers_on_one_real_sftp_directory_ingest_each_file_exactly_once(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    for i in range(30):
        (root / "in" / f"f{i:02}.hl7").write_bytes(HL7 + str(i).encode())
    with FakeSftpServer(root, [ed25519]) as srv:
        cfg_a = replace(cfg_for(srv, [pin(ed25519)]), instance="sftp-0")
        cfg_b = replace(cfg_a, instance="sftp-1")
        rec_a, rec_b = Rec(), Rec()
        a = SftpPoller(cfg_a, rec_a, lambda: connect(cfg_a, secrets))
        b = SftpPoller(cfg_b, rec_b, lambda: connect(cfg_b, secrets))

        async def go() -> None:
            await asyncio.gather(a.poll_once(), b.poll_once())

        asyncio.run(go())
    payloads = [m.payload for m in rec_a.calls + rec_b.calls]
    assert len(payloads) == 30 and len(set(payloads)) == 30
    assert list((root / "in").iterdir()) == []  # every file committed, none stranded under a claim name


def test_a_non_utf8_file_name_is_skipped_and_counted_not_fatal(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    (root / "in" / "good.hl7").write_bytes(HL7)
    try:
        (root / "in").joinpath(os.fsdecode(b"bad-\xff\xfe.hl7")).write_bytes(b"x")
    except OSError:
        pytest.skip("filesystem refuses non-UTF-8 names")
    with FakeSftpServer(root, [ed25519]) as srv:
        session = connect(cfg_for(srv, [pin(ed25519)]), secrets)
        try:
            entries = session.list_dir("/in", 100)
            assert [e.name for e in entries] == ["good.hl7"] and session.undecodable_names == 1
        finally:
            session.close()


def test_read_bounded_refuses_a_link_a_directory_and_a_missing_name_before_reading(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    (root / "in" / "real.hl7").write_bytes(HL7)
    (root / "in" / "link.hl7").symlink_to(root / "in" / "real.hl7")
    (root / "in" / "subdir").mkdir()
    with FakeSftpServer(root, [ed25519]) as srv:
        session = connect(cfg_for(srv, [pin(ed25519)]), secrets)
        try:
            assert session.read_bounded("/in/real.hl7", 1000) == HL7
            with pytest.raises(NotRegularFileError):
                session.read_bounded("/in/link.hl7", 1000)
            with pytest.raises(SessionError):  # a server may refuse the open itself; either way nothing is read
                session.read_bounded("/in/subdir", 1000)
            with pytest.raises(RemoteNotFoundError):
                session.read_bounded("/in/none.hl7", 1000)
        finally:
            session.close()


def test_a_name_that_no_longer_matches_the_opened_handle_is_refused(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    """The name is re-checked after the open: a size that differs from the handle's is a swapped object."""
    (root / "in" / "real.hl7").write_bytes(HL7)
    with FakeSftpServer(root, [ed25519]) as srv:
        session = connect(cfg_for(srv, [pin(ed25519)]), secrets)
        try:
            real_lstat = session._client.lstat

            def swapped(path: str) -> paramiko.SFTPAttributes:
                attrs = real_lstat(path)
                attrs.st_size = (attrs.st_size or 0) + 1
                return attrs

            session._client.lstat = swapped  # type: ignore[method-assign]
            with pytest.raises(NotRegularFileError):
                session.read_bounded("/in/real.hl7", 1000)
        finally:
            session.close()


def _readdir_reply(entries: list[bytes], claimed: int) -> paramiko.Message:
    msg = paramiko.Message()
    msg.add_int(claimed)
    for name in entries:
        msg.add_string(name)
        msg.add_string(b"")
        msg.add_int(0)  # attribute flags: none
    msg.rewind()
    return msg


class _ScriptedClient:
    """Answers OPENDIR, then a fixed list of READDIR replies; counts the READDIRs it was asked for."""

    def __init__(self, replies: list[paramiko.Message]) -> None:
        self.replies = replies
        self.readdirs = 0

    def _request(self, cmd: int, *_args: object) -> tuple[int, paramiko.Message]:
        from paramiko.sftp import CMD_CLOSE, CMD_OPENDIR, CMD_READDIR

        if cmd == CMD_OPENDIR:
            msg = paramiko.Message()
            msg.add_string(b"h")
            msg.rewind()
            return 0, msg
        if cmd == CMD_CLOSE:
            return 0, paramiko.Message()
        assert cmd == CMD_READDIR
        self.readdirs += 1
        if not self.replies:
            raise EOFError
        return 0, self.replies.pop(0)


def _scripted(replies: list[paramiko.Message]) -> tuple[ParamikoSession, _ScriptedClient]:
    client = _ScriptedClient(replies)
    return ParamikoSession(None, client), client  # type: ignore[arg-type]


def test_a_listing_limit_holds_per_entry_inside_one_reply() -> None:
    session, _ = _scripted([_readdir_reply([f"f{i}".encode() for i in range(500)], 500)])
    assert len(session.list_dir("/in", 10)) == 10


def test_a_hostile_entry_count_larger_than_the_reply_is_refused_not_looped() -> None:
    session, _ = _scripted([_readdir_reply([b"a"], 0xFFFFFFFF)])
    with pytest.raises(SessionError):
        session.list_dir("/in", 1000)


def test_a_server_that_only_returns_dot_entries_cannot_keep_a_listing_running() -> None:
    dots = [_readdir_reply([b"."] * 100, 100) for _ in range(10_000)]
    session, client = _scripted(dots)
    assert session.list_dir("/in", 10) == []
    assert client.readdirs < 100  # stopped by the scan bound long before the script ran out


def test_disabled_algorithm_set_is_pinned() -> None:
    d = disabled_algorithms([])
    assert set(d["ciphers"]) == {"3des-cbc", "aes128-cbc", "aes192-cbc", "aes256-cbc"}
    assert set(d["kex"]) == {
        "diffie-hellman-group1-sha1",
        "diffie-hellman-group14-sha1",
        "diffie-hellman-group-exchange-sha1",
        "diffie-hellman-group-exchange-sha256",  # server-chosen group, client minimum 1024 bits
    }
    assert set(d["macs"]) == {"hmac-md5", "hmac-md5-96", "hmac-sha1", "hmac-sha1-96"}
    assert d["keys"] == ["ssh-rsa"] and d["pubkeys"] == ["ssh-rsa"]
    assert "ssh-ed25519" in disabled_algorithms(["ssh-ed25519"])["keys"]


def test_negotiated_algorithms_are_never_from_the_disabled_set(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver
) -> None:
    with FakeSftpServer(root, [ed25519]) as srv:
        session = connect(cfg_for(srv, [pin(ed25519)]), secrets)
        try:
            t = session._transport
            assert t.remote_cipher not in DISABLED_ALGORITHMS["ciphers"]
            assert t.local_cipher not in DISABLED_ALGORITHMS["ciphers"]
            assert t.kex_engine.__class__.__name__ not in {"KexGroup1", "KexGroup14", "KexGex"}
            assert t.local_mac not in DISABLED_ALGORITHMS["macs"]
        finally:
            session.close()


@pytest.mark.parametrize("weak", ["aes128-cbc", "aes256-cbc", "3des-cbc"])
def test_a_server_that_only_offers_a_weak_cipher_is_refused(
    root: Path, ed25519: paramiko.PKey, secrets: DirSecretResolver, weak: str
) -> None:
    with FakeSftpServer(root, [ed25519], ciphers=(weak,)) as srv, pytest.raises(SessionError):
        connect(cfg_for(srv, [pin(ed25519)]), secrets)
    assert srv.auth_attempts == 0
