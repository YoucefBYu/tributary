"""`SftpLoader` against a real OpenSSH server (`atmoz/sftp`, testcontainers), proving invariant 4 and
README 3.4.4 for real: a message delivered twice leaves exactly one file with the right bytes, no `.part`
behind; a wrong host-key pin is refused before authentication; a dead reused connection is replaced.

The server's RSA host key is generated here and mounted, so its fingerprint is known and pinned. The
server also has an ed25519 key it offers first, so pinning the RSA key exercises the retry with the
mismatched key type disabled against a real sshd, not just the in-process server of `test_session.py`.

The genuine-redelivery half (a real `ConsumerLoop` replay from an uncommitted offset) is
`tests/integration/test_sftp_loader_duplicate_delivery_uncommitted_redelivery.py`.

`uv run pytest -m integration packages/tributary-loaders/tests/sftp`.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import paramiko
import pytest
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tributary.loaders.sftp.config import SftpDestinationConfig
from tributary.loaders.sftp.loader import SftpLoader
from tributary.loaders.sftp.session import connect, fingerprint, resolve_credentials
from tributary.sdk import Message

from .test_loader import make_config, make_message

pytestmark = pytest.mark.integration

IMAGE = "atmoz/sftp:alpine"
USER = "synth"
PW_REF, KEY_REF, BAD_REF = "pw", "key", "badpw"  # secretRef directory names under the test secrets dir
PASSWORD = uuid.uuid4().hex  # a fresh throwaway login for a container that lives for this module


class _Resolver:
    def __init__(self, base: Path) -> None:
        self._base = base

    def resolve_text(self, ref: str, key: str) -> str:
        return (self._base / ref / key).read_text(encoding="utf-8").strip()

    def resolve_file(self, ref: str, key: str) -> Path:
        return self._base / ref / key


class Server:
    def __init__(self, container: DockerContainer, host_key: paramiko.PKey, client_key: paramiko.PKey, secrets: Path):
        self.container = container
        self.host = container.get_container_host_ip()
        self.port = int(container.get_exposed_port(22))
        self.pin = fingerprint(host_key.asbytes())
        self.client_key = client_key
        self.resolver = _Resolver(secrets)

    def config(self, **overrides: object) -> SftpDestinationConfig:
        base: dict[str, object] = {
            "host": self.host,
            "port": self.port,
            "host_keys": (self.pin,),
            "auth_type": "password",
            "auth_secret_ref": PW_REF,
            "directory": "/upload",
            "file_name_prefix": "adt-",
            "file_extension": ".hl7",
            "timeout_s": 10.0,
        }
        base.update(overrides)
        return make_config(**base)

    def loader(self, **overrides: object) -> SftpLoader:
        config = self.config(**overrides)
        credentials = resolve_credentials(config, self.resolver)
        return SftpLoader(config, lambda: connect(config, credentials))

    def listing(self) -> list[str]:
        result = self.container.exec(["ls", "-1", f"/home/{USER}/upload"])
        assert result.exit_code == 0, result.output
        return [line for line in result.output.decode().splitlines() if line]

    def read(self, name: str) -> bytes:
        result = self.container.exec(["cat", f"/home/{USER}/upload/{name}"])
        assert result.exit_code == 0
        return bytes(result.output)


@pytest.fixture(scope="module")
def sftp(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Server]:
    base = tmp_path_factory.mktemp("sftp")
    host_key = paramiko.RSAKey.generate(2048)
    client_key = paramiko.RSAKey.generate(2048)
    host_key_path, pub_path = base / "ssh_host_rsa_key", base / "client.pub"
    host_key.write_private_key_file(str(host_key_path))
    host_key_path.chmod(0o600)
    pub_path.write_text(f"{client_key.get_name()} {client_key.get_base64()}\n", encoding="utf-8")
    secrets = base / "secrets"
    (secrets / "pw").mkdir(parents=True)
    (secrets / "pw" / "username").write_text(USER, encoding="utf-8")
    (secrets / "pw" / "password").write_text(PASSWORD, encoding="utf-8")
    (secrets / "key").mkdir()
    (secrets / "key" / "username").write_text(USER, encoding="utf-8")
    client_key.write_private_key_file(str(secrets / "key" / "private_key"))
    (secrets / "badpw").mkdir()
    (secrets / "badpw" / "username").write_text(USER, encoding="utf-8")
    (secrets / "badpw" / "password").write_text("not-the-password", encoding="utf-8")

    container = (
        DockerContainer(IMAGE)
        .with_exposed_ports(22)
        .with_command(f"{USER}:{PASSWORD}:::upload")
        .with_volume_mapping(str(host_key_path), "/etc/ssh/ssh_host_rsa_key", "ro")
        .with_volume_mapping(str(pub_path), f"/home/{USER}/.ssh/keys/client.pub", "ro")
        .waiting_for(LogMessageWaitStrategy("Server listening on").with_startup_timeout(60))
    )
    with container:
        yield Server(container, host_key, client_key, secrets)


def message_with(payload: bytes) -> Message:
    return make_message(payload)


def test_the_same_message_delivered_twice_leaves_one_file_with_the_right_bytes(sftp: Server) -> None:
    loader = sftp.loader()
    message = message_with(b"MSH|^~\\&|TRIBUTARY-SYNTH|x\rPID|1\r")
    name = f"adt-{message.msg_id}.hl7"

    first = loader.deliver(message)
    second = loader.deliver(message)  # a redelivery of the identical record

    assert (first.success, first.reason) == (True, "success")
    assert (second.success, second.reason) == (True, "duplicate")
    assert [f for f in sftp.listing() if message.msg_id in f] == [name]  # one file, and no .part
    assert sftp.read(name) == message.payload
    loader.close()


def test_two_different_messages_are_two_files(sftp: Server) -> None:
    loader = sftp.loader()
    a, b = message_with(b"one\r"), message_with(b"two\r")
    assert loader.deliver(a).success and loader.deliver(b).success
    names = sftp.listing()
    assert f"adt-{a.msg_id}.hl7" in names and f"adt-{b.msg_id}.hl7" in names
    loader.close()


def test_a_reprocessed_copy_with_a_new_msg_id_is_a_new_file_by_design(sftp: Server) -> None:
    """README 5: reprocess mints a new `tb-msg-id`, so it is not deduplicated away. Also the control that
    shows the one-file assertions above are not vacuous: the name really is keyed on the id."""
    loader = sftp.loader()
    payload = b"same bytes\r"
    a, b = message_with(payload), message_with(payload)
    assert loader.deliver(a).success and loader.deliver(b).success
    names = sftp.listing()
    assert sum(1 for n in names if a.msg_id in n or b.msg_id in n) == 2
    loader.close()


def test_a_real_leftover_part_file_does_not_block_delivery(sftp: Server) -> None:
    message = message_with(b"payload after a crash\r")
    name = f"adt-{message.msg_id}.hl7"
    part = f"/home/{USER}/upload/{name}.part"
    result = sftp.container.exec(["sh", "-c", f"echo half > {part}; chmod 666 {part}"])  # writable by the sftp user
    assert result.exit_code == 0
    loader = sftp.loader()
    assert loader.deliver(message).success
    assert sftp.read(name) == message.payload
    assert name + ".part" not in sftp.listing()
    loader.close()


def test_an_existing_file_of_another_size_is_refused_not_treated_as_a_duplicate(sftp: Server) -> None:
    message = message_with(b"the real payload\r")
    name = f"adt-{message.msg_id}.hl7"
    assert sftp.container.exec(["sh", "-c", f"echo foreign > /home/{USER}/upload/{name}"]).exit_code == 0
    outcome = sftp.loader().deliver(message)
    assert (outcome.reason, outcome.retryable) == ("SFTP_NAME_CONFLICT", False)


def test_private_key_authentication(sftp: Server) -> None:
    loader = sftp.loader(auth_type="private-key", auth_secret_ref=KEY_REF)
    message = message_with(b"key auth\r")
    assert loader.deliver(message).success
    assert f"adt-{message.msg_id}.hl7" in sftp.listing()
    loader.close()


def test_a_wrong_host_key_pin_is_refused_before_authentication_and_writes_nothing(sftp: Server) -> None:
    loader = sftp.loader(host_keys=("SHA256:" + "B" * 43,))
    message = message_with(b"never sent\r")
    outcome = loader.deliver(message)
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "SSH_HOST_KEY_MISMATCH", False)
    assert not any(message.msg_id in n for n in sftp.listing())


def test_a_wrong_password_is_a_non_retryable_auth_failure(sftp: Server) -> None:
    outcome = sftp.loader(auth_secret_ref=BAD_REF).deliver(message_with(b"x\r"))
    assert (outcome.reason, outcome.retryable) == ("SSH_AUTH_FAILED", False)


def test_a_missing_directory_is_sftp_2_and_not_retryable(sftp: Server) -> None:
    outcome = sftp.loader(directory="/no-such-dir").deliver(message_with(b"x\r"))
    assert (outcome.reason, outcome.retryable) == ("SFTP_2", False)


def test_a_dead_reused_connection_is_replaced_transparently(sftp: Server) -> None:
    loader = sftp.loader()
    assert loader.deliver(message_with(b"first\r")).success
    session: Any = loader._session
    session._transport.close()  # what a half-open or dropped connection looks like to the next call
    message = message_with(b"after the drop\r")
    outcome = loader.deliver(message)
    assert outcome.success is True, outcome
    assert f"adt-{message.msg_id}.hl7" in sftp.listing()
    loader.close()


def test_a_refused_connection_is_retryable_connect_failed() -> None:
    config = make_config(host="127.0.0.1", port=1, timeout_s=2.0)
    loader = SftpLoader(config, lambda: connect(config, resolve_credentials_stub()))
    outcome = loader.deliver(make_message(b"x\r"))
    assert (outcome.reason, outcome.retryable) == ("CONNECT_FAILED", True)


def resolve_credentials_stub() -> Any:
    from tributary.loaders.sftp.session import Credentials

    return Credentials("u", "p", None)
