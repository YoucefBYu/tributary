"""`SftpLoader` against an in-memory fake session: the deterministic name, write-then-rename, duplicate
handling, error classification and PHI-free logs. A real server is `test_integration_sftp_loader.py`."""

from __future__ import annotations

import pytest
from tributary.loaders.http.config import CircuitBreakerSpec
from tributary.loaders.sftp.config import SftpDestinationConfig
from tributary.loaders.sftp.loader import SftpLoader, remote_name
from tributary.loaders.sftp.session import (
    AuthFailedError,
    ConnectionLostError,
    HostKeyMismatchError,
    RemoteError,
    SessionTimeoutError,
)
from tributary.sdk import Message, ids
from tributary.sdk import headers as h
from tributary.sdk.fakes import RecordingLogger

PHI_MARKER = "SYNTH-PATIENT-ZZZ"


def make_message(payload: bytes | None = b"MSH|^~\\&|TRIBUTARY-SYNTH\r", msg_id: str | None = None) -> Message:
    return Message(
        headers={
            h.TB_MSG_ID: msg_id or ids.uuid7(),
            h.TB_CHANNEL: "c",
            h.TB_CHANNEL_VERSION: "0",
            h.TB_SOURCE: "mllp:i@p",
            h.TB_RECEIVED_AT: "2026-01-01T00:00:00.000Z",
            h.TB_CONTENT_TYPE: "application/hl7-v2",
            h.TB_ATTEMPT: "0",
            h.TRACEPARENT: ids.new_traceparent(),
            h.TB_DESTINATION: "sftp-out",
        },
        payload=payload,
    )


def make_config(**overrides: object) -> SftpDestinationConfig:
    base: dict[str, object] = {
        "channel": "c",
        "destination": "sftp-out",
        "host": "sftp.example.org",
        "port": 22,
        "host_keys": ("SHA256:" + "A" * 43,),
        "auth_type": "password",
        "auth_secret_ref": "sftp-out",
        "directory": "/out",
        "file_name_prefix": "adt-",
        "file_extension": ".hl7",
        "timeout_s": 2.0,
        "circuit_breaker": CircuitBreakerSpec(3, 30.0),
        "idempotency_technique": "deterministic-name",
        "backoff": "fixed",
        "initial_delay_ms": 10,
        "max_delay_ms": 100,
        "max_attempts": 3,
        "on_exhaust": "halt-partition",
    }
    base.update(overrides)
    return SftpDestinationConfig(**base)  # type: ignore[arg-type]


class FakeSession:
    """A remote directory in a dict. `fail` maps an operation name to an exception raised once."""

    def __init__(self, files: dict[str, bytes] | None = None) -> None:
        self.files: dict[str, bytes] = dict(files or {})
        self.fail: dict[str, Exception] = {}
        self.calls: list[tuple[str, str]] = []
        self.closed = False
        self.short_write = False

    def _maybe_fail(self, op: str) -> None:
        exc = self.fail.pop(op, None)
        if exc is not None:
            raise exc

    def size(self, path: str) -> int | None:
        self.calls.append(("size", path))
        self._maybe_fail("size")
        return len(self.files[path]) if path in self.files else None

    def write(self, path: str, data: bytes) -> None:
        self.calls.append(("write", path))
        self._maybe_fail("write")
        self.files[path] = data[:-1] if self.short_write else data

    def rename(self, src: str, dst: str) -> None:
        self.calls.append(("rename", f"{src}->{dst}"))
        self._maybe_fail("rename")
        if dst in self.files:
            raise RemoteError(4)  # a plain SFTP rename never overwrites
        self.files[dst] = self.files.pop(src)

    def remove(self, path: str) -> None:
        self.calls.append(("remove", path))
        self._maybe_fail("remove")
        self.files.pop(path, None)

    def close(self) -> None:
        self.closed = True


def loader_for(session: FakeSession, **cfg: object) -> tuple[SftpLoader, list[int]]:
    connects: list[int] = []

    def connect() -> FakeSession:
        connects.append(1)
        return session

    return SftpLoader(make_config(**cfg), connect), connects


def test_remote_name_is_prefix_msg_id_extension_and_nothing_else() -> None:
    assert remote_name(make_config(), "0199036e-7a3e") == "adt-0199036e-7a3e.hl7"


def test_delivery_writes_a_part_file_then_renames_it_into_place() -> None:
    session = FakeSession()
    loader, _ = loader_for(session)
    message = make_message()
    outcome = loader.deliver(message)
    assert (outcome.success, outcome.reason) == (True, "success")
    name = f"/out/adt-{message.msg_id}.hl7"
    assert session.files == {name: message.payload}
    ops = [c for c in session.calls if c[0] in ("write", "rename")]
    assert ops == [("write", name + ".part"), ("rename", f"{name}.part->{name}")]


def test_a_redelivery_is_a_duplicate_success_and_writes_nothing() -> None:
    session = FakeSession()
    loader, _ = loader_for(session)
    message = make_message()
    assert loader.deliver(message).reason == "success"
    writes_before = [c for c in session.calls if c[0] == "write"]
    second = loader.deliver(message)
    assert (second.success, second.reason) == (True, "duplicate")
    assert [c for c in session.calls if c[0] == "write"] == writes_before
    assert len(session.files) == 1


def test_a_same_named_file_of_another_size_is_a_non_retryable_conflict_not_a_success() -> None:
    message = make_message()
    session = FakeSession({f"/out/adt-{message.msg_id}.hl7": b"someone else's file, different length"})
    outcome = loader_for(session)[0].deliver(message)
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "SFTP_NAME_CONFLICT", False)


def test_a_leftover_part_file_from_a_crash_is_overwritten() -> None:
    message = make_message()
    name = f"/out/adt-{message.msg_id}.hl7"
    session = FakeSession({name + ".part": b"half"})
    outcome = loader_for(session)[0].deliver(message)
    assert outcome.success is True
    assert session.files == {name: message.payload}


def test_a_lost_rename_race_is_a_duplicate_and_cleans_up_the_part_file() -> None:
    message = make_message()
    name = f"/out/adt-{message.msg_id}.hl7"
    session = FakeSession()

    def race(op: str = "rename") -> None:
        session.files[name] = message.payload or b""  # another loader wins between our size() and rename()

    class Racing(FakeSession):
        def rename(self, src: str, dst: str) -> None:
            race()
            super().rename(src, dst)

    racing = Racing()
    session = racing
    outcome = loader_for(racing)[0].deliver(message)
    assert (outcome.success, outcome.reason) == (True, "duplicate")
    assert list(racing.files) == [name]  # no .part left behind


def test_a_short_write_is_a_retryable_failure_and_never_renamed() -> None:
    session = FakeSession()
    session.short_write = True
    outcome = loader_for(session)[0].deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, "SFTP_4", True)
    assert not any(op == "rename" for op, _ in session.calls)


@pytest.mark.parametrize(
    ("exc", "code", "retryable"),
    [
        (HostKeyMismatchError("x"), "SSH_HOST_KEY_MISMATCH", False),
        (AuthFailedError("x"), "SSH_AUTH_FAILED", False),
        (SessionTimeoutError("x"), "TIMEOUT", True),
        (ConnectionLostError("x"), "CONNECT_FAILED", True),
        (RemoteError(2), "SFTP_2", False),
        (RemoteError(3), "SFTP_3", False),
        (RemoteError(4), "SFTP_4", True),
    ],
)
def test_connect_and_operation_failures_are_classified(exc: Exception, code: str, retryable: bool) -> None:
    def connect() -> FakeSession:
        raise exc

    outcome = SftpLoader(make_config(), connect).deliver(make_message())
    assert (outcome.success, outcome.reason, outcome.retryable) == (False, code, retryable)


def test_a_fresh_connection_failure_is_not_retried_inside_one_delivery() -> None:
    calls: list[int] = []

    def connect() -> FakeSession:
        calls.append(1)
        raise ConnectionLostError("down")

    SftpLoader(make_config(), connect).deliver(make_message())
    assert len(calls) == 1  # one bounded wait against a down host, not two


def test_a_dead_reused_session_is_replaced_once_transparently() -> None:
    dead, alive = FakeSession(), FakeSession()
    queue = [dead, alive]

    def connect() -> FakeSession:
        return queue.pop(0)

    loader = SftpLoader(make_config(), connect)
    assert loader.deliver(make_message()).success  # uses `dead`, which works and stays cached
    dead.fail["size"] = ConnectionLostError("half-open")
    message = make_message()
    outcome = loader.deliver(message)
    assert outcome.success is True
    assert dead.closed is True
    assert f"/out/adt-{message.msg_id}.hl7" in alive.files


def test_the_session_is_dropped_after_a_failure_so_the_next_delivery_reconnects() -> None:
    first, second = FakeSession(), FakeSession()
    queue = [first, second]
    loader = SftpLoader(make_config(), lambda: queue.pop(0))
    first.fail["write"] = RemoteError(4)
    assert loader.deliver(make_message()).success is False
    assert first.closed is True
    assert loader.deliver(make_message()).success is True


@pytest.mark.parametrize("bad_id", ["../etc/passwd", "a/b", "id with space", "x" * 65, "", "a\nb"])
def test_a_msg_id_that_is_not_a_plain_token_never_becomes_a_name(bad_id: str) -> None:
    """`Message.msg_id` is already a validated UUIDv7, so this is defence in depth: the name builder
    refuses anything that could carry a separator, whatever the type system upstream promised."""
    with pytest.raises(ValueError, match="plain token"):
        remote_name(make_config(), bad_id)


def test_an_absent_payload_is_refused() -> None:
    outcome = loader_for(FakeSession())[0].deliver(make_message(payload=None))
    assert outcome.reason == "PAYLOAD_SHAPE_INVALID"


def test_circuit_opens_after_consecutive_retryable_failures() -> None:
    def connect() -> FakeSession:
        raise ConnectionLostError("down")

    loader = SftpLoader(make_config(), connect)
    for _ in range(3):
        loader.deliver(make_message())
    outcome = loader.deliver(make_message())
    assert (outcome.reason, outcome.retryable) == ("circuit-open", True)
    assert loader.metrics.circuit_opens == 1


def test_a_non_retryable_failure_does_not_move_the_breaker() -> None:
    def connect() -> FakeSession:
        raise HostKeyMismatchError("x")

    loader = SftpLoader(make_config(), connect)
    for _ in range(5):
        assert loader.deliver(make_message()).reason == "SSH_HOST_KEY_MISMATCH"
    assert loader.breaker.allow()


def test_logs_and_outcomes_carry_no_payload_or_file_name() -> None:
    log = RecordingLogger()
    session = FakeSession()
    session.fail["write"] = RemoteError(4)
    payload = f"PID|1||{PHI_MARKER}".encode()
    loader = SftpLoader(make_config(file_name_prefix="adt-"), lambda: session, log=log)
    message = make_message(payload)
    outcome = loader.deliver(message)
    dumped = repr(log.records) + repr(outcome)
    assert PHI_MARKER not in dumped
    assert "/out/" not in dumped and ".hl7" not in dumped and "adt-" not in dumped


def test_close_closes_the_session() -> None:
    session = FakeSession()
    loader, _ = loader_for(session)
    loader.deliver(make_message())
    loader.close()
    assert session.closed is True
