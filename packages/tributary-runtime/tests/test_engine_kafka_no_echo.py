"""No error path echoes a secret value, a secret file's content or the secrets directory (invariant 6).

A marker is planted in every place a leak could come from: the content of the secret file, the name of
the secrets directory (so a path in a message shows up), the text of an exception a foreign resolver
raises, and every value and unknown key a link check sees. Each refusal is then checked in four
renderings: `str`, `repr`, `args`, and the one `startup_refused` line `load_engine_kafka_or_exit` writes.

Also the security review's Lows on e6b47c4: a symlink loop exits 2 instead of a traceback, control
characters in SCRAM credentials are refused, `KafkaSettings` checks the copy it stores, and group- or
world-writable secret files are refused. All values are synthetic.
"""

from __future__ import annotations

import io
import json
import os
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from tributary.runtime import engine_kafka as ek
from tributary.runtime.clients import KafkaSettings
from tributary.runtime.errors import EngineKafkaError, KafkaSecurityError
from tributary.runtime.kafka_security import PINNED_CIPHER_SUITES, SECURITY_KEYS
from tributary.runtime.logs import JsonLogger
from tributary.runtime.secrets import DirSecretResolver, SecretResolutionError

MARKER = "SYNTH-SECRET-MARKER-c41d"
REF = "tb-worker-synth"
CA_REF = "tb-ca"


def document(protocol: str = "SASL_SSL") -> dict[str, Any]:
    security: dict[str, Any] = {"protocol": protocol, "secretRef": REF, "tls": {"secretRef": CA_REF}}
    if protocol == "SASL_SSL":
        security["saslMechanism"] = "SCRAM-SHA-512"
    return {
        "apiVersion": "tributary.dev/v1",
        "kind": "EngineKafkaConnection",
        "bootstrapServers": ["kafka-0.synth:9093"],
        "security": security,
    }


@pytest.fixture
def secrets_dir(tmp_path: Path) -> Path:
    """The directory's own name carries the marker, so any message quoting a path fails the checks."""
    root = tmp_path / f"secrets-{MARKER}"
    for ref, keys in {REF: ("username", "password", "tls.crt", "tls.key"), CA_REF: ("ca.crt",)}.items():
        (root / ref).mkdir(parents=True)
        for key in keys:
            (root / ref / key).write_text(f"{MARKER}-{key}\n", encoding="utf-8")
            (root / ref / key).chmod(0o600)
    return root


def _env(tmp_path: Path, secrets_dir: Path, protocol: str = "SASL_SSL") -> dict[str, str]:
    path = tmp_path / "engine-kafka.json"
    path.write_text(json.dumps(document(protocol)), encoding="utf-8")
    return {"TB_ENGINE_KAFKA_FILE": str(path), "TB_SECRETS_DIR": str(secrets_dir)}


def assert_clean(exc: BaseException, *extra: str) -> None:
    for rendering in (str(exc), repr(exc), repr(exc.args)):
        for needle in (MARKER, *extra):
            assert needle not in rendering, type(exc).__name__
    assert exc.__cause__ is None
    assert exc.__suppress_context__ or exc.__context__ is None


def refusal_line(environ: Mapping[str, str], **kwargs: Any) -> dict[str, Any]:
    """Run the entry point, prove it exits 2 with exactly one clean line, and return the line."""
    stream = io.StringIO()
    with pytest.raises(SystemExit) as exited:
        ek.load_engine_kafka_or_exit(environ, log=JsonLogger("receiver-synth", stream=stream), **kwargs)
    assert exited.value.code == 2
    raw = stream.getvalue()
    assert MARKER not in raw
    (line,) = [json.loads(text) for text in raw.splitlines()]
    assert line["event"] == "startup_refused" and "reason" in line
    return line


# --- DirSecretResolver failures, through the resolver and through the entry point --------------------


def _write(secrets_dir: Path, key: str, content: bytes, mode: int = 0o600) -> None:
    path = secrets_dir / REF / key
    path.write_bytes(content)
    path.chmod(mode)


TEXT_FAILURES: list[tuple[str, bytes, str]] = [
    ("empty-file", b"", "empty"),
    ("whitespace-only", b"  \n\t\n", "empty"),
    ("nul", f"{MARKER}\x00tail".encode(), "control-character"),
    ("inner-newline", f"{MARKER}\nsecond-line".encode(), "control-character"),
    ("inner-cr", f"{MARKER}\rtail".encode(), "control-character"),
    ("escape", f"\x1b[31m{MARKER}".encode(), "control-character"),
    ("del", f"{MARKER}\x7f".encode(), "control-character"),
    ("not-utf8", b"\xff\xfe" + MARKER.encode(), "not-utf8"),
    ("oversized", (MARKER * 400).encode(), "oversized"),
]


@pytest.mark.parametrize(("case", "content", "reason"), TEXT_FAILURES, ids=[c[0] for c in TEXT_FAILURES])
def test_text_secret_failures_never_echo_the_value(
    tmp_path: Path, secrets_dir: Path, case: str, content: bytes, reason: str
) -> None:
    _write(secrets_dir, "password", content)
    with pytest.raises(SecretResolutionError) as direct:
        DirSecretResolver(secrets_dir).resolve_text(REF, "password")
    assert direct.value.reason == reason and direct.value.code == "SECRET_UNRESOLVED"
    assert_clean(direct.value, str(secrets_dir))

    with pytest.raises(EngineKafkaError) as loaded:
        ek.load_engine_kafka(_env(tmp_path, secrets_dir))
    assert loaded.value.code == "ENGINE_KAFKA_SECRET_UNRESOLVED"
    assert f"{REF}/password" in str(loaded.value)
    assert_clean(loaded.value, str(secrets_dir))

    line = refusal_line(_env(tmp_path, secrets_dir))
    assert line["code"] == "ENGINE_KAFKA_SECRET_UNRESOLVED" and str(secrets_dir) not in line["reason"]


def test_surrounding_whitespace_is_still_stripped_not_refused(secrets_dir: Path) -> None:
    _write(secrets_dir, "password", b"  synthetic-pass\r\n")
    assert DirSecretResolver(secrets_dir).resolve_text(REF, "password") == "synthetic-pass"


@pytest.mark.parametrize("protocol", ["SASL_SSL", "SSL"])
def test_a_missing_key_never_echoes_the_path(tmp_path: Path, secrets_dir: Path, protocol: str) -> None:
    key = "password" if protocol == "SASL_SSL" else "tls.key"
    (secrets_dir / REF / key).unlink()
    with pytest.raises(SecretResolutionError) as direct:
        DirSecretResolver(secrets_dir).resolve_file(REF, key)
    assert direct.value.reason == "missing"
    assert_clean(direct.value, str(secrets_dir))
    line = refusal_line(_env(tmp_path, secrets_dir, protocol))
    assert f"{REF}/{key} does not exist" in line["reason"]


def test_a_missing_secrets_directory_never_echoes_its_path(tmp_path: Path) -> None:
    absent = tmp_path / f"absent-{MARKER}"
    assert refusal_line(_env(tmp_path, absent))["code"] == "ENGINE_KAFKA_SECRET_UNRESOLVED"


@pytest.fixture
def unreadable(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Permission bits do not stop root (these tests may run as root in a container), so the OS error is
    injected. Its text is what a real one carries: the path, here with the marker in it."""
    real_read = Path.read_bytes

    def denied(self: Path) -> bytes:
        if self.name == "password":
            raise PermissionError(13, "Permission denied", str(self))
        return real_read(self)

    monkeypatch.setattr(Path, "read_bytes", denied)
    yield


@pytest.mark.usefixtures("unreadable")
def test_an_unreadable_text_secret_never_echoes_the_os_error(tmp_path: Path, secrets_dir: Path) -> None:
    with pytest.raises(SecretResolutionError) as direct:
        DirSecretResolver(secrets_dir).resolve_text(REF, "password")
    assert direct.value.reason == "unreadable"
    assert_clean(direct.value, str(secrets_dir), "Permission denied")
    assert refusal_line(_env(tmp_path, secrets_dir))["reason"].endswith(
        f"secretRef key {REF}/password is not readable (under TB_SECRETS_DIR)"
    )


def test_a_stat_failure_never_echoes_the_os_error(
    tmp_path: Path, secrets_dir: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_stat = Path.stat

    def denied(self: Path, **kwargs: Any) -> os.stat_result:
        if self.name == "ca.crt":
            raise PermissionError(13, "Permission denied", str(self))
        return real_stat(self, **kwargs)

    monkeypatch.setattr(Path, "stat", denied)
    with pytest.raises(SecretResolutionError) as direct:
        DirSecretResolver(secrets_dir).resolve_file(CA_REF, "ca.crt")
    assert_clean(direct.value, str(secrets_dir), "Permission denied")
    assert refusal_line(_env(tmp_path, secrets_dir))["code"] == "ENGINE_KAFKA_SECRET_UNRESOLVED"


def test_a_symlink_loop_exits_2_without_a_traceback_or_path(tmp_path: Path, secrets_dir: Path) -> None:
    """Security review Low 1: `Path.resolve()` raised a bare RuntimeError holding the full path."""
    ca = secrets_dir / CA_REF / "ca.crt"
    ca.unlink()
    loop = secrets_dir / CA_REF / "loop"
    loop.symlink_to(ca)
    ca.symlink_to(loop)
    with pytest.raises(SecretResolutionError) as direct:
        DirSecretResolver(secrets_dir).resolve_file(CA_REF, "ca.crt")
    assert direct.value.reason in ("unreadable", "missing")
    assert_clean(direct.value, str(secrets_dir))
    assert refusal_line(_env(tmp_path, secrets_dir))["code"] == "ENGINE_KAFKA_SECRET_UNRESOLVED"


@pytest.mark.parametrize("mode", [0o620, 0o602, 0o666, 0o660])
@pytest.mark.parametrize("key", ["password", "tls.key"])
def test_group_or_world_writable_secret_files_are_refused(
    tmp_path: Path, secrets_dir: Path, mode: int, key: str
) -> None:
    """Security review Low 4. Checked on the file the symlink chain ends at (Kubernetes' `..data`)."""
    (secrets_dir / REF / key).chmod(mode)
    with pytest.raises(SecretResolutionError) as direct:
        DirSecretResolver(secrets_dir).resolve_file(REF, key)
    assert direct.value.reason == "writable"
    assert_clean(direct.value, str(secrets_dir))
    protocol = "SASL_SSL" if key == "password" else "SSL"
    assert "group- or world-writable" in refusal_line(_env(tmp_path, secrets_dir, protocol))["reason"]


def test_a_writable_target_behind_a_read_only_symlink_is_refused(secrets_dir: Path) -> None:
    target = secrets_dir / REF / "..data-password"
    target.write_text("synthetic\n", encoding="utf-8")
    target.chmod(0o666)
    (secrets_dir / REF / "password").unlink()
    (secrets_dir / REF / "password").symlink_to(target.name)
    with pytest.raises(SecretResolutionError, match="group- or world-writable"):
        DirSecretResolver(secrets_dir).resolve_text(REF, "password")


@pytest.mark.parametrize("mode", [0o400, 0o440, 0o444, 0o600, 0o644])
def test_read_only_and_owner_writable_modes_resolve(secrets_dir: Path, mode: int) -> None:
    (secrets_dir / REF / "password").chmod(mode)
    assert DirSecretResolver(secrets_dir).resolve_text(REF, "password") == f"{MARKER}-password"


def test_the_error_type_has_no_free_text_constructor() -> None:
    """A future `raise SecretResolutionError(f"... {value}")` does not type-check, and an unknown reason
    is not trusted into the message."""
    exc = SecretResolutionError(REF, "password", f"reason {MARKER}")
    assert exc.reason == "unresolvable"
    assert_clean(exc)
    assert_clean(SecretResolutionError(f"ref with {MARKER} and spaces", "password", "missing"))


# --- a foreign resolver ------------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error",
    [
        FileNotFoundError(2, "No such file", f"/run/{MARKER}"),
        PermissionError(13, "Permission denied", f"/run/{MARKER}"),
        ValueError(MARKER),
        KeyError(MARKER),
        RuntimeError(f"Symlink loop from '/run/{MARKER}'"),
        UnicodeDecodeError("utf-8", MARKER.encode(), 0, 1, "invalid start byte"),
        SecretResolutionError(REF, "password", "control-character"),
    ],
    ids=lambda e: type(e).__name__,
)
def test_a_foreign_resolver_error_is_rebuilt_from_names(tmp_path: Path, error: Exception) -> None:
    class Raising:
        def resolve_file(self, ref: str, key: str) -> Path:
            raise error

        def resolve_text(self, ref: str, key: str) -> str:
            raise error

    with pytest.raises(EngineKafkaError) as refused:
        ek.engine_kafka_settings(ek.parse_engine_kafka(document()), Raising())
    assert refused.value.code == "ENGINE_KAFKA_SECRET_UNRESOLVED"
    assert_clean(refused.value, "/run/")
    environ = _env(tmp_path, tmp_path)
    assert refusal_line(environ, resolver=Raising())["code"] == "ENGINE_KAFKA_SECRET_UNRESOLVED"


@pytest.mark.parametrize(
    "password",
    [f"{MARKER}\x00", f"{MARKER}\n", f"\x1b{MARKER}", f"{MARKER}\x7f", "", "   "],
    ids=["nul", "lf", "esc", "del", "empty", "blank"],
)
def test_a_foreign_resolver_returning_a_bad_value_is_refused_by_the_link_check(tmp_path: Path, password: str) -> None:
    """Security review Low 2: `KafkaSettings` refuses it even when no `DirSecretResolver` was involved."""

    class Returning:
        def resolve_file(self, ref: str, key: str) -> Path:
            return tmp_path / key

        def resolve_text(self, ref: str, key: str) -> str:
            return password if key == "password" else "tb-synthetic-user"

    with pytest.raises(EngineKafkaError) as refused:
        ek.engine_kafka_settings(ek.parse_engine_kafka(document()), Returning())
    assert refused.value.code == "ENGINE_KAFKA_LINK_REFUSED"
    assert_clean(refused.value)
    assert refusal_line(_env(tmp_path, tmp_path), resolver=Returning())["code"] == "ENGINE_KAFKA_LINK_REFUSED"


# --- link-check errors: the marker in every value and key position ---------------------------------


TLS_PINS: dict[str, Any] = {
    "enable.ssl.certificate.verification": True,
    "ssl.endpoint.identification.algorithm": "https",
    "ssl.cipher.suites": PINNED_CIPHER_SUITES,
}
GOOD_SASL: dict[str, Any] = {
    "security.protocol": "SASL_SSL",
    **TLS_PINS,
    "sasl.mechanism": "SCRAM-SHA-512",
    "sasl.username": f"user-{MARKER}",
    "sasl.password": f"pass-{MARKER}",
    "ssl.ca.location": f"/run/{MARKER}/ca.crt",
}

LINK_REFUSALS: list[tuple[str, str, Mapping[str, Any]]] = [
    ("stray-key", "kafka:9093", {**GOOD_SASL, f"x.{MARKER}": MARKER}),
    ("stray-bootstrap", "kafka:9093", {**GOOD_SASL, "bootstrap.servers": f"{MARKER}:9092"}),
    ("protocol", "kafka:9093", {**GOOD_SASL, "security.protocol": MARKER}),
    ("protocol-unhashable", "kafka:9093", {**GOOD_SASL, "security.protocol": [MARKER]}),  # was a TypeError
    ("plaintext-extras", "127.0.0.1:9092", {"security.protocol": "PLAINTEXT", "sasl.password": MARKER}),
    ("plaintext-remote", f"{MARKER.lower()}:9092", {"security.protocol": "PLAINTEXT"}),
    ("pin-verify", "kafka:9093", {**GOOD_SASL, "enable.ssl.certificate.verification": MARKER}),
    ("pin-hostname", "kafka:9093", {**GOOD_SASL, "ssl.endpoint.identification.algorithm": MARKER}),
    ("pin-ciphers", "kafka:9093", {**GOOD_SASL, "ssl.cipher.suites": MARKER}),
    ("path-type", "kafka:9093", {**GOOD_SASL, "ssl.ca.location": [MARKER]}),
    ("half-cert", "kafka:9093", {**GOOD_SASL, "ssl.certificate.location": f"/run/{MARKER}"}),
    ("ssl-with-sasl", "kafka:9093", {**GOOD_SASL, "security.protocol": "SSL"}),
    ("mechanism", "kafka:9093", {**GOOD_SASL, "sasl.mechanism": MARKER}),
    ("no-password", "kafka:9093", {**GOOD_SASL, "sasl.password": ""}),
    ("password-control", "kafka:9093", {**GOOD_SASL, "sasl.password": f"{MARKER}\x00"}),
    ("username-control", "kafka:9093", {**GOOD_SASL, "sasl.username": f"\n{MARKER}"}),
    ("bootstrap-scheme", f"PLAINTEXT://{MARKER}:9092", GOOD_SASL),
    ("bootstrap-space", f"{MARKER} :9092", GOOD_SASL),
    ("bootstrap-port", f"{MARKER.lower()}:99999", GOOD_SASL),
    ("bootstrap-ipv4", "127.1.1.300:9092", GOOD_SASL),
    ("bootstrap-type", MARKER.encode(), GOOD_SASL),  # type: ignore[list-item]
    ("security-type", "kafka:9093", [(MARKER, MARKER)]),  # type: ignore[list-item]
]


@pytest.mark.parametrize(("case", "bootstrap", "security"), LINK_REFUSALS, ids=[c[0] for c in LINK_REFUSALS])
def test_every_link_check_refusal_is_a_fixed_sentence(case: str, bootstrap: str, security: Mapping[str, Any]) -> None:
    with pytest.raises(KafkaSecurityError) as refused:
        KafkaSettings(bootstrap, security)
    assert_clean(refused.value, MARKER.lower(), "/run/")


def test_the_good_map_is_accepted_so_the_refusals_above_are_each_one_rule() -> None:
    assert KafkaSettings("kafka:9093", GOOD_SASL).protocol == "SASL_SSL"


VALUES = st.one_of(
    st.sampled_from([True, False, "https", PINNED_CIPHER_SUITES, "SCRAM-SHA-512", "SASL_SSL", "SSL", "PLAINTEXT"]),
    st.builds(lambda pre, post: f"{pre}{MARKER}{post}", st.text(max_size=3), st.text(max_size=3)),
    st.builds(lambda value: [value], st.just(MARKER)),
    st.none(),
)
KEYS = st.one_of(st.sampled_from(sorted(SECURITY_KEYS)), st.just(f"ssl.{MARKER}"), st.just(MARKER))
BOOTSTRAPS = st.one_of(
    st.sampled_from(["kafka:9093", "127.0.0.1:9092", "[::1]:9092", "localhost:9092"]),
    st.builds(lambda pre, port: f"{pre}{MARKER}:{port}", st.text(max_size=3), st.integers(0, 100_000)),
)


@settings(max_examples=400, deadline=None)
@given(bootstrap=BOOTSTRAPS, security=st.dictionaries(KEYS, VALUES, max_size=8))
def test_no_link_check_refusal_ever_contains_a_supplied_value(bootstrap: str, security: dict[str, Any]) -> None:
    try:
        KafkaSettings(bootstrap, security)
    except KafkaSecurityError as exc:
        assert MARKER not in str(exc) and MARKER not in repr(exc.args)


# --- security review Low 3: the check runs on the copy that is stored --------------------------------


class _LyingDict(dict[str, Any]):
    """`get` reports the pins; the stored contents turn verification off."""

    def get(self, key: str, default: Any = None) -> Any:
        return TLS_PINS.get(key, super().get(key, default))


class _LyingMapping(Mapping[str, Any]):
    """Answers differently each time it is read: whatever was checked must be what is kept."""

    def __init__(self) -> None:
        self._reads = 0

    def __getitem__(self, key: str) -> Any:
        if key == "enable.ssl.certificate.verification":
            self._reads += 1
            return self._reads == 1  # True for its first read only, False for any later one
        return {**GOOD_SASL}[key]

    def __iter__(self) -> Iterator[str]:
        return iter(GOOD_SASL)

    def __len__(self) -> int:
        return len(GOOD_SASL)


def test_a_dict_whose_get_lies_cannot_pass_with_verification_off() -> None:
    lying = _LyingDict({**GOOD_SASL, "enable.ssl.certificate.verification": False})
    assert lying.get("enable.ssl.certificate.verification") is True
    with pytest.raises(KafkaSecurityError, match="TLS must verify"):
        KafkaSettings("kafka:9093", lying)


def test_a_mapping_that_changes_between_reads_is_checked_as_stored() -> None:
    """Checked-then-copied, this passed with `True` and stored `False`. Copied-then-checked, the one read
    is what is both checked and kept."""
    settings_ = KafkaSettings("kafka:9093", _LyingMapping())
    assert settings_.security["enable.ssl.certificate.verification"] is True


def test_a_str_subclass_bootstrap_is_refused() -> None:
    class Sneaky(str):
        __slots__ = ()

    with pytest.raises(KafkaSecurityError, match="plain str"):
        KafkaSettings(Sneaky("127.0.0.1:9092"), {})
