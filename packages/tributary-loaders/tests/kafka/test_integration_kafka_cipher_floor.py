"""Security review, Medium: proves `client.py`'s `ssl.cipher.suites` pin (`PINNED_CIPHER_SUITES` ==
`ECDHE+AESGCM:ECDHE+CHACHA20`) is an actually-enforceable TLS floor, not a documentation-only claim --
against a **deliberately weakened** `OPENSSL_CONF` (`MinProtocol=TLSv1`, `CipherString=DEFAULT:
@SECLEVEL=0`) that would otherwise let this process negotiate TLS 1.0/1.1, a real `confluent_kafka.
Producer` built from `producer_settings_from_config`'s own config still fails the handshake against a
TLS-1.1-only `openssl s_server` listener ("no shared cipher"/"handshake failure" -- every cipher this
pin allows is an AEAD suite, and AEAD suites do not exist below TLS 1.2), while the same config
succeeds the handshake against a TLS-1.2 listener.

No Kafka broker involved -- `openssl s_server -www` is enough, since the SSL/TLS handshake happens
before a single byte of the Kafka wire protocol, and this test only cares whether the handshake itself
completes (`confluent_kafka.Producer`'s `error_cb` reports `KafkaError._SSL` iff the handshake fails
with `ssl/tls alert handshake failure` or `no shared cipher`).

**To run this probe by hand** (what this test automates): start a TLS-1.1-only listener with a weakened
config, then watch a producer built from this loader's own config fail against it:

```
cat > /tmp/weak-openssl.cnf <<'EOF'
openssl_conf = default_conf
[default_conf]
ssl_conf = ssl_sect
[ssl_sect]
system_default = system_default_sect
[system_default_sect]
MinProtocol = TLSv1
CipherString = DEFAULT:@SECLEVEL=0
EOF
openssl req -x509 -newkey rsa:2048 -nodes -keyout /tmp/k.pem -out /tmp/c.pem -days 1 -subj "/CN=localhost"
OPENSSL_CONF=/tmp/weak-openssl.cnf openssl s_server -accept 14443 -cert /tmp/c.pem -key /tmp/k.pem -tls1_1 -www &
OPENSSL_CONF=/tmp/weak-openssl.cnf python -c "
from confluent_kafka import Producer
errs = []
p = Producer({'bootstrap.servers': '127.0.0.1:14443', 'security.protocol': 'SSL',
              'ssl.cipher.suites': 'ECDHE+AESGCM:ECDHE+CHACHA20',
              'enable.ssl.certificate.verification': False, 'error_cb': errs.append})
[p.poll(0.2) for _ in range(15)]
print([e.name() for e in errs])  # expect ['_SSL', ...]
"
```
"""

from __future__ import annotations

import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest
from tributary.loaders.kafka.client import PINNED_CIPHER_SUITES, producer_settings_from_config
from tributary.loaders.kafka.config import (
    KafkaConnectionSpec,
    KafkaDestinationConfig,
    KafkaSecuritySpec,
)

pytestmark = pytest.mark.integration

_OPENSSL = shutil.which("openssl")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_config(port: int) -> KafkaDestinationConfig:
    return KafkaDestinationConfig(
        channel="c",
        destination="cipher-floor-probe",
        connection=KafkaConnectionSpec((f"127.0.0.1:{port}",), KafkaSecuritySpec("SSL")),
        topic="partner.events",
        key_mode="none",
        timeout_s=5.0,
        idempotency_technique="none",
        idempotency_header=None,
        circuit_breaker=None,
        backoff="fixed",
        initial_delay_ms=100,
        max_delay_ms=1000,
        max_attempts=3,
        on_exhaust="halt-partition",
    )


class _NoSecretResolver:
    def resolve_text(self, ref: str, key: str) -> str:
        raise AssertionError("no secretRef on this connection")

    def resolve_file(self, ref: str, key: str) -> Path:
        raise AssertionError("no secretRef on this connection")


def _weakened_openssl_conf(tmp_path: Path) -> Path:
    """`MinProtocol=TLSv1` plus `@SECLEVEL=0`: the shape the security review's finding names -- an
    operator (or a compromised dependency) pointing `OPENSSL_CONF` here would, absent this loader's own
    cipher pin, let this process negotiate TLS 1.0/1.1."""
    conf = tmp_path / "weak-openssl.cnf"
    conf.write_text(
        "openssl_conf = default_conf\n"
        "[default_conf]\n"
        "ssl_conf = ssl_sect\n"
        "[ssl_sect]\n"
        "system_default = system_default_sect\n"
        "[system_default_sect]\n"
        "MinProtocol = TLSv1\n"
        "CipherString = DEFAULT:@SECLEVEL=0\n"
    )
    return conf


def _self_signed_cert(tmp_path: Path) -> tuple[Path, Path]:
    key, cert = tmp_path / "key.pem", tmp_path / "cert.pem"
    subprocess.run(  # noqa: S603 - fixed argv, test fixture only
        [
            _OPENSSL or "openssl",
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            str(key),
            "-out",
            str(cert),
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
        ],
        check=True,
        capture_output=True,
        timeout=30,
    )
    return key, cert


def _run_probe(port: int, tls_flag: str, tmp_path: Path) -> list[str]:
    """Starts a weakened-`OPENSSL_CONF` `openssl s_server` pinned to one TLS version (`tls_flag`, e.g.
    `-tls1_1` or `-tls1_2`), then constructs a real `confluent_kafka.Producer` from this loader's own
    built settings against it and returns the `error_cb`-reported `KafkaError` names."""
    import os

    from confluent_kafka import Producer

    weak_conf = _weakened_openssl_conf(tmp_path)
    key, cert = _self_signed_cert(tmp_path)
    server = subprocess.Popen(  # noqa: S603 - fixed argv, test fixture only
        [
            _OPENSSL or "openssl",
            "s_server",
            "-accept",
            str(port),
            "-cert",
            str(cert),
            "-key",
            str(key),
            tls_flag,
            "-www",
        ],
        env={**os.environ, "OPENSSL_CONF": str(weak_conf)},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        time.sleep(1.0)  # s_server has no readiness signal on stdout worth parsing
        settings = producer_settings_from_config(_make_config(port), _NoSecretResolver())
        assert settings.config["ssl.cipher.suites"] == PINNED_CIPHER_SUITES
        errors: list[str] = []
        # librdkafka's linked OpenSSL reads `OPENSSL_CONF` from this process's own environment at
        # producer-construction time; set it here too, matching the weakened-listener side, so this
        # probe genuinely tests "does the pinned cipher list save us when OPENSSL_CONF is weakened",
        # not just "does a default-configured client handshake with a TLS-1.1 listener".
        old = os.environ.get("OPENSSL_CONF")
        os.environ["OPENSSL_CONF"] = str(weak_conf)
        try:
            producer = Producer(
                {
                    **settings.config,
                    "enable.ssl.certificate.verification": False,
                    "error_cb": lambda err: errors.append(err.name()),
                }
            )
            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline and not errors:
                producer.poll(0.2)
        finally:
            if old is None:
                os.environ.pop("OPENSSL_CONF", None)
            else:
                os.environ["OPENSSL_CONF"] = old
        return errors
    finally:
        server.terminate()
        server.wait(timeout=5)


@pytest.mark.skipif(_OPENSSL is None, reason="openssl binary not available")
def test_pinned_cipher_suites_reject_a_tls_1_1_handshake_even_under_a_weakened_openssl_conf(
    tmp_path: Path,
) -> None:
    errors = _run_probe(_free_port(), "-tls1_1", tmp_path)
    assert "_SSL" in errors, f"expected a handshake failure against a TLS 1.1-only listener, got {errors}"


@pytest.mark.skipif(_OPENSSL is None, reason="openssl binary not available")
def test_pinned_cipher_suites_still_allow_a_tls_1_2_handshake(tmp_path: Path) -> None:
    errors = _run_probe(_free_port(), "-tls1_2", tmp_path)
    assert "_SSL" not in errors, f"expected the TLS 1.2 handshake itself to succeed, got {errors}"
