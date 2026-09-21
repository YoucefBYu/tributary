import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import secret_scan as ss


def _hit(line: str) -> bool:
    return any(rx.search(line) for rx in ss.RULES.values())


def test_detects_private_key_and_tokens() -> None:
    assert _hit("-----BEGIN " + "RSA PRIVATE KEY-----")
    assert _hit("k = AKIA" + "A" * 16)
    assert _hit("gh" + "p_" + "a" * 36)
    assert _hit('password = "' + "hunter2hunter2" + '"')
    assert _hit("postgresql://app:" + "s3cretpass" + "@db.internal/x")


def test_ignores_references_and_placeholders() -> None:
    assert not _hit("password: ${POSTGRES_PASSWORD}")
    assert not _hit('token = os.environ["TOKEN"]')
    assert not _hit("postgresql://tributary:${PG_PASSWORD}@postgres:5432/tributary")
    assert not _hit('secretRef: {name: "kafka-creds"}')
