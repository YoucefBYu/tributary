"""ADR-004 C1/C2: the API reaches PostgreSQL over TLS with `verify-full` (chain AND host name), the credential
comes from a secretRef file, and anything weaker fails to connect or to start."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest
from api_kit import Idp, TlsPg
from sqlalchemy import text
from tributary.api.db.engine import make_tls_engine
from tributary.api.errors import ConfigurationError
from tributary.api.main import build_app

from tests.support.engine_kafka import engine_kafka_env


async def _ssl_in_use(pg: TlsPg, *, host: str, ca_file: str | None, password: str | None = None) -> bool:
    engine = make_tls_engine(
        host=host, port=pg.port, name=pg.db, user=pg.user, password=password or pg.password, ca_file=ca_file
    )
    try:
        async with engine.connect() as conn:
            row = await conn.execute(text("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()"))
            return bool(row.scalar_one())
    finally:
        await engine.dispose()


@pytest.mark.integration
def test_connection_is_tls_and_verify_full_refuses_a_wrong_ca_or_host_name(tls_pg: TlsPg, idp: Idp) -> None:
    assert asyncio.run(_ssl_in_use(tls_pg, host="127.0.0.1", ca_file=str(tls_pg.ca_file))) is True
    with pytest.raises(Exception) as untrusted:
        asyncio.run(_ssl_in_use(tls_pg, host="127.0.0.1", ca_file=str(idp.cert_file)))  # a different CA
    assert "certificate verify failed" in str(untrusted.value).lower() or "ssl" in str(untrusted.value).lower()
    with pytest.raises(Exception) as wrong_host:
        asyncio.run(_ssl_in_use(tls_pg, host="localhost", ca_file=str(tls_pg.ca_file)))  # the cert names 127.0.0.1 only
    assert "ssl" in str(wrong_host.value).lower() or "hostname" in str(wrong_host.value).lower()
    with pytest.raises(Exception):  # noqa: B017 - a wrong password is refused by the server
        asyncio.run(_ssl_in_use(tls_pg, host="127.0.0.1", ca_file=str(tls_pg.ca_file), password="wrong"))  # noqa: S106


def _environ(secrets: Path, idp: Idp, tmp: Path, **extra: str) -> dict[str, str]:
    import json

    from api_kit import AUDIENCE, ROLE_MAP

    from tests.support.kafka_helpers import free_port

    base = {
        "TB_DATABASE_HOST": "127.0.0.1",
        "TB_DATABASE_NAME": "tb",
        "TB_DATABASE_USER": "tbapi",
        "TB_DATABASE_SECRET_REF": "pgcred",
        "TB_DATABASE_CA_SECRET_REF": "pgca",
        "TB_SECRETS_DIR": str(secrets),
        "TB_OIDC_ISSUER": idp.issuer,
        "TB_OIDC_AUDIENCE": AUDIENCE,
        "TB_OIDC_ROLE_MAP": json.dumps(ROLE_MAP),
        "TB_OIDC_JWKS_FILE": str(idp.jwks_file(tmp / "jwks.json")),
        "TB_PROFILE": "lite",
        "PATH": "/usr/bin:/bin",
        **extra,
    }
    return engine_kafka_env(f"127.0.0.1:{free_port()}", base)


def test_startup_refuses_a_database_url_and_an_unresolvable_secret(idp: Idp, tmp_path: Path) -> None:
    secrets = tmp_path / "secrets"
    for ref, key in (("pgcred", "password"), ("pgca", "ca.crt")):
        (secrets / ref).mkdir(parents=True)
        (secrets / ref / key).write_text("x")
        (secrets / ref / key).chmod(0o600)
    with pytest.raises(ConfigurationError) as url:
        build_app(_environ(secrets, idp, tmp_path, TB_DATABASE_URL="postgresql://u:p@h/d"))
    assert url.value.code == "DATABASE_URL_REFUSED"
    (secrets / "pgcred" / "password").unlink()
    with pytest.raises(ConfigurationError) as missing:
        build_app(_environ(secrets, idp, tmp_path))
    assert missing.value.code == "DATABASE_SECRET_UNRESOLVED"
    assert str(secrets) not in str(missing.value)
