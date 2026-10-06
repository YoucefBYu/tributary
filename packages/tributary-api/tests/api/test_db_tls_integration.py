"""ADR-004 C1/C2: the API reaches PostgreSQL over TLS with `verify-full` (chain AND host name), the credential
comes from a secretRef file, and anything weaker fails to connect or to start."""

from __future__ import annotations

import asyncio
import ssl
from pathlib import Path

import asyncpg
import pytest
from api_kit import Idp, TlsPg
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
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
    with pytest.raises(ssl.SSLCertVerificationError, match="certificate verify failed"):
        asyncio.run(_ssl_in_use(tls_pg, host="127.0.0.1", ca_file=str(idp.cert_file)))  # a different CA
    with pytest.raises(ssl.SSLCertVerificationError, match=r"Hostname mismatch|IP address mismatch"):
        asyncio.run(_ssl_in_use(tls_pg, host="localhost", ca_file=str(tls_pg.ca_file)))  # the cert names 127.0.0.1
    with pytest.raises(DBAPIError) as wrong:  # SQLAlchemy wraps the driver's error; pin the driver class
        asyncio.run(_ssl_in_use(tls_pg, host="127.0.0.1", ca_file=str(tls_pg.ca_file), password="wrong"))  # noqa: S106
    assert isinstance(wrong.value.orig.__cause__, asyncpg.InvalidPasswordError)  # type: ignore[union-attr]


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


def test_a_bad_ca_bundle_is_a_refusal_not_a_traceback(idp: Idp, tmp_path: Path) -> None:
    secrets = tmp_path / "secrets"
    for ref, key, data in (("pgcred", "password", "x"), ("pgca", "ca.crt", "not a certificate")):
        (secrets / ref).mkdir(parents=True)
        (secrets / ref / key).write_text(data)
        (secrets / ref / key).chmod(0o600)
    with pytest.raises(ConfigurationError) as bad:
        build_app(_environ(secrets, idp, tmp_path))
    assert bad.value.code == "DATABASE_TLS_INVALID"


@pytest.mark.integration
def test_migrations_use_the_tls_secretref_engine_and_ignore_the_old_url(tls_pg: TlsPg, tmp_path: Path) -> None:
    from alembic import command
    from sqlalchemy import text as sql
    from tributary.api.db.migrate import make_config

    env = {**tls_pg.env(tls_pg.secrets_dir(tmp_path)), "TRIBUTARY_DATABASE_URL": "postgresql://nobody@127.0.0.1:1/x"}

    import os

    os.environ.update(env)
    try:
        cfg = make_config("")
        cfg.attributes.pop("url")
        command.downgrade(cfg, "base")
        command.upgrade(cfg, "head")
    finally:
        for key in env:
            os.environ.pop(key, None)

    async def tables() -> set[str]:
        engine = make_tls_engine(
            host=tls_pg.host,
            port=tls_pg.port,
            name=tls_pg.db,
            user=tls_pg.user,
            password=tls_pg.password,
            ca_file=str(tls_pg.ca_file),
        )
        try:
            async with engine.connect() as conn:
                rows = await conn.execute(sql("SELECT tablename FROM pg_tables WHERE schemaname='public'"))
                return {r[0] for r in rows}
        finally:
            await engine.dispose()

    assert {"channels", "deployments"} <= asyncio.run(tables())
