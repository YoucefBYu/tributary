"""Startup refusals (ADR-009 follow-up 1): the process must not start with a weak setting."""

from __future__ import annotations

import logging

import pytest
from tributary.api import config
from tributary.api.app import assert_access_log_off
from tributary.api.errors import ConfigurationError

BASE = {
    "TB_DATABASE_HOST": "db.example",
    "TB_DATABASE_NAME": "tb",
    "TB_DATABASE_USER": "tbapi",
    "TB_DATABASE_SECRET_REF": "pgcred",
    "TB_OIDC_ISSUER": "https://idp.example/realms/tb",
    "TB_OIDC_AUDIENCE": "tb-api",
    "TB_OIDC_ROLE_MAP": '{"tb-admin": "admin"}',
}


def refused(code: str, **env: str) -> None:
    with pytest.raises(ConfigurationError) as exc:
        config.from_env({**BASE, **env})
    assert exc.value.code == code


def test_a_valid_environment_loads_with_the_documented_defaults() -> None:
    s = config.from_env(BASE)
    assert (s.oidc.http_timeout_s, s.oidc.refresh_s, s.oidc.max_stale_s, s.bind) == (5.0, 300.0, 3600.0, "127.0.0.1")
    assert s.oidc.roles_claim == ("roles",) and not s.oidc.accept_jwt_typ


@pytest.mark.parametrize("raw", ["", "{}", "not json", "[]", '{"a": 1}', '{"a": "superuser"}', '{"": "admin"}'])
def test_empty_unparsable_or_out_of_vocabulary_role_map_refuses(raw: str) -> None:
    refused("OIDC_ROLE_MAP_INVALID", TB_OIDC_ROLE_MAP=raw)


def test_role_map_absent_refuses() -> None:
    env = {k: v for k, v in BASE.items() if k != "TB_OIDC_ROLE_MAP"}
    with pytest.raises(ConfigurationError) as exc:
        config.from_env(env)
    assert exc.value.code == "OIDC_ROLE_MAP_INVALID"


def test_audience_equal_to_ui_client_id_with_the_flag_refuses() -> None:
    refused("OIDC_AUDIENCE_IS_UI_CLIENT", TB_OIDC_ACCEPT_JWT_TYP="true", TB_OIDC_UI_CLIENT_ID="tb-api")
    refused("OIDC_UI_CLIENT_ID_REQUIRED", TB_OIDC_ACCEPT_JWT_TYP="true")
    ok = config.from_env({**BASE, "TB_OIDC_ACCEPT_JWT_TYP": "true", "TB_OIDC_UI_CLIENT_ID": "tb-ui"})
    assert ok.oidc.accept_jwt_typ


def test_non_https_issuer_or_jwks_uri_refuses() -> None:
    refused("OIDC_ISSUER_INVALID", TB_OIDC_ISSUER="http://idp.example")
    refused("OIDC_ISSUER_INVALID", TB_OIDC_ISSUER="")
    refused("OIDC_JWKS_URI_INVALID", TB_OIDC_JWKS_URI="http://idp.example/jwks")
    refused("OIDC_JWKS_URI_INVALID", TB_OIDC_JWKS_URI="file:///etc/jwks")
    assert config.from_env({**BASE, "TB_OIDC_JWKS_URI": "https://idp.example/jwks"}).oidc.jwks_uri


@pytest.mark.parametrize("value", ["299", "14401", "0", "-1", "nan", "abc"])
def test_max_stale_outside_300_to_14400_refuses(value: str) -> None:
    refused("OIDC_MAX_STALE_OUT_OF_RANGE", TB_OIDC_JWKS_MAX_STALE_S=value)


@pytest.mark.parametrize("value", ["300", "14400", "3600"])
def test_max_stale_inside_the_range_loads(value: str) -> None:
    assert config.from_env({**BASE, "TB_OIDC_JWKS_MAX_STALE_S": value}).oidc.max_stale_s == float(value)


def test_other_ranges() -> None:
    refused("OIDC_HTTP_TIMEOUT_OUT_OF_RANGE", TB_OIDC_HTTP_TIMEOUT_S="11")
    refused("OIDC_HTTP_TIMEOUT_OUT_OF_RANGE", TB_OIDC_HTTP_TIMEOUT_S="0.5")
    refused("OIDC_REFRESH_OUT_OF_RANGE", TB_OIDC_JWKS_REFRESH_S="59")
    refused("OIDC_LEEWAY_OUT_OF_RANGE", TB_OIDC_LEEWAY_S="61")
    refused("OIDC_AUDIENCE_MISSING", TB_OIDC_AUDIENCE="")
    refused("DATABASE_SETTINGS_MISSING", TB_DATABASE_HOST="")
    refused("DATABASE_SETTINGS_MISSING", TB_DATABASE_SECRET_REF="")
    refused("PORT_INVALID", TB_DATABASE_PORT="0")
    refused("PUBLISH_TIMEOUT_OUT_OF_RANGE", TB_PUBLISH_TIMEOUT_S="21")
    refused("PROFILE_INVALID", TB_PROFILE="dev")


def test_cors_wildcard_refuses_and_an_explicit_list_loads() -> None:
    refused("CORS_WILDCARD", TB_CORS_ORIGINS="*")
    assert config.from_env({**BASE, "TB_CORS_ORIGINS": "https://ui.example"}).cors_origins == ("https://ui.example",)


def test_static_jwks_file_needs_profile_lite_and_loopback() -> None:
    refused("OIDC_JWKS_FILE_NOT_ALLOWED", TB_OIDC_JWKS_FILE="/tmp/j.json")  # noqa: S108
    refused("OIDC_JWKS_FILE_NOT_ALLOWED", TB_OIDC_JWKS_FILE="/tmp/j.json", TB_PROFILE="lite", TB_API_BIND="0.0.0.0")  # noqa: S104, S108
    refused("OIDC_KEY_SOURCE_AMBIGUOUS", TB_OIDC_JWKS_FILE="/tmp/j.json", TB_OIDC_JWKS_URI="https://i/j")  # noqa: S108
    ok = config.from_env({**BASE, "TB_OIDC_JWKS_FILE": "/tmp/j.json", "TB_PROFILE": "lite"})  # noqa: S108
    assert ok.oidc.jwks_file


def test_access_log_handler_fails_the_self_check() -> None:
    logger = logging.getLogger("uvicorn.access")
    assert_access_log_off()
    handler = logging.NullHandler()
    logger.addHandler(handler)
    try:
        with pytest.raises(ConfigurationError) as exc:
            assert_access_log_off()
        assert exc.value.code == "ACCESS_LOG_ENABLED"
    finally:
        logger.removeHandler(handler)


def test_app_lifespan_refuses_to_start_with_an_access_log_handler(make_harness, idp) -> None:  # type: ignore[no-untyped-def]
    import asyncio

    h = make_harness()
    logger = logging.getLogger("uvicorn.access")
    handler = logging.NullHandler()
    logger.addHandler(handler)

    async def go() -> None:
        async with h.app.router.lifespan_context(h.app):
            pass

    try:
        with pytest.raises(ConfigurationError) as exc:
            asyncio.run(go())
        assert exc.value.code == "ACCESS_LOG_ENABLED"
        assert h.app.state.startup_error is exc.value  # `main` turns this into startup_refused and exit 2
    finally:
        logger.removeHandler(handler)


def test_a_database_url_in_the_environment_is_refused_not_ignored() -> None:
    """ADR-004 C1/C2: a URL carries the password in the environment. Even beside valid settings it refuses."""
    refused("DATABASE_URL_REFUSED", TB_DATABASE_URL="postgresql://u:p@db/x")
    refused("DATABASE_URL_REFUSED", TB_DATABASE_URL="")


def test_database_settings_load_with_a_secret_ref_and_optional_ca() -> None:
    s = config.from_env({**BASE, "TB_DATABASE_CA_SECRET_REF": "pgca", "TB_DATABASE_PORT": "6432"})
    assert (s.database.host, s.database.port, s.database.secret_ref, s.database.ca_secret_ref) == (
        "db.example",
        6432,
        "pgcred",
        "pgca",
    )
    assert config.from_env(BASE).database.ca_secret_ref is None


def test_the_producer_profile_refusal() -> None:
    """Invariant 1: a producer config that is not acks=all and idempotent refuses to start."""
    from tributary.api.main import check_producer_profile

    check_producer_profile({"acks": "all", "enable.idempotence": True})
    check_producer_profile({"acks": -1, "enable.idempotence": "true"})
    for bad in (
        {"acks": 1, "enable.idempotence": True},
        {"acks": "all", "enable.idempotence": False},
        {"acks": "all"},
        {},
    ):
        with pytest.raises(ConfigurationError) as exc:
            check_producer_profile(bad)
        assert exc.value.code == "PRODUCER_PROFILE"


def test_reconciler_settings_default_and_refuse_out_of_range() -> None:
    s = config.from_env(BASE)
    assert (s.reconcile_interval_s, s.reconcile_pending_grace_s) == (30.0, 120.0)
    assert config.from_env({**BASE, "TB_RECONCILE_INTERVAL_S": "0"}).reconcile_interval_s == 0.0
    refused("RECONCILE_INTERVAL_OUT_OF_RANGE", TB_RECONCILE_INTERVAL_S="-1")
    refused("RECONCILE_INTERVAL_OUT_OF_RANGE", TB_RECONCILE_INTERVAL_S="7200")
    refused("RECONCILE_GRACE_OUT_OF_RANGE", TB_RECONCILE_PENDING_GRACE_S="30")  # would expire a live deploy
