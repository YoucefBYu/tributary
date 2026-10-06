"""Process entry point: `python -m tributary.api`. Builds the app from the environment, refuses to start on
any weak setting (one `startup_refused` line, exit 2), and runs uvicorn with the access log off (ADR-009 D1).

Kafka comes from the runtime's engine-kafka loader (`TB_ENGINE_KAFKA_FILE`, `TB_SECRETS_DIR`) and the
runtime's `producer_config`, so the producer is `acks=all` and idempotent (invariant 1).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Awaitable, Callable

import uvicorn
from fastapi import FastAPI
from tributary.runtime.clients import producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka_or_exit
from tributary.runtime.errors import RuntimeConfigurationError
from tributary.runtime.logs import JsonLogger
from tributary.runtime.secrets import DirSecretResolver

from . import config
from .app import assert_access_log_off, create_app
from .auth.jwks import Fetch, FileJwksFetcher, HttpsJwksFetcher, KeySet
from .db.engine import make_session_factory, make_tls_engine
from .errors import ConfigurationError
from .operations import check_routes
from .publisher import KafkaChannelsPublisher

EXIT_CONFIG_REFUSED = 2


def check_producer_profile(kconf: dict[str, object]) -> None:
    """Invariant 1: the channels producer must be `acks=all` and idempotent, whatever the loaded settings say."""
    if kconf.get("acks") not in ("all", -1, "-1") or kconf.get("enable.idempotence") not in (True, "true"):
        raise ConfigurationError("PRODUCER_PROFILE", "the channels producer is not acks=all and idempotent")


def build_app(environ: dict[str, str] | None = None) -> FastAPI:
    from confluent_kafka import Producer

    env = dict(os.environ if environ is None else environ)
    settings = config.from_env(env)
    log = JsonLogger("api")
    kafka = load_engine_kafka_or_exit(env, log=log)
    kconf = producer_config(kafka, load_topic_defaults(), client_id="tb-api")
    kconf["delivery.timeout.ms"] = int(settings.publish_timeout_s * 1000)
    check_producer_profile(kconf)
    publisher = KafkaChannelsPublisher(Producer(kconf), timeout_s=settings.publish_timeout_s)
    oidc = settings.oidc
    fetch: Fetch
    if oidc.jwks_file is not None:
        fetch = FileJwksFetcher(oidc.jwks_file)
    else:
        fetch = HttpsJwksFetcher(oidc.issuer, jwks_uri=oidc.jwks_uri, timeout_s=oidc.http_timeout_s)
    keys = KeySet(fetch, refresh_s=oidc.refresh_s, max_stale_s=oidc.max_stale_s, timeout_s=oidc.http_timeout_s)
    # ADR-004 C1/C2: the credential and the optional trust bundle come from secretRef files; a missing or
    # unreadable one raises `SecretResolutionError` (fixed message, no path, no value), mapped below.
    resolver = DirSecretResolver.from_environ(env)
    db = settings.database
    try:
        password = resolver.resolve_text(db.secret_ref, "password")
        ca_file = str(resolver.resolve_file(db.ca_secret_ref, "ca.crt")) if db.ca_secret_ref else None
    except RuntimeConfigurationError:
        raise ConfigurationError("DATABASE_SECRET_UNRESOLVED", "the database secretRef did not resolve") from None
    engine = make_tls_engine(host=db.host, port=db.port, name=db.name, user=db.user, password=password, ca_file=ca_file)

    async def close_publisher() -> None:
        publisher.close()

    hooks: list[Callable[[], Awaitable[None]]] = [engine.dispose, close_publisher]
    return create_app(
        settings=settings,
        sessions=make_session_factory(engine),
        publisher=publisher,
        keys=keys,
        log=log,
        on_shutdown=hooks,
    )


def main() -> int:
    log = JsonLogger("api")
    app: FastAPI | None = None
    try:
        settings = config.from_env(os.environ)
        app = build_app()
        check_routes(list(app.routes))  # the same checks the lifespan makes, before any listener binds
        assert_access_log_off()
        server = uvicorn.Server(
            uvicorn.Config(
                app, host=settings.bind, port=settings.port, access_log=False, log_config=None, server_header=False
            )
        )
        assert_access_log_off()  # again: `uvicorn.Config` configures logging
        try:
            server.run()
        except SystemExit:
            # uvicorn exits 3 when the lifespan refuses startup. A refusal we raised is exit 2 with a code.
            refusal = getattr(app.state, "startup_error", None)
            if isinstance(refusal, ConfigurationError):
                raise refusal from None
            raise
    except ConfigurationError as exc:
        log.error("startup_refused", code=exc.code)
        return EXIT_CONFIG_REFUSED
    return 0


if __name__ == "__main__":
    sys.exit(main())
