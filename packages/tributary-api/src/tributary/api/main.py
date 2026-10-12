"""Process entry point: `python -m tributary.api`. Builds the app from the environment, refuses to start on
any weak setting (one `startup_refused` line, exit 2), and runs uvicorn with the access log off (ADR-009 D1).

Kafka comes from the runtime's engine-kafka loader (`TB_ENGINE_KAFKA_FILE`, `TB_SECRETS_DIR`) and the
runtime's `producer_config`, so the producer is `acks=all` and idempotent (invariant 1).
"""

from __future__ import annotations

import os
import ssl
import sys
from collections.abc import Awaitable, Callable

import uvicorn
from fastapi import FastAPI
from tributary.runtime.clients import KafkaSettings, consumer_config, producer_config
from tributary.runtime.config import TopicDefaults, load_topic_defaults
from tributary.runtime.engine_kafka import load_engine_kafka_or_exit
from tributary.runtime.errors import RuntimeConfigurationError
from tributary.runtime.logs import JsonLogger
from tributary.runtime.ops import OpsConfigError, OpsServer, resolve_ops_settings, start_ops_or_exit
from tributary.runtime.secrets import DirSecretResolver
from tributary.runtime.topics import platform_group

from . import config, tls
from .app import assert_access_log_off, create_app
from .audit import AuditHook, KafkaAuditSender
from .auth.jwks import Fetch, FileJwksFetcher, HttpsJwksFetcher, KeySet
from .db.connect import engine_from_settings
from .db.engine import make_session_factory
from .errors import ConfigurationError
from .operations import check_routes
from .publisher import KafkaChannelsPublisher
from .reconciler import KafkaTopicReader
from .retention import RetentionReader
from .tamper import AuditMetrics, ClickHouseHttp, TamperChecker

EXIT_CONFIG_REFUSED = 2


def check_producer_profile(kconf: dict[str, object]) -> None:
    """Invariant 1: the channels producer must be `acks=all` and idempotent, whatever the loaded settings say."""
    if kconf.get("acks") not in ("all", -1, "-1") or kconf.get("enable.idempotence") not in (True, "true"):
        raise ConfigurationError("PRODUCER_PROFILE", "the channels producer is not acks=all and idempotent")


def reconciler_consumer_config(kafka: KafkaSettings, defaults: TopicDefaults, env: str) -> dict[str, object]:
    """The reconciler's table-read consumer: the platform group `tb.<env>.reconciler` (topics/v0 3.3), never an
    env-less name, so the `api` role's group grant matches."""
    return consumer_config(kafka, defaults, group_id=platform_group(env, "reconciler"), enable_partition_eof=True)


def build_app(environ: dict[str, str] | None = None) -> FastAPI:
    from confluent_kafka import Consumer, Producer

    env = dict(os.environ if environ is None else environ)
    settings = config.from_env(env)
    log = JsonLogger("api")
    kafka = load_engine_kafka_or_exit(env, log=log)
    defaults = load_topic_defaults()
    kconf = producer_config(kafka, defaults, client_id="tb-api")
    kconf["delivery.timeout.ms"] = int(settings.publish_timeout_s * 1000)
    check_producer_profile(kconf)
    publisher = KafkaChannelsPublisher(Producer(kconf), timeout_s=settings.publish_timeout_s)
    audit_conf = producer_config(kafka, defaults, client_id="tb-api-audit")
    audit_conf["delivery.timeout.ms"] = int(settings.audit_timeout_s * 1000)
    check_producer_profile(audit_conf)  # invariant 1: the audit producer is acks=all and idempotent too
    audit_sender = KafkaAuditSender(Producer(audit_conf), timeout_s=settings.audit_timeout_s)
    audit = AuditHook(audit_sender, log)
    reader = KafkaTopicReader(lambda env: Consumer(reconciler_consumer_config(kafka, defaults, env)))
    tamper: TamperChecker | None = None
    retention: RetentionReader | None = None
    app_metrics: AuditMetrics | None = None
    if settings.archive is not None:
        archive = settings.archive
        try:
            resolver = DirSecretResolver.from_environ(env)
            password = resolver.resolve_text(archive.secret_ref, "password")
            ca_file = (
                None if archive.ca_secret_ref is None else str(resolver.resolve_file(archive.ca_secret_ref, "ca.crt"))
            )
        except RuntimeConfigurationError:
            raise ConfigurationError(
                "CLICKHOUSE_SECRET_UNRESOLVED", "the ClickHouse secretRef did not resolve"
            ) from None
        app_metrics = AuditMetrics()
        try:
            clickhouse = ClickHouseHttp(archive.url, archive.user, password, ca_file=ca_file)
        except (ssl.SSLError, OSError):  # a CA file that is not a readable PEM bundle
            raise ConfigurationError("CLICKHOUSE_CA_INVALID", "the ClickHouse CA bundle is not usable") from None
        retention = RetentionReader(clickhouse.query, archive.targets)
        tamper = TamperChecker(
            archive.targets,
            clickhouse.query,
            app_metrics,
            log,
            interval_s=archive.interval_s,
        )
    oidc = settings.oidc
    fetch: Fetch
    if oidc.jwks_file is not None:
        fetch = FileJwksFetcher(oidc.jwks_file)
    else:
        fetch = HttpsJwksFetcher(oidc.issuer, jwks_uri=oidc.jwks_uri, timeout_s=oidc.http_timeout_s)
    keys = KeySet(fetch, refresh_s=oidc.refresh_s, max_stale_s=oidc.max_stale_s, timeout_s=oidc.http_timeout_s)
    engine = engine_from_settings(settings.database, env)  # ADR-004 C1/C2

    async def close_publisher() -> None:
        publisher.close()

    async def close_audit() -> None:
        audit_sender.close()

    hooks: list[Callable[[], Awaitable[None]]] = [engine.dispose, close_publisher, close_audit]
    app = create_app(
        settings=settings,
        sessions=make_session_factory(engine),
        publisher=publisher,
        keys=keys,
        log=log,
        on_shutdown=hooks,
        topic_reader=reader,
        tamper=tamper,
        retention=retention,
        audit=audit,
    )
    app.state.audit_metrics = app_metrics
    app.state.tamper = tamper
    return app


def main() -> int:
    log = JsonLogger("api")
    app: FastAPI | None = None
    try:
        settings = config.from_env(os.environ)
        app = build_app()
        check_routes(list(app.routes))  # the same checks the lifespan makes, before any listener binds
        assert_access_log_off()
        resolver = DirSecretResolver.from_environ(os.environ) if settings.tls_secret_ref is not None else None
        server = uvicorn.Server(tls.server_config(app, settings, resolver, log))
        assert_access_log_off()  # again: `uvicorn.Config` configures logging
        ops = None
        metrics: AuditMetrics | None = app.state.audit_metrics
        if metrics is not None:  # the tamper gauges need a scrape target; loopback by default (ops/v0)
            try:
                ops_settings = resolve_ops_settings(bind=None, port=None)
            except OpsConfigError:
                raise ConfigurationError("OPS_SETTINGS_INVALID", "TB_OPS_BIND or TB_OPS_PORT is invalid") from None
            # /livez reflects the event loop and the tamper task (it ticks every few seconds), not just the process.
            alive = app.state.tamper.alive
            ops = start_ops_or_exit(
                OpsServer(ops_settings, component="api", metrics=metrics.render, liveness=alive, log=log), log
            )
        try:
            server.run()
        except SystemExit:
            # uvicorn exits 3 when the lifespan refuses startup. A refusal we raised is exit 2 with a code.
            refusal = getattr(app.state, "startup_error", None)
            if isinstance(refusal, ConfigurationError):
                raise refusal from None
            raise
        finally:
            if ops is not None:
                ops.close()
    except ConfigurationError as exc:
        log.error("startup_refused", code=exc.code)
        return EXIT_CONFIG_REFUSED
    return 0


if __name__ == "__main__":
    sys.exit(main())
