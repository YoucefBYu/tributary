"""`python -m tributary.archiver --env <env>`.

Kafka link: `load_engine_kafka_or_exit` (`TB_ENGINE_KAFKA_FILE`), before any client is built and before
`argparse` can echo a retired flag. ClickHouse: `TB_ARCHIVE_CLICKHOUSE_*` (`config.py`), credentials from a
`secretRef` directory. Retention (ADR-004 R6) is applied by `--migrate-only` only (`retention.py`); the
archiver itself creates `message_events` if absent and never alters a TTL.

Ops listener (W3-27): `--ops-bind` / `--ops-port` (`TB_OPS_BIND` / `TB_OPS_PORT`, default 127.0.0.1:9180) serves
`/livez` and `/metrics` (`exposition.py`) through `tributary.runtime.ops`. It is bound first, so a bind failure
refuses the start (O4) before any client exists. Instance id (W3-28c): `--instance-id` or `TB_INSTANCE_ID`
is required and used verbatim as `group.instance.id`; no pid default (it is identical in every container).
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
from collections.abc import Callable
from typing import Any, cast

from tributary.runtime import DirSecretResolver, RuntimeConfigurationError, load_engine_kafka_or_exit
from tributary.runtime.clients import consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.runtime.metrics import build_version
from tributary.runtime.ops import OpsServer, add_ops_arguments, ops_settings_or_exit, start_ops_or_exit
from tributary.runtime.startup import (
    QuietArgumentParser,
    add_instance_argument,
    refuse_configuration,
    resolve_instance_id,
)
from tributary.runtime.topics import events_topic, platform_topic

from .archiver import Archiver, ArchiverSettings, ConsumerLike, RowLike, parse_event_record
from .audit_record import COLUMNS as AUDIT_COLUMNS
from .audit_record import parse_audit_record
from .config import ArchiverConfig, load_config
from .content import CONTENT_TABLE, content_ddl
from .errors import (
    ArchiverError,
    ArchiverGrantsExcessive,
    ArchiverSettingsUnsafe,
    AuditLogEngineInvalid,
    AuditRejected,
    RetentionChanged,
    RetentionDecreaseRefused,
    SinkError,
)
from .exposition import ArchiverExposition
from .grants import check_audit_grants, check_session_settings
from .metrics import ArchiverMetrics
from .retention import TableSpec, apply_retention, check_audit_engine
from .schema import AUDIT_TABLE, ensure_schema
from .sink import ClickhouseSink, connect
from .tswindow import record_time

#: `/livez` fails when the loop has made no progress for this long (a consume, or a backoff slice).
LIVENESS_STALE_S = 60.0


def subscription_topics(config: ArchiverConfig) -> list[str]:
    """`_tb.<env>.events`, plus `_tb.<env>.audit` unless `TB_ARCHIVE_AUDIT_CONSUME=false`."""
    topics = [events_topic(config.env)]
    if config.audit_consume:
        topics.append(platform_topic(config.env, "audit"))
    return topics


def build_record_parser(env: str) -> Callable[[Any], RowLike]:
    """Events and audit records share one consumer (one group, one commit); the topic picks the parser."""
    audit = platform_topic(env, "audit")

    def parse_record(message: Any) -> RowLike:
        if message.topic() == audit:
            try:
                return parse_audit_record(
                    message.value(),
                    env=env,
                    partition=message.partition(),
                    offset=message.offset(),
                    anchor=record_time(message),
                )
            except AuditRejected:
                raise
            except Exception:  # whatever the record held, it is an audit rejection, counted as one
                raise AuditRejected("RECORD_UNPARSEABLE") from None
        return parse_event_record(message)

    return parse_record


def extra_tables(config: ArchiverConfig) -> tuple[TableSpec, ...]:
    if config.content_ttl_days is None:
        return ()
    return (TableSpec(CONTENT_TABLE, config.content_ttl_days, content_ddl),)


def migrate(args: argparse.Namespace, log: JsonLogger) -> None:
    """`--migrate-only`: create tables and apply retention (ADR-004 R6) with the MIGRATION credential, then exit.

    No Kafka. `message_content` joins the plan (create, TTL, decrease guard, `archive_settings` row) only when
    `TB_ARCHIVE_CONTENT=true`, with `TB_ARCHIVE_CONTENT_TTL_DAYS` (W3-2b).
    """
    resolver = DirSecretResolver(args.secrets_dir) if args.secrets_dir else DirSecretResolver.from_environ()
    try:
        config = load_config(args.env, os.environ, resolver, ref_env_name="TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF")
        client = connect(config.target)
        try:
            apply_retention(
                client,
                database=config.target.database,
                events_days=config.ttl_days,
                audit_days=config.audit_ttl_days,
                allow_decrease=config.allow_ttl_decrease,
                log=log,
                release=build_version("tributary-archiver"),
                extra=extra_tables(config),
                content_enabled=config.content_enabled,
            )
        except (RetentionChanged, RetentionDecreaseRefused, AuditLogEngineInvalid):
            raise
        except Exception:
            raise SinkError("schema migration failed") from None
        finally:
            ClickhouseSink(client).close()
    except RetentionDecreaseRefused as exc:
        log.error("startup_refused", error=type(exc).__name__, code=exc.code, tables=",".join(exc.tables))
        raise SystemExit(2) from None
    except AuditLogEngineInvalid as exc:
        log.error("startup_refused", error=type(exc).__name__, code=exc.code)
        raise SystemExit(2) from None
    except RetentionChanged as exc:
        log.error("startup_refused", error=type(exc).__name__, code=exc.code, table=exc.table)
        raise SystemExit(2) from None
    except (ArchiverError, ValueError) as exc:
        log.error("startup_refused", error=type(exc).__name__, code=getattr(exc, "code", "ARCHIVER_ERROR"))
        raise SystemExit(2) from None
    log.info(
        "archiver_migrated",
        env=config.env,
        ttl_days=config.ttl_days,
        audit_ttl_days=config.audit_ttl_days,
        content_ttl_days=config.content_ttl_days,
    )


def main() -> None:
    log = JsonLogger("archiver")
    parser = QuietArgumentParser(log, description=__doc__)
    parser.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    add_instance_argument(parser)
    add_ops_arguments(parser)
    parser.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR")
    parser.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR")
    parser.add_argument("--max-batch", type=int, default=5000)
    parser.add_argument("--flush-interval-s", type=float, default=1.0)
    parser.add_argument(
        "--migrate-only",
        action="store_true",
        help="run the schema migration with TB_ARCHIVE_CLICKHOUSE_MIGRATE_SECRET_REF and exit (no Kafka)",
    )
    parser.add_argument(
        "--skip-ddl",
        action="store_true",
        help="do not create or alter the table at startup (the runtime credential is then INSERT-only)",
    )
    if "--migrate-only" in sys.argv[1:]:  # no Kafka link is needed, or read
        migrate(parser.parse_args(), log)
        return
    kafka = load_engine_kafka_or_exit(argv=sys.argv[1:], log=log)
    args = parser.parse_args()
    try:
        instance = resolve_instance_id(args.instance_id)
    except RuntimeConfigurationError as exc:
        refuse_configuration(log, exc)
    ops_settings = ops_settings_or_exit(args, log)
    resolver = DirSecretResolver(args.secrets_dir) if args.secrets_dir else DirSecretResolver.from_environ()
    holder: list[Archiver] = []

    def alive() -> bool:
        return not holder or holder[0].seconds_since_tick() < LIVENESS_STALE_S

    metrics = ArchiverMetrics()
    try:
        exposition = ArchiverExposition(metrics, contracts_dir=args.contracts_dir, log=log)
    except RuntimeConfigurationError as exc:
        refuse_configuration(log, exc)
    ops = start_ops_or_exit(
        OpsServer(ops_settings, component="archiver", metrics=exposition.render, liveness=alive, log=log), log
    )
    try:
        config = load_config(args.env, os.environ, resolver)
        settings = ArchiverSettings(max_batch=args.max_batch, flush_interval_s=args.flush_interval_s)
        client = connect(config.target)
        check_audit_grants(client, database=config.target.database)
        check_session_settings(client)
        check_audit_engine(client, config.target.database)
        if not args.skip_ddl:  # default keeps today's behaviour; production sets --skip-ddl (ADR-004 A5)
            try:
                ensure_schema(client, database=config.target.database, ttl_days=config.ttl_days)
            except Exception:
                raise SinkError("schema migration failed (needs CREATE and ALTER, or use --skip-ddl)") from None
        defaults = load_topic_defaults(args.contracts_dir)
    except AuditLogEngineInvalid as exc:
        ops.close()
        log.error("startup_refused", error=type(exc).__name__, code=exc.code)
        raise SystemExit(2) from None
    except ArchiverSettingsUnsafe as exc:
        ops.close()
        log.error("startup_refused", error=type(exc).__name__, code=exc.code, setting=exc.setting)
        raise SystemExit(2) from None
    except ArchiverGrantsExcessive as exc:
        ops.close()
        log.error("startup_refused", error=type(exc).__name__, code=exc.code, privilege=exc.privilege, table=exc.table)
        raise SystemExit(2) from None
    except (ArchiverError, RuntimeConfigurationError, ValueError) as exc:
        ops.close()
        log.error("startup_refused", error=type(exc).__name__)
        raise SystemExit(2) from None

    from confluent_kafka import Consumer

    consumer = Consumer(consumer_config(kafka, defaults, group_id=config.group_id, group_instance_id=instance))
    consumer.subscribe(subscription_topics(config))
    sink = ClickhouseSink(client)
    audit_sink = ClickhouseSink(client, table=AUDIT_TABLE, columns=AUDIT_COLUMNS)
    parse_record = build_record_parser(config.env)

    archiver = Archiver(
        cast("ConsumerLike", consumer),
        sink,
        log=log,
        settings=settings,
        metrics=metrics,
        parser=parse_record,
        side_sinks={AUDIT_TABLE: audit_sink},
    )
    holder.append(archiver)

    def _shutdown(_signum: int, _frame: object) -> None:
        archiver.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    log.info("archiver_started", env=config.env, ttl_days=config.ttl_days)
    try:
        archiver.run()
    finally:
        consumer.close()
        sink.close()
        ops.close()


if __name__ == "__main__":
    main()
