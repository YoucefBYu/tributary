"""`python -m tributary.archiver.content_archiver --env <env>`: the content archiver (ADR-004 A1, W3-2b).

A separate process and principal from the events archiver: it is the one that reads every enabled channel's
payloads (`raw`, `out.*`, `dlq`), so it holds that Kafka grant and nothing else. ClickHouse: INSERT and SELECT only
(it never runs DDL; `--migrate-only` of the events archiver creates `message_content`). The startup checks of the
events archiver apply unchanged: no privilege beyond INSERT/SELECT on `audit_log`, and the pinned settings profile.

Refuses to start (exit 2, `startup_refused` with a code) when content is off or no channel is enabled, so a
mis-deployed process stores nothing rather than everything. `--backfill <channel> [--since <ISO-8601>]` re-reads
that channel's `raw` into `message_content` and exits (`content_backfill.py`).

Ops listener and instance id are the events archiver's (`--ops-*`, `--instance-id`).
"""

from __future__ import annotations

import os
import re
import signal
import sys
from datetime import UTC, datetime
from typing import Any, cast

from tributary.runtime import DirSecretResolver, RuntimeConfigurationError, load_engine_kafka_or_exit
from tributary.runtime.clients import consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.runtime.ops import OpsServer, add_ops_arguments, ops_settings_or_exit, start_ops_or_exit
from tributary.runtime.startup import (
    QuietArgumentParser,
    add_instance_argument,
    refuse_configuration,
    resolve_instance_id,
)

from .archiver import Archiver, ArchiverSettings, ConsumerLike
from .config import ArchiverConfig, content_group, load_config
from .content import COLUMNS, CONTENT_TABLE, ContentRow, parse_content_record
from .content_backfill import BackfillError, backfill_channel
from .errors import ArchiverError, ContentConfigError, SinkError
from .exposition import ArchiverExposition
from .grants import check_grants, check_session_settings
from .main import LIVENESS_STALE_S
from .metrics import ArchiverMetrics
from .retention import check_audit_engine
from .schema import AUDIT_TABLE
from .sink import ClickhouseSink, connect


def topic_pattern(env: str, channels: frozenset[str]) -> str:
    """The regex subscription for the enabled channels' `raw`, `out.<dest>` and `dlq` topics only."""
    names = "|".join(re.escape(c) for c in sorted(channels))
    return rf"^{re.escape(env)}\.({names})\.(raw|dlq|out\.[a-z][a-z0-9-]{{0,63}})$"


def row_builder(channels: frozenset[str]) -> Any:
    def build(message: Any) -> ContentRow:
        timestamp = message.timestamp()
        return parse_content_record(
            topic=message.topic(),
            raw_headers=message.headers(),
            value=message.value(),
            timestamp_ms=timestamp[1] if isinstance(timestamp, tuple) else None,
            partition=message.partition(),
            offset=message.offset(),
            enabled_channels=channels,
        )

    return build


def require_content(config: ArchiverConfig) -> None:
    if not config.content_enabled:
        raise ContentConfigError("CONTENT_DISABLED")
    if not config.content_channels:
        raise ContentConfigError("CONTENT_CHANNELS_EMPTY")


def require_table(client: Any, database: str) -> None:
    found = client.command(
        "SELECT count() FROM system.tables WHERE name = {t:String} AND database = {db:String}",
        parameters={"t": CONTENT_TABLE, "db": database},
    )
    if int(found or 0) == 0:
        raise ContentConfigError("CONTENT_SCHEMA_MISSING")


def _refusal_fields(exc: Exception) -> dict[str, object]:
    fields: dict[str, object] = {"error": type(exc).__name__}
    for name in ("code", "privilege", "table", "setting"):
        value = getattr(exc, name, None)
        if isinstance(value, str):
            fields[name] = value
    return fields


def main() -> None:
    log = JsonLogger("archiver")
    parser = QuietArgumentParser(log, description=__doc__)
    parser.add_argument("--env", required=True)
    add_instance_argument(parser)
    add_ops_arguments(parser)
    parser.add_argument("--secrets-dir")
    parser.add_argument("--contracts-dir")
    parser.add_argument("--max-batch", type=int, default=2000)
    parser.add_argument("--flush-interval-s", type=float, default=1.0)
    parser.add_argument(
        "--backfill", metavar="CHANNEL", help="re-read this channel's raw into message_content, then exit"
    )
    parser.add_argument("--since", metavar="ISO8601", help="with --backfill: first record at or after this time (UTC)")
    kafka = load_engine_kafka_or_exit(argv=sys.argv[1:], log=log)
    args = parser.parse_args()
    try:
        instance = resolve_instance_id(args.instance_id)
    except RuntimeConfigurationError as exc:
        refuse_configuration(log, exc)
    ops_settings = ops_settings_or_exit(args, log)
    resolver = DirSecretResolver(args.secrets_dir) if args.secrets_dir else DirSecretResolver.from_environ()
    holder: list[Archiver] = []
    metrics = ArchiverMetrics()
    try:
        exposition = ArchiverExposition(metrics, contracts_dir=args.contracts_dir, log=log, content=True)
    except RuntimeConfigurationError as exc:
        refuse_configuration(log, exc)
    ops = start_ops_or_exit(
        OpsServer(
            ops_settings,
            component="archiver",
            metrics=exposition.render,
            liveness=lambda: not holder or holder[0].seconds_since_tick() < LIVENESS_STALE_S,
            log=log,
        ),
        log,
    )
    try:
        config = load_config(args.env, os.environ, resolver)
        require_content(config)
        since_ms = None
        if args.backfill is not None:
            if args.backfill not in config.content_channels:
                raise ContentConfigError("CONTENT_CHANNEL_NOT_ENABLED")
            if args.since is not None:
                since_ms = int(
                    datetime.fromisoformat(args.since.replace("Z", "+00:00")).astimezone(UTC).timestamp() * 1000
                )
        settings = ArchiverSettings(
            max_batch=args.max_batch, flush_interval_s=args.flush_interval_s, consume_chunk=50
        )  # chunked consume: the batch also stops at 64 MiB of payload
        client = connect(config.target)
        # A content principal that could ALTER message_content could MODIFY TTL past the R6 guard or delete PHI.
        check_grants(client, database=config.target.database, tables=(AUDIT_TABLE, CONTENT_TABLE))
        check_session_settings(client)
        check_audit_engine(client, config.target.database)
        require_table(client, config.target.database)
        defaults = load_topic_defaults(args.contracts_dir)
    except (ArchiverError, RuntimeConfigurationError, ValueError) as exc:
        ops.close()
        log.error("startup_refused", **_refusal_fields(exc))
        raise SystemExit(2) from None

    from confluent_kafka import Consumer

    group = content_group(config.env)
    build = row_builder(config.content_channels)
    sink = ClickhouseSink(client, table=CONTENT_TABLE, columns=COLUMNS)
    if args.backfill is not None:
        reader = Consumer(
            consumer_config(kafka, defaults, group_id=group, enable_partition_eof=True)
        )  # assign() only: no join, no commit
        try:
            result = backfill_channel(reader, sink, build, env=config.env, channel=args.backfill, since_ms=since_ms)
        except (BackfillError, SinkError) as exc:
            log.error("content_backfill_failed", error=type(exc).__name__, code=exc.code)
            raise SystemExit(1) from None
        finally:
            reader.close()
            sink.close()
            ops.close()
        log.info(
            "content_backfilled",
            channel=args.backfill,
            scanned=result.scanned,
            stored=result.stored,
            rejected=result.rejected,
        )
        return

    consumer = Consumer(
        {
            **consumer_config(kafka, defaults, group_id=group, group_instance_id=instance),
            "topic.metadata.refresh.interval.ms": 30_000,  # a regex subscription finds a new destination's topic
        }
    )
    consumer.subscribe([topic_pattern(config.env, config.content_channels)])
    archiver = Archiver(cast("ConsumerLike", consumer), sink, log=log, settings=settings, metrics=metrics, parser=build)
    holder.append(archiver)

    def _shutdown(_signum: int, _frame: object) -> None:
        archiver.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)
    log.info(
        "content_archiver_started",
        env=config.env,
        channels=len(config.content_channels),
        content_ttl_days=config.content_ttl_days,
    )
    # ADR-004 A4: the system does not create a long-lived PHI store before the operator has confirmed custody.
    log.warning(
        "content_custody_notice",
        reason="stored content is PHI at rest; key custody (encrypted volume, customer-managed key) is the operator's",
    )
    try:
        archiver.run()
    finally:
        consumer.close()
        sink.close()
        ops.close()


if __name__ == "__main__":
    main()
