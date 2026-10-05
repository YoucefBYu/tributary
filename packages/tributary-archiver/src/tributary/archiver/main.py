"""`python -m tributary.archiver --env <env>`.

Kafka link: `load_engine_kafka_or_exit` (`TB_ENGINE_KAFKA_FILE`), before any client is built and before
`argparse` can echo a retired flag. ClickHouse: `TB_ARCHIVE_CLICKHOUSE_*` (`config.py`), credentials from a
`secretRef` directory. The process owns the schema (`ensure_schema`) because it is the only writer.
"""

from __future__ import annotations

import argparse
import os
import signal
import sys
from typing import cast

from tributary.runtime import DirSecretResolver, RuntimeConfigurationError, load_engine_kafka_or_exit
from tributary.runtime.clients import consumer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.logs import JsonLogger
from tributary.runtime.topics import events_topic

from .archiver import Archiver, ArchiverSettings, ConsumerLike
from .config import load_config
from .errors import ArchiverError, SinkError
from .schema import ensure_schema
from .sink import ClickhouseSink, connect


def main() -> None:
    log = JsonLogger("archiver")
    kafka = load_engine_kafka_or_exit(argv=sys.argv[1:], log=log)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    parser.add_argument("--instance-id", help="static membership id; defaults to archiver-<pid>")
    parser.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR")
    parser.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR")
    parser.add_argument("--max-batch", type=int, default=5000)
    parser.add_argument("--flush-interval-s", type=float, default=1.0)
    args = parser.parse_args()
    resolver = DirSecretResolver(args.secrets_dir) if args.secrets_dir else DirSecretResolver.from_environ()
    try:
        config = load_config(args.env, os.environ, resolver)
        settings = ArchiverSettings(max_batch=args.max_batch, flush_interval_s=args.flush_interval_s)
        client = connect(config.target)
        try:
            ensure_schema(client, database=config.target.database, ttl_days=config.ttl_days)
        except Exception:
            raise SinkError("schema migration failed (the runtime credential needs CREATE and ALTER)") from None
        defaults = load_topic_defaults(args.contracts_dir)
    except (ArchiverError, RuntimeConfigurationError, ValueError) as exc:
        log.error("startup_refused", error=type(exc).__name__)
        raise SystemExit(2) from None

    from confluent_kafka import Consumer

    instance = args.instance_id or f"archiver-{os.getpid()}"
    consumer = Consumer(consumer_config(kafka, defaults, group_id=config.group_id, group_instance_id=instance))
    consumer.subscribe([events_topic(config.env)])
    sink = ClickhouseSink(client)
    archiver = Archiver(cast("ConsumerLike", consumer), sink, log=log, settings=settings)

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


if __name__ == "__main__":
    main()
