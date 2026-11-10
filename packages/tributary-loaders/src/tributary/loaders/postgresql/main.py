"""`python -m tributary.loaders.postgresql --channel <name> --destination <name> --env <env>`.

Wires `PostgresqlRecordHandler` into `tributary.runtime`'s `ConsumerLoop`, mirroring
`http/main.py`: the same `clients.consumer_config`/`producer_config`, the same
`topics.consumer_group`/`out_topic`/`dlq_topic`/`events_topic`, the same `KafkaDlqWriter` and
`KafkaEventEmitter`, the same live `_tb.<env>.channels` spec source. See that module's docstring for
what each piece does; this file only differs in which loader/handler pair it builds, and in two things
that must happen before the `ConsumerLoop` starts, both needing an event loop this synchronous `main()`
does not otherwise have: opening the PostgreSQL pool (`client.create_pool`), and README 3.4.3's startup
proof that the destination can actually be idempotent (`PostgresqlLoader.verify_idempotency_constraint`)
— a channel whose table has no unique index on `idColumn` must not start silently producing duplicates.

**That loop must not be thrown away (bug fix).** `asyncpg.Pool` binds internal state to the event loop
it was created on; `build_loop` creates one `asyncio.new_event_loop()` and hands it to both `_prepare()`
(pool creation plus the idempotency check) and `PostgresqlRecordHandler`, which reuses the same loop for
every later `deliver()` call, instead of building the pool on a throwaway `asyncio.run()` loop that
closes as soon as `_prepare()` returns while delivery ran on a second, unrelated loop of its own — the
shape that used to crash the process with `RuntimeError: Event loop is closed` the first time the pool
needed to open a new physical connection after a dropped one. See `handler.py`'s module docstring.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from typing import cast

from tributary.runtime import load_engine_kafka_or_exit
from tributary.runtime.channels import open_spec_source
from tributary.runtime.clients import (
    KafkaSettings,
    consumer_config,
    events_producer_config,
    max_poll_interval_ms,
    producer_config,
)
from tributary.runtime.config import TopicDefaults, load_topic_defaults
from tributary.runtime.consumer import ConsumerLoop, KafkaConsumerLike, LoopSettings
from tributary.runtime.logs import JsonLogger
from tributary.runtime.ops import add_ops_arguments
from tributary.runtime.producer import KafkaEventEmitter, KafkaProducerLike, RecordProducer
from tributary.runtime.startup import QuietArgumentParser, add_instance_argument
from tributary.runtime.topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic
from tributary.runtime.worker import KafkaDlqWriter
from tributary.sdk.context import Logger

from ..ops import LoaderOps, start_loader_ops
from ..secrets import DirSecretResolver
from ..sent_events import with_sent_events
from ..startup import instance_or_exit, spec_reader_group_or_exit
from .client import connection_settings_from_config, create_pool
from .config import DEFAULT_MAX_POOL_SIZE, DEFAULT_MIN_POOL_SIZE, postgresql_destination_config_from_destination
from .handler import PostgresqlRecordHandler
from .loader import PostgresqlLoader


def emit_lite_profile_warning(log: Logger, *, profile: str) -> None:
    """See `http/main.py`'s copy of this function for the full rationale (`contracts/topics/v0`
    README 3.8): every data-plane component emits this, unsuppressed, against a lite deployment."""
    if profile != "lite":
        return
    log.warning(
        "lite_profile_active",
        detail="RF=1, min.insync.replicas=1: no RPO=0 guarantee for an acknowledged message "
        "(contracts/topics/v0/topic-defaults.json profiles.lite)",
    )


def build_loop(
    a: argparse.Namespace, kafka_settings: KafkaSettings, ops: LoaderOps
) -> tuple[ConsumerLoop, PostgresqlRecordHandler]:
    from confluent_kafka import Consumer, Producer

    defaults: TopicDefaults = load_topic_defaults(a.contracts_dir)
    instance = a.instance_id  # resolved and validated in main(): required, unique per replica
    log = JsonLogger("loader-postgresql", base={"channel": a.channel, "destination": a.destination})
    emit_lite_profile_warning(log, profile=a.profile)

    spec_group = spec_reader_group_or_exit(a.env, a.channel, instance, log)
    spec_source = open_spec_source(
        channel=a.channel,
        consumer=Consumer(
            consumer_config(
                kafka_settings,
                defaults,
                group_id=spec_group,
                enable_partition_eof=True,
            )
        ),
        topic=channels_topic(a.env),
        log=log,
    )
    spec = spec_source.current()
    destination = spec.destination(a.destination)
    config = postgresql_destination_config_from_destination(spec.name, destination)

    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else DirSecretResolver()
    settings = connection_settings_from_config(config, resolver)

    async def _prepare() -> PostgresqlLoader:
        pool = await create_pool(
            settings,
            min_size=DEFAULT_MIN_POOL_SIZE,
            max_size=DEFAULT_MAX_POOL_SIZE,
            command_timeout=config.timeout_s,
            connect_timeout=config.timeout_s,
        )
        loader = PostgresqlLoader(config, pool, log=log)
        await loader.verify_idempotency_constraint()  # README 3.4.3: refuse to start, not to deliver
        return loader

    # The pool must be built on the SAME loop that later drives every `deliver()` call (bug fix,
    # `handler.py`'s module docstring): `asyncio.run(_prepare())` used to build it on a throwaway loop
    # that `asyncio.run` closes as soon as it returns, while `PostgresqlRecordHandler` drove delivery on
    # a second, separate loop of its own — `asyncpg` remembers the loop a pool was created on, and
    # reopening a dropped physical connection on the wrong (closed) one crashed the process with
    # `RuntimeError: Event loop is closed`. One loop, created here, used for both.
    pg_loop = asyncio.new_event_loop()
    pg_loader = pg_loop.run_until_complete(_prepare())
    handler = PostgresqlRecordHandler(pg_loader, spec_source=spec_source, destination=a.destination, loop=pg_loop)

    data_producer = Producer(
        producer_config(kafka_settings, defaults, client_id=f"tb-{a.env}-{a.channel}-{a.destination}-{instance}")
    )
    producer = RecordProducer(cast("KafkaProducerLike", data_producer), log=log)
    dlq = None
    if destination.retry.on_exhaust == "dlq-and-continue":
        dlq = KafkaDlqWriter(producer, dlq_topic(a.env, a.channel), log=log, source=instance)

    emitter = KafkaEventEmitter(
        cast(
            "KafkaProducerLike",
            Producer(events_producer_config(kafka_settings, client_id=f"tb-events-loader-{a.destination}-{instance}")),
        ),
        events_topic(a.env),
        log=log,
    )

    consumer = cast(
        "KafkaConsumerLike",
        Consumer(
            consumer_config(
                kafka_settings,
                defaults,
                group_id=consumer_group(a.env, a.channel, "loader", a.destination),
                group_instance_id=instance,
                max_poll_interval=max_poll_interval_ms(spec),
            )
        ),
    )
    loop = ConsumerLoop(
        consumer,
        # `sent` after the destination acknowledged: fire-and-forget on the events emitter (events/v0 2.1)
        with_sent_events(
            handler,
            emitter=emitter,
            channel=a.channel,
            channel_version=spec.version,
            instance=instance,
            destination=a.destination,
            log=log,
        ),
        topics=[out_topic(a.env, a.channel, a.destination)],
        retry=destination.retry,
        settings=LoopSettings(commit_every=defaults.consumer.commit_every),
        emitter=emitter,
        log=log,
        channel=a.channel,
        channel_version=spec.version,
        instance=instance,
        dlq=dlq,
        destination=a.destination,
        read_stage="pending",
    )
    ops.bind_runtime(
        handler=handler, spec_source=spec_source, loop=loop, stale_after_s=max_poll_interval_ms(spec) / 1000.0
    )
    loop.start()
    log.info("loader_started", channel=a.channel, destination=a.destination, channel_version=spec.version)
    return loop, handler


def main() -> None:
    # Before parse_args: argparse echoes an unknown flag's value to stderr, so a retired
    # --bootstrap / --engine-security must reach the ENGINE_KAFKA_RETIRED_INPUT refusal first.
    kafka_settings = load_engine_kafka_or_exit(argv=sys.argv[1:], log=JsonLogger("loader-postgresql"))
    p = QuietArgumentParser(JsonLogger("loader-postgresql"), description=__doc__)
    p.add_argument("--channel", required=True, help="the deployed channel name (spec.metadata.name)")
    p.add_argument("--destination", required=True, help="the spec.destinations[].name to load")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    add_instance_argument(p)
    add_ops_arguments(p)
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR / the default contracts/ search")
    p.add_argument(
        "--profile",
        default=os.environ.get("TB_PROFILE", "lite"),
        choices=["lite", "production"],
        help="topic-defaults.json profile in use; 'lite' emits the mandatory RF=1 startup warning",
    )
    a = p.parse_args()
    log = JsonLogger("loader-postgresql", base={"channel": a.channel, "destination": a.destination})
    a.instance_id = instance_or_exit(a.instance_id, log)
    ops, ops_thread = start_loader_ops(
        a, log, channel=a.channel, destination=a.destination, destination_type="postgresql"
    )
    loop, handler = build_loop(a, kafka_settings, ops)

    def _shutdown(signum: int, _frame: object) -> None:
        loop.stop()

    signal.signal(signal.SIGINT, _shutdown)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _shutdown)

    started = time.monotonic()
    try:
        stats = loop.run()
    finally:
        metrics = handler.metrics_snapshot()
        handler.close()
        ops_thread.close()
    print(
        json.dumps(
            {
                "event": "loader_exit",
                "handled": stats.handled,
                "uptime_s": round(time.monotonic() - started, 1),
                "delivery_metrics": metrics,
            }
        )
    )


if __name__ == "__main__":
    main()
