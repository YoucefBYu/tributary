"""`python -m tributary.loaders.kafka --channel <name> --destination <name> --env <env>
--bootstrap <hosts>`.

Wires `KafkaRecordHandler` into `tributary.runtime`'s `ConsumerLoop`, mirroring `postgresql/main.py`
and `http/main.py`: the same `clients.consumer_config`/`producer_config`, the same `topics.
consumer_group`/`out_topic`/`dlq_topic`/`events_topic`, the same `KafkaDlqWriter` and
`KafkaEventEmitter`, the same live `_tb.<env>.channels` spec source, for the **engine's own** cluster
(`--bootstrap`). This file only differs from those two in which loader/handler pair it builds, and in
that the destination's own `confluent_kafka.Producer` (`client.producer_settings_from_config`) is a
second, unrelated Kafka client, deliberately never sharing `--bootstrap`/credentials with the ones
above (ADR-007 D11, `config.py`'s module docstring) — a `kafka` destination pointed at the very same
physical cluster the engine runs on still gets its own `Producer` with its own principal.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from typing import cast

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
from tributary.runtime.producer import KafkaEventEmitter, RecordProducer
from tributary.runtime.producer import KafkaProducerLike as EngineKafkaProducerLike
from tributary.runtime.topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic
from tributary.runtime.worker import KafkaDlqWriter
from tributary.sdk.context import Logger

from ..secrets import DirSecretResolver
from .client import producer_settings_from_config
from .config import kafka_destination_config_from_destination
from .handler import KafkaRecordHandler
from .loader import KafkaLoader, KafkaProducerLike, LastErrorSink


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


def build_loop(a: argparse.Namespace) -> tuple[ConsumerLoop, KafkaRecordHandler]:
    from confluent_kafka import Consumer, Producer

    kafka_settings = KafkaSettings(bootstrap_servers=a.bootstrap)
    defaults: TopicDefaults = load_topic_defaults(a.contracts_dir)
    instance = a.instance_id or f"loader-{a.destination}-{os.getpid()}"
    log = JsonLogger(f"loader-{a.destination}", base={"channel": a.channel, "destination": a.destination})
    emit_lite_profile_warning(log, profile=a.profile)

    spec_source = open_spec_source(
        channel=a.channel,
        consumer=Consumer(
            consumer_config(
                kafka_settings,
                defaults,
                group_id=f"tb.{a.env}.{a.channel}.channels.{instance}",
                enable_partition_eof=True,
            )
        ),
        topic=channels_topic(a.env),
        log=log,
    )
    spec = spec_source.current()  # raises if the channel is not deployed; fail closed at startup
    destination = spec.destination(a.destination)
    config = kafka_destination_config_from_destination(spec.name, destination)

    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else DirSecretResolver()
    settings = producer_settings_from_config(config, resolver)
    error_sink = LastErrorSink()
    destination_producer = cast("KafkaProducerLike", Producer({**settings.config, "error_cb": error_sink}))
    kafka_loader = KafkaLoader(config, destination_producer, error_sink=error_sink, log=log)
    handler = KafkaRecordHandler(kafka_loader, spec_source=spec_source, destination=a.destination)

    data_producer = Producer(
        producer_config(kafka_settings, defaults, client_id=f"tb-{a.env}-{a.channel}-{a.destination}-{instance}")
    )
    producer = RecordProducer(cast("EngineKafkaProducerLike", data_producer), log=log)
    dlq = None
    if destination.retry.on_exhaust == "dlq-and-continue":
        dlq = KafkaDlqWriter(producer, dlq_topic(a.env, a.channel), log=log, source=instance)

    emitter = KafkaEventEmitter(
        cast(
            "EngineKafkaProducerLike",
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
        handler,
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
    loop.start()
    log.info("loader_started", channel=a.channel, destination=a.destination, channel_version=spec.version)
    return loop, handler


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--channel", required=True, help="the deployed channel name (spec.metadata.name)")
    p.add_argument("--destination", required=True, help="the spec.destinations[].name to load")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--bootstrap", required=True, help="the ENGINE's Kafka bootstrap.servers, not the destination's")
    p.add_argument("--instance-id", help="defaults to loader-<destination>-<pid>")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR / the default contracts/ search")
    p.add_argument(
        "--profile",
        default=os.environ.get("TB_PROFILE", "lite"),
        choices=["lite", "production"],
        help="topic-defaults.json profile in use; 'lite' emits the mandatory RF=1 startup warning",
    )
    a = p.parse_args()
    loop, handler = build_loop(a)

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
