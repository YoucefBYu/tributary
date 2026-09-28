"""`python -m tributary.loaders.mllp --channel <name> --destination <name> --env <env>
--bootstrap <hosts>`.

Wires `MllpRecordHandler` into `tributary.runtime`'s `ConsumerLoop`, mirroring `clickhouse/main.py`
(synchronous, no event loop) and `postgresql/main.py`/`http/main.py` for everything else. There is no
startup idempotency proof to run here the way `clickhouse`/`postgresql` have one:
`config.py`'s module docstring already explains this loader cannot start-up-verify `sink-native` or
`none` the way a database schema can be inspected -- MLLP gives this loader no such catalog to check.
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
from tributary.runtime.producer import KafkaEventEmitter, KafkaProducerLike, RecordProducer
from tributary.runtime.topics import channels_topic, consumer_group, dlq_topic, events_topic, out_topic
from tributary.runtime.worker import KafkaDlqWriter
from tributary.sdk.context import Logger

from ..secrets import DirSecretResolver
from .client import connection_from_config
from .config import mllp_destination_config_from_destination
from .handler import MllpRecordHandler
from .loader import MllpLoader


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


def build_loop(a: argparse.Namespace) -> tuple[ConsumerLoop, MllpRecordHandler]:
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
    spec = spec_source.current()
    destination = spec.destination(a.destination)
    config = mllp_destination_config_from_destination(spec.name, destination)

    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else DirSecretResolver()
    connection = connection_from_config(config, resolver)
    mllp_loader = MllpLoader(config, connection, log=log)
    handler = MllpRecordHandler(mllp_loader, spec_source=spec_source, destination=a.destination)

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
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap.servers")
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
