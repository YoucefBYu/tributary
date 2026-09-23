"""`python -m tributary.loaders.http --channel-spec <path> --destination <name> --env <env>
--bootstrap <hosts>`.

Wires `HttpRecordHandler` into `tributary.runtime`'s `ConsumerLoop`, mirroring
`tributary.runtime.worker.PipelineWorker.build()`: the same `clients.consumer_config`/
`producer_config`, the same `topics.consumer_group`/`out_topic`/`dlq_topic`, the same
`KafkaDlqWriter`. One simplification versus the pipeline worker: the channel spec is read once from
a file rather than kept hot from a live `_tb.<env>.channels` cache — this loader does not yet reload
on a republished spec. That is a real gap (a channel change needs a restart to take effect here),
not a design choice; tracked as a follow-up once this loader needs the same hot-reload the pipeline
worker already has.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import time
from typing import cast

from tributary.runtime.clients import KafkaSettings, consumer_config, max_poll_interval_ms, producer_config
from tributary.runtime.config import load_topic_defaults
from tributary.runtime.consumer import ConsumerLoop, KafkaConsumerLike, LoopSettings
from tributary.runtime.events import NullEmitter
from tributary.runtime.logs import JsonLogger
from tributary.runtime.producer import KafkaProducerLike, RecordProducer
from tributary.runtime.spec import ChannelSpec
from tributary.runtime.topics import consumer_group, dlq_topic, out_topic
from tributary.runtime.worker import KafkaDlqWriter

from ..secrets import DirSecretResolver
from .config import http_destination_config_from_destination
from .handler import HttpRecordHandler
from .loader import HttpLoader


def build_loop(a: argparse.Namespace) -> tuple[ConsumerLoop, HttpRecordHandler]:
    from confluent_kafka import Consumer, Producer

    with open(a.channel_spec, encoding="utf-8") as f:
        channel_spec = ChannelSpec.from_document(json.load(f))
    destination = channel_spec.destination(a.destination)
    config = http_destination_config_from_destination(channel_spec.name, destination)

    log = JsonLogger(f"loader-{a.destination}", base={"channel": channel_spec.name, "destination": a.destination})
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else DirSecretResolver()
    handler = HttpRecordHandler(HttpLoader(config, secret_resolver=resolver, log=log))

    kafka_settings = KafkaSettings(bootstrap_servers=a.bootstrap)
    defaults = load_topic_defaults(a.contracts_dir)
    instance = a.instance_id or f"loader-{a.destination}-{os.getpid()}"

    data_producer = Producer(
        producer_config(
            kafka_settings, defaults, client_id=f"tb-{a.env}-{channel_spec.name}-{a.destination}-{instance}"
        )
    )
    producer = RecordProducer(cast("KafkaProducerLike", data_producer), log=log)
    dlq = None
    if destination.retry.on_exhaust == "dlq-and-continue":
        dlq = KafkaDlqWriter(producer, dlq_topic(a.env, channel_spec.name), log=log, source=instance)

    consumer = cast(
        "KafkaConsumerLike",
        Consumer(
            consumer_config(
                kafka_settings,
                defaults,
                group_id=consumer_group(a.env, channel_spec.name, "loader", a.destination),
                group_instance_id=instance,
                max_poll_interval=max_poll_interval_ms(channel_spec),
            )
        ),
    )
    loop = ConsumerLoop(
        consumer,
        handler,
        topics=[out_topic(a.env, channel_spec.name, a.destination)],
        retry=destination.retry,
        settings=LoopSettings(commit_every=defaults.consumer.commit_every),
        emitter=NullEmitter(),  # TODO(W2): lifecycle events for the loader stage, once it needs them
        log=log,
        channel=channel_spec.name,
        channel_version=channel_spec.version,
        instance=instance,
        dlq=dlq,
        destination=a.destination,
    )
    loop.start()
    log.info(
        "loader_started",
        channel=channel_spec.name,
        destination=a.destination,
        channel_version=channel_spec.version,
    )
    return loop, handler


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--channel-spec", required=True, help="path to a contracts/channel-spec/v0 JSON document")
    p.add_argument("--destination", required=True, help="the spec.destinations[].name to load")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap.servers")
    p.add_argument("--instance-id", help="defaults to loader-<destination>-<pid>")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    p.add_argument("--contracts-dir", help="overrides TB_CONTRACTS_DIR / the default contracts/ search")
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
