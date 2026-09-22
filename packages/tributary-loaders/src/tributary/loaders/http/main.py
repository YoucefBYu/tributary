"""`python -m tributary.loaders.http --channel-spec <path> --destination <name> --env <env>
--bootstrap <hosts>`.
"""

from __future__ import annotations

import argparse
import asyncio
import json

from ..secrets import DirSecretResolver
from .config import http_destination_config_from_channel_spec
from .consumer import OutConsumer
from .dlq import DlqPublisher
from .loader import HttpLoader
from .topics import dlq_topic_name, out_topic_name


async def amain(a: argparse.Namespace) -> None:
    with open(a.channel_spec, encoding="utf-8") as f:
        spec = json.load(f)
    config = http_destination_config_from_channel_spec(spec, a.destination)
    resolver = DirSecretResolver(a.secrets_dir) if a.secrets_dir else DirSecretResolver()
    loader = HttpLoader(config, secret_resolver=resolver)

    dlq_publisher = None
    if config.on_exhaust == "dlq-and-continue":
        dlq_publisher = DlqPublisher(a.bootstrap, dlq_topic_name(a.env, config.channel))

    consumer = OutConsumer(
        config,
        loader,
        bootstrap_servers=a.bootstrap,
        group_id=a.group_id,
        topic=out_topic_name(a.env, config.channel, config.destination),
        dlq_publisher=dlq_publisher,
    )
    print(
        json.dumps(
            {"event": "starting", "channel": config.channel, "destination": config.destination, "topic": consumer.topic}
        ),
        flush=True,
    )
    await consumer.run()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--channel-spec", required=True, help="path to a contracts/channel-spec/v0 JSON document")
    p.add_argument("--destination", required=True, help="the spec.destinations[].name to load")
    p.add_argument("--env", required=True, help="the topic-name env segment, e.g. 'dev' or 'prod'")
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap.servers")
    p.add_argument("--group-id", default="tributary-loader")
    p.add_argument("--secrets-dir", help="overrides TB_SECRETS_DIR / the default secrets mount")
    a = p.parse_args()
    asyncio.run(amain(a))


if __name__ == "__main__":
    main()
