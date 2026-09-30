"""Spawning the real MLLP receiver for tests, with its channel spec on `_tb.<env>.channels` (invariant 3).

The receiver has no file input: it reads its spec from the compacted channels topic, exactly as a
deployed one does. `publish_channel_spec` writes a spec record (or a tombstone) the way the control
plane's publisher would, so a test exercises the real read path rather than a stand-in for it.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from tributary.runtime.topics import channels_topic

from tests.support.engine_kafka import engine_kafka_env
from tests.support.kafka_helpers import create_topics_with_retry
from tests.support.processes import ManagedProcess


def ensure_channels_topic(bootstrap: str, env: str) -> None:
    """Create `_tb.<env>.channels` compacted, with as many replicas as the cluster has brokers (up to 3)."""
    brokers = len(AdminClient({"bootstrap.servers": bootstrap}).list_topics(timeout=15).brokers)
    topic = NewTopic(channels_topic(env), 1, max(1, min(3, brokers)), config={"cleanup.policy": "compact"})
    create_topics_with_retry(bootstrap, [topic])


def publish_channel_spec(bootstrap: str, env: str, channel: str, document: Mapping[str, Any] | None) -> None:
    """Publish `document` under key `channel`; `None` publishes a tombstone (undeploy)."""
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all", "enable.idempotence": True})
    value = None if document is None else json.dumps(document).encode()
    producer.produce(channels_topic(env), key=channel.encode(), value=value)
    if producer.flush(30) != 0:
        raise AssertionError("channel spec was not delivered within 30s")


def receiver_args(*, channel: str, env: str, instance: str) -> list[str]:
    return [
        sys.executable,
        "-m",
        "tributary.receivers.mllp",
        "--channel",
        channel,
        "--env",
        env,
        "--instance",
        instance,
    ]


def start_receiver(
    module: str,
    *,
    channel: str,
    env: str,
    bootstrap: str,
    instance: str,
    secrets_dir: Path | str | None = None,
    extra_args: Sequence[str] = (),
) -> ManagedProcess:
    """Spawn `python -m <module>` (a receiver) without waiting for it: it stays not ready until its channel's
    spec is on `_tb.<env>.channels`. Every receiver takes the same `--channel/--env/--instance` flags."""
    args = [sys.executable, "-m", module, "--channel", channel, "--env", env, "--instance", instance]
    if secrets_dir is not None:
        args += ["--secrets-dir", str(secrets_dir)]
    return ManagedProcess([*args, *extra_args], env=engine_kafka_env(bootstrap))


def start_mllp_receiver(
    *, channel: str, env: str, bootstrap: str, instance: str = "mllp-e2e", secrets_dir: Path | str | None = None
) -> ManagedProcess:
    """Spawn the receiver without waiting for it: it stays not-ready until a spec is on the topic.
    (`tests.support.processes.spawn_mllp_receiver_from_topic` is the same command plus the `listening` wait.)"""
    return start_receiver(
        "tributary.receivers.mllp",
        channel=channel,
        env=env,
        bootstrap=bootstrap,
        instance=instance,
        secrets_dir=secrets_dir,
    )


def spawn_mllp_receiver_with_spec_file(
    *, channel_spec: Path, env: str, bootstrap: str, instance: str = "mllp-e2e", timeout_s: float = 30.0
) -> ManagedProcess:
    """Publish the spec document in `channel_spec` to the channels topic, then spawn the receiver.

    The compatibility path for callers that still hold a spec file (the e2e, chaos and benchmark
    suites): the file is only ever read here, by the test process, and the receiver itself gets the spec
    from the topic."""
    document = json.loads(channel_spec.read_text(encoding="utf-8"))
    channel = document["metadata"]["name"]
    ensure_channels_topic(bootstrap, env)
    publish_channel_spec(bootstrap, env, channel, document)
    from tests.support.processes import spawn_mllp_receiver_from_topic

    return spawn_mllp_receiver_from_topic(
        channel=channel, env=env, bootstrap=bootstrap, instance=instance, timeout_s=timeout_s
    )
