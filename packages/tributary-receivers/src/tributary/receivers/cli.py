"""Entry-point plumbing shared by the receivers' `main()`: a quiet argument parser, the spec feed that
reads `_tb.<env>.channels`, and the signal-aware run loop. Operator input is never echoed."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from typing import Any, NoReturn

from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import TopicDefaults
from tributary.sdk.context import Logger

from .errors import (
    CHANNEL_NOT_DEPLOYED,
    CHANNEL_SPEC_UNAVAILABLE,
    CHANNELS_TOPIC_MISSING,
    CHANNELS_UNAUTHORIZED,
    spec_unavailable_error,
)
from .lifecycle import LiveSpecFeed, ReceiverLifecycle, SpecFeed

_AUTH_NAMES = frozenset({"TOPIC_AUTHORIZATION_FAILED", "CLUSTER_AUTHORIZATION_FAILED", "GROUP_AUTHORIZATION_FAILED"})


def classify_spec_unavailable(consumer: Any, topic: str, exc: BaseException, *, timeout_s: float = 2.0) -> str:
    """Why a spec could not be read, as a fixed code for the `spec_unavailable` log line: one bounded metadata
    request for the channels topic, then (topic readable) whether the failure was "not deployed". Never reads
    broker or exception text into the result."""
    try:
        metadata = consumer.list_topics(topic, timeout=timeout_s)
        entry = metadata.topics.get(topic)
    except Exception:
        return CHANNEL_SPEC_UNAVAILABLE
    error = getattr(entry, "error", None) if entry is not None else None
    if entry is None:
        return CHANNELS_TOPIC_MISSING
    if error is not None:
        name = error.name() if callable(getattr(error, "name", None)) else ""
        if name in _AUTH_NAMES:
            return CHANNELS_UNAUTHORIZED
        if name == "UNKNOWN_TOPIC_OR_PART":
            return CHANNELS_TOPIC_MISSING
        return CHANNEL_SPEC_UNAVAILABLE
    # Readable. `LiveSpecSource` raises "channel ... is not deployed" from `ChannelCache.require`.
    return CHANNEL_NOT_DEPLOYED if "is not deployed" in str(exc) else CHANNEL_SPEC_UNAVAILABLE


def spec_feed_opener(
    *, kafka: KafkaSettings, defaults: TopicDefaults, env: str, channel: str, group: str, log: Logger
) -> Callable[[], SpecFeed]:
    """`open_feed` for a lifecycle: a caught-up reader of `_tb.<env>.channels` for `channel`. It raises
    `ChannelSpecError` while the channel is not deployed or the topic cannot be read (the lifecycle
    stays not ready and retries)."""
    from confluent_kafka import Consumer
    from tributary.runtime.channels import open_spec_source
    from tributary.runtime.clients import consumer_config
    from tributary.runtime.errors import ChannelSpecError
    from tributary.runtime.topics import channels_topic

    def open_feed() -> SpecFeed:
        consumer = Consumer(consumer_config(kafka, defaults, group_id=group, enable_partition_eof=True))
        topic = channels_topic(env)
        try:
            source = open_spec_source(channel=channel, consumer=consumer, topic=topic, log=log)
        except ChannelSpecError as exc:
            code = classify_spec_unavailable(consumer, topic, exc)  # before the consumer is closed
            consumer.close()
            raise spec_unavailable_error(code) from exc
        except BaseException:
            consumer.close()
            raise
        return LiveSpecFeed(source, channel)

    return open_feed


def run_until_stopped(runner: ReceiverLifecycle[Any]) -> NoReturn:
    """Run `runner` on uvloop when present; SIGTERM/SIGINT stop it. Exits with its code (0: signal or a
    restart-only spec change, for the supervisor)."""

    async def run() -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, runner.request_stop)
        return await runner.run()

    try:
        import uvloop  # type: ignore[import-not-found]  # no Windows wheel; optional on this platform
    except ImportError:
        code = asyncio.run(run())
    else:
        code = uvloop.run(run())
    raise SystemExit(code)
