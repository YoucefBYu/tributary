"""Entry-point plumbing shared by the receivers' `main()`: a quiet argument parser, the spec feed that
reads `_tb.<env>.channels`, and the signal-aware run loop. Operator input is never echoed."""

from __future__ import annotations

import argparse
import asyncio
import signal
from collections.abc import Callable
from typing import Any, NoReturn

from tributary.runtime.clients import KafkaSettings
from tributary.runtime.config import TopicDefaults
from tributary.sdk.context import Logger

from .lifecycle import LiveSpecFeed, ReceiverLifecycle, SpecFeed
from .startup import refuse


class QuietArgumentParser(argparse.ArgumentParser):
    """argparse echoes an unknown flag, a bad `choices` value or a missing argument's name to stderr.
    Operator input is never echoed: one `startup_refused` line, exit 2."""

    def __init__(self, log: Logger, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._tb_log = log

    def error(self, message: str) -> NoReturn:
        refuse(
            self._tb_log,
            error="UsageError",
            code="ARGUMENTS_INVALID",
            reason="invalid command-line arguments; run with --help for the accepted flags",
        )


def spec_feed_opener(
    *, kafka: KafkaSettings, defaults: TopicDefaults, env: str, channel: str, group: str, log: Logger
) -> Callable[[], SpecFeed]:
    """`open_feed` for a lifecycle: a caught-up reader of `_tb.<env>.channels` for `channel`. It raises
    `ChannelSpecError` while the channel is not deployed or the topic cannot be read (the lifecycle
    stays not ready and retries)."""
    from confluent_kafka import Consumer
    from tributary.runtime.channels import open_spec_source
    from tributary.runtime.clients import consumer_config
    from tributary.runtime.topics import channels_topic

    def open_feed() -> SpecFeed:
        consumer = Consumer(consumer_config(kafka, defaults, group_id=group, enable_partition_eof=True))
        try:
            source = open_spec_source(channel=channel, consumer=consumer, topic=channels_topic(env), log=log)
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
