"""Topic names this loader reads and writes (`contracts/topics/v0` section 1)."""

from __future__ import annotations

import re

_ENV = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
_CHANNEL = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
_DESTINATION = re.compile(r"^[a-z][a-z0-9-]{0,63}$")


def _check(env: str, channel: str) -> None:
    if not _ENV.match(env):
        raise ValueError(f"env {env!r} does not match {_ENV.pattern}")
    if not _CHANNEL.match(channel):
        raise ValueError(f"channel {channel!r} does not match {_CHANNEL.pattern}")


def out_topic_name(env: str, channel: str, destination: str) -> str:
    """`<env>.<channel>.out.<destination>`: what this loader consumes."""
    _check(env, channel)
    if not _DESTINATION.match(destination):
        raise ValueError(f"destination {destination!r} does not match {_DESTINATION.pattern}")
    return f"{env}.{channel}.out.{destination}"


def dlq_topic_name(env: str, channel: str) -> str:
    """`<env>.<channel>.dlq`: shared across a channel's destinations, what `onExhaust:
    dlq-and-continue` publishes to."""
    _check(env, channel)
    return f"{env}.{channel}.dlq"
