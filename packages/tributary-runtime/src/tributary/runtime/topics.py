"""Topic names and consumer-group names, from `contracts/topics/v0` section 1.

The grammar is `env "." channel "." kind [ "." destination ]` for the data plane and
`"_tb." env "." platform-kind` for platform topics. It is parseable by `split(".")` only because no
segment may contain a dot, so every segment is validated before it is joined: a channel name with a
dot in it would silently produce a topic that ACL prefixes and metric names cannot address.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal, get_args

from .errors import TopicNameError, safe_token

ENV = re.compile(r"^[a-z][a-z0-9-]{0,15}$")
CHANNEL = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
DESTINATION = re.compile(r"^[a-z][a-z0-9-]{0,63}$")
#: A process instance id (`TB_INSTANCE_ID`, `--instance-id`, a pod name). Looser than the name segments
#: because it is operator-chosen, but never empty and never whitespace or control characters: the
#: `spec_reader` group ACL is a PREFIXED grant on `tb.<env>.<ch>.channels.`, so the instance is the
#: only free-form part of the group id.
INSTANCE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]{0,127}$")
#: A shadow-deployment label. The intersection of the old destination-shaped rule and the contract's
#: `shadow-[a-z0-9-]{1,32}` (`topic-name.schema.json` `consumerGroup`): every label accepted here
#: builds a group the schema accepts.
SHADOW = re.compile(r"^[a-z][a-z0-9-]{0,31}$")

TopicKind = Literal["raw", "out", "retry", "dlq", "reply"]
PlatformKind = Literal["channels", "events", "audit"]
#: Per-channel consumer-group roles (`contracts/topics/v0` 3.6, `topic-name.schema.json`). Set 1.8 took
#: `archiver` out: the archiver consumes `_tb.<env>.events` for every channel, so its group is the
#: environment-wide platform group `tb.<env>.archiver` (`platform_group`), and its ACL is LITERAL. A
#: per-channel `tb.<env>.<ch>.archiver` group is one nothing grants.
Role = Literal["pipeline", "loader", "replier"]
#: Environment-wide consumer groups with no channel segment (set 1.8, `consumer_groups.platform_groups`).
#: `reprocessor` (W3-29) is assign-only: its principal holds DESCRIBE on the literal group, no READ.
PlatformGroup = Literal["archiver", "reprocessor"]

MAX_TOPIC_LENGTH = 249


def _segment(kind: str, value: str, pattern: re.Pattern[str]) -> str:
    if not isinstance(value, str) or not pattern.match(value):
        raise TopicNameError(f"{kind} segment {safe_token(value)} does not match {pattern.pattern}")
    return value


def _joined(parts: list[str]) -> str:
    name = ".".join(parts)
    if len(name) > MAX_TOPIC_LENGTH:
        raise TopicNameError(f"topic name is {len(name)} characters, Kafka allows {MAX_TOPIC_LENGTH}")
    return name


def data_topic(env: str, channel: str, kind: TopicKind, destination: str | None = None) -> str:
    """A data-plane topic name. `out` requires a destination; nothing else accepts one."""
    parts = [_segment("env", env, ENV), _segment("channel", channel, CHANNEL), kind]
    if kind == "out":
        if destination is None:
            raise TopicNameError("kind out requires a destination segment")
        parts.append(_segment("destination", destination, DESTINATION))
    elif destination is not None:
        raise TopicNameError(f"kind {safe_token(kind)} does not take a destination segment")
    return _joined(parts)


def raw_topic(env: str, channel: str) -> str:
    return data_topic(env, channel, "raw")


def out_topic(env: str, channel: str, destination: str) -> str:
    return data_topic(env, channel, "out", destination)


def retry_topic(env: str, channel: str) -> str:
    return data_topic(env, channel, "retry")


def dlq_topic(env: str, channel: str) -> str:
    return data_topic(env, channel, "dlq")


def reply_topic(env: str, channel: str) -> str:
    return data_topic(env, channel, "reply")


def platform_topic(env: str, kind: PlatformKind) -> str:
    """`_tb.<env>.<kind>`. The environment segment is what stops a dev control plane from publishing
    channel specs that production workers consume (`contracts/topics/v0` 1.2)."""
    return _joined(["_tb", _segment("env", env, ENV), kind])


def channels_topic(env: str) -> str:
    return platform_topic(env, "channels")


def events_topic(env: str) -> str:
    return platform_topic(env, "events")


def consumer_group(
    env: str,
    channel: str,
    role: Role,
    destination: str | None = None,
    shadow: str | None = None,
) -> str:
    """`tb.<env>.<channel>.<role>[.<destination>][.shadow-<label>]` (`contracts/topics/v0` 3.6).

    The role is checked at run time as well as by the type: a role read from configuration (or the
    pre-1.8 `archiver`) must fail here, not build a group the ACL never grants.
    """
    if role not in get_args(Role):
        raise TopicNameError(f"consumer-group role {safe_token(role)} is not one of {', '.join(get_args(Role))}")
    parts = ["tb", _segment("env", env, ENV), _segment("channel", channel, CHANNEL), role]
    if destination is not None:
        parts.append(_segment("destination", destination, DESTINATION))
    if shadow is not None:
        parts.append(f"shadow-{_segment('shadow label', shadow, SHADOW)}")
    return ".".join(parts)


def platform_group(env: str, kind: PlatformGroup) -> str:
    """`tb.<env>.<kind>`: an environment-wide platform group, `tb.<env>.archiver` or `tb.<env>.reprocessor` (set 1.8).

    No channel segment and no suffix: the ACL is LITERAL on exactly this name. A PREFIXED
    `tb.<env>.archiver` grant would also match the groups of a channel named `archiver`
    (`tb.<env>.archiver.pipeline`), which is why the set 1.4 grant was withdrawn.
    """
    if kind not in get_args(PlatformGroup):
        raise TopicNameError(f"platform group {safe_token(kind)} is not one of {', '.join(get_args(PlatformGroup))}")
    return ".".join(["tb", _segment("env", env, ENV), kind])


def spec_reader_group(env: str, channel: str, instance: str) -> str:
    """`tb.<env>.<channel>.channels.<instance>`: the `_tb.<env>.channels` reader's per-process group.

    `contracts/topics/v0` 3.6 and `consumer_groups.spec_reader` (set 1.4). The reader uses `assign()`
    and never commits, but librdkafka still looks up the coordinator for a configured `group.id`, so
    the receiver, worker and loader principals hold READ on group PREFIXED `tb.<env>.<channel>.channels.`
    and every data-plane process must build its group id here, not as an f-string. One group per
    process, so readers never share offsets or partitions.
    """
    return ".".join(
        [
            "tb",
            _segment("env", env, ENV),
            _segment("channel", channel, CHANNEL),
            "channels",
            _segment("instance", instance, INSTANCE),
        ]
    )


@dataclass(frozen=True, slots=True)
class ParsedTopic:
    """The parts of a data-plane topic name."""

    env: str
    channel: str
    kind: TopicKind
    destination: str | None = None


def parse_data_topic(name: str) -> ParsedTopic:
    """Split a data-plane topic name back into its segments, validating each one."""
    parts = name.split(".")
    if len(parts) not in (3, 4):
        raise TopicNameError(f"topic {safe_token(name)} does not have the data-plane shape env.channel.kind[.dest]")
    env, channel, raw_kind = parts[0], parts[1], parts[2]
    if raw_kind not in ("raw", "out", "retry", "dlq", "reply"):
        raise TopicNameError(f"topic {safe_token(name)}: unknown kind {safe_token(raw_kind)}")
    kind: TopicKind = raw_kind  # type: ignore[assignment]  # narrowed by the membership test above
    destination = parts[3] if len(parts) == 4 else None
    rebuilt = data_topic(env, channel, kind, destination)  # validates every segment and the out/dest rule
    if rebuilt != name:
        raise TopicNameError(f"topic {safe_token(name)} is not canonical")
    return ParsedTopic(env=env, channel=channel, kind=kind, destination=destination)
