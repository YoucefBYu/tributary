"""Create the walking-skeleton topics from `contracts/topics/v0/topic-defaults.json`.

Reads every value it needs (partition counts, retention, `min.insync.replicas`,
`unclean.leader.election.enable`, the profile's replication factor and overrides) from that file, per
its own `must_not_hardcode` block: none of them may be restated as a literal here (architect,
2026-09-22). `derive_topics` is pure and unit-tested without a broker; `main` is the only part that
touches Kafka, via `confluent-kafka`'s `AdminClient`.

There is no `tributary-runtime` topic-creation tool yet (W1-4 is not that far along); this script is a
compose-only stand-in for the lite stack, not a contract. Once the runtime has one, this file should
call it instead of re-deriving the same configs.

Two things deliberately not created here, both documented in the contract itself:
  * `_tb.<env>.audit` -- its retention is an open legal question (topic-defaults.json `classes.audit`
    `retention_open_question`; architect, "no number is written").
  * `<channel>.reply` -- the demo channel (`adt-passthrough`) is not a sync-reply channel.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_DEFAULTS_PATH = Path("/contracts/topics/v0/topic-defaults.json")

logging.basicConfig(level=logging.INFO, format='{"level":"%(levelname)s","msg":%(message)s}')
log = logging.getLogger("topic-bootstrap")


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int
    configs: dict[str, str]


def load_defaults(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _class_configs(defaults: dict[str, Any], class_name: str, profile: str) -> dict[str, str]:
    configs = {str(k): str(v) for k, v in defaults["classes"][class_name]["configs"].items()}
    configs.update({str(k): str(v) for k, v in defaults["profiles"][profile]["class_config_overrides"].items()})
    return configs


def _replication_factor(defaults: dict[str, Any], profile: str) -> int:
    return int(defaults["profiles"][profile]["replication_factor"])  # type: ignore[no-any-return]


def data_topic_name(env: str, channel: str, kind: str, destination: str | None = None) -> str:
    parts = [env, channel, kind, *([destination] if destination else [])]
    return ".".join(parts)


def platform_topic_name(env: str, kind: str) -> str:
    return f"_tb.{env}.{kind}"


def derive_topics(
    defaults: dict[str, Any], *, env: str, profile: str, channel: str, destinations: tuple[str, ...]
) -> list[TopicSpec]:
    """Every topic the walking skeleton needs, with every number and config value read from
    `defaults` (never a literal here) -- see the `must_not_hardcode` block it carries."""
    rf = _replication_factor(defaults, profile)
    raw_partitions = int(defaults["classes"]["raw"]["partitions"])

    topics = [
        TopicSpec(
            data_topic_name(env, channel, "raw"),
            raw_partitions,
            rf,
            _class_configs(defaults, "raw", profile),
        ),
        TopicSpec(
            data_topic_name(env, channel, "dlq"),
            int(defaults["classes"]["dlq"]["partitions"]),
            rf,
            _class_configs(defaults, "dlq", profile),
        ),
        TopicSpec(
            data_topic_name(env, channel, "retry"),
            raw_partitions,  # "match the raw topic of the same channel"
            rf,
            _class_configs(defaults, "retry", profile),
        ),
    ]
    for dest in destinations:
        topics.append(
            TopicSpec(
                data_topic_name(env, channel, "out", dest),
                raw_partitions,  # "match the raw topic of the same channel" (ordering: none here too)
                rf,
                _class_configs(defaults, "out", profile),
            )
        )
    for platform_kind in ("channels", "events"):  # audit deliberately excluded, see module docstring
        topics.append(
            TopicSpec(
                platform_topic_name(env, platform_kind),
                int(defaults["classes"][platform_kind]["partitions"]),
                rf,
                _class_configs(defaults, platform_kind, profile),
            )
        )
    return topics


def emit_lite_warning(defaults: dict[str, Any], profile: str) -> None:
    """The mandatory, unsuppressable lite-profile startup warning (ADR-002 D14). This is the
    bootstrap job's own copy for defense in depth; it does not stand in for the per-component
    startup warning each data-plane process must also emit (architect, tracked separately)."""
    if profile != "lite":
        return
    warning = defaults["profiles"]["lite"]["warning"]
    log.warning(json.dumps({"component": "topic-bootstrap", "profile": "lite", "warning": warning}))


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--env", default=os.environ.get("TB_ENV", "dev"))
    p.add_argument("--profile", default=os.environ.get("TB_PROFILE", "lite"), choices=["lite", "production"])
    p.add_argument("--channel", default=os.environ.get("TB_CHANNEL", "adt-passthrough"))
    p.add_argument("--destination", action="append", default=None, help="repeatable; default: http-stub")
    p.add_argument(
        "--defaults", type=Path, default=Path(os.environ.get("TB_TOPIC_DEFAULTS", str(DEFAULT_DEFAULTS_PATH)))
    )
    args = p.parse_args(argv)

    from confluent_kafka.admin import AdminClient, NewTopic  # deferred: keeps derive_topics testable without it

    defaults = load_defaults(args.defaults)
    emit_lite_warning(defaults, args.profile)
    destinations = tuple(args.destination or ["http-stub"])
    topics = derive_topics(defaults, env=args.env, profile=args.profile, channel=args.channel, destinations=destinations)

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    new_topics = [
        NewTopic(t.name, num_partitions=t.partitions, replication_factor=t.replication_factor, config=t.configs)
        for t in topics
    ]
    futures = admin.create_topics(new_topics, request_timeout=30)
    failed = False
    for name, future in futures.items():
        try:
            future.result()
            log.info(json.dumps({"topic": name, "status": "created"}))
        except Exception as exc:  # noqa: BLE001 - TopicExistsError has no stable public import path
            if "already exists" in str(exc).lower():
                log.info(json.dumps({"topic": name, "status": "already-exists"}))
            else:
                failed = True
                log.error(json.dumps({"topic": name, "status": "failed", "reason": type(exc).__name__}))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
