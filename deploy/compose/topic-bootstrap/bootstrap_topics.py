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
import re
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


@dataclass(frozen=True, slots=True)
class Grant:
    """One ACL binding, before it is tied to a principal or to confluent-kafka types."""

    resource_type: str  # "TOPIC" | "GROUP" | "CLUSTER"
    name: str
    pattern: str  # "LITERAL" | "PREFIXED"
    operation: str  # AclOperation member name


# Which `security.acls` role each compose component takes, and which principal it runs as. The
# grants themselves are never restated here: they are parsed from the contract (topics README 3.7).
ROLE_OF_COMPONENT = {
    "receiver": "receiver",
    "worker": "pipeline_worker",
    "loader": "loader",
    "channel-publish": "control_plane",
}

_CLAUSE = re.compile(r"^(?P<ops>[A-Z_]+(?:, [A-Z_]+)*) on (?P<rest>.+)$")


def parse_grants(text: str, *, env: str, channel: str, destinations: tuple[str, ...]) -> list[Grant]:
    """Turn one `security.acls` role string into concrete bindings.

    The contract states grants as a sentence ("WRITE on <env>.<channel>.out.*, .retry, .dlq; ...").
    Only the first sentence is the base grant: later sentences are the set-1.2 sync-reply additions
    and prohibitions, which the demo channel does not use. Anything this grammar does not recognise
    raises, so a contract change can never quietly become a missing (or, worse, wider) grant."""
    first = text.split(". ")[0].rstrip(".")
    grants: list[Grant] = []
    for clause in first.split(";"):
        m = _CLAUSE.match(clause.strip())
        if m is None:
            raise ValueError(f"unrecognised ACL clause: {clause.strip()!r}")
        ops, rest = m.group("ops").split(", "), m.group("rest")
        if rest == "the cluster":
            grants += [Grant("CLUSTER", "kafka-cluster", "LITERAL", op) for op in ops]
            continue
        kind = "TOPIC"
        if rest.startswith("group "):
            kind, rest = "GROUP", rest[len("group ") :]
        for target in re.split(r", | and ", rest):
            if target.startswith("."):  # relative to the channel prefix
                target = f"<env>.<channel>{target}"
            for dest in destinations if "<dest>" in target else (None,):
                name = target.replace("<env>", env).replace("<channel>", channel)
                if dest is not None:
                    name = name.replace("<dest>", dest)
                if "<" in name or ">" in name:
                    raise ValueError(f"unresolved placeholder in ACL target: {target!r}")
                prefixed = name.endswith("*")
                grants += [
                    Grant(kind, name.rstrip("*") if prefixed else name, "PREFIXED" if prefixed else "LITERAL", op)
                    for op in ops
                ]
    return grants


def derive_acls(
    defaults: dict[str, Any], *, env: str, channel: str, destinations: tuple[str, ...], principals: dict[str, str]
) -> list[tuple[str, Grant]]:
    """(principal, grant) for every component in `principals` (component -> `User:<name>`)."""
    acls = defaults["security"]["acls"]
    out: list[tuple[str, Grant]] = []
    for component, principal in principals.items():
        role = ROLE_OF_COMPONENT[component]
        out += [(principal, g) for g in parse_grants(acls[role], env=env, channel=channel, destinations=destinations)]
    return out


def parse_principals(text: str) -> dict[str, str]:
    """`receiver=tb-lite-receiver,worker=...` -> component -> `User:<name>`."""
    pairs = (item.split("=", 1) for item in text.split(",") if item)
    return {component.strip(): f"User:{name.strip()}" for component, name in pairs}


def load_defaults(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]


def _class_configs(defaults: dict[str, Any], class_name: str, profile: str) -> dict[str, str]:
    configs = {str(k): str(v) for k, v in defaults["classes"][class_name]["configs"].items()}
    configs.update({str(k): str(v) for k, v in defaults["profiles"][profile]["class_config_overrides"].items()})
    return configs


def _replication_factor(defaults: dict[str, Any], profile: str) -> int:
    return int(defaults["profiles"][profile]["replication_factor"])


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
    p.add_argument("--env", default=os.environ.get("TB_ENV", "dev"))
    p.add_argument("--profile", default=os.environ.get("TB_PROFILE", "lite"), choices=["lite", "production"])
    p.add_argument("--channel", default=os.environ.get("TB_CHANNEL", "adt-passthrough"))
    p.add_argument("--destination", action="append", default=None, help="repeatable; default: http-stub")
    p.add_argument(
        "--principals",
        default=os.environ.get("TB_ACL_PRINCIPALS", ""),
        help="component=scram-user pairs to grant, e.g. receiver=tb-lite-receiver,worker=tb-lite-worker",
    )
    p.add_argument(
        "--defaults", type=Path, default=Path(os.environ.get("TB_TOPIC_DEFAULTS", str(DEFAULT_DEFAULTS_PATH)))
    )
    args = p.parse_args(argv)

    from confluent_kafka.admin import (  # type: ignore[attr-defined]  # deferred import
        AclBinding,
        AclOperation,
        AclPermissionType,
        AdminClient,
        NewTopic,
        ResourcePatternType,
        ResourceType,
    )
    from kafka_conn import KafkaConnError, load_client_config

    defaults = load_defaults(args.defaults)
    emit_lite_warning(defaults, args.profile)
    destinations = tuple(args.destination or ["http-stub"])
    topics = derive_topics(
        defaults, env=args.env, profile=args.profile, channel=args.channel, destinations=destinations
    )

    try:
        admin = AdminClient(load_client_config())
    except KafkaConnError as exc:
        log.error(json.dumps({"status": "no-kafka-connection", "reason": str(exc)}))
        return 1
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
        except Exception as exc:  # TopicExistsError has no stable public import path across versions
            if "already exists" in str(exc).lower():
                log.info(json.dumps({"topic": name, "status": "already-exists"}))
            else:
                failed = True
                log.error(json.dumps({"topic": name, "status": "failed", "reason": type(exc).__name__}))
    if failed:
        return 1

    # Deny-by-default cluster: nothing but these grants lets a data-plane principal touch a topic.
    acls = derive_acls(
        defaults,
        env=args.env,
        channel=args.channel,
        destinations=destinations,
        principals=parse_principals(args.principals),
    )
    bindings = [
        AclBinding(
            # confluent-kafka names the cluster resource (Kafka's "kafka-cluster") ResourceType.BROKER.
            getattr(ResourceType, "BROKER" if g.resource_type == "CLUSTER" else g.resource_type),
            g.name,
            getattr(ResourcePatternType, g.pattern),
            principal,
            "*",
            getattr(AclOperation, g.operation),
            AclPermissionType.ALLOW,
        )
        for principal, g in acls
    ]
    for future in admin.create_acls(bindings, request_timeout=30).values():
        try:
            future.result()
        except Exception as exc:
            failed = True
            log.error(json.dumps({"acl": "create", "status": "failed", "reason": type(exc).__name__}))
    log.info(json.dumps({"acls": len(bindings), "status": "failed" if failed else "applied"}))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
