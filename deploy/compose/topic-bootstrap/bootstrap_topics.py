"""Create the walking-skeleton topics from `contracts/topics/v0/topic-defaults.json`.

Reads every value it needs (partition counts, retention, `min.insync.replicas`,
`unclean.leader.election.enable`, the profile's replication factor and overrides) from that file, per
its own `must_not_hardcode` block: none of them may be restated as a literal here (architect,
2026-09-22). `derive_topics` is pure and unit-tested without a broker; `main` is the only part that
touches Kafka, via `confluent-kafka`'s `AdminClient`.

There is no `tributary-runtime` topic-creation tool yet (W1-4 is not that far along); this script is a
compose-only stand-in for the lite stack, not a contract. Once the runtime has one, this file should
call it instead of re-deriving the same configs.

One thing deliberately not created here, documented in the contract itself:
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
_SYNC_REPLY = "Set 1.2, sync-reply channels only: "
_PROHIBITION = re.compile(r"^No (?P<op>[A-Z_]+) on (?P<what>[a-z]+)(?:, so .+)?$")
_KNOWN_OPS = frozenset(
    {"READ", "WRITE", "DESCRIBE", "IDEMPOTENT_WRITE", "ALL", "CREATE", "DELETE", "ALTER", "CLUSTER_ACTION"}
)
DATA_PLANE = frozenset({"receiver", "worker", "loader"})
_PRINCIPAL = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")  # refuses "*" and "User:*" style wildcards


def _covers(grant: Grant, topic: str) -> bool:
    if grant.resource_type != "TOPIC":
        return False
    return grant.name == topic if grant.pattern == "LITERAL" else topic.startswith(grant.name)


def parse_grants(text: str, *, env: str, channel: str, destinations: tuple[str, ...]) -> list[Grant]:
    """Turn one `security.acls` role string into concrete bindings.

    The contract states grants as a sentence ("WRITE on <env>.<channel>.out.*, .retry, .dlq; ...").
    The first sentence is the base grant. Every later sentence must be one of two recognised forms, or
    this raises: `Set 1.2, sync-reply channels only: ...` (additions for sync-reply channels, which the
    demo channel is not, so they are not granted here) and `No <OP> on <suffix>, so ...` (a prohibition,
    enforced against the parsed grants below). Anything else could be a restriction that would silently
    turn into an unconditional grant if it were dropped, so it is an error, never skipped."""
    first, *extra = re.split(r"\.\s+", text.strip().rstrip("."))
    prohibitions: list[tuple[str, str]] = []
    for sentence in extra:
        if sentence.startswith(_SYNC_REPLY):
            continue
        if m := _PROHIBITION.match(sentence):
            prohibitions.append((m.group("op"), f"{env}.{channel}.{m.group('what')}"))
            continue
        raise ValueError(f"unrecognised ACL sentence: {sentence!r}")
    grants: list[Grant] = []
    for clause in first.split(";"):
        m = _CLAUSE.match(clause.strip())
        if m is None:
            raise ValueError(f"unrecognised ACL clause: {clause.strip()!r}")
        ops, rest = m.group("ops").split(", "), m.group("rest")
        if unknown := set(ops) - _KNOWN_OPS:
            raise ValueError(f"unknown ACL operation: {sorted(unknown)}")
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
                if not name.endswith("*"):
                    grants += [Grant(kind, name, "LITERAL", op) for op in ops]
                    continue
                stem = name[:-1]
                if dest is not None and stem.endswith(dest):
                    # `<dest>*` with no delimiter would let a grant for `fhir` also cover `fhir-backup`.
                    # The destination itself, and anything below it (`<dest>.<suffix>`), and nothing else.
                    grants += [Grant(kind, stem, "LITERAL", op) for op in ops]
                    grants += [Grant(kind, stem + ".", "PREFIXED", op) for op in ops]
                else:
                    grants += [Grant(kind, stem, "PREFIXED", op) for op in ops]
    for op, topic in prohibitions:
        for g in grants:
            if g.operation in {op, "ALL"} and _covers(g, topic):
                raise ValueError(f"contract prohibits {op} on {topic}, but a parsed grant allows it")
    return grants


def check_forbidden(component: str, grants: list[Grant], *, env: str) -> None:
    """`security.acls.forbidden`: no data-plane component gets cluster-wide WRITE, DELETE or ALTER, and
    none gets WRITE on `_tb.<env>.channels` (a compromised worker must not deploy a channel spec).
    Enforced on the result, whatever the contract's role strings say. `ALL` counts as each of them,
    and so does a topic grant on the empty prefix (every topic)."""
    if component not in DATA_PLANE:
        return
    channels = f"_tb.{env}.channels"
    for g in grants:
        if g.operation in {"WRITE", "DELETE", "ALTER", "ALL"}:
            if g.resource_type == "CLUSTER":
                raise ValueError(f"{component}: forbidden cluster-wide {g.operation}")
            if g.resource_type == "TOPIC" and g.pattern == "PREFIXED" and g.name == "":
                raise ValueError(f"{component}: forbidden {g.operation} on every topic")
        if g.operation in {"WRITE", "ALL"} and _covers(g, channels):
            raise ValueError(f"{component}: forbidden {g.operation} on {channels}")


def derive_acls(
    defaults: dict[str, Any],
    *,
    env: str,
    channel: str,
    destinations: tuple[str, ...],
    principals: dict[str, str],
    transactional: bool = False,
) -> list[tuple[str, Grant]]:
    """(principal, grant) for every component in `principals` (component -> `User:<name>`).

    `transactional`: the channel has `pipeline.transactional: true`. Contract set 1.4 puts the worker's
    TRANSACTIONAL_ID grant in a separate `pipeline_worker_transactional` key because this parser has no
    transactional-id target; so a transactional channel is refused rather than bootstrapped without it."""
    if transactional:
        raise ValueError(
            "transactional channels are not supported by the lite bootstrap: "
            "security.acls.pipeline_worker_transactional needs a TRANSACTIONAL_ID target"
        )
    users = list(principals.values())
    if len(set(users)) != len(users):
        raise ValueError("two components are mapped to one principal")
    acls = defaults["security"]["acls"]
    out: list[tuple[str, Grant]] = []
    for component, principal in principals.items():
        if component not in ROLE_OF_COMPONENT:
            raise ValueError(f"unknown component: {component!r}")
        if not _PRINCIPAL.match(principal.removeprefix("User:")):
            raise ValueError(f"invalid principal for {component}")
        grants = parse_grants(acls[ROLE_OF_COMPONENT[component]], env=env, channel=channel, destinations=destinations)
        check_forbidden(component, grants, env=env)
        out += [(principal, g) for g in grants]
    return out


def parse_principals(text: str) -> dict[str, str]:
    """`receiver=tb-lite-receiver,worker=...` -> component -> `User:<name>`. Refuses wildcards, unknown
    components, and two components mapped to one user (they would share ACLs)."""
    result: dict[str, str] = {}
    for item in filter(None, text.split(",")):
        component, sep, name = item.partition("=")
        component, name = component.strip(), name.strip()
        if not sep or component not in ROLE_OF_COMPONENT:
            raise ValueError(f"unknown component in principals: {component!r}")
        if not _PRINCIPAL.match(name):
            raise ValueError(f"invalid principal name for {component}")
        result[component] = f"User:{name}"
    if len(set(result.values())) != len(result):
        raise ValueError("two components are mapped to one principal")
    return result


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
    for platform_kind in ("channels", "events", "audit"):  # audit: a 30 d buffer (ADR-004 R7, set 1.8)
        topics.append(
            TopicSpec(
                platform_topic_name(env, platform_kind),
                int(defaults["classes"][platform_kind]["partitions"]),
                rf,
                _class_configs(defaults, platform_kind, profile),
            )
        )
    return topics


#: Topic configs re-applied to a topic that already exists, because `create_topics` sets config only on a new one.
#: Only settings that must hold on a long-lived cluster belong here (set 1.8: broker-stamped timestamps).
ENFORCED_ON_EXISTING = ("message.timestamp.type",)


def existing_topic_alterations(topics: list[TopicSpec], existing: set[str]) -> dict[str, dict[str, str]]:
    """The `ENFORCED_ON_EXISTING` settings the contract gives each topic in `existing` (idempotent SET)."""
    out: dict[str, dict[str, str]] = {}
    for t in topics:
        if t.name in existing and (cfg := {k: t.configs[k] for k in ENFORCED_ON_EXISTING if k in t.configs}):
            out[t.name] = cfg
    return out


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
    existing: set[str] = set()
    for name, future in futures.items():
        try:
            future.result()
            log.info(json.dumps({"topic": name, "status": "created"}))
        except Exception as exc:  # TopicExistsError has no stable public import path across versions
            if "already exists" in str(exc).lower():
                existing.add(name)
                log.info(json.dumps({"topic": name, "status": "already-exists"}))
            else:
                failed = True
                log.error(json.dumps({"topic": name, "status": "failed", "reason": type(exc).__name__}))
    if failed:
        return 1
    # Re-run safe: a topic created before a contract setting existed gets it now (incremental SET, no other key).
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry, ConfigResource  # type: ignore[attr-defined]

    alter = []
    for name, cfg in existing_topic_alterations(topics, existing).items():
        res = ConfigResource(ConfigResource.Type.TOPIC, name)
        for k, v in cfg.items():
            res.add_incremental_config(ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET))
        alter.append(res)
    if alter:
        for res, fut in admin.incremental_alter_configs(alter).items():
            try:
                fut.result()
                log.info(json.dumps({"topic": res.name, "status": "config-applied"}))
            except Exception as exc:
                failed = True
                log.error(json.dumps({"topic": res.name, "status": "config-failed", "reason": type(exc).__name__}))
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
