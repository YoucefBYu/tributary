"""Client and topic defaults, read from `contracts/topics/v0/topic-defaults.json`.

PHASE-0-DECISION section 8 item 1 is explicit: the measured defaults — 3 partitions per channel topic,
an explicit `session.timeout.ms`, `commit_every` above 1 — are in the contract already and **the
runtime must read them rather than hardcode its own**. A hardcoded copy is how the partition default
of 12 survived in the plan after S4 had disproved it.

The file therefore has to be readable from wherever the worker runs. Search order: an explicit path,
then `TB_CONTRACTS_DIR`, then a `contracts/` directory above this file (the repo checkout). A container
image must ship `contracts/` or set `TB_CONTRACTS_DIR`; the worker refuses to start otherwise rather
than falling back to invented numbers.
"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .errors import ConfigError, safe_token

CONTRACT_RELATIVE = Path("topics") / "v0" / "topic-defaults.json"

#: Likewise `out`/`retry` partitions are stated as "match the raw topic of the same channel".
MATCH_RAW = "match-raw"


@dataclass(frozen=True, slots=True)
class ConsumerDefaults:
    """The worker/loader consumer settings invariant 1 and invariant 5 depend on."""

    session_timeout_ms: int
    heartbeat_interval_ms: int
    partition_assignment_strategy: str
    enable_auto_commit: bool
    commit_every: int


@dataclass(frozen=True, slots=True)
class ReplyConsumerDefaults:
    """`clients.reply_consumer` (set 1.2, typed reading ruled in set 1.7, topics README 3.3.1).

    The receiver's consumer of `<env>.<channel>.reply`. Its own profile, not the worker's: the numbers
    are equal today, but a re-measurement of one must neither move nor fail to move the other (3.7).
    """

    session_timeout_ms: int
    heartbeat_interval_ms: int
    auto_offset_reset: str
    isolation_level: str
    partition_assignment_strategy: str
    enable_auto_commit: bool


@dataclass(frozen=True, slots=True)
class TopicDefaults:
    """The parts of the topics contract the runtime needs at startup."""

    source: Path
    partitions: Mapping[str, int | str]
    class_configs: Mapping[str, Mapping[str, Any]]
    consumer: ConsumerDefaults
    reply_consumer: ReplyConsumerDefaults
    producer: Mapping[str, Any]
    profiles: Mapping[str, Mapping[str, Any]]
    #: `clients.transactional_stage_producer.transactional.id`, e.g. `tb.<env>.<channel>.pipeline.<instance>`.
    transactional_id_template: str = ""
    #: `clients.worker_producer` (set 1.8, README 3.2.1) as written, **unvalidated**, or `None` when absent: the
    #: worker's non-transactional producer for `out`, `dlq` and `reply`. Only the worker uses it, so only the
    #: worker validates it (`worker_producer_profile`); a contract without it still loads for every other
    #: component (re-review MED: `load_topic_defaults` is shared by receivers, the replier and loaders).
    worker_producer: Mapping[str, Any] | None = None

    def startup_warning(self, profile: str) -> str | None:
        """The warning a profile requires at startup, or `None`.

        The **contract's own text** rather than a paraphrase: README 3.8 says to emit the warning in
        `profiles.<profile>.warning`, and a component that reworded it would drift from the sentence a
        human approved as the exception to invariant 1's durability promise (ADR-002 D14).
        """
        block = self.profiles.get(profile)
        if block is None or not block.get("startup_warning_required"):
            return None
        warning = block.get("warning")
        return warning if isinstance(warning, str) else None

    def barred_from_production(self, profile: str) -> bool:
        """True when the contract bars this profile from production use (README 3.8, rule 2)."""
        return str(self.profiles.get(profile, {}).get("production_use", "")) == "barred"

    def partitions_for(self, topic_class: str, *, raw_partitions: int | None = None) -> int:
        """The partition count for a class, resolving "match the raw topic" to an actual number."""
        try:
            value = self.partitions[topic_class]
        except KeyError:
            raise ConfigError(f"no partition default for class {safe_token(topic_class)}") from None
        if isinstance(value, int):
            return value
        if raw_partitions is None:
            raise ConfigError(
                f"class {safe_token(topic_class)} must match the raw partition count, which was not supplied"
            )
        return raw_partitions


def find_contracts_dir(explicit: str | Path | None = None) -> Path:
    """Locate the `contracts/` directory, or raise `ConfigError` saying how to point at it."""
    if explicit is not None:
        candidate = Path(explicit)
        if (candidate / CONTRACT_RELATIVE).is_file():
            return candidate
        raise ConfigError(f"no {CONTRACT_RELATIVE.as_posix()} under the given contracts directory")

    from_env = os.environ.get("TB_CONTRACTS_DIR")
    if from_env:
        candidate = Path(from_env)
        if (candidate / CONTRACT_RELATIVE).is_file():
            return candidate
        raise ConfigError("TB_CONTRACTS_DIR is set but does not contain topics/v0/topic-defaults.json")

    for parent in Path(__file__).resolve().parents:
        candidate = parent / "contracts"
        if (candidate / CONTRACT_RELATIVE).is_file():
            return candidate
    raise ConfigError(
        "cannot find contracts/topics/v0/topic-defaults.json: set TB_CONTRACTS_DIR, or ship contracts/ in the image"
    )


def load_topic_defaults(contracts_dir: str | Path | None = None) -> TopicDefaults:
    """Read and shape `topic-defaults.json`. Every number here is measured; none is invented."""
    root = find_contracts_dir(contracts_dir)
    path = root / CONTRACT_RELATIVE
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"cannot read {CONTRACT_RELATIVE.as_posix()}: {type(exc).__name__}") from exc

    profiles = document.get("profiles")
    if not isinstance(profiles, dict):
        raise ConfigError(f"{path.name}: profiles is missing or not an object")

    classes = _require_mapping(document, "classes", path)
    partitions: dict[str, int | str] = {}
    class_configs: dict[str, Mapping[str, Any]] = {}
    for name, block in classes.items():
        if not isinstance(block, dict):
            raise ConfigError(f"classes.{safe_token(name)} is not an object")
        value = block.get("partitions")
        partitions[name] = value if isinstance(value, int) else MATCH_RAW
        configs = block.get("configs")
        class_configs[name] = configs if isinstance(configs, dict) else {}

    clients = _require_mapping(document, "clients", path)
    consumer_block = clients.get("worker_consumer")
    if not isinstance(consumer_block, dict):
        raise ConfigError("clients.worker_consumer is missing from the topics contract")

    # No fallback: an absent or prose-valued commit_every is a refusal to start, not an invented
    # number. commit_every is the replay window after an unclean death, so guessing it here would
    # quietly change how many duplicates a destination sees.
    commit_every = _require_int(consumer_block, "commit_every", path)

    session_timeout = _require_int(consumer_block, "session.timeout.ms", path)
    heartbeat = _require_int(consumer_block, "heartbeat.interval.ms", path)
    if heartbeat * 3 > session_timeout + 1:
        # The contract asks for heartbeat at a third of the session timeout; a larger ratio means a
        # transient GC pause can expire the session, which on an ordered channel stops that partition
        # for the whole session timeout (ADR-003 D7).
        raise ConfigError("heartbeat.interval.ms must be about a third of session.timeout.ms")

    reply_consumer = _reply_consumer_defaults(clients, path)

    producer_block = clients.get("receiver_producer")
    if not isinstance(producer_block, dict):
        raise ConfigError("clients.receiver_producer is missing from the topics contract")
    worker_block = clients.get("worker_producer")

    txn_block = clients.get("transactional_stage_producer")
    txn_template = txn_block.get("transactional.id") if isinstance(txn_block, dict) else None
    if not isinstance(txn_template, str) or "<instance>" not in txn_template:
        # One id per worker, stable across restarts, is what lets init_transactions fence a zombie.
        raise ConfigError("clients.transactional_stage_producer.transactional.id must be a template with <instance>")

    return TopicDefaults(
        source=path,
        transactional_id_template=txn_template,
        partitions=partitions,
        class_configs=class_configs,
        consumer=ConsumerDefaults(
            session_timeout_ms=session_timeout,
            heartbeat_interval_ms=heartbeat,
            partition_assignment_strategy=str(consumer_block.get("partition.assignment.strategy", "")),
            enable_auto_commit=bool(consumer_block.get("enable.auto.commit", False)),
            commit_every=commit_every,
        ),
        reply_consumer=reply_consumer,
        profiles={name: block for name, block in profiles.items() if isinstance(block, dict)},
        producer={
            "acks": producer_block.get("acks", "all"),
            "enable.idempotence": producer_block.get("enable.idempotence", True),
            "max.in.flight.requests.per.connection": producer_block.get("max.in.flight.requests.per.connection", 5),
            "delivery.timeout.ms": producer_block.get("delivery.timeout.ms", 120000),
            "linger.ms": producer_block.get("linger.ms", 5),
            "compression.type": producer_block.get("compression.type", "lz4"),
        },
        worker_producer=dict(worker_block) if isinstance(worker_block, dict) else None,
    )


#: The keys of `clients.worker_producer` that reach librdkafka; every one is required (README 3.7).
_WORKER_PRODUCER_KEYS = (
    "acks",
    "enable.idempotence",
    "max.in.flight.requests.per.connection",
    "delivery.timeout.ms",
    "linger.ms",
    "compression.type",
)


#: librdkafka refuses idempotence above this many in-flight requests per connection.
MAX_IDEMPOTENT_IN_FLIGHT = 5


def worker_producer_profile(defaults: TopicDefaults) -> dict[str, Any]:
    """`clients.worker_producer`, validated: called on the worker's build path only, never at load.

    No fallback literal: a missing block or key is a contract defect, not a default (README 3.7). Invariant 1
    is checked here as well as in the contract: this producer writes `out`, `dlq` and `reply`, whose loss
    after a commit is a lost message. Raises `ConfigError`.
    """
    block = defaults.worker_producer
    if block is None:
        raise ConfigError("clients.worker_producer is missing from the topics contract")
    missing = [key for key in _WORKER_PRODUCER_KEYS if key not in block]
    if missing:
        raise ConfigError(f"clients.worker_producer is missing {', '.join(missing)}")
    profile = {key: block[key] for key in _WORKER_PRODUCER_KEYS}
    if str(profile["acks"]) not in {"all", "-1"} or profile["enable.idempotence"] is not True:
        raise ConfigError("clients.worker_producer must be acks=all with enable.idempotence (invariant 1)")
    in_flight = profile["max.in.flight.requests.per.connection"]
    if isinstance(in_flight, bool) or not isinstance(in_flight, int) or not 1 <= in_flight <= MAX_IDEMPOTENT_IN_FLIGHT:
        raise ConfigError(
            f"clients.worker_producer max.in.flight.requests.per.connection must be 1-{MAX_IDEMPOTENT_IN_FLIGHT}"
            " (idempotence)"
        )
    return profile


#: `clients.reply_consumer` string values the replier depends on, and what the contract allows for each.
#: A value outside these sets is a contract edit the replier was not written for, so it refuses to start.
_REPLY_STRINGS: Mapping[str, frozenset[str]] = {
    "auto.offset.reset": frozenset({"latest", "earliest"}),
    "isolation.level": frozenset({"read_uncommitted", "read_committed"}),
    "partition.assignment.strategy": frozenset({"cooperative-sticky"}),
}


def _reply_consumer_defaults(clients: Mapping[str, Any], path: Path) -> ReplyConsumerDefaults:
    """Parse `clients.reply_consumer`. Every value the replier sets is required: no fallback to the
    worker's profile and no literal (topics README 3.3.1 and 3.7, `must_not_hardcode`)."""
    block = clients.get("reply_consumer")
    if not isinstance(block, dict):
        raise ConfigError("clients.reply_consumer is missing from the topics contract")
    session_timeout = _require_int(block, "session.timeout.ms", path)
    heartbeat = _require_int(block, "heartbeat.interval.ms", path)
    if heartbeat * 3 > session_timeout + 1:
        raise ConfigError("clients.reply_consumer heartbeat.interval.ms must be about a third of session.timeout.ms")
    strings: dict[str, str] = {}
    for key, allowed in _REPLY_STRINGS.items():
        value = block.get(key)
        if not isinstance(value, str) or value not in allowed:
            raise ConfigError(
                f"{path.name}: clients.reply_consumer {safe_token(key)} is missing or not one of {sorted(allowed)}"
            )
        strings[key] = value
    auto_commit = block.get("enable.auto.commit")
    if auto_commit is not False:
        # The replier never commits (a reply older than the pending map is useless).
        raise ConfigError(f"{path.name}: clients.reply_consumer enable.auto.commit must be false")
    return ReplyConsumerDefaults(
        session_timeout_ms=session_timeout,
        heartbeat_interval_ms=heartbeat,
        auto_offset_reset=strings["auto.offset.reset"],
        isolation_level=strings["isolation.level"],
        partition_assignment_strategy=strings["partition.assignment.strategy"],
        enable_auto_commit=False,
    )


def _require_mapping(document: Any, key: str, path: Path) -> Mapping[str, Any]:
    value = document.get(key) if isinstance(document, dict) else None
    if not isinstance(value, dict):
        raise ConfigError(f"{path.name}: {safe_token(key)} is missing or not an object")
    return value


def _require_int(block: Mapping[str, Any], key: str, path: Path) -> int:
    value = block.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ConfigError(f"{path.name}: {safe_token(key)} must be an integer")
    return value
