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
class TopicDefaults:
    """The parts of the topics contract the runtime needs at startup."""

    source: Path
    partitions: Mapping[str, int | str]
    class_configs: Mapping[str, Mapping[str, Any]]
    consumer: ConsumerDefaults
    producer: Mapping[str, Any]

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

    producer_block = clients.get("receiver_producer")
    if not isinstance(producer_block, dict):
        raise ConfigError("clients.receiver_producer is missing from the topics contract")

    return TopicDefaults(
        source=path,
        partitions=partitions,
        class_configs=class_configs,
        consumer=ConsumerDefaults(
            session_timeout_ms=session_timeout,
            heartbeat_interval_ms=heartbeat,
            partition_assignment_strategy=str(consumer_block.get("partition.assignment.strategy", "")),
            enable_auto_commit=bool(consumer_block.get("enable.auto.commit", False)),
            commit_every=commit_every,
        ),
        producer={
            "acks": producer_block.get("acks", "all"),
            "enable.idempotence": producer_block.get("enable.idempotence", True),
            "max.in.flight.requests.per.connection": producer_block.get("max.in.flight.requests.per.connection", 5),
            "delivery.timeout.ms": producer_block.get("delivery.timeout.ms", 120000),
            "linger.ms": producer_block.get("linger.ms", 5),
            "compression.type": producer_block.get("compression.type", "lz4"),
        },
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
