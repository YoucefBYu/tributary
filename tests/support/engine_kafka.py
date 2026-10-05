"""One shared way for tests to give a spawned receiver or loader its engine Kafka link (ADR-008).

Every data-plane entry point reads `TB_ENGINE_KAFKA_FILE` (`engine-kafka/v0`) and has no other source of
broker address. The testcontainers brokers in this repo listen on loopback, where `PLAINTEXT` is allowed
(`contracts/engine-kafka/v0` README section 4), so the document these helpers write is the loopback one.
Nothing here is a secret: the file holds a broker address and a protocol name.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Mapping
from pathlib import Path

ENV_ENGINE_KAFKA_FILE = "TB_ENGINE_KAFKA_FILE"

#: One directory for the whole test session, removed at interpreter exit by `TemporaryDirectory`.
_DIRECTORY = tempfile.TemporaryDirectory(prefix="tb-engine-kafka-")


def engine_kafka_document(bootstrap: str, *, protocol: str = "PLAINTEXT") -> dict[str, object]:
    """The `engine-kafka/v0` document for `bootstrap` (comma-separated `host:port`)."""
    return {
        "apiVersion": "tributary.dev/v1",
        "kind": "EngineKafkaConnection",
        "bootstrapServers": [entry.strip() for entry in bootstrap.split(",") if entry.strip()],
        "security": {"protocol": protocol},
    }


def write_engine_kafka_file(directory: Path | str, bootstrap: str, *, protocol: str = "PLAINTEXT") -> Path:
    """Write the document into `directory` and return its absolute path."""
    path = Path(directory).resolve() / "engine-kafka.json"
    path.write_text(json.dumps(engine_kafka_document(bootstrap, protocol=protocol)), encoding="utf-8")
    return path


def engine_kafka_env(bootstrap: str, base: Mapping[str, str] | None = None) -> dict[str, str]:
    """A subprocess environment: `base` (default `os.environ`) with `TB_ENGINE_KAFKA_FILE` pointing at a
    document for `bootstrap`, and the retired `TB_BOOTSTRAP_SERVERS` removed (a process refuses to start
    with it present)."""
    env = dict(os.environ if base is None else base)
    env.pop("TB_BOOTSTRAP_SERVERS", None)
    directory = Path(_DIRECTORY.name) / hashlib.sha256(bootstrap.encode()).hexdigest()[:16]
    directory.mkdir(exist_ok=True)
    env[ENV_ENGINE_KAFKA_FILE] = str(write_engine_kafka_file(directory, bootstrap))
    return env


def receiver_env(env: Mapping[str, str]) -> dict[str, str]:
    """`env` plus `TB_OPS_PORT=0`: every receiver binds an ops listener on 127.0.0.1:9180 by default and exits 2
    when that port is taken, so tests that spawn receivers (several at once, or beside a dev stack) must ask
    for an ephemeral port. The one place this is set; every receiver spawn in `tests/` goes through it."""
    return {**env, "TB_OPS_PORT": "0"}
