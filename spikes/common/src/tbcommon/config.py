"""Cluster addresses and tunables for the Phase 0 spike cluster.

Everything is overridable by environment variable so a spike can point at a
different cluster without editing code.
"""

from __future__ import annotations

import os
from pathlib import Path

#: One host port per broker, so a spike can talk to a specific broker.
BROKER_PORTS: dict[str, int] = {"kafka1": 19092, "kafka2": 29092, "kafka3": 39092}

#: Docker Compose service names, in node-id order (node id = index + 1).
BROKERS: tuple[str, ...] = ("kafka1", "kafka2", "kafka3")

DEFAULT_BOOTSTRAP = ",".join(f"localhost:{port}" for port in BROKER_PORTS.values())

COMPOSE_PROJECT = "tributary-spikes"
COMPOSE_FILE = Path(__file__).resolve().parents[2] / "docker-compose.yml"


def bootstrap() -> str:
    """Bootstrap server list. Override with ``TB_BOOTSTRAP``."""
    return os.environ.get("TB_BOOTSTRAP", DEFAULT_BOOTSTRAP)


def broker_bootstrap(service: str) -> str:
    """Bootstrap string that reaches exactly one broker (for leader-kill tests)."""
    return f"localhost:{BROKER_PORTS[service]}"


def topic_prefix() -> str:
    """Namespace for spike topics. Override with ``TB_TOPIC_PREFIX``.

    Spike topics are prefixed so that a stray run never collides with the
    contract-defined ``<env>.<ch>.*`` names.
    """
    return os.environ.get("TB_TOPIC_PREFIX", "spike")


def replication_factor() -> int:
    return int(os.environ.get("TB_RF", "3"))


def min_insync_replicas() -> int:
    return int(os.environ.get("TB_MIN_ISR", "2"))
