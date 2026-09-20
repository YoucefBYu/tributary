"""Fault injection against the spike cluster, via Docker Compose.

Used by S2 (broker-failure test) and S5 (rebalance / broker restart / kill).
Keep it side-effect-obvious: every call shells out and is logged.
"""

from __future__ import annotations

import logging
import subprocess
import time
from collections.abc import Iterator
from contextlib import contextmanager

from . import admin, config

log = logging.getLogger("tbcommon.faults")


def _compose(*args: str, timeout: float = 120.0) -> str:
    cmd = ["docker", "compose", "-f", str(config.COMPOSE_FILE), *args]
    log.info("fault: %s", " ".join(cmd))
    proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
        cmd, capture_output=True, text=True, timeout=timeout, check=False
    )
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed ({proc.returncode}): {proc.stderr.strip()}")
    return proc.stdout


def stop_broker(service: str) -> None:
    """Graceful stop (SIGTERM). Controlled shutdown, leaders move cleanly."""
    _compose("stop", "-t", "20", service)


def kill_broker(service: str) -> None:
    """Hard kill (SIGKILL). No controlled shutdown; leader election on timeout."""
    _compose("kill", "-s", "SIGKILL", service)


def start_broker(service: str) -> None:
    _compose("start", service)


def restart_broker(service: str, down_seconds: float = 5.0, hard: bool = True) -> None:
    (kill_broker if hard else stop_broker)(service)
    time.sleep(down_seconds)
    start_broker(service)


def pause_broker(service: str) -> None:
    """SIGSTOP: the process stays connected but stops responding.

    This is the nastier fault -- the TCP connection stays open, so clients only
    notice at request timeout, not at connection reset.
    """
    _compose("pause", service)


def unpause_broker(service: str) -> None:
    _compose("unpause", service)


@contextmanager
def broker_down(service: str, hard: bool = True) -> Iterator[None]:
    (kill_broker if hard else stop_broker)(service)
    try:
        yield
    finally:
        start_broker(service)


def service_for_node_id(node_id: int) -> str:
    """KAFKA_NODE_ID -> compose service name (see docker-compose.yml)."""
    return config.BROKERS[node_id - 1]


def leader_service_for(topic: str, partition: int) -> str:
    return service_for_node_id(admin.partition_leaders(topic)[partition])


def wait_healthy(timeout: float = 180.0, expected_brokers: int = 3) -> list[int]:
    """Wait until all brokers are back in the metadata."""
    return admin.wait_for_cluster(expected_brokers=expected_brokers, timeout=timeout)


def compose_ps() -> str:
    return _compose("ps", "--format", "table")
