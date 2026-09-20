"""Topic and cluster admin helpers for spikes."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic

from . import config

log = logging.getLogger("tbcommon.admin")


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: str
    partitions: int = 12
    replication_factor: int = 3
    configs: dict[str, str] | None = None

    def to_new_topic(self) -> NewTopic:
        cfg = {
            "min.insync.replicas": str(config.min_insync_replicas()),
            **(self.configs or {}),
        }
        return NewTopic(
            self.name,
            num_partitions=self.partitions,
            replication_factor=self.replication_factor,
            config=cfg,
        )


def admin(bootstrap: str | None = None) -> AdminClient:
    return AdminClient({"bootstrap.servers": bootstrap or config.bootstrap()})


def wait_for_cluster(expected_brokers: int = 3, timeout: float = 120.0) -> list[int]:
    """Block until ``expected_brokers`` brokers are visible. Returns node ids."""
    client = admin()
    deadline = time.monotonic() + timeout
    last: list[int] = []
    while time.monotonic() < deadline:
        try:
            md = client.list_topics(timeout=5.0)
            last = sorted(md.brokers)
            if len(last) >= expected_brokers:
                return last
        except KafkaException as exc:  # broker not up yet
            log.debug("cluster not ready: %s", exc)
        time.sleep(1.0)
    raise TimeoutError(f"only {len(last)} broker(s) visible after {timeout}s: {last}")


def ensure_topics(
    specs: list[TopicSpec], timeout: float = 60.0, *, metadata_timeout: float = 10.0
) -> None:
    """Create topics, tolerating ones that already exist.

    Gotcha: ``create_topics`` returns immediately after the controller accepts
    the request; leaders may not be elected yet. We poll metadata afterwards so
    a following producer does not hit ``UNKNOWN_TOPIC_OR_PART``.

    ``metadata_timeout`` covers the ``list_topics`` calls. The 10 s default is
    ample on an idle cluster but not on one with thousands of topics, where the
    metadata response itself gets large (S4) -- raise it there rather than
    reading a timeout as a broken cluster.
    """
    client = admin()
    existing = set(client.list_topics(timeout=metadata_timeout).topics)
    todo = [s for s in specs if s.name not in existing]
    if todo:
        futures = client.create_topics([s.to_new_topic() for s in todo], request_timeout=timeout)
        for name, fut in futures.items():
            try:
                fut.result(timeout=timeout)
            except KafkaException as exc:
                if "already exists" not in str(exc).lower():
                    raise
                log.debug("topic %s already exists", name)
    _await_leaders(
        [s.name for s in specs], timeout=timeout, metadata_timeout=metadata_timeout
    )


def _await_leaders(names: list[str], timeout: float, metadata_timeout: float = 10.0) -> None:
    client = admin()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        md = client.list_topics(timeout=metadata_timeout)
        ok = True
        for name in names:
            topic = md.topics.get(name)
            if topic is None or topic.error is not None:
                ok = False
                break
            if any(p.leader < 0 for p in topic.partitions.values()):
                ok = False
                break
        if ok:
            return
        time.sleep(0.5)
    raise TimeoutError(f"topics {names} have no leader after {timeout}s")


def delete_topics(names: list[str], timeout: float = 60.0) -> None:
    client = admin()
    existing = set(client.list_topics(timeout=10.0).topics)
    todo = [n for n in names if n in existing]
    if not todo:
        return
    for name, fut in client.delete_topics(todo, request_timeout=timeout).items():
        try:
            fut.result(timeout=timeout)
        except KafkaException:
            log.warning("delete_topics failed for %s", name, exc_info=True)


def topic_config(name: str) -> dict[str, str]:
    client = admin()
    resource = ConfigResource(ConfigResource.Type.TOPIC, name)
    result = client.describe_configs([resource])[resource].result(timeout=30)
    return {k: (v.value or "") for k, v in result.items()}


def partition_leaders(name: str) -> dict[int, int]:
    """partition -> leader node id. Used to target a leader-kill test."""
    md = admin().list_topics(topic=name, timeout=10.0)
    topic = md.topics[name]
    if topic.error is not None:
        raise KafkaException(topic.error)
    return {p.id: p.leader for p in topic.partitions.values()}
