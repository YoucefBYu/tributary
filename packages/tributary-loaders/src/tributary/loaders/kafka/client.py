"""Building a `confluent_kafka.Producer` config for one `kafka` destination's own, explicit
connection (ADR-007 D11 — never the engine's principal, never the engine's bootstrap servers, even
when it happens to be the same physical cluster).

## Producer profile

`acks=all` plus `enable.idempotence=True`: the same durability floor `tributary.runtime.clients.
producer_config` gives the engine's own `out`/`retry`/`dlq` producers, for the same reason (invariant
1's "never ACK and drop" extends to what this loader itself claims delivered). The idempotent producer
only protects against **librdkafka's own** transport-level retry of one `produce()` call (a broker that
accepted a write but whose ack was lost) — it does **not** deduplicate two separate `deliver()` calls
for a redelivered `tb-msg-id` (a fresh logical call gets a fresh producer sequence number). That half of
invariant 4 is `idempotency-key-header`'s job: `loader.py` puts the same `tb-msg-id` value in the same
header on every attempt, so the record a downstream consumer sees is marked identically no matter which
attempt landed (README 5.1: "the test asserts that both deliveries carry the same `tb-msg-id`, which is
all the engine can promise").

## TLS and SASL (`$defs/kafkaSecurity`, `$defs/clientTls`)

`PLAINTEXT` is refused by the schema off a loopback host (defence in depth: `config.py` also checks
it), so this module does not re-derive that rule — it only turns whatever `security` the schema already
validated into librdkafka properties. `secretRef` on `kafkaSecurity` resolves to `username`/`password`
(SASL); `secretRef` on `clientTls` resolves to `ca.crt` and, when present, `tls.crt`/`tls.key` for a
client certificate (mTLS) — the same `resolve_file`/`resolve_text` split `postgresql/client.py` uses,
so a credential passes through this process only as a file path handed to librdkafka, never read into a
Python string this module could accidentally log.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..secrets import SecretResolver
from .config import KafkaDestinationConfig, KafkaSecuritySpec


class TransportError(Exception):
    """The producer could not be constructed against this configuration (a malformed connection
    string, an unreadable credential file). Always surfaces at loader startup, never mid-delivery —
    delivery-time transport failures are the delivery-callback's `KAFKA_<name>`/`CONNECT_FAILED`
    classification in `loader.py`."""


@dataclass(frozen=True, slots=True)
class ProducerSettings:
    config: dict[str, Any]


def _security_config(security: KafkaSecuritySpec, resolver: SecretResolver) -> dict[str, Any]:
    config: dict[str, Any] = {"security.protocol": security.protocol}
    if security.protocol == "PLAINTEXT":
        return config
    if security.tls is not None:
        config["ssl.endpoint.identification.algorithm"] = "https"  # hostname verification stays on
        config["ssl.min.version"] = security.tls.min_version
        if security.tls.secret_ref is not None:
            config["ssl.ca.location"] = str(resolver.resolve_file(security.tls.secret_ref, "ca.crt"))
    if security.protocol == "SASL_SSL":
        if security.secret_ref is None:  # config.py already requires this; defence in depth
            raise TransportError("SASL_SSL security has no secretRef to resolve credentials from")
        config["sasl.mechanism"] = security.sasl_mechanism
        config["sasl.username"] = resolver.resolve_text(security.secret_ref, "username")
        config["sasl.password"] = resolver.resolve_text(security.secret_ref, "password")
    elif security.protocol == "SSL" and security.secret_ref is not None:
        # Optional mutual TLS: a client certificate under the SAME secretRef as the trust bundle,
        # since $defs/kafkaSecurity has one secretRef slot per connection (unlike postgresql's
        # separate auth.secretRef/tls.secretRef — there is no second slot to put it in here).
        config["ssl.certificate.location"] = str(resolver.resolve_file(security.secret_ref, "tls.crt"))
        config["ssl.key.location"] = str(resolver.resolve_file(security.secret_ref, "tls.key"))
    return config


def producer_settings_from_config(config: KafkaDestinationConfig, resolver: SecretResolver) -> ProducerSettings:
    producer_config: dict[str, Any] = {
        "bootstrap.servers": ",".join(config.connection.bootstrap_servers),
        "acks": "all",
        "enable.idempotence": True,
        # One attempt's worth of internal transport retry, bounded by this destination's own
        # timeoutMs: `loader.py`'s `deliver()` is already the outer attempt boundary
        # (`retry.policy: blocking`, the runtime's `ConsumerLoop` holds the partition and calls
        # `deliver` again) — a large internal retry budget here would just add a second, invisible
        # retry loop underneath the one the channel spec's `retry` block actually describes.
        "message.timeout.ms": max(1000, int(config.timeout_s * 1000)),
        **_security_config(config.connection.security, resolver),
        **dict(config.extra_producer_config),
    }
    return ProducerSettings(producer_config)
