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

**`minVersion` is not enforceable *as a version knob* here (security review, High then Medium).**
`ssl.min.version` is not a real librdkafka configuration property — `confluent_kafka.Producer(
{"ssl.min.version": "1.2", ...})` raises `KafkaError{code=_INVALID_ARG, ..., "No such configuration
property: \"ssl.min.version\""}` against this package's pinned `confluent-kafka` (2.15.1 / bundled
librdkafka), confirmed by actually constructing one (`tests/kafka/test_client.py`), not assumed from
documentation. Since `$defs/clientTls.minVersion` is deploy-time-authored and this loader has no
version-named property to hand it to, `config.py`'s `_security_from_params` refuses any `minVersion`
other than `DEFAULT_MIN_TLS_VERSION` ("1.2") at config-build time.

**But `ssl.cipher.suites` *can* enforce the same floor, and the "OpenSSL's own default already refuses
old TLS" half of that reasoning does not hold** (security review, Medium, corrects the paragraph
above): the wheel-bundled OpenSSL 3.5.7's *default* `MinProtocol` is only ever as strong as whatever
`OPENSSL_CONF` says at process start — an operator (or a compromised dependency) pointing `OPENSSL_CONF`
at a config with `MinProtocol=TLSv1` and `CipherString=DEFAULT:@SECLEVEL=0` lets this client negotiate
TLS 1.1 or TLS 1.0, silently, with no librdkafka property rejecting anything. Every TLS 1.2+ cipher
suite this client would plausibly need is an AEAD suite (GCM or ChaCha20-Poly1305) offered only under
TLS 1.2 and 1.3 — TLS 1.0 and TLS 1.1 support only CBC/RC4/3DES suites, never GCM or ChaCha20 — so
pinning `ssl.cipher.suites` to an AEAD-only list makes an old-protocol handshake fail with "no shared
cipher" even when `OPENSSL_CONF` has weakened `MinProtocol`/`SECLEVEL` out from under it. `_security_
config` below always sets `ssl.cipher.suites=ECDHE+AESGCM:ECDHE+CHACHA20` whenever TLS is in play (`SSL`
or `SASL_SSL`), refuses to let a caller override it through `extra_producer_config`, and this is
confirmed against a real, deliberately weakened `OPENSSL_CONF` (`tests/kafka/test_cipher_floor.py`):
TLS 1.0 and TLS 1.1 fail the handshake with "no shared cipher", TLS 1.2 and TLS 1.3 still connect. This
is a *cipher-suite* floor, not a `ssl.min.version` property, but it is the actually-enforceable
mechanism this loader has, and unlike relying on OpenSSL's own default, an operator cannot silently
disable it via environment. `main.py`/`producer_settings_from_config` also logs a startup warning
(`openssl_conf_env_set`) if `OPENSSL_CONF` is set in this process's own environment, since that is the
one thing that could otherwise weaken TLS 1.2/1.3 itself (not just below it) — deliberately not unset,
since this process may not own that variable's other uses. This is still a narrower guarantee than the
Python-`ssl`-based loaders (`mllp`, `http`, `postgresql`, `clickhouse`) give, where `ssl.SSLContext.
minimum_version` really is a version check, not a cipher-shape proxy for one; documented here rather
than implied.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any

from tributary.sdk.context import Logger

from ..common import NullLogger
from ..secrets import SecretResolver
from .config import KafkaDestinationConfig, KafkaSecuritySpec

#: Security review, Medium (module docstring): AEAD-only, so a handshake pinned to this list cannot
#: complete under TLS 1.0/1.1 (no CBC/RC4/3DES fallback), regardless of what `OPENSSL_CONF`'s own
#: `MinProtocol`/`CipherString` says. Any caller-supplied `ssl.cipher.suites` in `extra_producer_config`
#: is refused (`_security_config` below) rather than silently overridden or silently allowed to widen
#: this floor back out.
PINNED_CIPHER_SUITES = "ECDHE+AESGCM:ECDHE+CHACHA20"


class TransportError(Exception):
    """The producer could not be constructed against this configuration (a malformed connection
    string, an unreadable credential file). Always surfaces at loader startup, never mid-delivery —
    delivery-time transport failures are the delivery-callback's `KAFKA_<name>`/`CONNECT_FAILED`
    classification in `loader.py`."""


@dataclass(frozen=True, slots=True)
class ProducerSettings:
    #: `repr=False` (security review, Low): this dict carries `sasl.password` in the clear whenever
    #: `security.protocol` is `SASL_SSL` -- the default dataclass `repr` would otherwise print it
    #: verbatim, e.g. from an uncaught exception's traceback locals (same reasoning as
    #: `clickhouse.client.ClickhouseTransport.auth_headers`'s identical `repr=False`).
    config: dict[str, Any] = field(repr=False)


def _security_config(security: KafkaSecuritySpec, resolver: SecretResolver) -> dict[str, Any]:
    config: dict[str, Any] = {"security.protocol": security.protocol}
    if security.protocol == "PLAINTEXT":
        return config
    # Every non-PLAINTEXT protocol here (`SSL`, `SASL_SSL`) is TLS: pin the cipher floor unconditionally,
    # not only when `security.tls` is given, since SASL_SSL negotiates TLS with or without an explicit
    # `tls` block (module docstring, security review, Medium).
    config["ssl.cipher.suites"] = PINNED_CIPHER_SUITES
    if security.tls is not None:
        config["ssl.endpoint.identification.algorithm"] = "https"  # hostname verification stays on
        # No `ssl.min.version` here: not a real librdkafka property (module docstring). `config.py`
        # already refused any `minVersion` other than the default before this function ever runs.
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


def producer_settings_from_config(
    config: KafkaDestinationConfig, resolver: SecretResolver, *, log: Logger | None = None
) -> ProducerSettings:
    log = log if log is not None else NullLogger()
    if "OPENSSL_CONF" in os.environ:
        # Security review, Medium (module docstring): the one thing this loader's own
        # `ssl.cipher.suites` pin cannot compensate for is `OPENSSL_CONF` weakening TLS 1.2/1.3 itself
        # (not just enabling something below the pinned cipher floor) -- flagged, not unset, since this
        # process does not own whatever else set that variable or why.
        log.warning(
            "openssl_conf_env_set",
            destination=config.destination,
            detail="OPENSSL_CONF is set in this process's environment; it can weaken the OpenSSL "
            "TLS/cipher defaults this loader's own ssl.cipher.suites pin relies on "
            "(client.py module docstring)",
        )
    if "ssl.cipher.suites" in config.extra_producer_config:
        # Security review, Medium: the pinned floor (module docstring, `PINNED_CIPHER_SUITES`) is not a
        # caller-configurable knob -- a destination's own `extra_producer_config` cannot widen it back
        # out to include a pre-TLS-1.2 cipher suite.
        raise TransportError(
            "with.connection's producer config may not set ssl.cipher.suites: this loader always pins "
            f"it to {PINNED_CIPHER_SUITES!r} to enforce a TLS 1.2 floor (client.py module docstring)"
        )
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
