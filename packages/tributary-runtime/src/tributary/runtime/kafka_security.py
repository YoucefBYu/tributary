"""The transport rules every Kafka client this runtime builds must satisfy (`engine-kafka/v0` section 4).

`KafkaSettings` calls `check_link` at construction, so no caller (the worker, a loader, a receiver,
the kafka source's foreign connection, a test) can hand librdkafka a configuration that:

* carries any property other than the security keys below (rule 4, ADR-008 D4). In particular
  `bootstrap.servers`, `metadata.broker.list`, `acks` and `enable.idempotence` cannot ride in the
  security map, so it can neither redirect the link past the loopback check nor weaken invariant 1;
* speaks `PLAINTEXT` to anything but a loopback broker (rule 1). **There is no opt-out**;
* speaks TLS without broker verification, hostname verification and the AEAD cipher pin (rule 2);
* uses SASL without a SCRAM-SHA-512 username and password (rule 3).

The allow-list is **narrower than rule 4's `ssl.*`**: only the `ssl.*` properties a `secretRef` resolves
to are accepted. librdkafka's `ssl.engine.location` and `ssl.providers` load a shared object into the
process, and `ssl.*.pem` / `ssl.key.password` would put key material in the map itself, so a prefix
match would be a code-loading passthrough in all but name.

Rules that concern only the **engine** link (an `SSL` connection must present a client certificate,
because the per-component ACLs need a principal) live in `engine_kafka`, because the kafka source also
builds `KafkaSettings` for a foreign cluster, where server-authenticated `SSL` is valid (ADR-007 D11).

Error messages name a rule, a property name from the allow-list, or an entry index. Never a host, a
credential, a path, or a key the caller supplied (invariant 6, CLAUDE.md "no secrets").
"""

from __future__ import annotations

import ipaddress
import re
from collections.abc import Mapping
from typing import Any, Final

from .errors import KafkaSecurityError

#: The AEAD pin every Tributary TLS client uses. librdkafka has no minimum-version property, so this is
#: what refuses a pre-TLS-1.2 handshake (`engine-kafka/v0` section 4 rule 2).
PINNED_CIPHER_SUITES: Final = "ECDHE+AESGCM:ECDHE+CHACHA20"
#: The only SASL mechanism either contract admits (`engine-kafka/v0`, channel-spec `kafkaSecurity`).
SCRAM_SHA_512: Final = "SCRAM-SHA-512"
PROTOCOLS: Final = frozenset({"PLAINTEXT", "SSL", "SASL_SSL"})

#: Exactly the keys a security map may carry. Everything else is refused at construction.
SECURITY_KEYS: Final = frozenset(
    {
        "security.protocol",
        "sasl.mechanism",
        "sasl.username",
        "sasl.password",
        "enable.ssl.certificate.verification",
        "ssl.cipher.suites",
        "ssl.endpoint.identification.algorithm",
        "ssl.ca.location",
        "ssl.certificate.location",
        "ssl.key.location",
    }
)
_TLS_PINS: Final[Mapping[str, object]] = {
    "enable.ssl.certificate.verification": True,
    "ssl.endpoint.identification.algorithm": "https",
    "ssl.cipher.suites": PINNED_CIPHER_SUITES,
}
_PATH_KEYS: Final = ("ssl.ca.location", "ssl.certificate.location", "ssl.key.location")

#: `engine-kafka/v0` and channel-spec `$defs/hostPort`, matched with `fullmatch` (the schema's `$`
#: would also accept a trailing newline under Python's `re.search`).
_HOST_PORT: Final = re.compile(r"(\[::1\]|[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?):([0-9]{2,5})")
_DOTTED_NUMERIC: Final = re.compile(r"[0-9.]+")


def split_bootstrap(bootstrap_servers: str) -> tuple[str, ...]:
    """The entries of a `bootstrap.servers` string, each checked against the contract's `hostPort`.

    Refused, each by index only: an empty list or entry, whitespace, a `PLAINTEXT://`-style scheme, a
    bracketed IPv6 literal other than `[::1]`, an unbracketed IPv6 form, a port outside 1-65535, and a
    dotted-numeric host that is not a valid IPv4 literal (`127.1.1.300`).
    """
    if not isinstance(bootstrap_servers, str) or not bootstrap_servers:
        raise KafkaSecurityError("bootstrap servers are empty")
    entries = tuple(bootstrap_servers.split(","))
    for index, entry in enumerate(entries):
        match = _HOST_PORT.fullmatch(entry)
        if match is None:
            raise KafkaSecurityError(
                f"bootstrap server {index} is not host:port (no scheme, no whitespace; IPv6 only as [::1])"
            )
        host, port = match.group(1), int(match.group(2))
        if not 1 <= port <= 65535:
            raise KafkaSecurityError(f"bootstrap server {index} has a port outside 1-65535")
        if _DOTTED_NUMERIC.fullmatch(host) and not _is_ipv4(host):
            raise KafkaSecurityError(f"bootstrap server {index} is not a valid IPv4 literal")
    return entries


def is_loopback_host_port(entry: str) -> bool:
    """True only for `localhost:<port>`, `<valid IPv4 in 127.0.0.0/8>:<port>` or `[::1]:<port>`.

    No name other than `localhost` is resolved or trusted: a Compose service name, a Kubernetes service
    and `host.docker.internal` all cross an interface other than `lo` (`engine-kafka/v0` section 4 rule 1).
    """
    match = _HOST_PORT.fullmatch(entry)
    if match is None or not 1 <= int(match.group(2)) <= 65535:
        return False
    host = match.group(1)
    if host in ("localhost", "[::1]"):
        return True
    return _is_ipv4(host) and ipaddress.IPv4Address(host).is_loopback


def _is_ipv4(host: str) -> bool:
    try:
        ipaddress.IPv4Address(host)
    except ValueError:
        return False
    return True


def check_link(bootstrap_servers: str, security: Mapping[str, Any]) -> None:
    """Raise `KafkaSecurityError` unless the link satisfies section 4 rules 1-4. See the module doc."""
    entries = split_bootstrap(bootstrap_servers)
    if not isinstance(security, Mapping):
        raise KafkaSecurityError("the security map is not a mapping")
    stray = sum(1 for key in security if key not in SECURITY_KEYS)
    if stray:
        raise KafkaSecurityError(
            f"the security map carries {stray} key(s) outside the allow-list; it may hold only "
            "security.protocol, sasl.mechanism/username/password, enable.ssl.certificate.verification and "
            "the ssl.* verification, CA and client-certificate properties (engine-kafka/v0 4 rule 4)"
        )
    protocol = security.get("security.protocol", "PLAINTEXT")
    if protocol not in PROTOCOLS:
        raise KafkaSecurityError("security.protocol must be PLAINTEXT (loopback only), SSL or SASL_SSL")

    if protocol == "PLAINTEXT":
        if len(security) > int("security.protocol" in security):
            raise KafkaSecurityError("a PLAINTEXT link carries no ssl.* or sasl.* property")
        for index, entry in enumerate(entries):
            if not is_loopback_host_port(entry):
                raise KafkaSecurityError(
                    f"refusing PLAINTEXT to non-loopback bootstrap server {index}: use SASL_SSL or SSL "
                    "(engine-kafka/v0 4 rule 1; there is no opt-out)"
                )
        return

    for key, expected in _TLS_PINS.items():
        value = security.get(key)
        if type(value) is not type(expected) or value != expected:
            raise KafkaSecurityError(
                f"TLS must verify the broker and use the pinned AEAD ciphers: {key} is not the required "
                "value (engine-kafka/v0 4 rule 2)"
            )
    for key in _PATH_KEYS:
        if key in security and not _non_empty_str(security[key]):
            raise KafkaSecurityError(f"{key} is not a non-empty path")
    if ("ssl.certificate.location" in security) != ("ssl.key.location" in security):
        raise KafkaSecurityError("a client certificate needs both ssl.certificate.location and ssl.key.location")

    sasl_keys = [key for key in security if key.startswith("sasl.")]
    if protocol == "SSL":
        if sasl_keys:
            raise KafkaSecurityError("an SSL link carries no sasl.* property; use SASL_SSL")
        return
    if security.get("sasl.mechanism") != SCRAM_SHA_512:
        raise KafkaSecurityError("SASL_SSL requires sasl.mechanism SCRAM-SHA-512")
    if not (_non_empty_str(security.get("sasl.username")) and _non_empty_str(security.get("sasl.password"))):
        raise KafkaSecurityError("SASL_SSL has no username or password (engine-kafka/v0 4 rule 3)")


def _non_empty_str(value: object) -> bool:
    return isinstance(value, str) and value.strip() != ""
