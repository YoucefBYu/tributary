"""The engine Kafka connection loader: `TB_ENGINE_KAFKA_FILE` -> `KafkaSettings` (`engine-kafka/v0`, ADR-008).

**Public API** (what every data-plane entry point uses; receivers, loaders and the archiver migrate onto
it, ADR-008 D3). Everything below is also exported from `tributary.runtime`.

``load_engine_kafka_or_exit(environ=None, *, argv=(), resolver=None, log=None) -> KafkaSettings``
    **The entry-point call.** Runs `load_engine_kafka`; on any `RuntimeConfigurationError` (that includes
    `EngineKafkaError`, `KafkaSecurityError` and `SecretResolutionError`) it writes exactly one
    `startup_refused` line to `log` (a `tributary.sdk.Logger`; default `JsonLogger("engine-kafka")`)
    and raises `SystemExit(2)`. The line carries `error` (class name), `code` (below) and, only for
    those three classes, whose messages are fixed sentences, `reason`. It never carries a secret value,
    a secret file's content, a path or the document's content. On success it logs `engine_kafka_link`
    with `KafkaSettings.describe()` and returns the settings.

``load_engine_kafka(environ=None, *, argv=(), resolver=None) -> KafkaSettings``
    The same load, raising instead of exiting (tests, embedding). Refuses the retired inputs
    (`TB_BOOTSTRAP_SERVERS` in `environ`, `--bootstrap` / `--engine-security` in `argv`), reads the
    absolute path in `TB_ENGINE_KAFKA_FILE`, validates the document, resolves its `secretRef`s from
    `TB_SECRETS_DIR` (or `resolver`), and returns validated `KafkaSettings`. Any failure raises
    `EngineKafkaError`; there is no default and no fallback. Pass `argv=sys.argv[1:]` so a leftover flag
    is refused rather than ignored.

**Error codes** (`EngineKafkaError.code`, stable): `ENGINE_KAFKA_RETIRED_INPUT` (a retired variable or
flag is present), `ENGINE_KAFKA_FILE_UNSET`, `ENGINE_KAFKA_FILE_UNREADABLE` (relative, unreadable or
oversized), `ENGINE_KAFKA_DOCUMENT_INVALID` (not JSON, or not `engine-kafka/v0`),
`ENGINE_KAFKA_SECRET_UNRESOLVED` (a `secretRef` key is missing, empty, oversized, not UTF-8, or the
resolver raised) and
`ENGINE_KAFKA_LINK_REFUSED` (the resolved map broke a `KafkaSettings` link rule). A `KafkaSecurityError`
raised directly has `KAFKA_LINK_REFUSED`.

**Migration recipe** (one receiver, loader or archiver entry point):

1. Replace the process's own Kafka settings code with one call, before any Kafka client is built::

       kafka = load_engine_kafka_or_exit(argv=sys.argv[1:], log=JsonLogger("<component>"))

   and build every client (producer, consumer, `_tb.<env>.channels` reader) from `kafka` via the
   `tributary.runtime.clients` builders. The call replaces: the `--bootstrap` / `TB_BOOTSTRAP_SERVERS`
   read, any `localhost:9092` default, any hand-built `security.protocol` / `sasl.*` / `ssl.*` map,
   any `--engine-security` parsing, and the `try: ... except ...: log; sys.exit(...)` around them.
2. Delete the `--bootstrap` and `--engine-security` arguments from the parser. Do not keep them as
   ignored options: `argv=sys.argv[1:]` must still see them, so a stale manifest refuses to start
   (`ENGINE_KAFKA_RETIRED_INPUT`) instead of connecting somewhere else.
3. Retired: `TB_BOOTSTRAP_SERVERS` (env), `--bootstrap` and `--engine-security` (flags). New inputs:
   `TB_ENGINE_KAFKA_FILE` (required, absolute path to the `engine-kafka/v0` document) and
   `TB_SECRETS_DIR` (existing; default `/var/run/secrets/tributary`).
4. A connector that resolves its own destination secrets keeps its own resolver for those; the engine
   link's `secretRef`s go through this module only. Pass `resolver=` only to share one instance.
5. Tests: assert `SystemExit` with code 2 and one `startup_refused` line for an unset file and for
   `TB_BOOTSTRAP_SERVERS`; build no client first (patch `confluent_kafka.Producer`/`Consumer` to fail).

``read_engine_kafka_file(path) -> EngineKafkaConnection``
``parse_engine_kafka(document) -> EngineKafkaConnection``
    The document alone, validated, secrets unresolved. `EngineKafkaConnection` holds names only
    (`secretRef`s, never values) and is safe to log or compare.

``engine_kafka_settings(connection, resolver) -> KafkaSettings``
    Resolve the secrets and build the librdkafka security map. Use it with your own `SecretResolver`
    when a process already has one; `tributary.runtime.secrets.DirSecretResolver` is the default.

``KafkaSettings.describe()`` is what the startup log may say: protocol, mechanism, whether a client
certificate is present, and the broker count.

**Validation.** `parse_engine_kafka` is a strict mirror of `engine-kafka.schema.json`, not a lenient
reader: this document decides who the process is and whether its link is encrypted, so an unknown field
is refused (unlike the channel spec, section 1.1 there). It is stdlib-only because the data plane has no
`jsonschema` dependency; `tests/test_engine_kafka.py` checks it against every contract example and,
by hypothesis, against `jsonschema` on the schema file itself. It is **stricter** than the schema in
exactly these ways, each of which the schema's regexes let through:

* patterns match the whole string (`re.fullmatch`): the schema's `$` also accepts a trailing newline;
* a port must be 1-65535 (the schema accepts `00` and `99999`);
* a dotted-numeric host must be a valid IPv4 literal (the schema accepts `127.1.1.300` as loopback);
* a JSON object with a duplicate key is refused (a parser would silently keep the last one);
* `SSL` and `SASL_SSL` secrets must be non-empty (section 4 rule 3, which a schema cannot see).

`KafkaSettings` then re-applies the link rules (`kafka_security.check_link`) to the resolved map, so a
defect in this module cannot produce an unchecked client.

**What never leaves this module:** a secret value, the secrets directory, or the document's content.
Messages carry JSON paths, rule names and `secretRef` names (invariant 6, section 4 rule 5).
"""

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final, Literal, NoReturn

from tributary.sdk.context import Logger

from .clients import KafkaSettings
from .errors import EngineKafkaError, KafkaSecurityError, RuntimeConfigurationError, safe_token
from .kafka_security import (
    PINNED_CIPHER_SUITES,
    SCRAM_SHA_512,
    is_loopback_host_port,
    split_bootstrap,
)
from .logs import JsonLogger
from .secrets import DirSecretResolver, SecretResolutionError, SecretResolver

__all__ = [
    "API_VERSION",
    "ENV_ENGINE_KAFKA_FILE",
    "KIND",
    "RETIRED_ENV",
    "RETIRED_FLAGS",
    "EngineKafkaConnection",
    "engine_kafka_settings",
    "load_engine_kafka",
    "load_engine_kafka_or_exit",
    "parse_engine_kafka",
    "read_engine_kafka_file",
    "refuse_retired_inputs",
]

ENV_ENGINE_KAFKA_FILE: Final = "TB_ENGINE_KAFKA_FILE"
API_VERSION: Final = "tributary.dev/v1"
KIND: Final = "EngineKafkaConnection"
#: README section 2: a process given one of these after its migration refuses to start, so two
#: sources of the same setting can never disagree.
RETIRED_ENV: Final = ("TB_BOOTSTRAP_SERVERS",)
RETIRED_FLAGS: Final = ("--bootstrap", "--engine-security")
#: The document is a few hundred bytes. A larger file is not this document.
MAX_DOCUMENT_BYTES: Final = 65_536
MAX_BOOTSTRAP_SERVERS: Final = 16
#: The exit status of a refused start (`load_engine_kafka_or_exit`): a usage/configuration error, so a
#: supervisor does not mistake it for a crash worth restarting in a tight loop.
EXIT_CONFIG_REFUSED: Final = 2
_UNREADABLE: Final = "ENGINE_KAFKA_FILE_UNREADABLE"
_SECRET_UNRESOLVED: Final = "ENGINE_KAFKA_SECRET_UNRESOLVED"  # noqa: S105 - an error code, not a secret
#: The exceptions whose messages are fixed sentences built from names (tested with planted markers). Only
#: these have their `reason` logged; any other `RuntimeConfigurationError` logs its class and code only.
_FIXED_MESSAGE_ERRORS: Final = (EngineKafkaError, KafkaSecurityError, SecretResolutionError)

LinkProtocol = Literal["SASL_SSL", "SSL", "PLAINTEXT"]
_PROTOCOLS: Final[tuple[LinkProtocol, ...]] = ("SASL_SSL", "SSL", "PLAINTEXT")
_SECRET_REF: Final = re.compile(r"[a-z][a-z0-9-]{0,63}")
_ROOT_KEYS: Final = frozenset({"apiVersion", "kind", "bootstrapServers", "security"})
_SECURITY_KEYS: Final = frozenset({"protocol", "saslMechanism", "secretRef", "tls"})
_TLS_KEYS: Final = frozenset({"secretRef"})


@dataclass(frozen=True, slots=True)
class EngineKafkaConnection:
    """A validated `engine-kafka/v0` document. Names only: every field is safe to log."""

    bootstrap_servers: tuple[str, ...]
    protocol: LinkProtocol
    sasl_mechanism: str | None = None
    #: The principal: `username`/`password` (SASL_SSL) or `tls.crt`/`tls.key` (SSL).
    secret_ref: str | None = None
    #: The trust bundle only (`ca.crt`). None means the platform trust store.
    tls_secret_ref: str | None = None


def refuse_retired_inputs(environ: Mapping[str, str], argv: Sequence[str] = ()) -> None:
    """Raise if a retired input is present, naming its replacement (README section 2)."""
    for name in RETIRED_ENV:
        if name in environ:
            raise EngineKafkaError(
                f"{name} is retired: put bootstrapServers in the {ENV_ENGINE_KAFKA_FILE} document",
                code="ENGINE_KAFKA_RETIRED_INPUT",
            )
    for arg in argv:
        flag = arg.split("=", 1)[0]
        for retired in RETIRED_FLAGS:  # the constant is formatted, never the argument (it may carry a value)
            if flag == retired:
                raise EngineKafkaError(
                    f"{retired} is retired: use the {ENV_ENGINE_KAFKA_FILE} document (engine-kafka/v0)",
                    code="ENGINE_KAFKA_RETIRED_INPUT",
                )


def load_engine_kafka(
    environ: Mapping[str, str] | None = None,
    *,
    argv: Sequence[str] = (),
    resolver: SecretResolver | None = None,
) -> KafkaSettings:
    """`TB_ENGINE_KAFKA_FILE` to validated `KafkaSettings`, or `EngineKafkaError`. See the module doc."""
    env = os.environ if environ is None else environ
    refuse_retired_inputs(env, argv)
    path = env.get(ENV_ENGINE_KAFKA_FILE)
    if not path:
        raise EngineKafkaError(
            f"{ENV_ENGINE_KAFKA_FILE} is not set: every data-plane process needs an engine-kafka/v0 document "
            "(there is no default broker)",
            code="ENGINE_KAFKA_FILE_UNSET",
        )
    connection = read_engine_kafka_file(path)
    return engine_kafka_settings(connection, resolver if resolver is not None else DirSecretResolver.from_environ(env))


def load_engine_kafka_or_exit(
    environ: Mapping[str, str] | None = None,
    *,
    argv: Sequence[str] = (),
    resolver: SecretResolver | None = None,
    log: Logger | None = None,
) -> KafkaSettings:
    """`load_engine_kafka`, or one `startup_refused` log line and `SystemExit(2)`. See the module doc.

    On success it logs `engine_kafka_link` with `KafkaSettings.describe()` (protocol, mechanism, whether a
    client certificate is present, broker count) and returns the settings.
    """
    logger: Logger = log if log is not None else JsonLogger("engine-kafka")
    try:
        settings = load_engine_kafka(environ, argv=argv, resolver=resolver)
    except RuntimeConfigurationError as exc:
        _refuse(logger, exc)
    logger.info("engine_kafka_link", **settings.describe())
    return settings


def _refuse(logger: Logger, exc: RuntimeConfigurationError) -> NoReturn:
    fields: dict[str, object] = {"error": safe_token(type(exc).__name__), "code": safe_token(exc.code)}
    if type(exc) in _FIXED_MESSAGE_ERRORS:
        fields["reason"] = str(exc)
    try:
        logger.error("startup_refused", **fields)
    except Exception:  # a guard refusing the reason must not turn exit 2 into a traceback
        fields.pop("reason", None)
        try:
            logger.error("startup_refused", **fields)
        except Exception:  # the stream itself is gone; the exit status still says it
            sys.stderr.write('{"event":"startup_refused","level":"error"}\n')
    raise SystemExit(EXIT_CONFIG_REFUSED)


def read_engine_kafka_file(path: str | Path) -> EngineKafkaConnection:
    """Read and validate the document at `path`, which must be absolute (README section 2)."""
    candidate = Path(path)
    if not candidate.is_absolute():
        raise EngineKafkaError(f"{ENV_ENGINE_KAFKA_FILE} must be an absolute path", code=_UNREADABLE)
    try:
        with candidate.open("rb") as handle:
            raw = handle.read(MAX_DOCUMENT_BYTES + 1)
    except OSError as exc:
        raise EngineKafkaError(
            f"{ENV_ENGINE_KAFKA_FILE} is not readable ({safe_token(type(exc).__name__)})", code=_UNREADABLE
        ) from None
    if len(raw) > MAX_DOCUMENT_BYTES:
        raise EngineKafkaError(f"{ENV_ENGINE_KAFKA_FILE} is larger than {MAX_DOCUMENT_BYTES} bytes", code=_UNREADABLE)
    try:
        document = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_keys, parse_constant=_no_constant)
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        # The message of a JSON error can quote the document; the class name cannot.
        raise EngineKafkaError(
            f"{ENV_ENGINE_KAFKA_FILE} is not a valid JSON document ({safe_token(type(exc).__name__)})",
            code="ENGINE_KAFKA_DOCUMENT_INVALID",
        ) from None
    return parse_engine_kafka(document)


def _unique_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise EngineKafkaError("duplicate object key")
        result[key] = value
    return result


def _no_constant(_name: str) -> Any:
    raise EngineKafkaError("NaN and Infinity are not JSON")


def parse_engine_kafka(document: object) -> EngineKafkaConnection:
    """Validate an already-decoded document against `engine-kafka/v0` (strict; see the module doc)."""
    root = _object(document, "$", _ROOT_KEYS)
    for key in sorted(_ROOT_KEYS):
        if key not in root:
            raise _invalid("$", f"required property {key} is missing")
    if root["apiVersion"] != API_VERSION:
        raise _invalid("$.apiVersion", f"must be {API_VERSION}")
    if root["kind"] != KIND:
        raise _invalid("$.kind", f"must be {KIND}")

    servers = _bootstrap_servers(root["bootstrapServers"])
    security = _object(root["security"], "$.security", _SECURITY_KEYS)
    protocol = security.get("protocol")
    if "protocol" not in security:
        raise _invalid("$.security", "required property protocol is missing")
    if protocol not in _PROTOCOLS:
        raise _invalid("$.security.protocol", "must be SASL_SSL, SSL or PLAINTEXT")

    mechanism = security.get("saslMechanism")
    if "saslMechanism" in security and mechanism != SCRAM_SHA_512:
        raise _invalid("$.security.saslMechanism", "must be SCRAM-SHA-512")
    secret_ref = _secret_ref(security, "$.security")
    tls_secret_ref: str | None = None
    if "tls" in security:
        tls = _object(security["tls"], "$.security.tls", _TLS_KEYS)
        tls_secret_ref = _secret_ref(tls, "$.security.tls")

    if protocol == "SASL_SSL":
        if mechanism is None or secret_ref is None:
            raise _invalid("$.security", "SASL_SSL requires saslMechanism and secretRef")
    elif mechanism is not None:
        raise _invalid("$.security", "saslMechanism is only valid with SASL_SSL")
    if protocol == "SSL" and secret_ref is None:
        raise _invalid("$.security", "SSL requires secretRef naming the client certificate (tls.crt, tls.key)")
    if protocol == "PLAINTEXT":
        if "secretRef" in security or "tls" in security:
            raise _invalid("$.security", "PLAINTEXT carries no secretRef or tls")
        for index, entry in enumerate(servers):
            if not is_loopback_host_port(entry):
                raise _invalid(
                    f"$.bootstrapServers[{index}]",
                    "PLAINTEXT is allowed only when every bootstrap server is loopback (127.0.0.0/8, [::1], "
                    "localhost); a Compose or Kubernetes service name is not. There is no opt-out",
                )
    return EngineKafkaConnection(
        bootstrap_servers=servers,
        protocol=protocol,
        sasl_mechanism=mechanism,
        secret_ref=secret_ref,
        tls_secret_ref=tls_secret_ref,
    )


def engine_kafka_settings(connection: EngineKafkaConnection, resolver: SecretResolver) -> KafkaSettings:
    """Resolve `connection`'s secrets and build validated `KafkaSettings` (section 4 rules 2-4)."""
    security: dict[str, Any] = {"security.protocol": connection.protocol}
    try:
        if connection.protocol != "PLAINTEXT":
            security["enable.ssl.certificate.verification"] = True
            security["ssl.endpoint.identification.algorithm"] = "https"
            security["ssl.cipher.suites"] = PINNED_CIPHER_SUITES
            if connection.tls_secret_ref is not None:
                security["ssl.ca.location"] = str(resolver.resolve_file(connection.tls_secret_ref, "ca.crt"))
            if connection.secret_ref is None:  # parse_engine_kafka requires it; fail closed regardless
                raise EngineKafkaError(f"{connection.protocol} has no secretRef", code="ENGINE_KAFKA_DOCUMENT_INVALID")
            if connection.protocol == "SASL_SSL":
                security["sasl.mechanism"] = SCRAM_SHA_512
                security["sasl.username"] = resolver.resolve_text(connection.secret_ref, "username")
                security["sasl.password"] = resolver.resolve_text(connection.secret_ref, "password")
            else:
                security["ssl.certificate.location"] = str(resolver.resolve_file(connection.secret_ref, "tls.crt"))
                security["ssl.key.location"] = str(resolver.resolve_file(connection.secret_ref, "tls.key"))
    except SecretResolutionError as exc:
        raise EngineKafkaError(f"cannot resolve the engine Kafka credentials: {exc}", code=_SECRET_UNRESOLVED) from None
    except OSError as exc:  # a caller-supplied resolver may raise FileNotFoundError and friends
        raise EngineKafkaError(
            f"cannot resolve the engine Kafka credentials ({type(exc).__name__}); "
            "check the secretRef directories under TB_SECRETS_DIR",
            code=_SECRET_UNRESOLVED,
        ) from None
    try:
        return KafkaSettings(bootstrap_servers=",".join(connection.bootstrap_servers), security=security)
    except KafkaSecurityError as exc:  # e.g. a resolver that returned an empty password
        raise EngineKafkaError(f"engine Kafka link refused: {exc}", code="ENGINE_KAFKA_LINK_REFUSED") from None


def _invalid(path: str, rule: str) -> EngineKafkaError:
    return EngineKafkaError(f"engine-kafka document invalid at {path}: {rule}")


def _object(value: object, path: str, allowed: frozenset[str]) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise _invalid(path, "must be an object")
    unknown = sum(1 for key in value if key not in allowed)
    if unknown:
        # The unknown names are not echoed: a mistyped field could be a pasted credential.
        raise _invalid(path, f"{unknown} unknown field(s); allowed: {', '.join(sorted(allowed))}")
    return value


def _secret_ref(block: Mapping[str, Any], path: str) -> str | None:
    if "secretRef" not in block:
        return None
    value = block["secretRef"]
    if not isinstance(value, str) or _SECRET_REF.fullmatch(value) is None:
        raise _invalid(f"{path}.secretRef", "must match ^[a-z][a-z0-9-]{0,63}$")
    return value


def _bootstrap_servers(value: object) -> tuple[str, ...]:
    path = "$.bootstrapServers"
    if not isinstance(value, list) or not value:
        raise _invalid(path, "must be a non-empty array")
    if len(value) > MAX_BOOTSTRAP_SERVERS:
        raise _invalid(path, f"at most {MAX_BOOTSTRAP_SERVERS} entries")
    if not all(isinstance(entry, str) for entry in value):
        raise _invalid(path, "every entry must be a string")
    if len(set(value)) != len(value):
        raise _invalid(path, "entries must be unique")
    for index, entry in enumerate(value):
        try:
            if "," in entry:  # one item, one server: a comma would smuggle a second past the item check
                raise KafkaSecurityError("comma")
            split_bootstrap(entry)
        except KafkaSecurityError:
            raise _invalid(
                f"{path}[{index}]", "must be host:port (no scheme or comma; IPv6 only as [::1]; port 1-65535)"
            ) from None
    return tuple(value)
