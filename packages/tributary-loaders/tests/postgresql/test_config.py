"""`postgresql_destination_config_from_destination`: refuses what this loader cannot honour (wrong
type, unsupported idempotency technique or auth type, a malformed identifier, `tls.mode: disable` off
loopback) rather than silently ignoring it. Fixtures come from `contracts/channel-spec/v0/examples`
where one exists; the rest build `Destination` objects directly, matching
`tests/http/test_handler.py::make_destination`."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.errors import ConfigError
from tributary.loaders.postgresql.config import postgresql_destination_config_from_destination
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load_example(name: str, destination_name: str) -> Destination:
    spec = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    spec["metadata"].setdefault("version", 0)
    return ChannelSpec.from_document(spec).destination(destination_name)


def make_destination(**overrides: object) -> Destination:
    base: dict[str, object] = {
        "name": "warehouse",
        "type": "postgresql",
        "params": {
            "host": "db.example.org",
            "port": 5432,
            "database": "clinical",
            "table": "observations",
            "idColumn": "tb_msg_id",
            "auth": {"type": "password", "secretRef": "results-db"},
            "tls": {"mode": "verify-full", "secretRef": "results-db-ca"},
            "timeoutMs": 5000,
        },
        "idempotency": Idempotency(technique="sink-native"),
        "retry": RetryPolicy(policy="blocking", max_attempts=5, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def test_builds_config_from_the_published_example() -> None:
    destination = load_example("http-fhir-ingest-to-fhir-and-postgresql.json", "results-db")
    cfg = postgresql_destination_config_from_destination("fhir-observation-ingest", destination)
    assert cfg.host == "db.example.org"
    assert cfg.port == 5432
    assert cfg.database == "clinical"
    assert cfg.table == "staging.observations"
    assert cfg.id_column == "tb_msg_id"
    assert cfg.auth.type == "password"
    assert cfg.auth.secret_ref == "results-db"  # noqa: S105 - a secretRef name, not a credential value
    assert cfg.tls.mode == "verify-full"
    assert cfg.timeout_s == 5.0
    assert cfg.idempotency_technique == "sink-native"


def test_default_port_and_id_column() -> None:
    params = {
        "host": "db.example.org",
        "database": "clinical",
        "table": "observations",
        "auth": {"type": "password", "secretRef": "results-db"},
        "tls": {"mode": "verify-full"},
        "timeoutMs": 5000,
    }
    cfg = postgresql_destination_config_from_destination("c", make_destination(params=params))
    assert cfg.port == 5432
    assert cfg.id_column == "tb_msg_id"


def test_schema_qualified_table_is_accepted() -> None:
    cfg = postgresql_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "host": "db.example.org",
                "database": "clinical",
                "table": "staging.observations",
                "auth": {"type": "password", "secretRef": "results-db"},
                "tls": {"mode": "verify-full"},
                "timeoutMs": 5000,
            }
        ),
    )
    assert cfg.table == "staging.observations"


def test_wrong_type_is_refused() -> None:
    with pytest.raises(ConfigError, match="not 'postgresql'"):
        postgresql_destination_config_from_destination("c", make_destination(type="http"))


def test_idempotency_key_header_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        postgresql_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="idempotency-key-header", header="X-Id"))
        )


def test_idempotency_none_is_refused_even_with_a_note() -> None:
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        postgresql_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="none", note="dedupe not needed"))
        )


def test_unsupported_auth_type_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"auth\.type"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "db.example.org",
                    "database": "clinical",
                    "table": "observations",
                    "auth": {"type": "trust", "secretRef": "results-db"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_client_certificate_auth_needs_tls() -> None:
    with pytest.raises(ConfigError, match="client-certificate"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "127.0.0.1",
                    "database": "clinical",
                    "table": "observations",
                    "auth": {"type": "client-certificate", "secretRef": "results-db-cert"},
                    "tls": {"mode": "disable"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_tls_disable_is_refused_for_a_remote_host() -> None:
    with pytest.raises(ConfigError, match="loopback"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "db.example.org",
                    "database": "clinical",
                    "table": "observations",
                    "auth": {"type": "password", "secretRef": "results-db"},
                    "tls": {"mode": "disable"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_tls_disable_is_accepted_for_loopback() -> None:
    cfg = postgresql_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "host": "127.0.0.1",
                "database": "clinical",
                "table": "observations",
                "auth": {"type": "password", "secretRef": "results-db"},
                "tls": {"mode": "disable"},
                "timeoutMs": 5000,
            }
        ),
    )
    assert cfg.tls.mode == "disable"


@pytest.mark.parametrize(
    "bad_table",
    ["Observations", "1observations", "observations;drop table x", "a.b.c", "observations ", "'observations'"],
)
def test_malformed_table_identifier_is_refused(bad_table: str) -> None:
    with pytest.raises(ConfigError, match="table"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "db.example.org",
                    "database": "clinical",
                    "table": bad_table,
                    "auth": {"type": "password", "secretRef": "results-db"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_malformed_id_column_is_refused() -> None:
    with pytest.raises(ConfigError, match="idColumn"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "db.example.org",
                    "database": "clinical",
                    "table": "observations",
                    "idColumn": "Tb-Msg-Id",
                    "auth": {"type": "password", "secretRef": "results-db"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_missing_host_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="host"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "database": "clinical",
                    "table": "observations",
                    "auth": {"type": "password", "secretRef": "results-db"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_missing_table_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="table"):
        postgresql_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "db.example.org",
                    "database": "clinical",
                    "auth": {"type": "password", "secretRef": "results-db"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_circuit_breaker_is_optional_and_parsed_when_present() -> None:
    cfg = postgresql_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "host": "db.example.org",
                "database": "clinical",
                "table": "observations",
                "auth": {"type": "password", "secretRef": "results-db"},
                "tls": {"mode": "verify-full"},
                "timeoutMs": 5000,
                "circuitBreaker": {"failureThreshold": 4, "openSeconds": 20},
            }
        ),
    )
    assert cfg.circuit_breaker is not None
    assert cfg.circuit_breaker.failure_threshold == 4
    assert cfg.circuit_breaker.open_seconds == 20.0
