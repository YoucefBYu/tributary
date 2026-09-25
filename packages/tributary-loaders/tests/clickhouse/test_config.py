"""`clickhouse_destination_config_from_destination`: refuses what this loader cannot honour, mirroring
`tests/postgresql/test_config.py` (same `sqlSinkParams` shape, same rules)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.loaders.clickhouse.config import clickhouse_destination_config_from_destination
from tributary.loaders.errors import ConfigError
from tributary.runtime.spec import ChannelSpec, Destination, Idempotency, RetryPolicy

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load_example(name: str, destination_name: str) -> Destination:
    spec = json.loads((EXAMPLES / name).read_text(encoding="utf-8"))
    spec["metadata"].setdefault("version", 0)
    return ChannelSpec.from_document(spec).destination(destination_name)


def make_destination(**overrides: object) -> Destination:
    base: dict[str, object] = {
        "name": "adt-analytics",
        "type": "clickhouse",
        "params": {
            "host": "clickhouse.example.org",
            "port": 8443,
            "database": "analytics",
            "table": "adt_events",
            "idColumn": "tb_msg_id",
            "auth": {"type": "password", "secretRef": "clickhouse-adt"},
            "tls": {"mode": "verify-full", "secretRef": "clickhouse-ca"},
            "timeoutMs": 5000,
        },
        "idempotency": Idempotency(technique="sink-native"),
        "retry": RetryPolicy(policy="blocking", max_attempts=20, on_exhaust="halt-partition"),
    }
    base.update(overrides)
    return Destination(**base)  # type: ignore[arg-type]


def test_builds_config_from_the_published_example() -> None:
    destination = load_example("adt-mllp-to-fhir-clickhouse-mllp.json", "adt-analytics")
    cfg = clickhouse_destination_config_from_destination("adt-to-fhir-and-analytics", destination)
    assert cfg.host == "clickhouse.example.org"
    assert cfg.port == 8443
    assert cfg.database == "analytics"
    assert cfg.table == "adt_events"
    assert cfg.id_column == "tb_msg_id"
    assert cfg.auth.type == "password"
    assert cfg.tls.mode == "verify-full"
    assert cfg.timeout_s == 5.0
    assert cfg.idempotency_technique == "sink-native"


def test_default_port_and_id_column() -> None:
    params = {
        "host": "clickhouse.example.org",
        "database": "analytics",
        "table": "adt_events",
        "auth": {"type": "password", "secretRef": "clickhouse-adt"},
        "tls": {"mode": "verify-full"},
        "timeoutMs": 5000,
    }
    cfg = clickhouse_destination_config_from_destination("c", make_destination(params=params))
    assert cfg.port == 8123
    assert cfg.id_column == "tb_msg_id"


def test_wrong_type_is_refused() -> None:
    with pytest.raises(ConfigError, match="not 'clickhouse'"):
        clickhouse_destination_config_from_destination("c", make_destination(type="http"))


def test_idempotency_key_header_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        clickhouse_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="idempotency-key-header", header="X-Id"))
        )


def test_idempotency_none_is_refused_even_with_a_note() -> None:
    with pytest.raises(ConfigError, match=r"idempotency\.technique"):
        clickhouse_destination_config_from_destination(
            "c", make_destination(idempotency=Idempotency(technique="none", note="dedupe not needed"))
        )


def test_unsupported_auth_type_is_refused() -> None:
    with pytest.raises(ConfigError, match=r"auth\.type"):
        clickhouse_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "clickhouse.example.org",
                    "database": "analytics",
                    "table": "adt_events",
                    "auth": {"type": "trust", "secretRef": "clickhouse-adt"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_client_certificate_auth_needs_tls() -> None:
    with pytest.raises(ConfigError, match="client-certificate"):
        clickhouse_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "127.0.0.1",
                    "database": "analytics",
                    "table": "adt_events",
                    "auth": {"type": "client-certificate", "secretRef": "clickhouse-cert"},
                    "tls": {"mode": "disable"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_tls_disable_is_refused_for_a_remote_host() -> None:
    with pytest.raises(ConfigError, match="loopback"):
        clickhouse_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "clickhouse.example.org",
                    "database": "analytics",
                    "table": "adt_events",
                    "auth": {"type": "password", "secretRef": "clickhouse-adt"},
                    "tls": {"mode": "disable"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_tls_disable_is_accepted_for_loopback() -> None:
    cfg = clickhouse_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "host": "127.0.0.1",
                "database": "analytics",
                "table": "adt_events",
                "auth": {"type": "password", "secretRef": "clickhouse-adt"},
                "tls": {"mode": "disable"},
                "timeoutMs": 5000,
            }
        ),
    )
    assert cfg.tls.mode == "disable"


@pytest.mark.parametrize(
    "bad_table",
    ["Adt_Events", "1adt_events", "adt_events;drop table x", "a.b.c", "adt_events ", "'adt_events'"],
)
def test_malformed_table_identifier_is_refused(bad_table: str) -> None:
    with pytest.raises(ConfigError, match="table"):
        clickhouse_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "clickhouse.example.org",
                    "database": "analytics",
                    "table": bad_table,
                    "auth": {"type": "password", "secretRef": "clickhouse-adt"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_malformed_id_column_is_refused() -> None:
    with pytest.raises(ConfigError, match="idColumn"):
        clickhouse_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "host": "clickhouse.example.org",
                    "database": "analytics",
                    "table": "adt_events",
                    "idColumn": "Tb-Msg-Id",
                    "auth": {"type": "password", "secretRef": "clickhouse-adt"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_missing_host_is_a_config_error() -> None:
    with pytest.raises(ConfigError, match="host"):
        clickhouse_destination_config_from_destination(
            "c",
            make_destination(
                params={
                    "database": "analytics",
                    "table": "adt_events",
                    "auth": {"type": "password", "secretRef": "clickhouse-adt"},
                    "tls": {"mode": "verify-full"},
                    "timeoutMs": 5000,
                }
            ),
        )


def test_circuit_breaker_is_optional_and_parsed_when_present() -> None:
    cfg = clickhouse_destination_config_from_destination(
        "c",
        make_destination(
            params={
                "host": "clickhouse.example.org",
                "database": "analytics",
                "table": "adt_events",
                "auth": {"type": "password", "secretRef": "clickhouse-adt"},
                "tls": {"mode": "verify-full"},
                "timeoutMs": 5000,
                "circuitBreaker": {"failureThreshold": 4, "openSeconds": 20},
            }
        ),
    )
    assert cfg.circuit_breaker is not None
    assert cfg.circuit_breaker.failure_threshold == 4
    assert cfg.circuit_breaker.open_seconds == 20.0
