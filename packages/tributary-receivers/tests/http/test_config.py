"""`http_settings_from_channel_spec`: accept the contract's own http example, refuse (loudly, at
startup) everything the receiver cannot honour. No Kafka."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.errors import BindError, ConfigError
from tributary.receivers.http.config import HttpSettings, http_settings_from_channel_spec
from tributary.receivers.http.server import HttpServer

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load() -> dict[str, Any]:
    return json.loads((EXAMPLES / "http-fhir-ingest-to-fhir-and-postgresql.json").read_text(encoding="utf-8"))


def build(spec: dict[str, Any]) -> HttpSettings:
    return http_settings_from_channel_spec(spec, env="dev")


def test_contract_example_is_accepted() -> None:
    cfg = build(load())
    assert cfg.path == "/fhir/ingest"
    assert cfg.auth.mode == "bearer"
    assert cfg.tls is not None and cfg.tls.min_version == "1.3"
    assert cfg.key_expression is not None  # the example is ordering: per-key
    assert cfg.nack_on == frozenset({"parse-error", "queue-full"})


def test_secret_ref_is_not_in_repr() -> None:
    assert "fhir-ingest-token" not in repr(build(load()).auth)


def test_empty_tls_object_is_configured_not_absent() -> None:
    spec = load()
    spec["spec"]["source"]["tls"] = {}
    # `tls: {}` must never be read as "no tls": it is a malformed tls block and is refused as such.
    with pytest.raises(ConfigError, match="secretRef"):
        build(spec)


def test_wildcard_bind_without_tls_is_refused_by_the_server(tmp_path: Path) -> None:
    spec = load()
    del spec["spec"]["source"]["tls"]
    cfg = build(spec)
    with pytest.raises(BindError):
        HttpServer(cfg, object())  # type: ignore[arg-type]


def test_auth_none_on_a_non_loopback_bind_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["auth"] = {"type": "none"}
    with pytest.raises(ConfigError, match="loopback"):
        build(spec)


def test_mtls_auth_requires_client_auth_required() -> None:
    spec = load()
    spec["spec"]["source"]["auth"] = {"type": "mtls"}
    with pytest.raises(ConfigError, match="clientAuth"):
        build(spec)
    spec["spec"]["source"]["tls"]["clientAuth"] = "required"
    assert build(spec).auth.mode == "mtls"


def test_spec_invalid_nack_on_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["ack"]["nackOn"] = ["spec-invalid"]
    with pytest.raises(ConfigError, match="spec-invalid"):
        build(spec)


def test_ack_mode_other_than_after_commit_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["ack"]["mode"] = "on-receive"
    with pytest.raises(ConfigError, match="after-commit"):
        build(spec)


def test_oversize_limit_above_inline_limit_is_refused() -> None:
    spec = load()
    spec["spec"]["source"]["maxMessageBytes"] = 1_000_001
    with pytest.raises(ConfigError, match="inline limit"):
        build(spec)


def test_max_message_above_queue_budget_is_refused() -> None:
    spec = copy.deepcopy(load())
    spec["spec"]["source"]["queue"] = {"maxBytes": 1000}
    with pytest.raises(ConfigError, match=r"queue\.maxBytes"):
        build(spec)


def test_key_scheme_that_cannot_apply_is_refused_at_startup() -> None:
    spec = load()
    spec["spec"]["partitionKey"] = "record:key"  # a Kafka-only scheme
    with pytest.raises(ConfigError):
        build(spec)


def test_missing_partition_key_on_per_key_is_refused() -> None:
    spec = load()
    del spec["spec"]["partitionKey"]
    with pytest.raises(ConfigError, match="partitionKey"):
        build(spec)


def test_ordering_none_with_a_key_is_refused() -> None:
    spec = load()
    spec["spec"]["ordering"] = "none"
    with pytest.raises(ConfigError, match="forbidden"):
        build(spec)


def test_sync_reply_is_refused_because_http_does_not_implement_it() -> None:
    """`source.reply` is schema-valid on http, but only MLLP answers synchronously: accepting it and answering
    `202` would tell the sender its message was routed when only the commit is known."""
    spec = load()
    spec["spec"]["source"]["reply"] = {}
    with pytest.raises(ConfigError, match=r"source\.reply is not supported by the http receiver"):
        build(spec)
