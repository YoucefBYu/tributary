"""`mllp_settings_from_channel_spec` against the real `contracts/channel-spec/v0` examples: this
package must accept what it can serve and refuse, loudly, what it cannot — never silently drop a
declared behaviour (the "specified and quietly not applied" mistake the Phase 0 gate flagged twice)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from tributary.receivers.errors import ConfigError
from tributary.receivers.mllp.config import mllp_settings_from_channel_spec

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def load(name: str) -> dict:
    return json.loads((EXAMPLES / name).read_text(encoding="utf-8"))


def test_walking_skeleton_loopback_is_accepted() -> None:
    cfg = mllp_settings_from_channel_spec(load("walking-skeleton-loopback.json"), env="dev")
    assert cfg.channel == "adt-passthrough"
    assert cfg.bind == "127.0.0.1"
    assert cfg.tls is None
    assert cfg.nack_on == frozenset({"parse-error", "queue-full"})


def test_metadata_version_defaults_to_zero_for_an_authored_spec() -> None:
    cfg = mllp_settings_from_channel_spec(load("walking-skeleton-loopback.json"), env="dev")
    assert cfg.channel_version == 0


def test_published_spec_carries_its_version() -> None:
    # published-with-version.json is itself ordering: per-key (rejected below), so the version
    # passthrough is checked on the loopback skeleton with a version field added.
    spec = load("walking-skeleton-loopback.json")
    spec["metadata"]["version"] = 7
    cfg = mllp_settings_from_channel_spec(spec, env="dev")
    assert cfg.channel_version == 7


@pytest.mark.parametrize("example", ["adt-to-fhir-per-key-mtls.json", "published-with-version.json"])
def test_per_key_ordering_is_refused_in_v0(example: str) -> None:
    with pytest.raises(ConfigError, match="ordering"):
        mllp_settings_from_channel_spec(load(example), env="dev")


def test_spec_invalid_in_nack_on_is_refused_rather_than_silently_ignored() -> None:
    # two-destinations-fanout.json declares nackOn: [parse-error, spec-invalid, queue-full]; this
    # wave's receiver cannot ever detect spec-invalid (no live _tb.<env>.channels consumption yet),
    # so it must refuse to start rather than accept a promise it cannot keep.
    with pytest.raises(ConfigError, match="spec-invalid"):
        mllp_settings_from_channel_spec(load("two-destinations-fanout.json"), env="dev")


def test_mtls_spec_produces_a_tls_spec_with_required_client_auth() -> None:
    # None of the four valid examples combine ordering: none with tls, so this takes the
    # loopback skeleton's shape and adds the tls object adt-to-fhir-per-key-mtls.json uses.
    spec = load("walking-skeleton-loopback.json")
    spec["spec"]["source"]["tls"] = {"secretRef": "mllp-tls", "clientAuth": "required", "minVersion": "1.3"}
    cfg = mllp_settings_from_channel_spec(spec, env="dev")
    assert cfg.tls is not None
    assert cfg.tls.client_auth == "required"
    assert cfg.tls.min_version == "1.3"


def test_empty_tls_block_is_a_config_error_not_a_silent_none() -> None:
    """Security review: `source.tls: {}` must not be silently read the same as `tls` being absent
    (`config.py`'s truthiness-bug fix) -- it is a malformed `tls` object (missing the required
    `secretRef`/`clientAuth`), not an intentional "no TLS" request, and must fail loudly."""
    spec = load("walking-skeleton-loopback.json")
    spec["spec"]["source"]["tls"] = {}
    with pytest.raises(ConfigError, match="secretRef"):
        mllp_settings_from_channel_spec(spec, env="dev")


def test_queue_max_bytes_defaults_when_queue_object_absent() -> None:
    from tributary.receivers.backpressure import DEFAULT_MAX_BYTES

    cfg = mllp_settings_from_channel_spec(load("walking-skeleton-loopback.json"), env="dev")
    assert cfg.queue_max_bytes == DEFAULT_MAX_BYTES


def test_missing_metadata_name_is_a_config_error() -> None:
    spec = load("walking-skeleton-loopback.json")
    del spec["metadata"]["name"]
    with pytest.raises(ConfigError, match="name"):
        mllp_settings_from_channel_spec(spec, env="dev")


def test_non_mllp_source_type_is_rejected() -> None:
    spec = load("walking-skeleton-loopback.json")
    spec["spec"]["source"]["type"] = "http"
    with pytest.raises(ConfigError, match="mllp"):
        mllp_settings_from_channel_spec(spec, env="dev")


def test_max_message_bytes_bigger_than_queue_max_bytes_is_refused() -> None:
    # a max-size message could never be admitted to a smaller queue budget: a config error caught
    # at startup, not the first time such a message arrives (see backpressure.ByteBudget.reserve).
    spec = load("walking-skeleton-loopback.json")
    spec["spec"]["source"]["maxMessageBytes"] = 2_000_000
    spec["spec"]["source"]["queue"] = {"maxBytes": 1_048_576}
    with pytest.raises(ConfigError, match="maxMessageBytes"):
        mllp_settings_from_channel_spec(spec, env="dev")
