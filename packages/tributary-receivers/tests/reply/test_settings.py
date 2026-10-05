"""`source.reply` as settings: defaults, bounds, the refusals the receiver can hold, and which changes are live."""

from __future__ import annotations

import copy
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from tributary.receivers.errors import ConfigError
from tributary.receivers.mllp.config import mllp_settings_from_channel_spec, mllp_settings_from_spec
from tributary.receivers.mllp.reload import live_fields_changed, restart_required_fields
from tributary.receivers.reply.settings import ReplySettings
from tributary.runtime.spec import ChannelSpec

EXAMPLES = Path(__file__).resolve().parents[4] / "contracts" / "channel-spec" / "v0" / "examples" / "valid"


def skeleton(**reply: Any) -> dict[str, Any]:
    doc = json.loads((EXAMPLES / "walking-skeleton-loopback.json").read_text(encoding="utf-8"))
    doc["metadata"]["version"] = 1
    doc["spec"]["source"]["reply"] = reply
    return doc


def test_the_contract_example_is_accepted_with_its_values() -> None:
    doc = json.loads((EXAMPLES / "lab-query-mllp-sync-reply.json").read_text(encoding="utf-8"))
    doc["spec"]["ordering"] = "none"  # per-key needs an evaluable key; this test is about `reply`
    doc["spec"].pop("partitionKey", None)
    doc["spec"]["source"].pop("tls")
    doc["spec"]["source"]["bind"] = "127.0.0.1"
    cfg = mllp_settings_from_channel_spec(doc, env="dev")
    assert cfg.reply == ReplySettings(timeout_s=10.0, max_pending=10_000, body="ack")


def test_an_empty_reply_block_means_every_default_and_no_block_means_async() -> None:
    assert mllp_settings_from_channel_spec(skeleton(), env="dev").reply == ReplySettings()
    plain = skeleton()
    del plain["spec"]["source"]["reply"]
    assert mllp_settings_from_channel_spec(plain, env="dev").reply is None
    assert ReplySettings().timeout_s == 10.0 and ReplySettings().max_pending == 10_000


def test_the_runtime_spec_path_agrees_with_the_document_path() -> None:
    doc = skeleton(timeoutMs=4000, maxPending=50, body="step")
    from_doc = mllp_settings_from_channel_spec(doc, env="dev")
    from_spec = mllp_settings_from_spec(ChannelSpec.from_document(doc), env="dev")
    assert from_doc.reply == from_spec.reply == ReplySettings(timeout_s=4.0, max_pending=50, body="step")


@pytest.mark.parametrize(
    "reply",
    [
        {"timeoutMs": 999},
        {"timeoutMs": 60_001},
        {"timeoutMs": "10000"},
        {"timeoutMs": True},
        {"maxPending": 0},
        {"maxPending": 50_001},
        {"body": "both"},
        {"after": "delivered"},
    ],
)
def test_out_of_bound_or_unsupported_values_are_refused(reply: dict[str, Any]) -> None:
    with pytest.raises(ConfigError):
        mllp_settings_from_channel_spec(skeleton(**reply), env="dev")


def test_partitions_is_accepted_and_ignored_since_the_receiver_never_creates_the_topic() -> None:
    assert mllp_settings_from_channel_spec(skeleton(partitions=5), env="dev").reply == ReplySettings()


def test_a_transactional_pipeline_is_refused_in_both_paths() -> None:
    doc = skeleton()
    doc["spec"]["pipeline"]["transactional"] = True
    with pytest.raises(ConfigError, match="transactional"):
        mllp_settings_from_channel_spec(doc, env="dev")
    with pytest.raises(ConfigError, match="transactional"):
        mllp_settings_from_spec(ChannelSpec.from_document(doc), env="dev")


@pytest.mark.parametrize("where", ["pipeline", "destination"])
def test_a_split_step_is_refused_in_both_paths(where: str) -> None:
    doc = skeleton()
    splitter = {"name": "fan-out", "uses": "tributary.core.split", "with": {"on": "segment", "segment": "OBX"}}
    if where == "pipeline":
        doc["spec"]["pipeline"]["steps"].append(splitter)
    else:
        doc["spec"]["destinations"][0]["steps"] = [splitter]
    with pytest.raises(ConfigError, match="splitter"):
        mllp_settings_from_channel_spec(doc, env="dev")
    with pytest.raises(ConfigError, match="splitter"):
        mllp_settings_from_spec(ChannelSpec.from_document(doc), env="dev")


def test_a_split_step_is_fine_on_an_async_channel() -> None:
    doc = skeleton()
    del doc["spec"]["source"]["reply"]
    doc["spec"]["pipeline"]["steps"].append({"name": "fan-out", "uses": "tributary.core.split", "with": {}})
    assert mllp_settings_from_channel_spec(doc, env="dev").reply is None


def test_reply_values_change_live_but_presence_needs_a_restart() -> None:
    base = mllp_settings_from_channel_spec(skeleton(), env="dev")
    tuned = replace(base, reply=ReplySettings(timeout_s=3.0, max_pending=5, body="step"))
    assert restart_required_fields(base, tuned) == []
    assert live_fields_changed(base, tuned) == ["reply"]
    off = replace(base, reply=None)
    assert restart_required_fields(base, off) == ["reply"]
    assert restart_required_fields(off, base) == ["reply"]
    assert "reply" not in live_fields_changed(base, off)


def test_the_spec_is_not_mutated_by_parsing() -> None:
    doc = skeleton(timeoutMs=2000)
    before = copy.deepcopy(doc)
    mllp_settings_from_channel_spec(doc, env="dev")
    assert doc == before
