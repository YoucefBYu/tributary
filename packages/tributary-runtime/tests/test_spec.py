"""The channel-spec parse, checked against the contract's own examples.

Every valid example is parsed by the shipping parser, and every example the contract calls invalid for
a reason the runtime also enforces must be refused. The interesting asymmetry is `metadata.version`:
the schema makes it optional because one schema validates the authored file and the published record,
while section 6 rule 1 makes it mandatory on the topic — so the authored examples must be **rejected**
until a version is stamped on them.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from tributary.runtime.errors import ChannelSpecError
from tributary.runtime.spec import DEFAULT_MAX_DELAY_MS, DEFAULT_STEP_RETRIES, ChannelSpec

EXAMPLES = Path(__file__).resolve().parents[3] / "contracts" / "channel-spec" / "v0" / "examples"


def load(relative: str) -> dict[str, Any]:
    path = EXAMPLES / relative
    assert path.is_file(), f"{relative} not found: the suite would pass vacuously"
    document: dict[str, Any] = json.loads(path.read_text(encoding="utf-8"))
    return document


def published(document: dict[str, Any], version: int = 1) -> dict[str, Any]:
    """The authored form plus the version the control plane stamps at publish time."""
    document["metadata"] = {**document["metadata"], "version": version}
    return document


def valid_examples() -> list[str]:
    files = sorted(path.name for path in (EXAMPLES / "valid").glob("*.json"))
    assert files, "no valid examples found"
    return files


@pytest.mark.parametrize("name", valid_examples())
def test_every_valid_example_parses_once_it_is_published(name: str) -> None:
    spec = ChannelSpec.from_document(published(load(f"valid/{name}")))
    assert spec.name and spec.version >= 0
    assert spec.destinations
    assert spec.pipeline.steps


@pytest.mark.parametrize("name", valid_examples())
def test_an_authored_spec_without_a_version_is_refused(name: str) -> None:
    """Contract section 6 rule 1: reject the record and keep the previously applied spec running."""
    document = load(f"valid/{name}")
    document["metadata"].pop("version", None)
    with pytest.raises(ChannelSpecError, match="version"):
        ChannelSpec.from_document(document)


def test_the_walking_skeleton_spec_parses_into_what_the_worker_needs() -> None:
    spec = ChannelSpec.from_document(published(load("valid/walking-skeleton-loopback.json"), version=2))
    assert spec.name == "adt-passthrough"
    assert spec.version == 2
    assert spec.ordering == "none" and spec.ordered is False
    assert spec.partition_key is None
    assert [step.name for step in spec.pipeline.steps] == ["passthrough"]
    assert spec.pipeline.step_retries == DEFAULT_STEP_RETRIES
    assert spec.pipeline.transactional is False

    destination = spec.destination("http-stub")
    assert destination.type == "http"
    assert destination.retry.policy == "blocking"
    assert destination.retry.max_attempts == 5
    assert destination.retry.on_exhaust == "halt-partition"
    assert destination.retry.max_delay_ms == DEFAULT_MAX_DELAY_MS
    assert destination.idempotency.technique == "none" and destination.idempotency.note


def test_a_per_key_channel_carries_its_key_expression() -> None:
    spec = ChannelSpec.from_document(published(load("valid/adt-to-fhir-per-key-mtls.json")))
    assert spec.ordered is True
    assert spec.partition_key == "hl7:PID.3.1"


def test_retry_topic_is_refused_by_the_runtime_as_well_as_the_schema() -> None:
    """Defence in depth: a spec that reached the topic through an older validator must still not run."""
    with pytest.raises(ChannelSpecError, match="retry-topic"):
        ChannelSpec.from_document(published(load("invalid/retry-topic-selected.json")))


@pytest.mark.parametrize(
    ("example", "match"),
    [
        ("invalid/missing-ordering.json", "ordering"),
        ("invalid/per-key-without-partition-key.json", "partitionKey"),
        ("invalid/ordering-none-with-partition-key.json", "partitionKey"),
        ("invalid/step-neither-uses-nor-ref.json", "uses or ref"),
        ("invalid/step-uses-and-ref.json", "uses or ref"),
        ("invalid/idempotency-none-without-note.json", "note"),
        ("invalid/channel-name-not-topic-safe.json", "topic-safe"),
    ],
)
def test_contract_violations_the_runtime_also_enforces_are_refused(example: str, match: str) -> None:
    with pytest.raises(ChannelSpecError, match=match):
        ChannelSpec.from_document(published(load(example)))


def test_unknown_fields_are_ignored_not_rejected() -> None:
    """Contract 1.1: a worker that refuses an additive change takes the data plane down (invariant 3)."""
    document = published(load("valid/walking-skeleton-loopback.json"))
    document["spec"]["somethingNewerControlPlanesSet"] = {"nested": True}
    document["metadata"]["labels"] = {"domain": "adt"}
    document["spec"]["destinations"][0]["futureField"] = 1
    spec = ChannelSpec.from_document(document)
    assert spec.name == "adt-passthrough"


def test_a_non_channel_document_is_refused() -> None:
    with pytest.raises(ChannelSpecError):
        ChannelSpec.from_document({"kind": "Secret", "metadata": {"name": "x", "version": 1}, "spec": {}})
    with pytest.raises(ChannelSpecError):
        ChannelSpec.from_document("not-json-object")


def test_duplicate_destination_names_are_refused() -> None:
    document = published(load("valid/two-destinations-fanout.json"))
    document["spec"]["destinations"][1]["name"] = document["spec"]["destinations"][0]["name"]
    with pytest.raises(ChannelSpecError, match="unique"):
        ChannelSpec.from_document(document)
