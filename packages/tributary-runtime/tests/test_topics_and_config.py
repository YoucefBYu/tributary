"""Topic naming and the contract-sourced defaults.

The defaults tests carry a mutation control: the loader is pointed at a copy of the contract with
different numbers and must return those. Asserting only that `partitions == 3` would pass just as
happily against a hardcoded 3, which is the exact failure PHASE-0-DECISION section 8 item 1 warns
about — the planning value of 12 survived in the plan after S4 disproved it.
"""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from tributary.runtime import topics
from tributary.runtime.config import find_contracts_dir, load_topic_defaults
from tributary.runtime.errors import ConfigError, TopicNameError

CONTRACTS = Path(__file__).resolve().parents[3] / "contracts"
NAMES = CONTRACTS / "topics" / "v0" / "examples" / "topic-names.json"


def names() -> dict[str, list[object]]:
    assert NAMES.is_file(), "topic-name examples not found: the suite would pass vacuously"
    return json.loads(NAMES.read_text(encoding="utf-8"))


def test_valid_data_plane_names_round_trip() -> None:
    for name in names()["valid"]:
        assert isinstance(name, str)
        if name.startswith("_tb."):
            continue
        parsed = topics.parse_data_topic(name)
        rebuilt = topics.data_topic(parsed.env, parsed.channel, parsed.kind, parsed.destination)
        assert rebuilt == name


def test_every_invalid_example_is_refused() -> None:
    for entry in names()["invalid"]:
        assert isinstance(entry, dict)
        with pytest.raises(TopicNameError):
            topics.parse_data_topic(str(entry["name"]))


def test_constructors_match_the_grammar() -> None:
    assert topics.raw_topic("prod", "adt-to-fhir") == "prod.adt-to-fhir.raw"
    assert topics.out_topic("prod", "adt-to-fhir", "fhir-server") == "prod.adt-to-fhir.out.fhir-server"
    assert topics.dlq_topic("dev", "a") == "dev.a.dlq"
    assert topics.channels_topic("prod") == "_tb.prod.channels"
    assert topics.events_topic("prod") == "_tb.prod.events"


def test_out_requires_a_destination_and_others_refuse_one() -> None:
    with pytest.raises(TopicNameError):
        topics.data_topic("prod", "ch", "out")
    with pytest.raises(TopicNameError):
        topics.data_topic("prod", "ch", "raw", "fhir-server")


def test_consumer_group_naming() -> None:
    assert topics.consumer_group("prod", "adt-to-fhir", "pipeline") == "tb.prod.adt-to-fhir.pipeline"
    assert (
        topics.consumer_group("prod", "adt-to-fhir", "loader", "fhir-server")
        == "tb.prod.adt-to-fhir.loader.fhir-server"
    )
    assert topics.consumer_group("prod", "a", "pipeline", shadow="v2") == "tb.prod.a.pipeline.shadow-v2"


def test_defaults_come_from_the_contract_file() -> None:
    defaults = load_topic_defaults()
    assert defaults.source == CONTRACTS / "topics" / "v0" / "topic-defaults.json"
    assert defaults.partitions_for("raw") == 3
    assert defaults.consumer.session_timeout_ms == 15000
    assert defaults.consumer.heartbeat_interval_ms == 5000
    assert defaults.consumer.enable_auto_commit is False
    assert defaults.consumer.partition_assignment_strategy == "cooperative-sticky"
    assert defaults.producer["acks"] == "all"
    assert defaults.producer["enable.idempotence"] is True


def test_the_loader_reads_the_file_rather_than_hardcoding_the_measured_numbers(tmp_path: Path) -> None:
    """Mutation control: change the contract copy, and the loaded values must change with it."""
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["classes"]["raw"]["partitions"] = 12
    document["clients"]["worker_consumer"]["session.timeout.ms"] = 9000
    document["clients"]["worker_consumer"]["heartbeat.interval.ms"] = 3000
    document["clients"]["worker_consumer"]["commit_every"] = 25
    path.write_text(json.dumps(document), encoding="utf-8")

    defaults = load_topic_defaults(root)
    assert defaults.partitions_for("raw") == 12
    assert defaults.consumer.session_timeout_ms == 9000
    assert defaults.consumer.commit_every == 25


def test_commit_every_is_read_from_the_contract_and_has_no_fallback(tmp_path: Path) -> None:
    """S5 measured 50. A prose or absent value is a refusal to start, not an invented number:
    commit_every is the replay window after an unclean death, so guessing it changes how many
    duplicates a destination with `idempotency.technique: none` actually sees."""
    assert load_topic_defaults().consumer.commit_every == 50

    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["clients"]["worker_consumer"]["commit_every"] = "tunable per destination"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_topic_defaults(root)


def test_out_partitions_resolve_to_the_raw_count() -> None:
    defaults = load_topic_defaults()
    assert defaults.partitions_for("out", raw_partitions=6) == 6
    with pytest.raises(ConfigError):
        defaults.partitions_for("out")


def test_a_heartbeat_that_cannot_keep_a_session_alive_is_refused(tmp_path: Path) -> None:
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["clients"]["worker_consumer"]["heartbeat.interval.ms"] = 14000
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_topic_defaults(root)


def test_a_missing_contracts_directory_is_a_refusal_to_start(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        find_contracts_dir(tmp_path)
