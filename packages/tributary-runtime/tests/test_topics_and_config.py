"""Topic naming and the contract-sourced defaults.

The defaults tests carry a mutation control: the loader is pointed at a copy of the contract with
different numbers and must return those. Asserting only that `partitions == 3` would pass just as
happily against a hardcoded 3, which is the exact failure PHASE-0-DECISION section 8 item 1 warns
about — the planning value of 12 survived in the plan after S4 disproved it.
"""

from __future__ import annotations

import json
import re
import shutil
from pathlib import Path
from typing import Any, cast, get_args

import pytest
from jsonschema import Draft202012Validator
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


def _consumer_group_schema() -> Any:
    schema = json.loads((CONTRACTS / "topics" / "v0" / "topic-name.schema.json").read_text(encoding="utf-8"))
    return Draft202012Validator({**schema["$defs"]["consumerGroup"], "$defs": schema["$defs"]})


def test_roles_and_platform_groups_match_the_contract() -> None:
    """Set 1.8: `archiver` is a platform group, not a per-channel role. The schema's role alternation and
    `consumer_groups.platform_groups` are the source; the type aliases must not drift from them."""
    pattern = json.loads((CONTRACTS / "topics" / "v0" / "topic-name.schema.json").read_text(encoding="utf-8"))["$defs"][
        "consumerGroup"
    ]["anyOf"][0]["pattern"]
    match = re.search(r"\\\.\(([a-z|]+)\)", pattern)
    assert match is not None, "role alternation not found in the consumerGroup pattern"
    assert set(get_args(topics.Role)) == set(match.group(1).split("|"))
    platform = json.loads((CONTRACTS / "topics" / "v0" / "topic-defaults.json").read_text(encoding="utf-8"))[
        "consumer_groups"
    ]["platform_groups"]
    assert {k: v for k, v in platform.items() if not k.startswith("$")} == {
        kind: f"tb.<env>.{kind}" for kind in get_args(topics.PlatformGroup)
    }
    assert "archiver" not in get_args(topics.Role)


def test_archiver_is_refused_as_a_per_channel_role() -> None:
    # A role that arrives untyped (configuration, the pre-1.8 call) must not build `tb.<env>.<ch>.archiver`,
    # a group no ACL grants, nor anything a PREFIXED `tb.<env>.archiver` grant would once have covered.
    with pytest.raises(TopicNameError):
        topics.consumer_group("prod", "adt", cast(Any, "archiver"))
    with pytest.raises(TopicNameError):
        topics.consumer_group("prod", "adt", cast(Any, "pipeline.x"))


def test_archiver_platform_group_is_the_literal_contract_name() -> None:
    validator = _consumer_group_schema()
    group = topics.platform_group("prod", "archiver")
    assert group == "tb.prod.archiver"
    assert validator.is_valid(group)
    with pytest.raises(TopicNameError):
        topics.platform_group("Prod", "archiver")
    with pytest.raises(TopicNameError):
        topics.platform_group("prod", cast(Any, "pipeline"))
    # The literal grant cannot reach a channel named `archiver`: its groups have more segments.
    assert topics.consumer_group("prod", "archiver", "pipeline") != group
    assert validator.is_valid(topics.consumer_group("prod", "archiver", "pipeline"))


@pytest.mark.parametrize("kind", get_args(topics.PlatformGroup))
def test_every_platform_group_builds_a_contract_valid_literal_name(kind: str) -> None:
    # Set 1.8 added `reconciler` (the API's spec reconciler); each kind must build `tb.<env>.<kind>`.
    group = topics.platform_group("prod", cast(Any, kind))
    assert group == f"tb.prod.{kind}"
    assert _consumer_group_schema().is_valid(group)


@pytest.mark.parametrize(
    ("role", "destination", "shadow"),
    [
        ("pipeline", None, None),
        ("pipeline", None, "v2"),
        ("pipeline", None, "a" + "b" * 31),
        ("loader", "fhir-server", None),
        ("replier", None, None),
    ],
)
def test_every_built_consumer_group_validates_against_the_schema(
    role: topics.Role, destination: str | None, shadow: str | None
) -> None:
    assert _consumer_group_schema().is_valid(topics.consumer_group("prod", "adt", role, destination, shadow))


@pytest.mark.parametrize("shadow", ["a" + "b" * 32, "V2", "", "-x", "a.b"])
def test_a_shadow_label_the_schema_would_refuse_is_refused(shadow: str) -> None:
    with pytest.raises(TopicNameError):
        topics.consumer_group("prod", "adt", "pipeline", shadow=shadow)


def test_spec_reader_group_matches_the_contract_pattern_and_acl_prefix() -> None:
    group = topics.spec_reader_group("dev", "adt-passthrough", "pipeline-1")
    assert group == "tb.dev.adt-passthrough.channels.pipeline-1"
    spec_reader = json.loads((CONTRACTS / "topics" / "v0" / "topic-defaults.json").read_text())["consumer_groups"][
        "spec_reader"
    ]
    assert spec_reader["pattern"] == "tb.<env>.<channel>.channels.<instance>"
    # The ACL is PREFIXED on `tb.<env>.<ch>.channels.` with the trailing delimiter: every group we build
    # must sit strictly under it, so another channel's prefix can never match.
    assert group.startswith("tb.dev.adt-passthrough.channels.") and len(group) > len("tb.dev.adt-passthrough.channels.")
    assert not topics.spec_reader_group("dev", "adt", "p-1").startswith("tb.dev.adt-passthrough.channels.")


def test_spec_reader_group_is_reexported_beside_the_reader() -> None:
    from tributary.runtime import channels

    assert channels.spec_reader_group is topics.spec_reader_group


@pytest.mark.parametrize(
    ("env", "channel", "instance"),
    [
        ("dev", "adt", ""),
        ("dev", "adt", " pipeline-1"),
        ("dev", "adt", "pipeline 1"),
        ("dev", "adt", "pipe\nline"),
        ("dev", "adt", "x" * 129),
        ("dev", "a.b", "pipeline-1"),
        ("Dev", "adt", "pipeline-1"),
    ],
)
def test_spec_reader_group_rejects_bad_segments(env: str, channel: str, instance: str) -> None:
    with pytest.raises(TopicNameError):
        topics.spec_reader_group(env, channel, instance)


def test_spec_reader_group_accepts_the_instance_ids_processes_default_to() -> None:
    for instance in ("pipeline-4242", "loader-http-stub-4242", "reply-it-lab-query", "worker-2.pod:0@node"):
        assert topics.spec_reader_group("prod", "lab-query", instance).endswith(f".channels.{instance}")


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


# -- the lite profile's mandatory startup warning (topics/v0 README 3.8) ---------------------------


def test_the_lite_profile_requires_a_startup_warning_and_production_does_not() -> None:
    """The acceptance criterion is two-sided on purpose: a warning that fires for every profile is a
    warning an operator learns to ignore, which is the same as no warning."""
    defaults = load_topic_defaults()
    assert defaults.startup_warning("lite") is not None
    assert defaults.startup_warning("production") is None
    assert defaults.startup_warning("nonexistent") is None


def test_the_warning_text_is_the_contract_s_own_not_a_paraphrase() -> None:
    """README 3.8 says to emit the warning in `profiles.lite.warning`. A reworded copy would drift from
    the sentence a human approved as the exception to invariant 1's durability promise (ADR-002 D14)."""
    document = json.loads((CONTRACTS / "topics" / "v0" / "topic-defaults.json").read_text(encoding="utf-8"))
    assert load_topic_defaults().startup_warning("lite") == document["profiles"]["lite"]["warning"]


def test_lite_is_barred_from_production_use() -> None:
    defaults = load_topic_defaults()
    assert defaults.barred_from_production("lite") is True
    assert defaults.barred_from_production("production") is False


def test_a_profile_whose_warning_is_not_required_stays_silent(tmp_path: Path) -> None:
    """Mutation control on the flag rather than the profile name: the runtime keys off
    `startup_warning_required`, so flipping it in a copy of the contract must silence the warning —
    proving the emit path reads the contract instead of hardcoding "lite"."""
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["profiles"]["lite"]["startup_warning_required"] = False
    path.write_text(json.dumps(document), encoding="utf-8")

    assert load_topic_defaults(root).startup_warning("lite") is None


def test_a_contract_without_profiles_is_a_refusal_to_start(tmp_path: Path) -> None:
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    del document["profiles"]
    path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ConfigError):
        load_topic_defaults(root)


def test_kafka_settings_repr_never_shows_the_security_block() -> None:
    """Receivers security review: `security` carries SCRAM passwords, and a repr reaches tracebacks."""
    import dataclasses

    from tributary.runtime.clients import KafkaSettings

    settings = KafkaSettings(
        bootstrap_servers="broker:9093",
        security={
            "security.protocol": "SASL_SSL",
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
            "ssl.cipher.suites": "ECDHE+AESGCM:ECDHE+CHACHA20",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": "tb-synthetic-user",
            "sasl.password": "SYNTH-SECRET-MARKER",
        },
    )
    assert "SYNTH-SECRET-MARKER" not in repr(settings)
    assert "SYNTH-SECRET-MARKER" not in str(settings)
    assert "SYNTH-SECRET-MARKER" not in repr(settings.security)
    assert "SYNTH-SECRET-MARKER" not in repr(dataclasses.asdict(settings))
    assert "broker:9093" in repr(settings)


# --- clients.reply_consumer (set 1.7, topics README 3.3.1 and 3.7) ---------------------------------

REPLY_KEYS = (
    "session.timeout.ms",
    "heartbeat.interval.ms",
    "auto.offset.reset",
    "isolation.level",
    "partition.assignment.strategy",
    "enable.auto.commit",
)


def _contract_clients() -> dict[str, dict[str, object]]:
    document = json.loads((CONTRACTS / "topics" / "v0" / "topic-defaults.json").read_text(encoding="utf-8"))
    return document["clients"]  # type: ignore[no-any-return]


def _reply_config(root: Path | None = None) -> dict[str, object]:
    from tributary.runtime.clients import KafkaSettings, consumer_config

    return consumer_config(
        KafkaSettings(bootstrap_servers="localhost:9092"),
        load_topic_defaults(root),
        group_id="tb.dev.adt.replier",
        group_instance_id="mllp-0-p1",
        profile="reply_consumer",
    )


def test_reply_consumer_effective_config_equals_the_contract() -> None:
    """3.7 acceptance for the replier: every value the contract types is the one the client gets."""
    contract = _contract_clients()["reply_consumer"]
    config = _reply_config()
    for key in REPLY_KEYS:
        assert config[key] == contract[key], key
    assert config["group.instance.id"] == "mllp-0-p1"
    # Every must_not_hardcode key of this profile is covered by the comparison above.
    document = json.loads((CONTRACTS / "topics" / "v0" / "topic-defaults.json").read_text(encoding="utf-8"))
    pinned = {
        key.removeprefix("clients.reply_consumer.")
        for key in document["must_not_hardcode"]["values"]
        if key.startswith("clients.reply_consumer.")
    }
    assert "isolation.level" in pinned
    assert pinned <= set(REPLY_KEYS)


def test_reply_consumer_values_move_with_the_contract_not_with_the_worker(tmp_path: Path) -> None:
    """Mutation control in both directions: a reply-profile edit moves only the replier, and a
    worker-profile edit moves only the worker (the defect set 1.7 rules out)."""
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["clients"]["reply_consumer"]["session.timeout.ms"] = 12000
    document["clients"]["reply_consumer"]["heartbeat.interval.ms"] = 4000
    document["clients"]["reply_consumer"]["auto.offset.reset"] = "earliest"
    document["clients"]["reply_consumer"]["isolation.level"] = "read_committed"
    document["clients"]["worker_consumer"]["session.timeout.ms"] = 9000
    document["clients"]["worker_consumer"]["heartbeat.interval.ms"] = 3000
    path.write_text(json.dumps(document), encoding="utf-8")

    config = _reply_config(root)
    assert config["session.timeout.ms"] == 12000
    assert config["heartbeat.interval.ms"] == 4000
    assert config["auto.offset.reset"] == "earliest"
    assert config["isolation.level"] == "read_committed"
    defaults = load_topic_defaults(root)
    assert defaults.consumer.session_timeout_ms == 9000
    assert defaults.reply_consumer.session_timeout_ms == 12000


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("isolation.level", None),
        ("isolation.level", "read_whatever"),
        ("auto.offset.reset", None),
        ("session.timeout.ms", "15 s"),
        ("heartbeat.interval.ms", 14000),
        ("enable.auto.commit", True),
    ],
)
def test_a_reply_consumer_profile_without_its_typed_values_is_refused(tmp_path: Path, key: str, value: object) -> None:
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    if value is None:
        del document["clients"]["reply_consumer"][key]
    else:
        document["clients"]["reply_consumer"][key] = value
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ConfigError):
        load_topic_defaults(root)


def test_reply_consumer_refuses_an_auto_offset_reset_override() -> None:
    from tributary.runtime.clients import KafkaSettings, consumer_config

    with pytest.raises(ConfigError):
        consumer_config(
            KafkaSettings(bootstrap_servers="localhost:9092"),
            load_topic_defaults(),
            group_id="tb.dev.adt.replier",
            auto_offset_reset="earliest",
            profile="reply_consumer",
        )


def test_worker_profile_is_unchanged_by_the_reply_profile() -> None:
    from tributary.runtime.clients import KafkaSettings, consumer_config

    config = consumer_config(KafkaSettings(bootstrap_servers="localhost:9092"), load_topic_defaults(), group_id="g")
    assert config["auto.offset.reset"] == "earliest"
    assert "isolation.level" not in config
    assert config["session.timeout.ms"] == _contract_clients()["worker_consumer"]["session.timeout.ms"]


# -- clients.worker_producer (set 1.8, README 3.2.1; 3.7 acceptance) ------------------------------------

WORKER_PRODUCER_KEYS = (
    "acks",
    "enable.idempotence",
    "max.in.flight.requests.per.connection",
    "delivery.timeout.ms",
    "linger.ms",
    "compression.type",
)


def _producer_configs(root: Path | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    from tributary.runtime.clients import KafkaSettings, producer_config, worker_producer_config

    settings, defaults = KafkaSettings(bootstrap_servers="localhost:9092"), load_topic_defaults(root)
    return (
        worker_producer_config(settings, defaults, client_id="tb-dev-adt-pipeline-1"),
        producer_config(settings, defaults, client_id="tb-dev-adt-rx"),
    )


def test_worker_producer_effective_config_equals_the_contract() -> None:
    """3.7: every value of `clients.worker_producer` is the one the worker's producer gets; linger is 0."""
    contract = _contract_clients()["worker_producer"]
    worker, receiver = _producer_configs()
    for key in WORKER_PRODUCER_KEYS:
        assert worker[key] == contract[key], key
    assert worker["linger.ms"] == 0
    # The receiver keeps its own block (linger 5, the S2 TODO): only the worker moved.
    assert receiver["linger.ms"] == _contract_clients()["receiver_producer"]["linger.ms"]


def _edited(tmp_path: Path, edit: Any) -> Path:
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS / "topics", root / "topics")
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    edit(document["clients"])
    path.write_text(json.dumps(document), encoding="utf-8")
    return root


def test_worker_producer_values_move_with_their_own_block(tmp_path: Path) -> None:
    def edit(clients: dict[str, Any]) -> None:
        clients["worker_producer"]["linger.ms"] = 2
        clients["worker_producer"]["delivery.timeout.ms"] = 90000
        clients["receiver_producer"]["linger.ms"] = 7

    worker, receiver = _producer_configs(_edited(tmp_path, edit))
    assert (worker["linger.ms"], worker["delivery.timeout.ms"]) == (2, 90000)
    assert receiver["linger.ms"] == 7 and receiver["delivery.timeout.ms"] == 120000


@pytest.mark.parametrize(
    "edit",
    [
        lambda clients: clients.pop("worker_producer"),
        lambda clients: clients["worker_producer"].pop("linger.ms"),
        lambda clients: clients["worker_producer"].update({"acks": 1}),
        lambda clients: clients["worker_producer"].update({"enable.idempotence": False}),
        lambda clients: clients["worker_producer"].update({"max.in.flight.requests.per.connection": 6}),
        lambda clients: clients["worker_producer"].update({"max.in.flight.requests.per.connection": 0}),
    ],
    ids=["block-missing", "key-missing", "acks-1", "not-idempotent", "in-flight-6", "in-flight-0"],
)
def test_a_defective_worker_producer_block_refuses_only_the_worker(tmp_path: Path, edit: Any) -> None:
    """Re-review MED: `load_topic_defaults` is shared, so a stale contract must not stop a receiver, the
    replier or a loader; only the worker's producer config refuses, with a typed error."""
    from tributary.runtime.clients import KafkaSettings, consumer_config, producer_config, worker_producer_config

    defaults = load_topic_defaults(_edited(tmp_path, edit))  # loads for everyone
    settings = KafkaSettings(bootstrap_servers="localhost:9092")
    assert producer_config(settings, defaults, client_id="tb-dev-adt-rx")["acks"] == "all"  # receiver
    consumer_config(settings, defaults, group_id="tb.dev.adt.replier", profile="reply_consumer")  # replier
    consumer_config(settings, defaults, group_id="tb.dev.adt.loader.http")  # loader
    with pytest.raises(ConfigError):
        worker_producer_config(settings, defaults, client_id="tb-dev-adt-pipeline-1")


def test_the_worker_build_refuses_a_contract_without_the_block_before_any_client(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import io

    import confluent_kafka
    from tributary.runtime.clients import KafkaSettings
    from tributary.runtime.logs import JsonLogger
    from tributary.runtime.worker import PipelineWorker, WorkerSettings

    def no_client(*_args: Any, **_kwargs: Any) -> Any:
        raise AssertionError("a Kafka client was built before the contract was checked")

    monkeypatch.setattr(confluent_kafka, "Consumer", no_client)
    monkeypatch.setattr(confluent_kafka, "Producer", no_client)
    root = tmp_path / "contracts"
    shutil.copytree(CONTRACTS, root)  # whole tree: the worker also loads the metrics contract first
    path = root / "topics" / "v0" / "topic-defaults.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    document["clients"].pop("worker_producer")
    path.write_text(json.dumps(document), encoding="utf-8")
    worker = PipelineWorker(
        WorkerSettings(
            env="dev",
            channel="adt",
            kafka=KafkaSettings(bootstrap_servers="localhost:9092"),
            instance_id="pipeline-1",
            contracts_dir=str(root),
        ),
        log=JsonLogger("worker", stream=io.StringIO()),
    )
    with pytest.raises(ConfigError, match="worker_producer is missing"):
        worker.build()
