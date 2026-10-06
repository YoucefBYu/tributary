"""Every topic-defaults value the lite stack uses must come from the contract file, never a literal
here (architect, `contracts/topics/v0/topic-defaults.json` `must_not_hardcode`, acceptance:
"devops-engineer ... keep a test asserting the component's effective configuration equals the
value here"). These tests load the real committed file and assert equality against it field by
field, so a change to the contract either updates these assertions or breaks them -- it cannot
silently drift from what `bootstrap_topics.py` actually creates.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# `topic-bootstrap` has a hyphen, so it cannot be a Python package (mypy: "not a valid Python package
# name"); a bare `sys.path` entry makes `bootstrap_topics` importable as a top-level module instead,
# matching the pattern already used for `.github/scripts`.
sys.path.insert(0, str(Path(__file__).parent))
from bootstrap_topics import data_topic_name, derive_topics, load_defaults, platform_topic_name

CONTRACT_PATH = Path(__file__).resolve().parents[3] / "contracts" / "topics" / "v0" / "topic-defaults.json"


@pytest.fixture
def defaults() -> dict:  # type: ignore[type-arg]
    return load_defaults(CONTRACT_PATH)


def test_topic_names_match_the_naming_grammar(defaults: dict) -> None:  # type: ignore[type-arg]
    topics = derive_topics(defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",))
    names = {t.name for t in topics}
    assert names == {
        "dev.adt-passthrough.raw",
        "dev.adt-passthrough.dlq",
        "dev.adt-passthrough.retry",
        "dev.adt-passthrough.out.http-stub",
        "_tb.dev.channels",
        "_tb.dev.events",
        "_tb.dev.audit",
    }


def test_audit_topic_is_created_with_the_contract_buffer_config(defaults: dict) -> None:  # type: ignore[type-arg]
    # ADR-004 R7 / set 1.8: `_tb.<env>.audit` is a 30 d transport buffer; every value comes from the contract.
    topics = {
        t.name: t
        for t in derive_topics(
            defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",)
        )
    }
    audit = topics[platform_topic_name("dev", "audit")]
    assert audit.partitions == defaults["classes"]["audit"]["partitions"]
    assert audit.configs["retention.ms"] == str(defaults["classes"]["audit"]["configs"]["retention.ms"])


def test_reply_topic_is_never_created_for_a_non_sync_reply_channel(defaults: dict) -> None:  # type: ignore[type-arg]
    topics = derive_topics(defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",))
    assert data_topic_name("dev", "adt-passthrough", "reply") not in {t.name for t in topics}


@pytest.mark.parametrize("profile", ["lite", "production"])
def test_replication_factor_and_min_insync_replicas_come_from_the_contract(defaults: dict, profile: str) -> None:  # type: ignore[type-arg]
    topics = derive_topics(defaults, env="dev", profile=profile, channel="adt-passthrough", destinations=("http-stub",))
    expected_rf = defaults["profiles"][profile]["replication_factor"]
    expected_isr = str(
        defaults["profiles"][profile]["class_config_overrides"].get(
            "min.insync.replicas", defaults["durability_floor"]["min.insync.replicas"]
        )
    )
    for t in topics:
        assert t.replication_factor == expected_rf
        assert t.configs["min.insync.replicas"] == expected_isr


def test_out_and_retry_partitions_match_raw_per_the_contract_constraint(defaults: dict) -> None:  # type: ignore[type-arg]
    topics = {
        t.name: t
        for t in derive_topics(
            defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",)
        )
    }
    raw_partitions = topics["dev.adt-passthrough.raw"].partitions
    assert topics["dev.adt-passthrough.out.http-stub"].partitions == raw_partitions
    assert topics["dev.adt-passthrough.retry"].partitions == raw_partitions


def test_dlq_and_platform_topic_partitions_match_the_contract_exactly(defaults: dict) -> None:  # type: ignore[type-arg]
    topics = {
        t.name: t
        for t in derive_topics(
            defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",)
        )
    }
    assert topics["dev.adt-passthrough.dlq"].partitions == defaults["classes"]["dlq"]["partitions"]
    assert topics["_tb.dev.channels"].partitions == defaults["classes"]["channels"]["partitions"]
    assert topics["_tb.dev.events"].partitions == defaults["classes"]["events"]["partitions"]


def test_every_class_config_value_matches_the_contract_after_the_profile_override(defaults: dict) -> None:  # type: ignore[type-arg]
    topics = {
        t.name: t
        for t in derive_topics(
            defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",)
        )
    }
    raw = topics["dev.adt-passthrough.raw"].configs
    contract_raw = defaults["classes"]["raw"]["configs"]
    overrides = defaults["profiles"]["lite"]["class_config_overrides"]
    for key, value in contract_raw.items():
        expected = overrides.get(key, value)
        assert raw[key] == str(expected)


def test_unclean_leader_election_is_always_pinned_false(defaults: dict) -> None:  # type: ignore[type-arg]
    # Invariant 1: this is topic-level, so a missing value silently inherits a broker default.
    topics = derive_topics(defaults, env="dev", profile="lite", channel="adt-passthrough", destinations=("http-stub",))
    for t in topics:
        assert t.configs["unclean.leader.election.enable"] == "false"


# --- ACLs (ADR-008 D5): parsed from the contract's security.acls, deny by default otherwise ----------

from bootstrap_topics import check_forbidden, derive_acls, parse_grants, parse_principals  # noqa: E402

ENV, CHANNEL, DESTS = "dev", "adt-passthrough", ("http-stub",)


def _grants(defaults: dict, role: str) -> set[tuple[str, str, str, str]]:  # type: ignore[type-arg]
    return {
        (g.resource_type, g.name, g.pattern, g.operation)
        for g in parse_grants(defaults["security"]["acls"][role], env=ENV, channel=CHANNEL, destinations=DESTS)
    }


def test_receiver_may_write_raw_only(defaults: dict) -> None:  # type: ignore[type-arg]
    # Set 1.4: READ on the channels topic and its group prefix, ahead of the receiver's invariant-3 migration.
    assert _grants(defaults, "receiver") == {
        ("TOPIC", "dev.adt-passthrough.raw", "LITERAL", "WRITE"),
        ("TOPIC", "dev.adt-passthrough.raw", "LITERAL", "DESCRIBE"),
        ("CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"),
        ("TOPIC", "_tb.dev.channels", "LITERAL", "READ"),
        ("GROUP", "tb.dev.adt-passthrough.channels.", "PREFIXED", "READ"),
    }


def test_worker_grants_and_no_write_on_channels(defaults: dict) -> None:  # type: ignore[type-arg]
    g = _grants(defaults, "pipeline_worker")
    assert ("TOPIC", "dev.adt-passthrough.raw", "LITERAL", "READ") in g
    assert ("TOPIC", "dev.adt-passthrough.out.", "PREFIXED", "WRITE") in g
    assert ("TOPIC", "dev.adt-passthrough.dlq", "LITERAL", "WRITE") in g
    assert ("TOPIC", "_tb.dev.events", "LITERAL", "WRITE") in g
    assert ("TOPIC", "_tb.dev.channels", "LITERAL", "READ") in g
    # Set 1.4: group grants are delimited, and the unused `.retry` grants are gone.
    assert ("GROUP", "tb.dev.adt-passthrough.pipeline", "LITERAL", "READ") in g
    assert ("GROUP", "tb.dev.adt-passthrough.pipeline.shadow-", "PREFIXED", "READ") in g
    assert ("GROUP", "tb.dev.adt-passthrough.channels.", "PREFIXED", "READ") in g
    assert not any(n.endswith(".retry") for _, n, _, _ in g)
    # A compromised worker must not be able to deploy a channel spec (topics README 3.4).
    assert not any(n == "_tb.dev.channels" and op == "WRITE" for _, n, _, op in g)
    assert not any(op in {"DELETE", "ALTER"} for *_, op in g)


def test_loader_grants_are_per_destination(defaults: dict) -> None:  # type: ignore[type-arg]
    g = _grants(defaults, "loader")
    assert ("TOPIC", "dev.adt-passthrough.out.http-stub", "LITERAL", "READ") in g
    assert ("TOPIC", "dev.adt-passthrough.dlq", "LITERAL", "WRITE") in g
    assert ("TOPIC", "_tb.dev.channels", "LITERAL", "READ") in g
    assert ("GROUP", "tb.dev.adt-passthrough.loader.http-stub", "LITERAL", "READ") in g
    assert ("GROUP", "tb.dev.adt-passthrough.channels.", "PREFIXED", "READ") in g
    # Nothing on retry (retry-topic is not in v0), nothing writable on the channels topic.
    assert not any(n.endswith(".retry") for _, n, _, _ in g)
    assert not any(n == "_tb.dev.channels" and op == "WRITE" for _, n, _, op in g)
    # A second destination gets its own group, and never a prefix that reaches the first's.
    other = {
        (x.name, x.pattern)
        for x in parse_grants(defaults["security"]["acls"]["loader"], env=ENV, channel=CHANNEL, destinations=("fhir",))
    }
    assert ("tb.dev.adt-passthrough.loader.fhir", "LITERAL") in other
    assert ("tb.dev.adt-passthrough.loader.http-stub", "LITERAL") not in other


def test_transactional_channels_fail_loudly_not_silently(defaults: dict) -> None:  # type: ignore[type-arg]
    """Set 1.4 keeps the TRANSACTIONAL_ID grant out of `pipeline_worker` because this parser has no such
    target. A transactional channel must therefore be refused here, never bootstrapped without it (the
    worker would fail at init_transactions with TRANSACTIONAL_ID_AUTHORIZATION_FAILED)."""
    assert "pipeline_worker_transactional" in defaults["security"]["acls"]
    with pytest.raises(ValueError, match="transactional"):
        derive_acls(
            defaults, env=ENV, channel=CHANNEL, destinations=DESTS, principals={"worker": "User:w"}, transactional=True
        )


def test_control_plane_stand_in_writes_channels(defaults: dict) -> None:  # type: ignore[type-arg]
    g = _grants(defaults, "control_plane")
    assert ("TOPIC", "_tb.dev.channels", "LITERAL", "WRITE") in g
    assert ("CLUSTER", "kafka-cluster", "LITERAL", "DESCRIBE") in g


def test_sync_reply_additions_are_not_granted(defaults: dict) -> None:  # type: ignore[type-arg]
    for role in ("receiver", "pipeline_worker"):
        assert not any(n.endswith(".reply") or "replier" in n for _, n, _, _ in _grants(defaults, role))


def test_unrecognised_clause_fails_loudly() -> None:
    with pytest.raises(ValueError, match="unrecognised"):
        parse_grants("permit everything to everyone", env=ENV, channel=CHANNEL, destinations=DESTS)
    with pytest.raises(ValueError, match="placeholder"):
        parse_grants("READ on <env>.<channel>.<mystery>", env=ENV, channel=CHANNEL, destinations=DESTS)


def test_every_component_role_exists_in_the_contract(defaults: dict) -> None:  # type: ignore[type-arg]
    acls = derive_acls(
        defaults,
        principals=parse_principals("receiver=r,worker=w,loader=l,channel-publish=c"),
        env=ENV,
        channel=CHANNEL,
        destinations=DESTS,
    )
    assert {p for p, _ in acls} == {"User:r", "User:w", "User:l", "User:c"}


def test_destination_group_grant_does_not_cover_a_sibling_destination() -> None:
    """`loader.<dest>*` with no delimiter made a grant for `fhir` also cover `fhir-backup`."""
    grants = parse_grants(
        "READ on group tb.<env>.<channel>.loader.<dest>*", env=ENV, channel=CHANNEL, destinations=("fhir",)
    )
    assert {(g.name, g.pattern) for g in grants} == {
        ("tb.dev.adt-passthrough.loader.fhir", "LITERAL"),
        ("tb.dev.adt-passthrough.loader.fhir.", "PREFIXED"),
    }
    sibling = "tb.dev.adt-passthrough.loader.fhir-backup"
    assert not any(g.name == sibling or (g.pattern == "PREFIXED" and sibling.startswith(g.name)) for g in grants)


def test_extra_sentence_that_is_not_a_known_form_raises() -> None:
    """A dropped restriction becomes an unconditional grant, so an unknown sentence is an error."""
    with pytest.raises(ValueError, match="sentence"):
        parse_grants("READ on <env>.<channel>.raw. Only from 10.0.0.0/8", env=ENV, channel=CHANNEL, destinations=DESTS)


def test_sync_reply_sentence_is_recognised_and_not_granted() -> None:
    g = parse_grants(
        "READ on <env>.<channel>.raw. Set 1.2, sync-reply channels only: WRITE on <env>.<channel>.reply",
        env=ENV,
        channel=CHANNEL,
        destinations=DESTS,
    )
    assert [(x.name, x.operation) for x in g] == [("dev.adt-passthrough.raw", "READ")]


def test_prohibition_sentence_is_enforced_against_the_grants() -> None:
    ok = parse_grants(
        "WRITE on <env>.<channel>.raw. No WRITE on reply, so nobody forges",
        env=ENV,
        channel=CHANNEL,
        destinations=DESTS,
    )
    assert ok
    with pytest.raises(ValueError, match="prohibits"):
        parse_grants(
            "WRITE on <env>.<channel>.reply. No WRITE on reply, so nobody forges",
            env=ENV,
            channel=CHANNEL,
            destinations=DESTS,
        )
    with pytest.raises(ValueError, match="prohibits"):  # a prefix grant that reaches it
        parse_grants("ALL on <env>.<channel>.*. No WRITE on reply", env=ENV, channel=CHANNEL, destinations=DESTS)


def test_unknown_operation_raises() -> None:
    with pytest.raises(ValueError, match="operation"):
        parse_grants("SUPERPOWER on the cluster", env=ENV, channel=CHANNEL, destinations=DESTS)


@pytest.mark.parametrize(
    "text", ["ALL on the cluster", "WRITE on the cluster", "ALTER on the cluster", "DELETE on the cluster"]
)
def test_forbidden_cluster_wide_grants_are_refused_for_data_plane(text: str) -> None:
    grants = parse_grants(text, env=ENV, channel=CHANNEL, destinations=DESTS)
    for component in ("receiver", "worker", "loader"):
        with pytest.raises(ValueError, match="forbidden"):
            check_forbidden(component, grants, env=ENV)
    check_forbidden("channel-publish", grants, env=ENV)  # the control-plane stand-in is not data plane


@pytest.mark.parametrize(
    "text", ["WRITE on _tb.<env>.channels", "ALL on _tb.<env>.channels", "WRITE on _tb.*", "WRITE on *"]
)
def test_write_on_channels_is_refused_for_data_plane(text: str) -> None:
    grants = parse_grants(text, env=ENV, channel=CHANNEL, destinations=DESTS)
    with pytest.raises(ValueError, match="forbidden"):
        check_forbidden("worker", grants, env=ENV)


def test_derive_acls_applies_the_forbidden_check(defaults: dict) -> None:  # type: ignore[type-arg]
    bad = {
        **defaults,
        "security": {
            **defaults["security"],
            "acls": {**defaults["security"]["acls"], "pipeline_worker": "WRITE on _tb.<env>.channels"},
        },
    }
    with pytest.raises(ValueError, match="forbidden"):
        derive_acls(bad, env=ENV, channel=CHANNEL, destinations=DESTS, principals={"worker": "User:w"})


def test_the_contract_still_states_what_check_forbidden_enforces(defaults: dict) -> None:  # type: ignore[type-arg]
    text = defaults["security"]["acls"]["forbidden"]
    assert all(word in text for word in ("WRITE", "DELETE", "ALTER", "_tb.<env>.channels"))


@pytest.mark.parametrize(
    "text", ["receiver=*", "receiver=User:*", "receiver=", "nobody=x", "receiver=a,worker=a", "receiver=a b"]
)
def test_bad_principals_are_refused(text: str) -> None:
    with pytest.raises(ValueError):
        parse_principals(text)


def test_derive_acls_refuses_a_shared_principal(defaults: dict) -> None:  # type: ignore[type-arg]
    with pytest.raises(ValueError, match="one principal"):
        derive_acls(
            defaults,
            env=ENV,
            channel=CHANNEL,
            destinations=DESTS,
            principals={"receiver": "User:x", "worker": "User:x"},
        )
