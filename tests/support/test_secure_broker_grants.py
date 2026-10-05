"""The receiver grant table the W3-23 integration tests apply, pinned to `contracts/topics/v0` 3.4.1 as written.

`contract_grants` parses `security.acls` with the lite stack's own parser, so the integration tests follow the
contract. This test is the independent reading: the table from 3.4.1 stated by hand, so that a contract edit or
a parser change that widens or narrows what a receiver may do fails here, in a fast test, before it reaches a
broker. No Docker needed.
"""

from __future__ import annotations

from tests.support.secure_broker import contract_grants, sync_reply_receiver_grants

ENV, CHANNEL = "dev", "adt"


def test_receiver_grants_are_the_set_1_4_table() -> None:
    got = {g.as_tuple() for g in contract_grants("receiver", env=ENV, channel=CHANNEL)}
    assert got == {
        ("TOPIC", "dev.adt.raw", "LITERAL", "WRITE"),
        ("TOPIC", "dev.adt.raw", "LITERAL", "DESCRIBE"),
        ("CLUSTER", "kafka-cluster", "LITERAL", "IDEMPOTENT_WRITE"),
        ("TOPIC", "_tb.dev.channels", "LITERAL", "READ"),
        ("GROUP", "tb.dev.adt.channels.", "PREFIXED", "READ"),
    }


def test_sync_reply_additions_read_reply_and_never_write_it() -> None:
    got = {g.as_tuple() for g in sync_reply_receiver_grants(ENV, CHANNEL)}
    assert got == {
        ("TOPIC", "dev.adt.reply", "LITERAL", "READ"),
        ("GROUP", "tb.dev.adt.replier", "LITERAL", "READ"),
    }
    assert not any(g[3] == "WRITE" and g[1].endswith(".reply") for g in got)


def test_no_receiver_grant_is_cluster_wide_write_or_covers_the_spec_topic_for_writing() -> None:
    for g in contract_grants("receiver", env=ENV, channel=CHANNEL):
        assert not (g.resource_type == "CLUSTER" and g.operation in {"WRITE", "DELETE", "ALTER", "ALL"})
        assert not (g.name == "_tb.dev.channels" and g.operation in {"WRITE", "ALL"})
