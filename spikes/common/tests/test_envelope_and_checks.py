"""Fast tests for the shared spike helpers. No Kafka required."""

from __future__ import annotations

import uuid

from hypothesis import given
from hypothesis import strategies as st

from tbcommon import checks, envelope, payloads


def test_uuid7_is_a_valid_v7_uuid_and_time_ordered() -> None:
    first = uuid.UUID(envelope.uuid7())
    second = uuid.UUID(envelope.uuid7())
    assert first.version == 7
    assert first.variant == uuid.RFC_4122
    # Same millisecond is possible, so only require non-decreasing.
    assert second.int >= (first.int >> 80) << 80


def test_traceparent_child_keeps_trace_id_and_changes_span_id() -> None:
    parent = envelope.new_traceparent()
    child = envelope.child_traceparent(parent)
    assert child.split("-")[1] == parent.split("-")[1]
    assert child.split("-")[2] != parent.split("-")[2]
    assert child.split("-")[3] == parent.split("-")[3]


def test_child_traceparent_of_garbage_is_a_fresh_traceparent() -> None:
    assert envelope.child_traceparent("nonsense").count("-") == 3
    assert envelope.child_traceparent(None).count("-") == 3


@given(
    seq=st.integers(min_value=0, max_value=1_000_000),
    attempt=st.integers(min_value=0, max_value=255),
    correlation=st.one_of(st.none(), st.text(min_size=1, max_size=40)),
)
def test_envelope_headers_round_trip(seq: int, attempt: int, correlation: str | None) -> None:
    original = envelope.Envelope(
        channel="spike", seq=seq, attempt=attempt, correlation_id=correlation
    )
    restored = envelope.Envelope.from_headers(original.to_headers())
    assert restored.msg_id == original.msg_id
    assert restored.seq == seq
    assert restored.attempt == attempt
    assert restored.correlation_id == correlation
    assert restored.traceparent == original.traceparent


def test_next_stage_preserves_msg_id_and_trace_id() -> None:
    first = envelope.Envelope(channel="spike", seq=0, stage="raw")
    second = first.next_stage("out.fhir")
    assert second.msg_id == first.msg_id
    assert second.stage == "out.fhir"
    assert second.traceparent.split("-")[1] == first.traceparent.split("-")[1]


def _obs(key: str, seq: int, msg_id: str | None = None) -> checks.Observation:
    return checks.Observation(key=key, seq=seq, msg_id=msg_id or f"{key}#{seq}")


def test_in_order_delivery_is_clean() -> None:
    observed = [_obs("k", i) for i in range(10)]
    report = checks.check(observed, expected=observed)
    assert report.order_preserved
    assert report.no_loss
    assert report.duplicates == 0


def test_replay_of_the_same_seq_counts_as_duplicate_not_reordering() -> None:
    observed = [_obs("k", 0), _obs("k", 1), _obs("k", 0), _obs("k", 2)]
    report = checks.check(observed)
    assert report.duplicates == 1
    assert report.order_preserved


def test_genuine_reordering_is_detected() -> None:
    observed = [_obs("k", 0), _obs("k", 2), _obs("k", 1)]
    report = checks.check(observed)
    assert not report.order_preserved
    assert report.reordered_keys["k"] == [(2, 1)]


def test_interleaving_across_keys_is_not_reordering() -> None:
    observed = [_obs("a", 0), _obs("b", 0), _obs("a", 1), _obs("b", 1)]
    assert checks.check(observed).order_preserved


def test_loss_is_reported_against_expected() -> None:
    expected = [_obs("k", i) for i in range(5)]
    report = checks.check(expected[:3], expected=expected)
    assert not report.no_loss
    assert len(report.lost) == 2
    assert checks.gaps_per_key(expected[:3]) == {}


def test_gaps_per_key_finds_holes() -> None:
    assert checks.gaps_per_key([_obs("k", 0), _obs("k", 3)]) == {"k": [1, 2]}


@given(count=st.integers(min_value=1, max_value=200), keys=st.integers(min_value=1, max_value=20))
def test_payload_stream_is_monotonic_per_key_and_synthetic(count: int, keys: int) -> None:
    seen: dict[str, int] = {}
    for key, seq, payload in payloads.stream(count, keys, seed=7):
        assert seq == seen.get(key, 0)
        seen[key] = seq + 1
        assert payload.startswith(b"MSH|^~\\&|TRIBUTARY-SYNTH|")


def test_payload_stream_is_deterministic_for_a_seed() -> None:
    assert payloads.stream(50, 5, seed=3) == payloads.stream(50, 5, seed=3)
