"""`@splitter`: fan-out as its own node type, with deterministic child ids (README 3.3.2)."""

from __future__ import annotations

import hashlib
import struct
import uuid
from collections.abc import Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st
from tributary.sdk import Drop, Message, Reject, Splitter, StepReturnError, splitter, step
from tributary.sdk import headers as h
from tributary.sdk.codes import SPLIT_TOO_LARGE
from tributary.sdk.ids import derived_uuid7, uuid7
from tributary.sdk.step import MAX_SPLIT_CHILDREN
from tributary.sdk.testing import make_message, run_pipeline, run_splitter


@splitter
def by_line(msg: Message) -> list[Message]:
    return [msg.with_payload(line) for line in (msg.payload or b"").split(b"\n") if line]


def parent(payload: bytes = b"one\ntwo\nthree", **kwargs: object) -> Message:
    return make_message(payload, key=b"SYNTH-KEY-9", correlation_id=str(uuid.uuid4()), **kwargs)  # type: ignore[arg-type]


def test_children_carry_parent_id_parent_key_and_inherited_identity() -> None:
    source = parent()
    children = run_splitter(by_line, source)
    assert isinstance(children, tuple)
    assert [child.payload for child in children] == [b"one", b"two", b"three"]
    for index, child in enumerate(children):
        assert child.msg_id == derived_uuid7(source.msg_id, index)
        assert child.parent_id == source.msg_id
        assert child.key == source.key, "children carry the parent's key (split rule 2)"
        assert child.received_at == source.received_at
        assert child.correlation_id == source.correlation_id
        assert child.attempt == 0
        assert child.traceparent is not None and child.traceparent.split("-")[1] == source.traceparent.split("-")[1]  # type: ignore[union-attr]


def test_splitting_the_same_parent_twice_yields_the_same_ids() -> None:
    """Split rule 1: a redelivered parent reproduces its children, so loader idempotency absorbs them."""
    source = parent()
    first = run_splitter(by_line, source)
    second = run_splitter(by_line, source)
    assert isinstance(first, tuple) and isinstance(second, tuple)
    assert [child.msg_id for child in first] == [child.msg_id for child in second]
    assert len({child.msg_id for child in first}) == 3


@given(st.integers(min_value=0, max_value=MAX_SPLIT_CHILDREN))
def test_derived_ids_are_valid_uuid7_with_the_parent_timestamp(index: int) -> None:
    parent_id = uuid7()
    child = uuid.UUID(derived_uuid7(parent_id, index))
    assert child.version == 7
    assert child.variant == uuid.RFC_4122
    assert child.int >> 80 == uuid.UUID(parent_id).int >> 80
    assert derived_uuid7(parent_id, index) == str(child)
    assert str(child) != parent_id


def _independent_derivation(parent_id: str, index: int) -> str:
    """README 3.3.3 written out as bit strings, sharing no code with `derived_uuid7`.

    Bits are counted from the most significant: rand_a = D[0:12], D[12:18] discarded, rand_b = D[18:80].
    """
    parent = uuid.UUID(parent_id)
    digest = hashlib.sha256(parent.bytes + struct.pack(">I", index)).digest()[:10]
    d_bits = "".join(f"{byte:08b}" for byte in digest)
    parent_bits = f"{parent.int:0128b}"
    child_bits = parent_bits[:48] + "0111" + d_bits[0:12] + "10" + d_bits[18:80]
    assert len(child_bits) == 128
    return str(uuid.UUID(int=int(child_bits, 2)))


VECTORS = [
    (0, "01923a7b-4c5d-7a07-9a78-dd094f20ee20"),
    (1, "01923a7b-4c5d-7075-b061-620e49e95a83"),
    (9999, "01923a7b-4c5d-780f-9067-11c550358874"),
]


@pytest.mark.parametrize(("index", "expected"), VECTORS)
def test_the_readme_3_3_3_vectors_are_recomputed_independently(index: int, expected: str) -> None:
    """The architect asked for an independent recomputation: the table was produced by this SDK."""
    parent_id = "01923a7b-4c5d-7e6f-8a9b-0c1d2e3f4a5b"
    assert _independent_derivation(parent_id, index) == expected
    assert derived_uuid7(parent_id, index) == expected


@given(st.integers(min_value=0, max_value=MAX_SPLIT_CHILDREN))
def test_the_implementation_matches_the_independent_derivation(index: int) -> None:
    parent_id = uuid7()
    assert derived_uuid7(parent_id, index) == _independent_derivation(parent_id, index)


@pytest.mark.parametrize("bad", ["not-a-uuid", str(uuid.uuid4())])
def test_a_derived_id_needs_a_uuid7_parent(bad: str) -> None:
    with pytest.raises(ValueError):
        derived_uuid7(bad, 0)


def test_an_empty_split_is_a_drop_not_a_failure() -> None:
    outcome = run_splitter(by_line, parent(b""))
    assert outcome == Drop(reason="EMPTY_SPLIT")


def test_more_than_the_cap_is_rejected_without_draining_the_generator() -> None:
    pulled = 0

    @splitter
    def endless(msg: Message) -> Iterator[Message]:
        nonlocal pulled
        while True:
            pulled += 1
            yield msg

    with pytest.raises(Reject) as caught:
        run_splitter(endless, parent())
    assert caught.value.code == SPLIT_TOO_LARGE
    assert pulled == MAX_SPLIT_CHILDREN + 1


def test_exactly_the_cap_is_allowed() -> None:
    @splitter
    def many(msg: Message) -> Iterator[Message]:
        return (msg for _ in range(MAX_SPLIT_CHILDREN))

    children = run_splitter(many, parent())
    assert isinstance(children, tuple) and len(children) == MAX_SPLIT_CHILDREN


@pytest.mark.parametrize("returned", [None, "text", b"bytes", {"a": 1}, 7])
def test_a_splitter_that_returns_a_non_iterable_or_a_scalar_is_a_typed_error(returned: object) -> None:
    @splitter
    def wrong(msg: Message) -> object:
        return returned

    with pytest.raises(StepReturnError):
        run_splitter(wrong, parent())


def test_a_splitter_returning_one_message_is_told_to_be_a_step() -> None:
    @splitter
    def single(msg: Message) -> Message:
        return msg

    with pytest.raises(StepReturnError, match="@step"):
        run_splitter(single, parent())


def test_a_non_message_child_is_a_typed_error_that_does_not_render_it() -> None:
    @splitter
    def leaky(msg: Message) -> list[object]:
        return [b"SYNTH^JANE"]

    with pytest.raises(StepReturnError) as caught:
        run_splitter(leaky, parent())
    assert "SYNTH" not in str(caught.value)


def test_a_splitter_is_not_a_step_and_run_pipeline_says_where_to_go() -> None:
    @step
    def noop(msg: Message) -> None:
        return None

    assert isinstance(by_line, Splitter)
    with pytest.raises(TypeError, match="run_splitter"):
        run_pipeline([noop, by_line], parent())  # type: ignore[list-item]


def test_a_splitter_binds_with_like_a_step() -> None:
    @splitter
    def by_separator(msg: Message, *, separator: str) -> list[Message]:
        return [msg.with_payload(part) for part in (msg.payload or b"").split(separator.encode())]

    children = run_splitter(by_separator, parent(b"a|b"), with_={"separator": "|"})
    assert isinstance(children, tuple) and [child.payload for child in children] == [b"a", b"b"]


def test_a_child_built_from_scratch_still_gets_the_parent_s_envelope() -> None:
    @splitter
    def from_scratch(msg: Message) -> list[Message]:
        return [Message(headers={h.TB_CONTENT_TYPE: "text/plain"}, payload=b"x")]

    source = parent()
    children = run_splitter(from_scratch, source)
    assert isinstance(children, tuple)
    child = children[0]
    assert child.channel == source.channel
    assert child.content_type == "text/plain", "headers the splitter set are kept"
    assert child.source == source.source
