"""`ctx.reply` on `FakeContext`: the rules of channel-spec README 3.2.6, with no Kafka (invariant 7).

The fake and the runtime share `tributary.sdk.reply`, so these are the runtime's rules too; the
runtime's own tests cover what only the runtime can do (discarding a failed attempt's body).
"""

from __future__ import annotations

import pytest
from tributary.sdk import Message, ReplyError, step
from tributary.sdk.fakes import FakeContext
from tributary.sdk.reply import MAX_REPLY_BODY_BYTES, ReplyBody
from tributary.sdk.testing import make_message, run_step

#: Synthetic, and a marker a leak check can search for.
BODY = b"MSH|^~\\&|TRIBUTARY-SYNTH|LAB|||20260928||ACK^Q11|SYNTH-CTRL-MARKER|P|2.5.1\rMSA|AA|SYNTH-CTRL-MARKER\r"
HL7 = "application/hl7-v2+er7"


@step
def answers(message: Message, ctx: FakeContext) -> None:
    ctx.reply(BODY, content_type=HL7)


def test_a_pipeline_step_reply_is_recorded() -> None:
    ctx = FakeContext(source_type="mllp")
    run_step(answers, make_message(), ctx=ctx)
    assert ctx.replied is not None
    assert ctx.replied.body == BODY
    assert ctx.replied.content_type == HL7


def test_a_second_call_raises() -> None:
    ctx = FakeContext()
    ctx.reply(BODY, content_type=HL7)
    with pytest.raises(ReplyError, match="at most once"):
        ctx.reply(BODY, content_type=HL7)
    assert ctx.replied is not None and ctx.replied.body == BODY, "the first body stands"


def test_a_destination_step_may_not_reply() -> None:
    ctx = FakeContext(destination="lis")
    with pytest.raises(ReplyError, match="pipeline steps only"):
        ctx.reply(BODY, content_type=HL7)
    assert ctx.replied is None


def test_an_mllp_source_answers_with_hl7_v2_only() -> None:
    ctx = FakeContext(source_type="mllp")
    with pytest.raises(ReplyError, match="mllp"):
        ctx.reply(b'{"synthetic": true}', content_type="application/json")
    ctx.reply(BODY, content_type="application/hl7-v2+er7; charset=UTF-8")  # parameters are additive


def test_an_http_source_may_answer_with_any_registered_type() -> None:
    ctx = FakeContext(source_type="http")
    ctx.reply(b'{"synthetic": true}', content_type="application/fhir+json; fhirVersion=4.0.1")


@pytest.mark.parametrize("content_type", ["text/html", "application/x-custom", "", "APPLICATION"])
def test_an_unregistered_content_type_is_refused_without_echoing_it(content_type: str) -> None:
    ctx = FakeContext()
    with pytest.raises(ReplyError) as caught:
        ctx.reply(BODY, content_type=content_type)
    if content_type:
        assert content_type not in str(caught.value)


def test_the_body_limit_is_the_inline_limit() -> None:
    ctx = FakeContext()
    ctx.reply(b"x" * MAX_REPLY_BODY_BYTES, content_type="text/plain")
    with pytest.raises(ReplyError, match="1000001 bytes"):
        FakeContext().reply(b"x" * (MAX_REPLY_BODY_BYTES + 1), content_type="text/plain")


def test_the_body_must_be_bytes() -> None:
    with pytest.raises(ReplyError, match="must be bytes"):
        FakeContext().reply("MSA|AA|", content_type=HL7)  # type: ignore[arg-type]


def test_no_error_and_no_repr_quotes_the_body() -> None:
    """Invariant 6: the body is payload. Neither the fake, the held body nor an error may print it."""
    ctx = FakeContext(source_type="mllp")
    ctx.reply(BODY, content_type=HL7)
    with pytest.raises(ReplyError) as caught:
        ctx.reply(BODY, content_type=HL7)
    for rendered in (repr(ctx), str(ctx), repr(ctx.replied), str(caught.value)):
        assert "SYNTH-CTRL-MARKER" not in rendered
    assert repr(ctx.replied) == f"ReplyBody(content_type='{HL7}', size={len(BODY)})"


def test_a_refused_call_leaves_nothing_behind() -> None:
    ctx = FakeContext(source_type="mllp")
    with pytest.raises(ReplyError):
        ctx.reply(BODY, content_type="application/json")
    assert ctx.replied is None
    ctx.reply(BODY, content_type=HL7)  # a later valid call is the first one
    assert isinstance(ctx.replied, ReplyBody)


def test_the_fake_is_a_reply_context_and_a_plain_context_still_need_not_be() -> None:
    """`ReplyContext` is additive: `Context` implementations written before set 1.2 stay valid."""
    from tributary.sdk import Context, ReplyContext

    assert isinstance(FakeContext(), ReplyContext)

    class Minimal:
        def reply(self) -> None: ...

    assert not isinstance(Minimal(), Context)
    assert "reply" not in {name for name in dir(Context) if not name.startswith("_")}
