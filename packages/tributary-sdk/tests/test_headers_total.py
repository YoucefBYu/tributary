"""Gate requirement 13: every integer header parse rejects, never crashes.

SR-10 asked for this to be generalised from the spike's `tb-x-reply-partition` to every integer header
in the envelope contract. The property is not "bad input raises" but "**only** `HeaderError` ever comes
out": a header arrives from the network, so any other exception type is an unhandled crash in the
consumer loop where the contract wants a reject.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st
from tributary.sdk import headers as h
from tributary.sdk.errors import HeaderError, HeaderValueError, MissingHeaderError

# Values that a naive int(value) would accept or that crash a naive parser. int("1_0") is 10 and
# int("١٢") is 12, so a parser built on int() silently accepts header values the envelope
# contract forbids ("decimal ASCII, no sign, no padding").
INT_LOOKALIKES = [
    "1_0",
    "١٢",  # Arabic-Indic digits
    "１",  # noqa: RUF001 - a fullwidth digit one is exactly the lookalike under test
    "+1",
    "-1",
    " 1",
    "1 ",
    "\n1",
    "01",
    "1.0",
    "1e3",
    "0x10",
    "",
    "  ",
    "one",
    "∞",
    "9" * 400,
]


@pytest.mark.parametrize("value", INT_LOOKALIKES)
@pytest.mark.parametrize("name", sorted(h.INTEGER_HEADERS))
def test_integer_lookalikes_are_rejected(name: str, value: str) -> None:
    with pytest.raises(HeaderValueError):
        h.parse_int(name, value)


@pytest.mark.parametrize("name", sorted(h.INTEGER_HEADERS))
def test_integer_header_respects_the_contract_maximum(name: str) -> None:
    limit = h.INTEGER_HEADERS[name]
    assert h.parse_int(name, str(limit)) == limit
    with pytest.raises(HeaderValueError):
        h.parse_int(name, str(limit + 1))


@pytest.mark.parametrize("name", sorted(h.INTEGER_HEADERS))
def test_missing_integer_header_is_typed(name: str) -> None:
    with pytest.raises(MissingHeaderError):
        h.parse_int(name, None)


@given(value=st.text(max_size=64))
def test_parse_int_is_total_over_arbitrary_text(value: str) -> None:
    """Either an int, or a HeaderError. Never a ValueError, TypeError or anything else."""
    try:
        parsed = h.parse_int(h.TB_ATTEMPT, value)
    except HeaderError:
        return
    assert isinstance(parsed, int)
    assert str(parsed) == value


@given(value=st.text(max_size=64))
def test_parse_uuid_is_total_over_arbitrary_text(value: str) -> None:
    try:
        parsed = h.parse_uuid(h.TB_MSG_ID, value, version7=True)
    except HeaderError:
        return
    assert parsed == value


@given(value=st.text(max_size=64))
def test_parse_timestamp_is_total_over_arbitrary_text(value: str) -> None:
    try:
        h.parse_timestamp(h.TB_RECEIVED_AT, value)
    except HeaderError:
        return


@given(value=st.text(max_size=80))
def test_parse_traceparent_is_total_over_arbitrary_text(value: str) -> None:
    try:
        h.parse_traceparent(h.TRACEPARENT, value)
    except HeaderError:
        return


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-20T11:22:33.123Z",
        "2026-09-20T11:22:33.123+00:00",
        "2026-09-20T11:22:33.123456789Z",  # 9 fractional digits: fromisoformat alone would raise
    ],
)
def test_contract_legal_timestamps_parse(value: str) -> None:
    moment = h.parse_timestamp(h.TB_RECEIVED_AT, value)
    assert moment.tzinfo is not None
    assert moment.year == 2026


@pytest.mark.parametrize(
    "value",
    [
        "2026-09-20T11:22:33Z",  # no fractional digits
        "2026-09-20T11:22:33.12Z",  # two digits, contract needs three
        "2026-09-20T11:22:33.123+01:00",  # not UTC
        "2026-09-20 11:22:33.123Z",  # space instead of T
        "2026-13-45T99:99:99.123Z",  # shape is right, the date is not real
    ],
)
def test_malformed_timestamps_are_rejected(value: str) -> None:
    with pytest.raises(HeaderValueError):
        h.parse_timestamp(h.TB_RECEIVED_AT, value)


def test_decode_headers_rejects_invalid_utf8_rather_than_replacing_it() -> None:
    with pytest.raises(HeaderValueError) as caught:
        h.decode_headers([("tb-channel", b"\xff\xfe")])
    assert "tb-channel" in str(caught.value)


def test_decode_headers_last_value_wins() -> None:
    decoded = h.decode_headers([("tb-attempt", b"1"), ("tb-attempt", b"2")])
    assert decoded["tb-attempt"] == "2"


def test_header_error_never_quotes_the_value() -> None:
    """A rejected header value is never echoed: it can be a peer address or channel-defined text."""
    rejected = "carrier-of-something-sensitive"
    with pytest.raises(HeaderValueError) as caught:
        h.parse_int(h.TB_ATTEMPT, rejected)
    assert rejected not in str(caught.value)
    assert str(len(rejected)) in str(caught.value)


def test_format_timestamp_rejects_naive_datetimes() -> None:
    from datetime import datetime

    with pytest.raises(HeaderValueError):
        h.format_timestamp(datetime(2026, 9, 20, 11, 22, 33))  # a naive datetime is the point of the test


def test_format_timestamp_round_trips_through_the_parser() -> None:
    from datetime import UTC, datetime

    moment = datetime(2026, 9, 20, 11, 22, 33, 123456, tzinfo=UTC)
    text = h.format_timestamp(moment)
    assert text == "2026-09-20T11:22:33.123Z"
    assert h.parse_timestamp(h.TB_RECEIVED_AT, text).replace(microsecond=0) == moment.replace(microsecond=0)
