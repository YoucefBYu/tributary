"""Fast tests for the query library: parameterisation, validation, cursor, and PHI-free errors."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from tributary.archiver.errors import QueryError
from tributary.archiver.query import _VIEW_NAMES, ArchiveQueries, SearchFilter, _Cursor

SINCE = datetime(2026, 1, 1, tzinfo=UTC)
CANARY = "CANARY-VALUE-7731"


class FakeClient:
    def __init__(self, rows: list[list[Any]] | None = None, error: Exception | None = None) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.settings: list[dict[str, Any]] = []
        self._rows = rows or []
        self._error = error

    def query(self, sql: str, parameters: dict[str, Any], settings: dict[str, Any]) -> Any:
        self.calls.append((sql, parameters))
        self.settings.append(settings)
        if self._error:
            raise self._error

        class Result:
            column_names = _VIEW_NAMES
            result_rows = self._rows

        return Result()


def row(i: int) -> list[Any]:
    base = dict.fromkeys(_VIEW_NAMES)
    base.update(
        ts=SINCE + timedelta(seconds=i), kind="message", channel="c", channel_version=1, stage="received",
        status="ok", instance="w", message_id=uuid.UUID(int=i + 1), destination="", attempt=0, content_type="",
        size_bytes=0, claim_check="", topic="", error_code="", error_message="", traceparent="",
        attrs={"x_hmac": "a" * 64, "kind": "adt"},
    )  # fmt: skip
    return [base[n] for n in _VIEW_NAMES]


def test_no_caller_value_reaches_the_sql_text() -> None:
    client = FakeClient()
    flt = SearchFilter(
        channel="chan-a", since=SINCE, until=SINCE + timedelta(days=1), stages=["sent"], status="error",
        destination="dest-a", error_code="HTTP_503", attrs={"kind": "adt"},
    )  # fmt: skip
    ArchiveQueries(client).search(flt, limit=7)
    sql, params = client.calls[0]
    for value in ("chan-a", "dest-a", "HTTP_503", "adt", "sent", "error"):
        assert f"'{value}'" not in sql and f'"{value}"' not in sql
    assert params["channel"] == "chan-a" and params["destination"] == "dest-a" and params["limit"] == 8
    assert "LIMIT 1 BY" in sql and " OFFSET " not in sql.upper()


@pytest.mark.parametrize(
    "attrs", [{"patient_hmac": "a" * 64}, {"mrn": "x"}, {"Bad Name": "x"}, {"kind": "has space"}, {"k": "v" * 65}]
)
def test_identifying_or_malformed_attr_filters_are_refused(attrs: dict[str, str]) -> None:
    with pytest.raises(QueryError) as raised:
        ArchiveQueries(FakeClient()).search(SearchFilter(channel="c", since=SINCE, attrs=attrs))
    assert raised.value.code == "QUERY_INVALID"
    assert "v" * 65 not in str(raised.value)


@pytest.mark.parametrize(
    "flt",
    [
        SearchFilter(channel="Bad Channel", since=SINCE),
        SearchFilter(channel="c", since=datetime(2026, 1, 1)),  # naive
        SearchFilter(channel="c", since=SINCE, until=SINCE),
        SearchFilter(channel="c", since=SINCE, stages=["nope"]),
        SearchFilter(channel="c", since=SINCE, status="nope"),
        SearchFilter(channel="c", since=SINCE, error_code="lower"),
        SearchFilter(channel="c", since=SINCE, destination="X;DROP"),
    ],
)
def test_invalid_filters_are_refused_before_any_query(flt: SearchFilter) -> None:
    client = FakeClient()
    with pytest.raises(QueryError):
        ArchiveQueries(client).search(flt)
    assert client.calls == []


def test_limit_bounds() -> None:
    for bad in (0, 501, True):
        with pytest.raises(QueryError):
            ArchiveQueries(FakeClient()).search(SearchFilter(channel="c", since=SINCE), limit=bad)


def test_cursor_round_trip_and_rejection() -> None:
    cursor = _Cursor(1_767_225_600_123, "c", str(uuid.UUID(int=5)), "sent", "d", 2)
    assert _Cursor.decode(cursor.encode()) == cursor
    for token in ("", "not base64!", "e30", _Cursor(1, "c", "x", "sent", "", 0).encode(), "A" * 600):
        with pytest.raises(QueryError) as raised:
            ArchiveQueries(FakeClient()).search(SearchFilter(channel="c", since=SINCE), cursor=token)
        assert raised.value.code == "CURSOR_INVALID"


def test_page_cursor_is_set_only_when_a_further_row_exists() -> None:
    rows = [row(i) for i in range(3)]
    page = ArchiveQueries(FakeClient(rows)).search(SearchFilter(channel="c", since=SINCE), limit=2)
    assert len(page.items) == 2 and page.next_cursor is not None
    last = _Cursor.decode(page.next_cursor)
    assert last.message_id == str(page.items[-1].message_id)
    done = ArchiveQueries(FakeClient(rows[:2])).search(SearchFilter(channel="c", since=SINCE), limit=2)
    assert done.next_cursor is None


def test_hmac_attributes_are_never_returned() -> None:
    page = ArchiveQueries(FakeClient([row(0)])).search(SearchFilter(channel="c", since=SINCE))
    assert dict(page.items[0].attrs) == {"kind": "adt"}


def test_timeline_prunes_by_uuid7_time_and_binds_the_id() -> None:
    client = FakeClient()
    mid = "0194f5a0-0000-7000-8000-000000000001"
    ArchiveQueries(client).timeline(mid, channel="c")
    sql, params = client.calls[0]
    assert mid not in sql and params["message_id"] == mid and "since_ms" in params
    with pytest.raises(QueryError) as raised:  # no channel and no v7 time: a full-table read
        ArchiveQueries(client).timeline(uuid.uuid4())
    assert raised.value.code == "QUERY_INVALID"
    ArchiveQueries(client).timeline(uuid.uuid4(), channel="c")
    assert "since_ms" not in client.calls[1][1]


def test_backend_errors_carry_the_class_name_only() -> None:
    client = FakeClient(error=RuntimeError(f"Code: 62. SELECT ... {CANARY}"))
    with pytest.raises(QueryError) as raised:
        ArchiveQueries(client).timeline(uuid.uuid4(), channel="c")
    assert CANARY not in str(raised.value) and raised.value.__cause__ is None and raised.value.code == "QUERY_FAILED"


def test_table_and_database_are_validated_identifiers() -> None:
    with pytest.raises(QueryError):
        ArchiveQueries(FakeClient(), database="x`; DROP")
    with pytest.raises(QueryError):
        ArchiveQueries(FakeClient(), table="a b")


def test_every_query_carries_the_limits_and_readonly_last() -> None:
    client = FakeClient()
    ArchiveQueries(client).search(SearchFilter(channel="c", since=SINCE))
    settings = client.settings[0]
    assert list(settings)[-1] == "readonly" and settings["readonly"] == 1
    assert settings["max_memory_usage"] > 0 and settings["max_rows_to_read"] > 0 and settings["max_execution_time"] > 0


def test_cursor_is_applied_only_after_the_dedup() -> None:
    client = FakeClient()
    cursor = _Cursor(1_767_225_600_123, "c", str(uuid.UUID(int=5)), "sent", "d", 2).encode()
    ArchiveQueries(client).search(SearchFilter(channel="c", since=SINCE), cursor=cursor)
    sql = client.calls[0][0]
    inner, _, outer = sql.partition("LIMIT 1 BY")
    assert "c_ts" not in inner and "c_ts" in outer
