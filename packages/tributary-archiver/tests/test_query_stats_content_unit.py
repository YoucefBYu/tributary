"""Stats, content read and the content flag in the query library (W3-6): parameterised, readonly, PHI-free errors."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from tributary.archiver.errors import QueryError
from tributary.archiver.query import ArchiveQueries

SINCE = datetime(2026, 10, 1, tzinfo=UTC)
CANARY = "CANARY-VALUE-7731"
MSG = str(uuid.uuid4())


class Client:
    """Returns the next scripted row set per call; records SQL, parameters and settings."""

    def __init__(self, *row_sets: list[tuple[Any, ...]], error: Exception | None = None) -> None:
        self.row_sets, self.error = list(row_sets), error
        self.calls: list[tuple[str, dict[str, Any], dict[str, Any]]] = []

    def query(self, sql: str, parameters: dict[str, Any], settings: dict[str, Any]) -> Any:
        self.calls.append((sql, parameters, settings))
        if self.error:
            raise self.error
        rows = self.row_sets.pop(0) if self.row_sets else []
        return type("R", (), {"result_rows": rows, "column_names": ()})()


def queries(client: Client) -> ArchiveQueries:
    return ArchiveQueries(client, database="arch")


def test_channel_stats_maps_rows_and_every_statement_is_parameterised_and_readonly() -> None:
    last = datetime(2026, 10, 1, 12, tzinfo=UTC)
    client = Client(
        [("received", 5), ("sent", 3), ("weird", 9)],
        [("sink", 3, 1, 0, 2, 12.5, float("nan"))],
        [("DEST_FAILED", 4)],
        [(7, 0, last)],
    )
    stats = queries(client).channel_stats("alpha", SINCE)
    assert stats.counts["received"] == 5 and stats.counts["sent"] == 3 and "weird" not in stats.counts
    assert stats.counts["dlq"] == 0
    (dest,) = stats.destinations
    assert (dest.destination, dest.sent, dest.dlq, dest.duration_ms_p50, dest.duration_ms_p95) == (
        "sink",
        3,
        2,
        12.5,
        None,
    )
    assert stats.errors == (("DEST_FAILED", 4),)
    assert (stats.versions[0].version, stats.versions[0].instances, stats.versions[0].last_seen) == (7, 1, last)
    assert len(client.calls) == 4
    for sql, params, settings in client.calls:
        assert settings["readonly"] == 1 and list(settings)[-1] == "readonly"
        assert "alpha" not in sql and params["channel"] == "alpha" and "{channel:String}" in sql
        assert "`arch`.message_events" in sql and "kind = 'message'" in sql


def test_read_content_maps_a_row_and_binds_every_value() -> None:
    archived = datetime(2026, 10, 1, 12, tzinfo=UTC)
    parent = uuid.uuid4()
    client = Client([(3, "routed", "sink", "text/plain", 5, "aGVsbG8=", "", parent, None, 2, "", archived)])
    view = queries(client).read_content("alpha", MSG, "routed", "sink")
    assert view is not None
    assert (view.stage, view.destination, view.size_bytes, view.payload_base64) == ("routed", "sink", 5, "aGVsbG8=")
    assert (view.parent_id, view.correlation_id, view.attempt, view.archived_at) == (parent, None, 2, archived)
    sql, params, settings = client.calls[0]
    assert params == {"channel": "alpha", "message_id": MSG, "stage": "routed", "destination": "sink"}
    assert MSG not in sql and "`arch`.message_content" in sql and settings["readonly"] == 1


def test_read_content_returns_the_locator_and_no_bytes_for_a_claim_check() -> None:
    row = (1, "received", "", "text/plain", 9, "WFg=", "s3://synth-bucket/obj/k1", None, None, 0, "", SINCE)
    view = queries(Client([row])).read_content("alpha", MSG, "received")
    assert view is not None and view.claim_ref == "s3://synth-bucket/obj/k1" and view.payload_base64 == ""


def test_read_content_never_returns_a_locator_the_grammar_refuses() -> None:
    row = (1, "received", "", "text/plain", 9, "", "file:///etc/passwd", None, None, 0, "", SINCE)
    with pytest.raises(QueryError) as raised:
        queries(Client([row])).read_content("alpha", MSG, "received")
    assert raised.value.code == "QUERY_FAILED" and "etc" not in str(raised.value)


def test_read_content_not_found_is_none() -> None:
    assert queries(Client([])).read_content("alpha", MSG, "dlq") is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"channel": "Bad Channel"},
        {"message_id": "not-a-uuid"},
        {"stage": "transformed"},
        {"destination": "Bad Dest"},
    ],
)
def test_read_content_validates_before_any_query(kwargs: dict[str, str]) -> None:
    client = Client()
    args = {"channel": "alpha", "message_id": MSG, "stage": "received", "destination": "", **kwargs}
    with pytest.raises(QueryError) as raised:
        queries(client).read_content(**args)
    assert raised.value.code == "QUERY_INVALID" and client.calls == []


def test_channel_stats_validates_channel_and_requires_an_aware_since() -> None:
    for channel, since in (("Bad", SINCE), ("alpha", datetime(2026, 1, 1))):
        with pytest.raises(QueryError):
            queries(Client()).channel_stats(channel, since)


class TableMissing(Exception):
    ch_code = 60


def test_a_missing_content_table_is_an_empty_index_but_a_content_read_error() -> None:
    assert queries(Client(error=TableMissing())).content_index("alpha", MSG) == ()
    with pytest.raises(QueryError) as raised:
        queries(Client(error=TableMissing())).read_content("alpha", MSG, "received")
    assert raised.value.code == "CONTENT_TABLE_MISSING"
    with pytest.raises(QueryError) as text_only:
        queries(Client(error=RuntimeError("Code: 60. DB::Exception: Unknown table"))).read_content(
            "alpha", MSG, "received"
        )
    assert text_only.value.code == "CONTENT_TABLE_MISSING"


def test_driver_text_never_reaches_the_error() -> None:
    with pytest.raises(QueryError) as raised:
        queries(Client(error=RuntimeError(f"boom {CANARY}"))).channel_stats("alpha", SINCE)
    assert CANARY not in str(raised.value) and raised.value.code == "QUERY_FAILED"


def test_content_index_maps_rows() -> None:
    items = queries(
        Client([("received", "", "text/plain", 5, False), ("routed", "sink", "text/plain", 0, True)])
    ).content_index("alpha", MSG)
    assert [(i.stage, i.destination, i.is_claim) for i in items] == [("received", "", False), ("routed", "sink", True)]


@pytest.mark.parametrize(
    ("rows", "expected"),
    [([(0, None)], None), ([(1, True)], True), ([(2, False)], False), ([(1, None)], None), ([], None)],
)
def test_content_storage_flag(rows: list[tuple[Any, ...]], expected: bool | None) -> None:
    client = Client(rows)
    assert queries(client).content_storage_enabled() is expected
    sql, params, settings = client.calls[0]
    assert "`arch`.archive_settings" in sql and params == {"t": "content_storage"} and settings["readonly"] == 1


def test_flag_is_none_when_archive_settings_does_not_exist() -> None:
    assert queries(Client(error=TableMissing())).content_storage_enabled() is None
