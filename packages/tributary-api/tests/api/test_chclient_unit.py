"""The ClickHouse client shape the archive library needs, over a scripted transport: parameter encoding, result
conversion by column type, setting order, and errors that carry no text."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from tributary.api.chclient import ArchiveClient, ArchiveClientError, convert, encode_parameter
from tributary.api.tamper import TamperQueryError


class Transport:
    def __init__(self, document: Any = None, *, error: TamperQueryError | None = None) -> None:
        self.document, self.error = document, error
        self.calls: list[dict[str, Any]] = []

    def request(self, sql: str, **kw: Any) -> bytes:
        self.calls.append({"sql": sql, **kw})
        if self.error is not None:
            raise self.error
        return json.dumps(self.document).encode() if not isinstance(self.document, bytes) else self.document


def test_parameters_are_escaped_for_clickhouse_text_syntax_and_arrays_are_literals() -> None:
    assert encode_parameter("a\\b\tc\nd") == "a\\\\b\\tc\\nd"
    assert encode_parameter(7) == "7" and encode_parameter(True) == "1"
    u = uuid.uuid4()
    assert encode_parameter(u) == str(u)
    assert encode_parameter(["sent", "it's", "back\\slash"]) == "['sent','it\\'s','back\\\\slash']"
    with pytest.raises(TypeError):
        encode_parameter(object())


def test_values_convert_by_the_column_type_clickhouse_reports() -> None:
    u = str(uuid.uuid4())
    assert convert("UUID", u) == uuid.UUID(u)
    assert convert("Nullable(UUID)", None) is None and convert("Nullable(UUID)", u) == uuid.UUID(u)
    assert convert("LowCardinality(String)", "x") == "x" and convert("UInt64", 5) == 5
    assert convert("DateTime64(3, 'UTC')", "2026-10-06 07:00:00.123") == datetime(
        2026, 10, 6, 7, 0, 0, 123000, tzinfo=UTC
    )
    assert convert("DateTime64(3, 'UTC')", "2026-10-06 07:00:00").tzinfo is UTC
    assert convert("Map(String, String)", {"a": "b"}) == {"a": "b"}
    assert convert("Array(LowCardinality(String))", ["a", "b"]) == ["a", "b"]
    assert convert("Nullable(Float32)", 1.5) == 1.5 and convert("Nullable(Float32)", None) is None


def test_query_orders_settings_with_readonly_last_and_sends_parameters_server_side() -> None:
    document = {"meta": [{"name": "n", "type": "UInt64"}, {"name": "t", "type": "String"}], "data": [[3, "x"]]}
    transport = Transport(document)
    client = ArchiveClient(transport)  # type: ignore[arg-type]
    result = client.query(
        "SELECT n, t WHERE c = {c:String}",
        {"c": "it's"},
        {"max_execution_time": 30, "max_memory_usage": 4, "readonly": 1},
    )
    assert result.column_names == ("n", "t") and result.result_rows == [(3, "x")]
    call = transport.calls[0]
    assert "it's" not in call["sql"]  # a value never reaches the SQL text
    assert call["params"] == {"c": "it's"}
    keys = list(call["settings"])
    assert keys[-1] == "readonly" and "default_format" in keys[:-1] and keys.index("max_execution_time") < len(keys) - 1
    assert call["settings"]["output_format_json_quote_64bit_integers"] == "0"
    assert call["deadline_s"] == 35.0  # the statement's own limit plus a margin


def test_failures_carry_no_text_and_unknown_table_keeps_its_code() -> None:
    for err in (TamperQueryError(60), TamperQueryError()):
        with pytest.raises(ArchiveClientError) as e:
            ArchiveClient(Transport(error=err)).query("SELECT 1")  # type: ignore[arg-type]
        assert e.value.ch_code == err.ch_code and str(e.value) == "archive query failed"
    for junk in (
        b"not json",
        json.dumps({"meta": 1}).encode(),
        json.dumps({"meta": [{"name": "a", "type": "UUID"}], "data": [["nope"]]}).encode(),
    ):
        with pytest.raises(ArchiveClientError):
            ArchiveClient(Transport(junk)).query("SELECT 1")  # type: ignore[arg-type]
