"""A `clickhouse-connect`-shaped client over the API's own stdlib HTTP transport, for the archive library.

`tributary.archiver.query.ArchiveQueries` takes any object with `query(sql, parameters=, settings=)` returning
`.column_names` and `.result_rows`. The real driver would pull `certifi` (MPL-2.0) into the API, which the licence
gate does not allow, so this is the narrow part of it the library uses, over `tamper.ClickHouseHttp` (verified TLS,
no redirects, no proxy variables, a hard overall deadline).

* Parameters are sent as server-side typed parameters (`param_<name>` beside `{name:Type}` in the SQL), never
  spliced into the SQL text. Strings are escaped for ClickHouse's text parameter syntax.
* Results are read as `JSONCompact` and converted by the column types ClickHouse reports.
* Errors carry no text: a bare `ArchiveClientError` with ClickHouse's numeric code at most.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any, Final

from .tamper import ClickHouseHttp, TamperQueryError

#: Largest answer accepted: a content row can be 1 MiB of payload, which is ~1.4 MiB as base64.
MAX_RESULT_BYTES: Final = 8 * 1024 * 1024


class ArchiveClientError(Exception):
    """The archive did not answer a query. No response text; `ch_code` is ClickHouse's numeric code if any."""

    def __init__(self, ch_code: int | None = None) -> None:
        super().__init__("archive query failed")
        self.ch_code = ch_code


@dataclass(frozen=True, slots=True)
class QueryResult:
    column_names: tuple[str, ...]
    result_rows: list[tuple[Any, ...]]


def _text(value: str) -> str:
    """ClickHouse text parameter syntax: backslash escapes are interpreted, so escape the ones that matter."""
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _array_item(value: str) -> str:
    return "'" + _text(value).replace("'", "\\'") + "'"


def encode_parameter(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, str):
        return _text(value)
    if isinstance(value, Sequence) and not isinstance(value, bytes):
        return "[" + ",".join(_array_item(str(v)) for v in value) + "]"
    raise TypeError("unsupported query parameter type")


_NULLABLE = re.compile(r"^Nullable\((.*)\)$")
_LOWCARD = re.compile(r"^LowCardinality\((.*)\)$")
_ARRAY = re.compile(r"^Array\((.*)\)$")
_DATETIME = re.compile(r"^DateTime(64)?\b")


def convert(type_name: str, value: Any) -> Any:
    """One JSON value to the Python value the library expects for this ClickHouse type."""
    if value is None:
        return None
    if (m := _NULLABLE.match(type_name)) or (m := _LOWCARD.match(type_name)):
        return convert(m.group(1), value)
    if m := _ARRAY.match(type_name):
        return [convert(m.group(1), v) for v in value]
    if type_name.startswith("Map("):
        return dict(value)
    if type_name == "UUID":
        return uuid.UUID(value)
    if _DATETIME.match(type_name):
        text = str(value)
        fmt = "%Y-%m-%d %H:%M:%S.%f" if "." in text else "%Y-%m-%d %H:%M:%S"
        return datetime.strptime(text, fmt).replace(tzinfo=UTC)
    if type_name == "Date":
        return date.fromisoformat(value)
    return value  # String, integers, floats and Bool arrive as the right JSON types


class ArchiveClient:
    """Stateless apart from the transport: safe to share between requests and threads."""

    def __init__(self, http: ClickHouseHttp, *, max_result_bytes: int = MAX_RESULT_BYTES) -> None:
        self._http = http
        self._max = max_result_bytes

    def query(
        self, sql: str, parameters: Mapping[str, Any] | None = None, settings: Mapping[str, Any] | None = None
    ) -> QueryResult:
        # The library's settings end with `readonly`; the format settings go before it, never after.
        ordered: dict[str, str] = {}
        given = dict(settings or {})
        given.pop("readonly", None)  # never the caller's say: every statement from this client is readonly
        ordered.update({k: str(v) for k, v in given.items()})
        ordered.update({"default_format": "JSONCompact", "output_format_json_quote_64bit_integers": "0"})
        ordered["readonly"] = "1"
        deadline = float(given.get("max_execution_time", 30)) + 5.0
        try:
            raw = self._http.request(
                sql,
                settings=ordered,
                params={k: encode_parameter(v) for k, v in (parameters or {}).items()},
                max_bytes=self._max,
                deadline_s=deadline,
            )
            document = json.loads(raw)
            names = tuple(str(c["name"]) for c in document["meta"])
            kinds = [str(c["type"]) for c in document["meta"]]
            rows = [tuple(convert(t, v) for t, v in zip(kinds, row, strict=True)) for row in document["data"]]
        except TamperQueryError as exc:
            raise ArchiveClientError(exc.ch_code) from None
        except (ValueError, KeyError, TypeError):  # bad JSON or an unexpected shape: say nothing about it
            raise ArchiveClientError from None
        return QueryResult(names, rows)
