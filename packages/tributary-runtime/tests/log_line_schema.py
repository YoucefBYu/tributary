"""`contracts/observability/v0/log-line.schema.json`, loaded once, for tests that capture `JsonLogger` output.

Scenario tests call `assert_lines_conform(stream.getvalue())` so the lines the runtime really writes on
its failure paths are checked, not only lines a conformance test builds by hand. A failure names the
event, the field and the schema keyword, never the value (invariant 6 applies to test output).
"""

from __future__ import annotations

import json
from functools import cache
from pathlib import Path
from typing import Any

import orjson
from jsonschema import Draft202012Validator

SCHEMA_PATH = Path(__file__).resolve().parents[3] / "contracts" / "observability" / "v0" / "log-line.schema.json"


@cache
def validator() -> Draft202012Validator:
    assert SCHEMA_PATH.is_file(), "log-line schema not found: conformance would pass vacuously"
    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def violations(record: Any) -> list[str]:
    """`event:field:keyword` for every schema error in one parsed line."""
    event = record.get("event") if isinstance(record, dict) else None
    return [
        f"{event}:{error.path[0] if error.path else '-'}:{error.validator}" for error in validator().iter_errors(record)
    ]


def assert_lines_conform(text: str, *, expect_at_least: int = 1) -> list[dict[str, Any]]:
    """Every line of `text` is one JSON object that validates. Returns the parsed lines."""
    # Split on LF only, as a log shipper does: `str.splitlines` also splits on U+0085 and U+2028, which
    # orjson writes unescaped inside a value and the contract (L5) permits.
    lines = [orjson.loads(line) for line in text.split("\n") if line]
    assert len(lines) >= expect_at_least, f"expected at least {expect_at_least} log lines, got {len(lines)}"
    problems = [problem for line in lines for problem in violations(line)]
    assert not problems, f"log lines violate observability/v0 log-line.schema.json: {sorted(set(problems))}"
    return lines
