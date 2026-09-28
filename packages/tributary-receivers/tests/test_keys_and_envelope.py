"""The receiver-side wiring around `tributary.sdk.keys` (the one evaluator), the shared raw-envelope
builder and `resolve_text`. Evaluator semantics themselves are `tributary-sdk`'s tests, not repeated."""

from __future__ import annotations

from pathlib import Path

import pytest
from tributary.receivers.envelope import build_raw_envelope
from tributary.receivers.errors import ConfigError
from tributary.receivers.keys import key_expression_from_spec, resolve_key
from tributary.receivers.secrets import DirSecretResolver
from tributary.sdk.projection import validate_for_produce


def test_ordering_none_yields_no_expression_and_a_null_outcome() -> None:
    assert key_expression_from_spec({"ordering": "none"}, channel="c", source_type="http") is None
    out = resolve_key(None, payload=b"x", content_type="text/plain")
    assert (out.key, out.reason) == (None, None)


def test_per_key_needs_a_key_and_none_forbids_one() -> None:
    with pytest.raises(ConfigError, match="required"):
        key_expression_from_spec({"ordering": "per-key"}, channel="c", source_type="http")
    with pytest.raises(ConfigError, match="forbidden"):
        key_expression_from_spec({"ordering": "none", "partitionKey": "header:X"}, channel="c", source_type="http")


def test_scheme_incompatible_with_source_is_a_startup_error() -> None:
    with pytest.raises(ConfigError):
        key_expression_from_spec({"ordering": "per-key", "partitionKey": "record:key"}, channel="c", source_type="sftp")


def test_unresolved_key_carries_a_reason_code_never_a_value() -> None:
    expr = key_expression_from_spec(
        {"ordering": "per-key", "partitionKey": "json:/a"},
        channel="c",
        source_type="http",
        content_types={"application/json"},
    )
    out = resolve_key(expr, payload=b'{"b": "SYNTHVALUE"}', content_type="application/json")
    assert out.key is None and out.reason is not None and "SYNTHVALUE" not in out.reason


@pytest.mark.parametrize(("ordering", "key"), [("none", None), ("per-key", b"k")])
def test_built_envelope_passes_the_produce_validator(ordering: str, key: bytes | None) -> None:
    m = build_raw_envelope(
        channel="c1", channel_version=2, source="sftp:s-0@h", payload=b"p", key=key, content_type="text/csv"
    )
    validate_for_produce(m, topic_class="raw", ordering=ordering)  # type: ignore[arg-type]
    assert m.headers["tb-content-type"] == "text/csv" and m.headers["tb-attempt"] == "0"


def test_resolve_text_reads_and_strips_and_stays_inside_the_secrets_dir(tmp_path: Path) -> None:
    (tmp_path / "ref").mkdir()
    (tmp_path / "ref" / "password").write_text("  s3cret \n", encoding="utf-8")
    (tmp_path.parent / "outside").write_text("x", encoding="utf-8")
    r = DirSecretResolver(tmp_path)
    assert r.resolve_text("ref", "password") == "s3cret"
    with pytest.raises((FileNotFoundError, ValueError, PermissionError)):
        r.resolve_text("..", "outside")
    with pytest.raises(FileNotFoundError):
        r.resolve_text("ref", "missing")
