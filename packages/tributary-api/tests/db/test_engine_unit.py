from __future__ import annotations

import re

import pytest
from alembic.script import ScriptDirectory
from sqlalchemy.exc import DBAPIError
from tributary.api.db.engine import to_async_url
from tributary.api.db.errors import (
    AlreadyExistsError,
    InUseError,
    InvalidValueError,
    RepositoryError,
    TransientError,
)
from tributary.api.db.migrate import make_config
from tributary.api.db.models import NAME_PATTERN
from tributary.api.db.repository import _classify, spec_digest


def test_url_driver_is_rewritten() -> None:
    assert to_async_url("postgresql://u:p@h:5432/d").startswith("postgresql+asyncpg://u:p@h:5432/d")
    assert to_async_url("postgresql+psycopg2://u:p@h/d").startswith("postgresql+asyncpg://")


def test_non_postgres_url_refused() -> None:
    with pytest.raises(ValueError, match="PostgreSQL only"):
        to_async_url("sqlite:///x.db")


def test_spec_digest_is_key_order_independent() -> None:
    assert spec_digest({"a": 1, "b": {"c": 2}}) == spec_digest({"b": {"c": 2}, "a": 1})
    assert spec_digest({"a": 1}) != spec_digest({"a": 2})


def test_single_migration_head() -> None:
    script = ScriptDirectory.from_config(make_config("postgresql://x/y"))
    assert script.get_heads() == ["0001"]


class _FakeOrig(Exception):
    def __init__(self, sqlstate: str | None) -> None:
        super().__init__("secret bound value")
        self.sqlstate = sqlstate


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("23505", AlreadyExistsError),
        ("23514", InvalidValueError),
        ("22001", InvalidValueError),
        ("23503", InUseError),
        ("40001", TransientError),
        ("40P01", TransientError),
        ("08006", TransientError),
        ("42P01", RepositoryError),
        (None, RepositoryError),
    ],
)
def test_classify_by_sqlstate(code: str | None, expected: type[RepositoryError]) -> None:
    err = _classify(DBAPIError("stmt", {"p": "phi-ish"}, _FakeOrig(code)), "thing")
    assert type(err) is expected
    assert "phi-ish" not in str(err)
    assert "secret" not in str(err)


def test_spec_digest_rejects_nan() -> None:
    with pytest.raises(ValueError, match="Out of range"):
        spec_digest({"a": float("nan")})


def test_name_pattern_cap() -> None:
    assert re.fullmatch(NAME_PATTERN, "a" * 63)
    assert not re.fullmatch(NAME_PATTERN, "a" * 64)
    assert not re.fullmatch(NAME_PATTERN, "-a")
