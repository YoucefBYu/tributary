from __future__ import annotations

from collections.abc import Iterator

import pytest
from testcontainers.community.postgres import PostgresContainer


@pytest.fixture(scope="session")
def pg_url() -> Iterator[str]:
    """Our own PostgreSQL container per session. `driver=None`: no SQLAlchemy driver is imported by
    testcontainers; the URL is rewritten to the async driver by `to_async_url`."""
    with PostgresContainer("postgres:16-alpine", driver=None) as pg:
        yield pg.get_connection_url()
