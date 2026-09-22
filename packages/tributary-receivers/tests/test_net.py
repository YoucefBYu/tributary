"""The bind rule (gate requirement 8): no wildcard default, no plaintext listener off loopback."""

from __future__ import annotations

import pytest
from tributary.receivers.errors import BindError
from tributary.receivers.net import is_loopback, require_safe_bind


@pytest.mark.parametrize("bind", ["127.0.0.1", "127.0.0.42", "localhost", "::1"])
def test_is_loopback_true(bind: str) -> None:
    assert is_loopback(bind) is True


@pytest.mark.parametrize("bind", ["0.0.0.0", "::", "10.0.0.5", "example.org", "not-an-address"])  # noqa: S104
def test_is_loopback_false(bind: str) -> None:
    assert is_loopback(bind) is False


def test_loopback_bind_without_tls_is_allowed() -> None:
    require_safe_bind("127.0.0.1", tls_configured=False)  # must not raise


def test_wildcard_bind_without_tls_is_refused() -> None:
    with pytest.raises(BindError):
        require_safe_bind("0.0.0.0", tls_configured=False)  # noqa: S104 - this is what the test checks


def test_wildcard_bind_with_tls_is_allowed() -> None:
    require_safe_bind("0.0.0.0", tls_configured=True)  # noqa: S104 - must not raise, tls covers it


def test_ipv6_wildcard_without_tls_is_refused() -> None:
    with pytest.raises(BindError):
        require_safe_bind("::", tls_configured=False)


def test_non_loopback_literal_without_tls_is_refused() -> None:
    with pytest.raises(BindError):
        require_safe_bind("10.0.0.5", tls_configured=False)
