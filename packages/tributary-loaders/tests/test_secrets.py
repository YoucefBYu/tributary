"""`DirSecretResolver.resolve_text` rejects control characters at resolution time (security review, Medium:
a secret with a newline made `http.client` raise an error whose message held the whole `Authorization`
value). The error is typed and fixed: the value, ref and key are never in it."""

from __future__ import annotations

from pathlib import Path

import pytest
from tributary.loaders.errors import LoaderError
from tributary.loaders.secrets import DirSecretResolver, SecretFormatError


def write(base: Path, value: bytes) -> DirSecretResolver:
    (base / "ref").mkdir()
    (base / "ref" / "token").write_bytes(value)
    return DirSecretResolver(base)


def test_a_trailing_newline_is_stripped_as_before(tmp_path: Path) -> None:
    assert write(tmp_path, b"tok-123\n").resolve_text("ref", "token") == "tok-123"


@pytest.mark.parametrize("value", [b"tok\nX-Injected: 1", b"tok\rx", b"tok\x00x", b"tok\x1bx", b"tok\x7fx"])
def test_a_control_character_inside_the_secret_is_refused_without_echoing_it(tmp_path: Path, value: bytes) -> None:
    resolver = write(tmp_path, value)
    with pytest.raises(SecretFormatError) as exc_info:
        resolver.resolve_text("ref", "token")
    assert isinstance(exc_info.value, LoaderError)
    text = str(exc_info.value)
    assert "tok" not in text and "ref" not in text and "token" not in text


def test_invalid_utf8_is_the_same_typed_error(tmp_path: Path) -> None:
    resolver = write(tmp_path, b"\xff\xfe")
    with pytest.raises(SecretFormatError):
        resolver.resolve_text("ref", "token")
