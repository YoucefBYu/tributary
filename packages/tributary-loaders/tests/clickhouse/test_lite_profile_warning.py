"""The mandatory, unsuppressable lite-profile startup warning. Mirrors
`tests/postgresql/test_lite_profile_warning.py`."""

from __future__ import annotations

from tributary.loaders.clickhouse.main import emit_lite_profile_warning
from tributary.sdk.fakes import RecordingLogger


def test_lite_profile_emits_the_warning() -> None:
    log = RecordingLogger()
    emit_lite_profile_warning(log, profile="lite")
    assert "lite_profile_active" in log.events()


def test_production_profile_emits_nothing() -> None:
    log = RecordingLogger()
    emit_lite_profile_warning(log, profile="production")
    assert log.events() == []
