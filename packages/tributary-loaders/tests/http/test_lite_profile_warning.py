"""The mandatory, unsuppressable lite-profile startup warning (`contracts/topics/v0` README 3.8;
topic-defaults.json `profiles.lite.warning_acceptance`)."""

from __future__ import annotations

from tributary.loaders.http.main import emit_lite_profile_warning
from tributary.sdk.fakes import RecordingLogger


def test_lite_profile_emits_the_warning() -> None:
    log = RecordingLogger()
    emit_lite_profile_warning(log, profile="lite")
    assert "lite_profile_active" in log.events()


def test_production_profile_emits_nothing() -> None:
    log = RecordingLogger()
    emit_lite_profile_warning(log, profile="production")
    assert log.events() == []
