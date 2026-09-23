"""The mandatory, unsuppressable lite-profile startup warning (`contracts/topics/v0` README 3.8;
topic-defaults.json `profiles.lite.warning_acceptance`: "emitted... on by default, with no flag to
suppress it... under lite and not otherwise")."""

from __future__ import annotations

import json

import pytest
from tributary.receivers.mllp.server import emit_lite_profile_warning


def test_lite_profile_emits_the_warning(capsys: pytest.CaptureFixture[str]) -> None:
    emit_lite_profile_warning(profile="lite")
    out = json.loads(capsys.readouterr().out.strip())
    assert out["event"] == "lite_profile_active"


def test_production_profile_emits_nothing(capsys: pytest.CaptureFixture[str]) -> None:
    emit_lite_profile_warning(profile="production")
    assert capsys.readouterr().out == ""
