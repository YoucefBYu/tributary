"""Pytest fixture for a bundle testing FHIR mapping steps: `fhir_ctx`.

Registered through the `pytest11` entry point (`pyproject.toml`), the same mechanism
`tributary.sdk.pytest_plugin` uses, so a bundle that depends on `tributary-fhir` gets it with no
conftest:

    def test_to_fhir_bundle(run_step, make_message, fhir_ctx):
        out = run_step(to_fhir_bundle, make_message(b"..."), ctx=fhir_ctx)
"""

from __future__ import annotations

import pytest

from .context import FhirContext
from .fakes import fake_fhir_context


@pytest.fixture
def fhir_ctx() -> FhirContext:
    """A fresh `FhirContext`: `FakeContext` plus `LocalFhir` (deterministic ids) and `LocalTerminology`."""
    return fake_fhir_context()
