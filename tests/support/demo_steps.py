"""A no-op pass-through pipeline step for the e2e and chaos tests.

`contracts/channel-spec/v0` says a channel's `pipeline.steps[].ref` is a dotted path inside
`pipeline.bundle`, an OCI artifact. Nothing in the repo fetches an OCI bundle yet
(`packages/tributary-runtime/src/tributary/runtime/steps.py::_from_dotted_path` just does
`importlib.import_module`, assuming the bundle's code is already importable) and no package ships an
`adt_flow` module anywhere — confirmed empirically against the compose stack's own worker image
(`docker exec tb-worker python -c "import adt_flow"` -> `ModuleNotFoundError`) and reported to
architect/devops-engineer 2026-09-23. Wave 1 has no HL7-to-FHIR mapping regardless (that is wave 2), so
the walking skeleton only ever needs a step that changes nothing.

`tests/support/channel_spec.py` points a test channel's `steps[0].ref` at
`tests.support.demo_steps.passthrough` instead of the contract example's `adt_flow.steps.passthrough`,
with the repo root on `PYTHONPATH` for whichever process resolves it (`tests/support/processes.py`
sets this for every worker subprocess it spawns) — same dotted-path mechanism the contract describes,
just pointed at a module this package actually ships instead of an unbuilt bundle-fetch step.
"""

from __future__ import annotations

from tributary.sdk import Message, step


@step
def passthrough(message: Message) -> None:
    """Changes nothing. `None` means "unchanged" (`tributary.sdk.step` docstring)."""
    return None
