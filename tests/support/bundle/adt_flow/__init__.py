"""Test-owned stand-in for the channel bundle the contract example names (`adt_flow.*`).

A `ref:` step is bundle code by design (`tributary.runtime.steps`): the example's own
`adt_flow.filters.only_admit_transfer_discharge` and `adt_flow.mapping.to_adt_event_row` are the
authoring team's code, not something the engine ships. The e2e test puts this directory on the worker's
`PYTHONPATH`. The FHIR mapping is NOT stubbed here: the flow uses the real W2-3 step
`uses: tributary.content_hl7_fhir.adt`.
"""
