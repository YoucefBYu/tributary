"""Bundle code for W3-9a's mutation control only: a step that couples the message path to the archive store.

This is the mutant of invariant 9 (the archive is off the critical path): a worker whose pipeline waits on
ClickHouse. It is loaded as a `ref` step (`w9a_mutant.steps.archive_in_path`) with this directory on the
worker's `PYTHONPATH`. No real step looks like this; it exists so the outage check can be shown to go red.
"""
