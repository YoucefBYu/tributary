"""The runtime's copy of the engine step-namespace registry (`contracts/channel-spec/v0/step-namespaces.json`).

Set 1.6, channel-spec README 3.3.5 (rules N1-N6), ADR-007 D23. A `uses` name's **namespace** is its first
two dot-separated segments, and a `uses` name has at least three (N1). Every namespace whose first segment
is `tributary` is reserved to this registry (N2); any other namespace is bound per channel by
`spec.pipeline.stepNamespaces` (N3). Either way the binding is one distribution, PEP 503 normalised,
and only an entry point from that distribution is ever imported (N4, `steps.StepResolver`).

A copy, not a file read at startup, for the same reason as `capabilities.py`: the worker must not need
`contracts/` on disk to refuse a spec. `tests/test_step_namespaces.py` checks the copy against
the contract both ways. Adding an engine namespace is a contract change first, then a line here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType

#: The entry-point group every `uses` step is registered in (ADR-001 6.2).
ENTRY_POINT_GROUP = "tributary.steps"

#: N2: a namespace with this first segment is bound only by `STEP_NAMESPACES`, never by a spec.
RESERVED_FIRST_SEGMENT = "tributary"

#: A namespace: exactly two segments (the schema's `stepNamespaces` key grammar, without its length cap).
NAMESPACE_PATTERN = re.compile(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*")
NAMESPACE_MAX_LENGTH = 64

#: A binding's value: a distribution name already in PEP 503 normalised form (one spelling).
DISTRIBUTION_PATTERN = re.compile(r"[a-z0-9]+(-[a-z0-9]+)*")
DISTRIBUTION_MAX_LENGTH = 100

#: Engine namespace -> the one distribution (PEP 503 normalised) that may supply its steps.
STEP_NAMESPACES: Mapping[str, str] = MappingProxyType(
    {
        "tributary.core": "tributary-runtime",
        "tributary.hl7v2": "tributary-hl7v2",
        "tributary.content_hl7_fhir": "tributary-content-hl7-fhir",
    }
)


def namespace_of(uses: str) -> str | None:
    """N1: the first two segments of a `uses` name, or `None` when it has fewer than three."""
    segments = uses.split(".")
    if len(segments) < 3:
        return None
    return f"{segments[0]}.{segments[1]}"


def is_reserved(namespace: str) -> bool:
    """N2: whether `namespace` belongs to the engine registry and may never be bound by a spec."""
    return namespace.split(".", 1)[0] == RESERVED_FIRST_SEGMENT
