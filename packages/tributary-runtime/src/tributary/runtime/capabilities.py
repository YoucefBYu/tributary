"""The runtime's copy of the context-capability registry (`contracts/context-extension/v0/capabilities.json`).

A copy, not a file read at startup: the runtime must not need `contracts/` on disk to refuse a spec,
and a copy drifting from the contract is caught by a two-way test (`tests/test_capabilities_registry.py`),
the same arrangement as the engine error-code registry. Adding a name is a contract change (architect
approval plus `security-reviewer` on the package, `context-extension/v0` section 4), then a line here.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from types import MappingProxyType

#: The entry-point group (`context-extension/v0` section 1).
ENTRY_POINT_GROUP = "tributary.context"

#: Capability name grammar: the entry-point name and a `spec.pipeline.context` item.
NAME_PATTERN = re.compile(r"[a-z][a-z0-9_]{0,31}")

#: Capability name -> the one distribution (PEP 503 normalised) that may supply it.
CAPABILITIES: Mapping[str, str] = MappingProxyType({"fhir": "tributary-fhir"})


#: ASCII upper case to lower case, and nothing else.
_ASCII_LOWER = str.maketrans("ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz")


def normalise_distribution(name: str) -> str:
    """PEP 503: lowercase, and every run of `-`, `_` or `.` becomes one `-`.

    ASCII lowercasing only. `str.lower()` folds some non-ASCII letters into ASCII (U+212A KELVIN SIGN
    becomes `k`), which would let a look-alike name normalise onto a registered one. A non-ASCII
    character survives here unchanged, so it can never equal an (ASCII) registry value. To refuse such
    a name outright, use `provenance.canonical_distribution`.
    """
    return re.sub(r"[-_.]+", "-", name).translate(_ASCII_LOWER)
