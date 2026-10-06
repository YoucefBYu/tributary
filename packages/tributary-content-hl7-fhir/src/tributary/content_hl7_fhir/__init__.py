"""Reference HL7 v2 to FHIR R4 mapping steps: ADT A01/A03/A08 and ORU R01.

    from tributary.content_hl7_fhir import adt_to_fhir, oru_r01_to_fhir

Both are ordinary `(message, ctx)` steps (`tributary.sdk.step`), unit-testable with no Kafka
(`tributary.sdk.testing.run_step`/`run_pipeline`) and no network (`tributary.fhir.fake_fhir_context`).
Each expects `tributary.hl7v2.parse` to have already run in the same pipeline. See `adt.py` and `oru.py`
for the mapping rules and the identifier system each resource type's Bundle entry is conditional on, and
`docs/MAPPING.md` for the short, one-page version with the public IG citations.

Built clean-room from the public HL7 v2-to-FHIR Implementation Guide's mapping pages (CLAUDE.md
invariant 10): no code from Mirth, the Open Integration Engine, or any other MPL/GPL source.
"""

from __future__ import annotations

from .adt import adt_to_fhir
from .errors import MappingError, RequiredFieldMissingError, UnsupportedTriggerError
from .oru import oru_r01_to_fhir
from .patient import map_patient

__all__ = [
    "MappingError",
    "RequiredFieldMissingError",
    "UnsupportedTriggerError",
    "adt_to_fhir",
    "map_patient",
    "oru_r01_to_fhir",
]
