"""`fhir` destination loader: POSTs a step-built FHIR transaction Bundle to a FHIR server
(`contracts/channel-spec/v0/README.md` section 3.4.2, ADR-007 D12). See `loader.py`'s module
docstring for the idempotency argument and the outcome table, and `config.py` for what is refused."""

from __future__ import annotations

from .config import FhirDestinationConfig, fhir_destination_config_from_channel_spec
from .handler import FhirRecordHandler
from .loader import FhirLoader

__all__ = [
    "FhirDestinationConfig",
    "FhirLoader",
    "FhirRecordHandler",
    "fhir_destination_config_from_channel_spec",
]
