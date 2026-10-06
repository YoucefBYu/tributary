# Provenance: fhir.schema.json

- File: `fhir.schema.json`, unmodified, as extracted from the HL7 archive.
- Source URL: https://hl7.org/fhir/R4/fhir.schema.json.zip
- FHIR version: 4.0.1 (R4). The schema's `id` is `http://hl7.org/fhir/json-schema/4.0` and it declares JSON Schema draft-06; patch level 4.0.1 is confirmed by the URL and by `CapabilityStatement.fhirVersion` ending at 4.0.1.
- Retrieved: 2026-09-28
- SHA-256 of `fhir.schema.json.zip` (as retrieved; the zip is not vendored): `75e5560da3cf503895a44c8ca7af17a83b4cca6c2cb5ba1883d2aec0d1cb5ac6`
- SHA-256 of `fhir.schema.json`: `2230406893b4cf002a4ee1e5e2bbeca22ac5d2d4931b3e9ef7b9594bbc376a01`
- Licence: CC0 1.0 (public domain dedication), as published by HL7 for FHIR specification artifacts.
- Approval: a human approved vendoring on 2026-09-28 as **test data only**, not a code dependency
  (see `docs/agent-team/STATUS.md` decisions log). It is not imported by any package source.

Used by `tests/test_golden_fhir_r4_schema.py`, which checks the SHA-256 above against the file.
