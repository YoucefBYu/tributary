import tributary.content_hl7_fhir as mod


def test_package_imports() -> None:
    assert mod.__doc__
    assert mod.adt_to_fhir.name == "tributary.content_hl7_fhir.adt"
    assert mod.oru_r01_to_fhir.name == "tributary.content_hl7_fhir.oru_r01"
