"""Controls for the integration-count check (W3-21): it must go red when a test disappears, appears, or the file
is edited inconsistently. Fast, no collection, no Docker."""

from __future__ import annotations

from tests.support.integration_expected import diff, expected_document, modules_from_collection

OUTPUT = """tests/integration/test_a.py::test_one
tests/integration/test_a.py::test_two[x]
tests/integration/test_a.py::test_two[y]
packages/p/tests/test_b.py::test_three

164/3539 tests collected (3375 deselected) in 2.85s
"""


def test_modules_are_counted_per_test_id_including_parametrised_cases() -> None:
    assert modules_from_collection(OUTPUT.splitlines()) == {
        "packages/p/tests/test_b.py": 1,
        "tests/integration/test_a.py": 3,
    }


def test_an_exact_match_has_no_differences() -> None:
    modules = modules_from_collection(OUTPUT.splitlines())
    assert diff(expected_document(modules), modules) == []


def test_a_deleted_test_is_reported() -> None:
    modules = modules_from_collection(OUTPUT.splitlines())
    smaller = {**modules, "tests/integration/test_a.py": 2}
    problems = diff(expected_document(modules), smaller)
    assert any("test_a.py: expected 3, collected 2" in p for p in problems) and any(
        p.startswith("total") for p in problems
    )


def test_a_deleted_module_and_a_new_module_are_reported() -> None:
    modules = modules_from_collection(OUTPUT.splitlines())
    changed = {"tests/integration/test_a.py": 3, "tests/integration/test_new.py": 1}
    problems = diff(expected_document(modules), changed)
    assert any("test_b.py: expected 1, collected None" in p for p in problems)
    assert any("test_new.py: expected None, collected 1" in p for p in problems)


def test_a_file_whose_count_disagrees_with_its_modules_is_reported() -> None:
    modules = modules_from_collection(OUTPUT.splitlines())
    document = {**expected_document(modules), "count": 99}
    assert any("inconsistent" in p for p in diff(document, modules))
