"""The response validator has to be able to fail: a valid example passes, and each kind of break is caught."""

from __future__ import annotations

import json
import re
from pathlib import Path

import httpx
import pytest

from tests.support.openapi_contract import OpenApiContract

EXAMPLES = Path(__file__).resolve().parents[2] / "contracts" / "openapi" / "v0" / "examples" / "valid"
SEARCH = "/v1/environments/e1/channels/c1/messages/search"


def response(path: str, body: object, *, status: int = 200, media: str = "application/json") -> httpx.Response:
    return httpx.Response(
        status, json=body, headers={"content-type": media}, request=httpx.Request("POST", f"http://x{path}?q=1")
    )


def test_a_valid_example_passes_and_a_broken_one_fails() -> None:
    contract = OpenApiContract()
    page = json.loads((EXAMPLES / "response-message-search-page.json").read_text())
    contract.check("POST", response(SEARCH, page))
    broken = {**page, "unexpected": 1}
    with pytest.raises(AssertionError):
        contract.check("POST", response(SEARCH, broken))


def test_an_undeclared_status_and_a_wrong_media_type_fail() -> None:
    contract = OpenApiContract()
    with pytest.raises(AssertionError):
        contract.check("POST", response(SEARCH, {}, status=418))
    with pytest.raises(AssertionError):
        contract.check("POST", response(SEARCH, {"items": []}, media="text/plain"))


def test_an_unknown_path_fails_and_a_template_path_resolves() -> None:
    contract = OpenApiContract()
    assert contract.template_for("/v1/environments/e1/channels/c1/replays/abc") == (
        "/v1/environments/{env}/channels/{channel}/replays/{replay_id}"
    )
    with pytest.raises(AssertionError):
        contract.template_for("/v1/nothing")


def test_problem_bodies_are_validated_for_5xx_and_a_wrong_problem_fails() -> None:
    contract = OpenApiContract()
    problem = json.loads((EXAMPLES / "response-problem-ordering-would-break.json").read_text())
    contract.check("POST", response(SEARCH, problem, status=503, media="application/problem+json"))
    with pytest.raises(AssertionError):
        contract.check("POST", response(SEARCH, {"title": 1}, status=503, media="application/problem+json"))


def test_a_declared_empty_response_must_have_an_empty_body() -> None:
    contract = OpenApiContract()
    # No operation here declares a body-less 2xx; build the case on a copy of the document.
    contract.doc["paths"]["/v1/environments/{env}/channels/{channel}/messages/search"]["post"]["responses"]["204"] = {
        "description": "no content"
    }
    empty = httpx.Response(204, request=httpx.Request("POST", f"http://x{SEARCH}"))
    contract.check("POST", empty)
    with pytest.raises(AssertionError):
        contract.check("POST", response(SEARCH, {"x": 1}, status=204))


def test_the_most_specific_template_wins() -> None:
    contract = OpenApiContract()
    contract._templates.append((re.compile(r"^/v1/channels/[^/]+$"), "/v1/channels/{name}"))
    contract._templates.append((re.compile(r"^/v1/channels/validate$"), "/v1/channels/validate"))
    assert contract.template_for("/v1/channels/validate") == "/v1/channels/validate"
    assert contract.template_for("/v1/channels/abc") == "/v1/channels/{channel}"  # equal specificity: the first listed
