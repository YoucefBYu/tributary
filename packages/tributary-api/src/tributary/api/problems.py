"""RFC 9457 problems with a closed code list (`openapi/v0` section 4, invariant 6).

`detail` is fixed text per code, there is no `instance` member, and nothing from the request (path, body,
header, claim) is ever copied into a body.
"""

from __future__ import annotations

import uuid
from typing import Any

from fastapi.responses import JSONResponse

#: code -> (default status, title, detail)
CODES: dict[str, tuple[int, str, str]] = {
    "AUTH_REQUIRED": (401, "Authentication required", "A bearer access token is required."),
    "TOKEN_INVALID": (401, "Token invalid", "The access token was not accepted."),
    "FORBIDDEN": (403, "Forbidden", "No role of the caller grants this operation."),
    "NOT_FOUND": (404, "Not found", "The resource does not exist."),
    "METHOD_NOT_ALLOWED": (405, "Method not allowed", "The method is not allowed for this resource."),
    "PAYLOAD_TOO_LARGE": (413, "Payload too large", "The request body exceeds 262144 bytes."),
    "UNSUPPORTED_MEDIA_TYPE": (415, "Unsupported media type", "The body must be application/json."),
    "REQUEST_INVALID": (400, "Request invalid", "The request is malformed or a parameter is out of range."),
    "SPEC_INVALID": (422, "Channel document invalid", "The document does not satisfy the channel schema."),
    "VERSION_SET_BY_CLIENT": (422, "Version set by client", "metadata.version is assigned by the server."),
    "CHANNEL_NAME_MISMATCH": (422, "Channel name mismatch", "The document name differs from the path."),
    "CHANNEL_EXISTS": (409, "Channel exists", "A channel with this name already exists."),
    "CURRENT_VERSION_MISMATCH": (409, "Current version mismatch", "The current version is not the expected one."),
    "MANAGEMENT_MODE": (409, "Management mode", "The environment is git-managed; only git-origin versions apply."),
    "NOTHING_TO_ROLL_BACK": (409, "Nothing to roll back", "There is no earlier published version."),
    "NOT_DEPLOYED": (409, "Not deployed", "The channel is not deployed in this environment."),
    "UNAVAILABLE": (503, "Unavailable", "A dependency is unavailable; retry later."),
    "MESSAGE_NOT_FOUND": (
        404,
        "Message not found",
        "The archive holds no event for this message; it may not have arrived yet.",
    ),
    "CONTENT_NOT_FOUND": (404, "Content not found", "No content is stored for that stage of the message."),
    "CONTENT_STORAGE_OFF": (409, "Content storage off", "Message content is not stored in this environment."),
    "ORDERING_WOULD_BREAK": (
        409,
        "Ordering would break",
        "The channel is per-key ordered; pass allow_reorder to accept it.",
    ),
    "DESTINATION_SCOPED": (409, "Destination scoped", "A destination-scoped failure cannot be re-injected into raw."),
    "CHILD_MESSAGE": (409, "Child message", "The message has a parent; reprocess the root message."),
    "NO_PAYLOAD_COORDINATES": (409, "No payload coordinates", "The archive has no raw coordinates for this message."),
    "RETENTION_LAPSED": (410, "Retention lapsed", "The original raw record is below the topic's log start."),
    "OFFSET_NOT_FOUND": (409, "Offset not found", "The raw record is not at the recorded offset."),
    "RECORD_MISMATCH": (409, "Record mismatch", "The record at the recorded offset is a different message."),
    "RECORD_UNDECODABLE": (409, "Record undecodable", "The original raw record cannot be decoded."),
    "ENVELOPE_INVALID": (409, "Envelope invalid", "The record cannot be re-published: its envelope is invalid."),
    "REQUEST_ID_REUSED": (409, "Request id reused", "That id was used with a different request."),
    "SOURCE_ERROR": (503, "Source error", "The raw topic could not be read; retry later."),
    "SOURCE_TIMEOUT": (503, "Source timeout", "Reading the raw topic timed out; retry later."),
    "PRODUCE_FAILED": (503, "Produce failed", "The broker did not acknowledge the re-published record."),
    "PRODUCE_TIMEOUT": (503, "Produce timeout", "The broker did not acknowledge the re-published record in time."),
    "PRODUCE_REFUSED": (503, "Produce refused", "The re-publish was refused before it was sent."),
    "TOO_MANY_STREAMS": (
        429,
        "Too many streams",
        "This subject already holds the maximum number of open event streams.",
    ),
    "AUDIT_UNAVAILABLE": (
        503,
        "Audit unavailable",
        "The audit record could not be committed, so the action was not performed.",
    ),
    "ARCHIVE_UNAVAILABLE": (503, "Archive unavailable", "The message archive is unavailable; retry later."),
    "INTERNAL": (500, "Internal error", "The request failed."),
}


class ApiProblem(Exception):
    """Raised anywhere in a request; rendered by one handler."""

    def __init__(
        self,
        code: str,
        *,
        status: int | None = None,
        errors: list[dict[str, str]] | None = None,
        headers: dict[str, str] | None = None,
        republished: int | None = None,
    ) -> None:
        super().__init__(code)
        self.republished = republished
        self.code = code
        self.status = status if status is not None else CODES[code][0]
        self.errors = errors
        self.headers = headers or {}


def unauthorized(code: str, *, token_sent: bool) -> ApiProblem:
    challenge = 'Bearer error="invalid_token"' if token_sent else "Bearer"
    return ApiProblem(code, headers={"WWW-Authenticate": challenge})


def render(problem: ApiProblem, request_id: str | None) -> JSONResponse:
    _, title, detail = CODES[problem.code]
    body: dict[str, Any] = {
        "type": "about:blank",
        "title": title,
        "status": problem.status,
        "code": problem.code,
        "detail": detail,
        "request_id": request_id if request_id is not None else str(uuid.uuid4()),
    }
    if problem.errors:
        body["errors"] = problem.errors[:100]
    if problem.republished is not None:
        body["republished"] = min(problem.republished, 1)
    return JSONResponse(
        body, status_code=problem.status, media_type="application/problem+json", headers=problem.headers
    )
