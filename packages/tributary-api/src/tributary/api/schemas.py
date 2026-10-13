"""Request bodies (`openapi/v0`): strict, `additionalProperties: false`, no coercion."""

from __future__ import annotations

from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator

VersionInt = Annotated[int, Field(strict=True, ge=1, le=2147483647)]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Origin(_Strict):
    kind: Literal["git", "ui", "api"]
    commit: Annotated[str, Field(pattern=r"^([0-9a-f]{40}|[0-9a-f]{64})$")] | None = None
    path: Annotated[str, Field(pattern=r"^[A-Za-z0-9._/-]{1,256}$")] | None = None

    @model_validator(mode="after")
    def _git_needs_commit_and_path(self) -> Self:
        if self.kind == "git" and (self.commit is None or self.path is None):
            raise ValueError("git origin needs commit and path")
        return self

    def stored(self) -> dict[str, Any]:
        return self.model_dump(exclude_none=True)


class ChannelVersionCreate(_Strict):
    document: dict[str, Any]
    origin: Origin


class DeployRequest(_Strict):
    version: VersionInt
    expected_current_version: VersionInt | None = None


class RollbackRequest(_Strict):
    expected_current_version: VersionInt
    to_version: VersionInt | None = None


class UndeployRequest(_Strict):
    expected_current_version: StrictInt = Field(ge=1, le=2147483647)


# -- part 2 (archive) ---------------------------------------------------------------------------------

TIMESTAMP = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$"
UUID7 = r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
Stamp = Annotated[str, Field(pattern=TIMESTAMP)]
MessageStage = Literal[
    "received", "queued", "filtered", "transformed", "routed", "pending", "sent", "retrying", "replied", "error", "dlq"
]
Name = Annotated[str, Field(pattern=r"^[a-z][a-z0-9-]{0,63}$")]


class MessageSearchRequest(_Strict):
    since: Stamp
    until: Stamp | None = None
    stages: Annotated[list[MessageStage], Field(min_length=1, max_length=11)] | None = None
    status: Literal["ok", "error", "timeout", "dropped"] | None = None
    destination: Name | None = None
    error_code: Annotated[str, Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")] | None = None
    attrs: Annotated[dict[str, str], Field(max_length=4)] | None = None
    limit: Annotated[int, Field(strict=True, ge=1, le=200)] = 50
    cursor: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{1,512}$")] | None = None

    @model_validator(mode="after")
    def _unique_stages(self) -> Self:
        if self.stages is not None and len(set(self.stages)) != len(self.stages):
            raise ValueError("stages repeat")
        return self


class ReprocessRequest(_Strict):
    operation_id: Annotated[str, Field(pattern=UUID7)]
    allow_reorder: bool = False


class ReplayRequest(_Strict):
    replay_id: Annotated[str, Field(pattern=UUID7)]
    start: Stamp
    end: Stamp
    allow_reorder: bool = False
    max_records: Annotated[int, Field(strict=True, ge=1, le=1_000_000)] = 100_000
    deadline_s: Annotated[int, Field(strict=True, ge=1, le=3600)] = 900
