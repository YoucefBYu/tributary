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
