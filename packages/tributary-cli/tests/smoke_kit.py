"""Shared type for the smoke fixtures (a module of its own: `conftest` is not importable by name)."""

from __future__ import annotations

from dataclasses import dataclass

from api_kit import FakePublisher, Idp


@dataclass
class LiveApi:
    url: str
    idp: Idp
    publisher: FakePublisher
    pg_url: str

    def token(self, role: str = "tb-admin") -> str:
        return self.idp.mint(roles=[role])
