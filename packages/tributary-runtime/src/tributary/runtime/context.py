"""`WorkerContext`: the runtime's implementation of the SDK `ctx` protocol.

One context per message. `vars` is message-scoped and thrown away afterwards; `attrs` and `tb-usr-*`
values collected through it are applied by the worker — `attrs` onto the lifecycle events for that
message, `tb-usr-*` onto the outgoing record's headers (envelope contract 2.2: they are propagated in
headers and are NOT projected into `_tb.events`).

Validation happens here, at the call site, exactly as `FakeContext` does it. A step that sets a PHI-
shaped attr therefore fails in its own unit test rather than at the archive.
"""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from tributary.sdk.context import KeyValueStore, Logger, SecretStore
from tributary.sdk.errors import SecretNotFoundError
from tributary.sdk.guards import validate_attr, validate_attrs, validate_usr


class EnvSecretStore:
    """Resolves a `secretRef` from the process environment.

    Invariant 3: secrets resolve at component startup from the deployment's secret store, never through
    the API, or the data plane could not start while the control plane is down (channel-spec contract 6
    rule 2). A mounted-file or KMS-backed store is a deployment concern for `devops-engineer`; the
    protocol is what the SDK promises a step.
    """

    __slots__ = ("_prefix", "_values")

    def __init__(self, values: Mapping[str, str] | None = None, *, prefix: str = "TB_SECRET_") -> None:
        self._values = dict(values or {})
        self._prefix = prefix

    def get(self, ref: str) -> str:
        import os

        if ref in self._values:
            return self._values[ref]
        env_name = self._prefix + ref.upper().replace("-", "_")
        value = os.environ.get(env_name)
        if value is None:
            # The ref, never the value, and never the environment variable's contents.
            raise SecretNotFoundError(ref)
        return value


class UnavailableKeyValueStore:
    """`ctx.kv` before the control plane exists (Wave 3).

    It raises rather than silently returning `None`, because a step that reads shared state and gets a
    plausible-looking empty answer makes a wrong decision quietly.
    """

    def get(self, key: str) -> tuple[str, int] | None:
        raise NotImplementedError("ctx.kv arrives with the control plane in Wave 3")

    def put(self, key: str, value: str, *, expected_version: int | None = None) -> int:
        raise NotImplementedError("ctx.kv arrives with the control plane in Wave 3")


class WorkerContext:
    """The `ctx` a step receives inside a pipeline worker."""

    __slots__ = (
        "_attrs",
        "_kv",
        "_log",
        "_secrets",
        "_usr",
        "_vars",
        "attempt",
        "channel",
        "channel_version",
        "destination",
        "instance",
        "traceparent",
    )

    def __init__(
        self,
        *,
        channel: str,
        channel_version: int,
        instance: str,
        log: Logger,
        secrets: SecretStore | None = None,
        kv: KeyValueStore | None = None,
        destination: str | None = None,
        attempt: int = 0,
        traceparent: str | None = None,
    ) -> None:
        self.channel = channel
        self.channel_version = channel_version
        self.instance = instance
        self.destination = destination
        self.attempt = attempt
        self.traceparent = traceparent
        self._log = log
        self._secrets: SecretStore = secrets if secrets is not None else EnvSecretStore()
        self._kv: KeyValueStore = kv if kv is not None else UnavailableKeyValueStore()
        self._vars: dict[str, Any] = {}
        self._attrs: dict[str, str] = {}
        self._usr: dict[str, str] = {}

    @property
    def vars(self) -> MutableMapping[str, object]:
        return self._vars

    @property
    def log(self) -> Logger:
        return self._log

    @property
    def secrets(self) -> SecretStore:
        return self._secrets

    @property
    def kv(self) -> KeyValueStore:
        return self._kv

    def now(self) -> datetime:
        return datetime.now(UTC)

    def set_attr(self, name: str, value: str) -> None:
        validate_attr(name, value)
        candidate = {**self._attrs, name: value}
        validate_attrs(candidate)
        self._attrs = candidate

    def attrs(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._attrs))

    def set_usr(self, name: str, value: str) -> None:
        validate_usr(name, value)
        self._usr[name] = value

    def usr_headers(self) -> Mapping[str, str]:
        return MappingProxyType(dict(self._usr))

    def fork(self, *, destination: str | None = None, traceparent: str | None = None) -> WorkerContext:
        """A copy for one branch of the executor tree: a split child, or one destination's steps.

        `vars`, `attrs` and `tb-usr-*` are copied, not shared (README 3.5: each destination gets its own
        copy of the pipeline output), so an attr one destination's step sets does not appear on another
        destination's events, and a `tb-usr-*` header does not leak to another sink. `vars` is copied
        shallowly: a parsed object a pipeline step stashed is shared, and a step that mutates one in
        place rather than replacing it affects its siblings. Values are immutable by convention.
        """
        clone = WorkerContext(
            channel=self.channel,
            channel_version=self.channel_version,
            instance=self.instance,
            log=self._log,
            secrets=self._secrets,
            kv=self._kv,
            destination=destination if destination is not None else self.destination,
            attempt=self.attempt,
            traceparent=traceparent if traceparent is not None else self.traceparent,
        )
        clone._vars = dict(self._vars)
        clone._attrs = dict(self._attrs)
        clone._usr = dict(self._usr)
        return clone
