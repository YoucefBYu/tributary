"""The ClickHouse write path: `clickhouse-connect` (human-approved), batch inserts, no hidden retries.

**Retries are the archiver's, not the driver's.** `query_retries=0`; the urllib3 `retries` budget
(`http_retries`, default 1 inside the driver) is set to 0 after construction.

**The driver's one-shot resend** (`http_sync.py`, "remotely closed connection": on `ConnectionResetError` or
`BrokenPipeError` it re-sends once, whatever `query_retries` says) has no supported switch. It is made
unreachable through the supported `pool_mgr` argument instead: `_NoResendPoolManager` re-raises every
transport error as a plain `urllib3.exceptions.HTTPError` with no `__cause__` or `__context__`, so the
driver's `isinstance` test on those never matches and the error surfaces as `OperationalError`, which this
module turns into `SinkError`. Alongside it, `Connection: close` is sent so a pooled keep-alive socket
that the server dropped can never be reused (without that, every idle gap would fail instead of silently
resending). Cost: one TCP (and TLS) handshake per batch; measured in `tests/test_bench_throughput.py`.
Tested by `test_no_resend_*`. This relies on the driver's private exception-chain check, so a
clickhouse-connect upgrade must re-run those tests (the dependency is pinned to the tested minor).

**Transport.** TLS is on by default. Plain HTTP is accepted only to a loopback host (`config.py`), and
the process environment's `HTTP_PROXY`/`HTTPS_PROXY` are never read: our own pool manager is passed in, so
the driver's proxy lookup is bypassed (the `clickhouse` loader ignores them as well).

No exception text leaves this module: the driver can echo server messages and `INSERT` fragments, so
`SinkError` is raised `from None` with a fixed sentence and a stable `error_type` class name only.
"""

from __future__ import annotations

import contextlib
import ipaddress
import logging
import re
import ssl
import struct
from collections.abc import Sequence
from dataclasses import dataclass
from ssl import CertificateError
from typing import Any, Final, Protocol

import urllib3

from .errors import ArchiverConfigError, SinkError
from .event import COLUMNS
from .schema import TABLE

#: ClickHouse server codes where a retry of the same batch cannot succeed without operator action:
#: 16 NO_SUCH_COLUMN_IN_TABLE, 47 UNKNOWN_IDENTIFIER, 53 TYPE_MISMATCH, 60 UNKNOWN_TABLE, 81 UNKNOWN_DATABASE,
#: 497 ACCESS_DENIED, 516 AUTHENTICATION_FAILED.
PERMANENT_CODES: Final = frozenset({16, 47, 53, 60, 81, 497, 516})
_CODE = re.compile(r"Code: (\d{1,4})")
#: Raised by the driver while it turns a Python value into a column (a string for a UUID, a number out of range).
#: Local, deterministic and a property of the row: permanent, so the loop can isolate the row instead of retrying.
LOCAL_CONVERSION_ERRORS: Final = (
    AttributeError,
    TypeError,
    ValueError,
    OverflowError,
    KeyError,
    IndexError,
    UnicodeError,
    struct.error,
)


def is_permanent(exc: BaseException) -> bool:
    """Whether retrying the same insert is pointless. Reads the driver's text only to extract a number;
    nothing from it is logged or kept."""
    from clickhouse_connect.driver import (
        exceptions as ch_exc,  # extra `clickhouse`; lazy so importing this module needs no driver
    )

    if isinstance(exc, ch_exc.OperationalError):
        return False
    if isinstance(exc, ch_exc.DataError | ch_exc.ProgrammingError):
        return True
    if isinstance(exc, LOCAL_CONVERSION_ERRORS):
        return True  # the driver could not serialise a value: retrying the same bytes fails the same way
    match = _CODE.search(str(exc))
    return match is not None and int(match.group(1)) in PERMANENT_CODES


class Sink(Protocol):
    def insert(self, rows: Sequence[Sequence[Any]]) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ClickhouseTarget:
    host: str
    port: int
    database: str
    username: str
    password: str = ""
    secure: bool = True
    ca_cert: str | None = None  # None with secure=True means certifi's bundle (the only runtime use of certifi)
    connect_timeout_s: int = 5
    send_receive_timeout_s: int = 30

    def __post_init__(self) -> None:
        if not self.secure and not is_loopback(self.host):
            raise ArchiverConfigError("plain HTTP to ClickHouse is only permitted for a loopback host")

    def __repr__(self) -> str:  # never print the password
        return (
            f"ClickhouseTarget(host={self.host!r}, port={self.port}, database={self.database!r}, secure={self.secure})"
        )


_LOOPBACK_WORDS: Final = frozenset({"localhost", "ip6-localhost"})


def is_loopback(host: str) -> bool:
    """Same rule as `tributary.loaders.clickhouse.config._is_loopback`."""
    if host in _LOOPBACK_WORDS:
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


class _TlsVerifyError(urllib3.exceptions.HTTPError):
    """Marks a refused certificate or hostname check. Carries no cause: only the marker survives."""


def _is_tls_verify_failure(exc: BaseException) -> bool:
    seen: set[int] = set()
    stack: list[BaseException | None] = [exc]
    while stack:
        current = stack.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ssl.SSLCertVerificationError | CertificateError):
            return True
        stack.extend([current.__cause__, current.__context__, getattr(current, "reason", None)])
        stack.extend(a for a in current.args if isinstance(a, BaseException))
    return False


def quiet_driver_logging() -> None:
    """The driver logs tracebacks on stream errors (`streaming.py`, `transform.py`) that can carry values.
    Nothing it logs is wanted: failures reach us as exceptions, which `SinkError` reduces to a class name."""
    driver_logger = logging.getLogger("clickhouse_connect")
    driver_logger.setLevel(logging.CRITICAL + 1)
    driver_logger.propagate = False
    if not any(isinstance(h, logging.NullHandler) for h in driver_logger.handlers):
        driver_logger.addHandler(logging.NullHandler())
    urllib3_logger = logging.getLogger("urllib3.connectionpool")  # "Retrying ... after connection broken by <err>"
    urllib3_logger.setLevel(logging.CRITICAL + 1)


class _NoResendPoolManager(urllib3.PoolManager):
    """Hides the cause of a transport error from the driver so its resend-once path cannot trigger."""

    def request(self, method: str, url: str, **kwargs: Any) -> Any:  # type: ignore[override]
        tls_failure = False
        try:
            return super().request(method, url, **kwargs)
        except urllib3.exceptions.HTTPError as exc:
            tls_failure = _is_tls_verify_failure(exc)
        # Raised outside the `except` block on purpose: inside it, `__context__` would carry the original.
        raise _TlsVerifyError("tls verification failed") if tls_failure else urllib3.exceptions.HTTPError("transport")


def _code(exc: BaseException) -> str:
    return "TLS_VERIFY_FAILED" if isinstance(exc.__cause__, _TlsVerifyError) else "SINK_ERROR"


def build_pool(target: ClickhouseTarget) -> urllib3.PoolManager:
    from clickhouse_connect.driver.httputil import get_pool_manager_options

    options = get_pool_manager_options(
        ca_cert=(target.ca_cert or "certifi") if target.secure else None,
        verify=target.secure,
        retries=0,
    )
    return _NoResendPoolManager(**options)


def connect(target: ClickhouseTarget) -> Any:
    """A client with driver-side retries off. Raises `SinkError` (fixed text) if the server is unreachable."""
    try:
        import clickhouse_connect
    except ImportError:
        raise SinkError(
            "clickhouse-connect is not installed; install tributary-archiver[clickhouse]",
            transient=False,
            code="DRIVER_MISSING",
        ) from None
    quiet_driver_logging()
    pool = build_pool(target)
    try:
        client = clickhouse_connect.get_client(
            host=target.host,
            port=target.port,
            username=target.username,
            password=target.password,
            database=target.database,
            secure=target.secure,
            verify=target.secure,
            ca_cert=(target.ca_cert or "certifi") if target.secure else None,
            query_retries=0,
            connect_timeout=target.connect_timeout_s,
            send_receive_timeout=target.send_receive_timeout_s,
            compress="lz4",
            autogenerate_session_id=False,
            pool_mgr=pool,
            headers={"Connection": "close"},
        )
    except Exception as exc:
        pool.clear()
        raise SinkError(f"clickhouse connect failed ({type(exc).__name__})", code=_code(exc)) from None
    driver: Any = client
    driver.http_retries = 0  # the pool-level retry budget; see module doc
    driver.tb_pool = pool  # closed with the client: the driver does not own a pool it was handed
    return client


class ClickhouseSink:
    """Inserts one batch per call. Thread-confined: one client per archiver process (a session is not shared)."""

    def __init__(self, client: Any, *, table: str = TABLE, columns: Sequence[str] = COLUMNS) -> None:
        self._client = client
        self._table = table
        self._columns = list(columns)

    def insert(self, rows: Sequence[Sequence[Any]]) -> None:
        if not rows:
            return
        try:
            self._client.insert(self._table, list(rows), column_names=self._columns)
        except Exception as exc:
            raise SinkError(
                f"clickhouse insert failed ({type(exc).__name__})", transient=not is_permanent(exc), code=_code(exc)
            ) from None

    def close(self) -> None:
        with contextlib.suppress(Exception):  # shutdown: nothing useful to do with a failed close
            self._client.close()
            pool = getattr(self._client, "tb_pool", None)
            if pool is not None:
                pool.clear()
