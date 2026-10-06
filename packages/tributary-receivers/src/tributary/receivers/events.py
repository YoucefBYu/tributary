"""The structured log line for the modules that have no logger handed to them (the SFTP poller, the Kafka-source
receiver, the HTTP access line, the TLS peer refusal, the lite-profile warning).

They used to `print(json.dumps(...))` or log a JSON string, which lacked `ts`, `level` and `component`
(`contracts/observability/v0` section 5). Each now goes through `tributary.runtime.logs.JsonLogger`, so the field
guard (invariant 6) and the line schema apply, with the same event names and fields as before.

The stream is looked up at write time (`_Stderr`), so a test that swaps `sys.stderr` sees the lines.
"""

from __future__ import annotations

import sys
from functools import lru_cache

from tributary.runtime.logs import JsonLogger


class _Stderr:
    """Whatever `sys.stderr` is now."""

    def write(self, text: str) -> int:
        return sys.stderr.write(text)

    def flush(self) -> None:
        sys.stderr.flush()


@lru_cache(maxsize=16)
def event_logger(component: str) -> JsonLogger:
    return JsonLogger(component, stream=_Stderr())  # type: ignore[arg-type]
