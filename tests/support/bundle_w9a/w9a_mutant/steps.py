from __future__ import annotations

import os
import time
import urllib.error
import urllib.request

from tributary.sdk import Context, Message, step

#: A blocking retry bounded at 5 x 20 ms per message while the store is unreachable: what a naive
#: "write the archive row before continuing" step would do.
_ATTEMPTS = 5
_BACKOFF_S = 0.02


@step
def archive_in_path(message: Message, ctx: Context) -> Message:
    url = os.environ["TB_W9A_MUTANT_CH_PING"]
    for _ in range(_ATTEMPTS):
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:  # noqa: S310 - loopback test store
                response.read()
            break
        except (urllib.error.URLError, OSError):
            time.sleep(_BACKOFF_S)
    return message
