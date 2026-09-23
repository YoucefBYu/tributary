"""Publish one channel-spec example to `_tb.<env>.channels` for the lite stack.

There is no control plane yet, so nothing else stamps `metadata.version` and produces the record the
worker's `ChannelCache` reads (`tributary.runtime.channels`: "a worker's only source of truth for a
channel is this topic"). This is a compose-only stand-in for that, the same way `topic-bootstrap` is a
stand-in for a topic-creation tool -- once a CLI or control plane exists, this file should call it
instead of building the record by hand.

The key is the channel name (`ChannelCache._apply` decodes the record key as the name) and the value is
the published document: the authored file plus `metadata.version`, matching
`packages/tributary-runtime/tests/test_spec.py`'s `published()` helper and
`contracts/channel-spec/v0/examples/valid/published-with-version.json`.

If `--published-out` is given, the same published document is also written there as a file, so the
receiver and the loader can be pointed at *this* channel spec instead of the raw, unpublished contract
example -- `tributary.loaders.http.main` requires `metadata.version` outright (no default), and even
where a default exists (the receiver) using the file the worker actually applied keeps `tb-channel-
version` consistent everywhere instead of each component picking its own number.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

logging.basicConfig(level=logging.INFO, format='{"level":"%(levelname)s","msg":%(message)s}')
log = logging.getLogger("channel-publish")


def published(document: dict[str, Any], *, version: int) -> dict[str, Any]:
    """The authored form plus the version a control plane stamps at publish time."""
    document = dict(document)
    document["metadata"] = {**document["metadata"], "version": version}
    return document


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--env", default=os.environ.get("TB_ENV", "dev"))
    p.add_argument(
        "--channel-spec", type=Path, default=Path(os.environ.get("TB_CHANNEL_SPEC", "/contracts/channel.json"))
    )
    p.add_argument("--version", type=int, default=int(os.environ.get("TB_CHANNEL_VERSION", "1")))
    p.add_argument(
        "--published-out",
        type=Path,
        default=(Path(v) if (v := os.environ.get("TB_PUBLISHED_OUT")) else None),
        help="also write the published document here, e.g. for other containers to mount read-only",
    )
    args = p.parse_args(argv)

    from confluent_kafka import Producer  # deferred: keeps `published` testable without it

    document = json.loads(args.channel_spec.read_text(encoding="utf-8"))
    channel = document["metadata"]["name"]
    record = published(document, version=args.version)

    if args.published_out is not None:
        args.published_out.parent.mkdir(parents=True, exist_ok=True)
        args.published_out.write_text(json.dumps(record), encoding="utf-8")

    producer = Producer({"bootstrap.servers": args.bootstrap})
    failure: BaseException | None = None

    def _report(err: Any, _msg: Any) -> None:
        nonlocal failure
        failure = err

    producer.produce(
        f"_tb.{args.env}.channels",
        key=channel.encode("utf-8"),
        value=json.dumps(record).encode("utf-8"),
        callback=_report,
    )
    producer.flush(30)
    if failure is not None:
        log.error(json.dumps({"channel": channel, "status": "failed", "reason": str(failure)}))
        return 1
    log.info(json.dumps({"channel": channel, "version": args.version, "status": "published"}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
