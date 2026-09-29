from __future__ import annotations

import json

from tributary.hl7v2 import Hl7Message
from tributary.sdk import Context, Message, step


@step
def to_adt_event_row(message: Message, ctx: Context) -> Message:
    """One flat ClickHouse row; the loader adds `tb_msg_id` itself (idColumn)."""
    assert message.payload is not None
    hl7 = Hl7Message.parse(message.payload)
    kind = hl7.message_type()
    row = {
        "event_type": f"{kind[0]}_{kind[1]}",
        "mrn": hl7.get("PID.3.1") or "",
        "visit_number": hl7.get("PV1.19.1") or "",
    }
    return message.with_payload(json.dumps(row).encode(), content_type="application/json")
