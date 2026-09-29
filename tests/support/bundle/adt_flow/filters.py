from __future__ import annotations

from tributary.hl7v2 import Hl7Message
from tributary.sdk import Context, Drop, Message, step


@step
def only_admit_transfer_discharge(message: Message, ctx: Context) -> Message | Drop:
    if message.payload is None:
        return Drop("claim-checked payload")
    kind = Hl7Message.parse(message.payload).message_type()
    return message if kind[0] == "ADT" and kind[1] in {"A01", "A02", "A03", "A08"} else Drop("not an ADT event")
