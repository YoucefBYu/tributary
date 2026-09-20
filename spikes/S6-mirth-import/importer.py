"""S6 throwaway importer prototype: TCP/MLLP listener source -> Tributary receiver spec.

Clean-room: written from the observable channel XML layout and public docs only.
Output is a plain dict (YAML/JSON-ready); it is a draft, not a contract-validated spec.
Passwords/secret-looking properties are never copied; they become secretRef TODOs.
"""

from __future__ import annotations

from typing import Any
from xml.etree import ElementTree as ET

SECRET_HINTS = ("password", "secret", "keystore", "truststore", "token")


def _text(el: ET.Element | None, path: str, default: str = "") -> str:
    if el is None:
        return default
    found = el.find(path)
    return (found.text or default).strip() if found is not None else default


def _trivial(body: str) -> bool:
    """Empty, comment-only or bare `return` bodies (the default lifecycle stubs)."""
    code = [ln.strip() for ln in body.splitlines() if ln.strip() and not ln.strip().startswith("//")]
    return all(ln in ("return;", "return true;", "return false;") for ln in code)


def _int(props: ET.Element, path: str, default: int, todo: list[str]) -> int:
    raw = _text(props, path, str(default)) or str(default)
    try:
        return int(raw)
    except ValueError:
        todo.append(f"'{path.rsplit('/', 1)[-1]}' is not a plain integer (templated?): default {default} used")
        return default


def import_tcp_receiver(props: ET.Element) -> tuple[dict[str, Any], list[str]]:
    """Map a TCP listener properties element. Returns (receiver, needs_attention)."""
    todo: list[str] = []
    mode = "server" if _text(props, "serverMode", "true") == "true" else "client"
    if mode == "client":
        todo.append("source connector in client mode: no equivalent receiver, needs a poller/connector decision")
    recv: dict[str, Any] = {
        "type": "mllp",
        "host": _text(props, "listenerConnectorProperties/host", "0.0.0.0"),
        "port": _int(props, "listenerConnectorProperties/port", 6661, todo),
        "maxConnections": _int(props, "maxConnections", 10, todo),
        "idleTimeoutMs": _int(props, "receiveTimeout", 0, todo),
        "charset": _text(props, "charsetEncoding", "DEFAULT_ENCODING"),
        "ack": "after-commit",  # invariant 1: fixed, not configurable
    }
    if recv["charset"] == "DEFAULT_ENCODING":
        recv["charset"] = "utf-8"
        todo.append("charset DEFAULT_ENCODING mapped to utf-8: confirm sender encoding")
    if _text(props, "respondOnNewConnection", "0") not in ("0", ""):
        todo.append("respondOnNewConnection has no equivalent; document gap")
    if _text(props, "dataTypeBinary", "false") == "true":
        todo.append("binary data type: payload passes as opaque bytes, verify framing")
    for child in props.iter():
        if any(h in child.tag.lower() for h in SECRET_HINTS) and (child.text or "").strip():
            recv.setdefault("secretRefs", {})[child.tag] = "TODO-secretRef"
            todo.append(f"credential-like property '{child.tag}' dropped; supply secretRef")
    return recv, todo


def import_channel(xml_text: str) -> dict[str, Any]:
    chan = ET.fromstring(xml_text)
    todo: list[str] = []
    props = chan.find("sourceConnector/properties")
    cls = (props.get("class") if props is not None else "") or ""
    receiver: dict[str, Any] | None = None
    if props is not None and cls.endswith("TcpReceiverProperties"):
        receiver, t = import_tcp_receiver(props)
        todo += t
    else:
        todo.append(f"source connector type '{cls.rsplit('.', 1)[-1] or '?'}' not supported by prototype")

    steps: list[dict[str, str]] = []
    for kind in ("sourceConnector/filter", "sourceConnector/transformer"):
        for el in chan.findall(f"{kind}/elements/*"):
            body = _text(el, "script")
            name = _text(el, "name") or el.tag.rsplit(".", 1)[-1]
            if body:
                commented = "\n".join("# JS: " + ln for ln in body.splitlines())
                steps.append({"name": name, "kind": kind.rsplit("/", 1)[-1],
                              "python": "def step(message, ctx):\n    raise NotImplementedError\n\n" + commented})
                todo.append(f"step '{name}': JavaScript body needs porting")
            else:
                todo.append(f"step '{name}': non-script step type '{el.tag.rsplit('.', 1)[-1]}' needs manual mapping")
    life = ("preprocessingScript", "postprocessingScript", "deployScript", "undeployScript")
    for tag in life:
        for el in chan.iter(tag):
            if (el.text or "").strip() and not _trivial(el.text or ""):
                todo.append(f"lifecycle script '{tag}' is non-trivial: needs porting")
    for d in chan.findall("destinationConnectors/connector"):
        for el in d.iter("script"):
            if (el.text or "").strip() and not _trivial(el.text or ""):
                todo.append(f"destination '{_text(d, 'name') or '?'}': filter/transformer script needs porting")
        dcls = ((d.find("properties").get("class") if d.find("properties") is not None else "")) or ""
        todo.append(f"destination '{_text(d, 'name') or '?'}' ({dcls.rsplit('.', 1)[-1] or '?'}): not supported by prototype")
    return {
        "name": _text(chan, "name"),
        "source": receiver,
        "steps": steps,
        "needsAttention": todo,
        "fullyAutomatic": not todo and receiver is not None,
    }
