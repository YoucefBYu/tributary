"""Synthetic channel XML hand-authored from the public layout. No real configuration.
HL7 sender in any sample message would be TRIBUTARY-SYNTH; none is needed here."""

from importer import import_channel

PASSTHROUGH = """<channel version="3.x"><name>TRIBUTARY-SYNTH passthrough</name>
<sourceConnector><properties class="com.mirth.connect.connectors.tcp.TcpReceiverProperties">
<listenerConnectorProperties><host>0.0.0.0</host><port>6670</port></listenerConnectorProperties>
<maxConnections>5</maxConnections><receiveTimeout>30000</receiveTimeout>
<charsetEncoding>UTF-8</charsetEncoding></properties>
<filter><elements/></filter><transformer><elements/></transformer></sourceConnector>
<destinationConnectors/></channel>"""

WITH_JS = PASSTHROUGH.replace(
    "<elements/></transformer>",
    "<elements><com.x.JavaScriptStep><name>fix</name><script>msg['PID']['PID.5'] = 'X';</script>"
    "</com.x.JavaScriptStep></elements></transformer>",
).replace("<charsetEncoding>UTF-8</charsetEncoding>",
          "<charsetEncoding>UTF-8</charsetEncoding><keystorePassword>hunter2</keystorePassword>")


def test_passthrough_is_fully_automatic() -> None:
    r = import_channel(PASSTHROUGH)
    assert r["fullyAutomatic"] and r["source"]["port"] == 6670
    assert r["source"]["ack"] == "after-commit"


def test_js_step_flagged_and_secret_dropped() -> None:
    r = import_channel(WITH_JS)
    assert not r["fullyAutomatic"]
    assert "hunter2" not in repr(r)
    assert r["steps"][0]["python"].startswith("def step(message, ctx)")
    assert any("JavaScript" in t for t in r["needsAttention"])


def test_destination_is_flagged_not_silently_dropped() -> None:
    xml = PASSTHROUGH.replace(
        "<destinationConnectors/>",
        '<destinationConnectors><connector><name>d1</name>'
        '<properties class="com.x.HttpDispatcherProperties"/></connector></destinationConnectors>',
    )
    r = import_channel(xml)
    assert not r["fullyAutomatic"]
    assert any("d1" in t for t in r["needsAttention"])
