#!/bin/bash
# Broker entrypoint for the lite stack. Formats KRaft storage (wiping it first whenever kafka-tls-init
# wrote a new credentials generation, so old ACLs and users never outlive the credentials they belong
# to), then starts the broker on the generated server.properties. No password is ever an argument of a
# process here: the SCRAM users are created afterwards by kafka-users-init, from files.
set -euo pipefail
CONF=/etc/tributary/broker
DATA=/var/lib/kafka/data
CLUSTER_ID=ZEBnsPTsTmysrIwkF8yYDw

# No JMX agent. Not empty: an empty value makes kafka-run-class.sh install its unauthenticated
# `-Dcom.sun.management.jmxremote.*` defaults, and any `-Dcom.sun.management.*` flag (even `=false`)
# starts the agent with a local RMI connector. A JVM flag that has nothing to do with management
# leaves the default alone, and this one also disables the attach mechanism (jcmd, jconsole).
unset JMX_PORT
export KAFKA_JMX_OPTS=-XX:+DisableAttachMechanism

mkdir -p "$DATA"
if [[ ! -f "$DATA/meta.properties" || "$(cat "$DATA/tb-generation" 2>/dev/null || true)" != "$(cat "$CONF/generation")" ]]; then
  find "$DATA" -mindepth 1 -delete
  /opt/kafka/bin/kafka-storage.sh format --config "$CONF/server.properties" --cluster-id "$CLUSTER_ID" \
    --ignore-formatted >/dev/null
  cp "$CONF/generation" "$DATA/tb-generation"
fi
exec /opt/kafka/bin/kafka-server-start.sh "$CONF/server.properties"
