#!/bin/bash
# Broker entrypoint for the lite stack. Formats KRaft storage (wiping it first whenever kafka-tls-init
# wrote a new credentials generation; the data directory is a tmpfs, so it is empty at every start
# anyway), starts the broker on the generated server.properties, then grants `kafka-users-init` its one
# ACL (ALTER on the cluster: all AlterUserScramCredentials needs) using the broker's own certificate,
# and only then reports ready. No password is ever an argument of a process here: the SCRAM users are
# created afterwards by kafka-users-init, from files.
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
rm -f /tmp/tb-ready

/opt/kafka/bin/kafka-server-start.sh "$CONF/server.properties" &
broker=$!
trap 'kill -TERM "$broker" 2>/dev/null || true' TERM INT

granted=0
for _ in $(seq 1 60); do
  if /opt/kafka/bin/kafka-acls.sh --bootstrap-server 127.0.0.1:9096 --command-config "$CONF/broker-client.properties" \
       --add --allow-principal "User:CN=tb-lite-users-init" --operation Alter --cluster >/dev/null 2>&1; then
    granted=1
    break
  fi
  kill -0 "$broker" 2>/dev/null || break
  sleep 2
done
if [[ $granted -ne 1 ]]; then
  echo "could not grant kafka-users-init its ACL" >&2
  kill -TERM "$broker" 2>/dev/null || true
  wait "$broker" || true
  exit 1
fi
touch /tmp/tb-ready
# `wait` returns early when the trap fires; wait again until the broker is really gone.
while kill -0 "$broker" 2>/dev/null; do wait "$broker" || true; done
