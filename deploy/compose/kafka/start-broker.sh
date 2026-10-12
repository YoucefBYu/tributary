#!/bin/bash
# Broker entrypoint for the lite stack. Formats KRaft storage (wiping it first whenever kafka-tls-init
# wrote a new credentials generation; the data directory is a tmpfs, so it is empty at every start
# anyway), starts the broker on the generated server.properties, then reports ready and, on request,
# grants `kafka-users-init` ALTER on the cluster (all AlterUserScramCredentials needs) using the broker's
# own certificate, for the length of that one-shot only (it revokes it; see the grant window below). No password is ever an argument of a process here: the SCRAM users are
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

# Wait until the broker answers on the admin listener (ACL-less principal: this grants nothing).
up=0
for _ in $(seq 1 60); do
  if /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server 127.0.0.1:9096 \
       --command-config "$CONF/healthcheck.properties" >/dev/null 2>&1; then
    up=1
    break
  fi
  kill -0 "$broker" 2>/dev/null || break
  sleep 2
done
if [[ $up -ne 1 ]]; then
  echo "broker did not come up" >&2
  kill -TERM "$broker" 2>/dev/null || true
  wait "$broker" || true
  exit 1
fi

# Grant window for kafka-users-init (W3-16); the loop is kafka/grant-loop.sh (unit-tested with a stub kafka-acls). The one-shot writes a random nonce to $GRANT/request; this loop
# grants ALTER on the cluster and answers by writing the same nonce to $GRANT/granted (only after
# kafka-acls has returned success). The one-shot revokes its own ACL and removes its request when done; this
# loop removes the ACL too when the request is gone, and ALWAYS once the grant is older than TTL seconds, so
# a killed one-shot cannot leave ALTER in place. Each side deletes only files it created (the directory is
# sticky, mode 1770, group 10002). Re-run safe: every new nonce gets a new grant. No secret in the directory.
export GRANT=/var/run/tb-grant TB_ACLS_BIN=/opt/kafka/bin/kafka-acls.sh
export TB_ACL_ARGS="--bootstrap-server 127.0.0.1:9096 --command-config $CONF/broker-client.properties --allow-principal User:CN=tb-lite-users-init --operation Alter --cluster"
rm -f "$GRANT/granted"
$TB_ACLS_BIN $TB_ACL_ARGS --remove --force >/dev/null 2>&1 || true
bash /opt/tb/grant-loop.sh "$broker" &
touch /tmp/tb-ready
# `wait` returns early when the trap fires; wait again until the broker is really gone.
while kill -0 "$broker" 2>/dev/null; do wait "$broker" || true; done
