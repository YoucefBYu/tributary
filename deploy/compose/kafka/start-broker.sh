#!/bin/bash
# Broker entrypoint for the lite stack. Formats KRaft storage with the per-component SCRAM users that
# kafka-tls-init generated (SCRAM users live in the cluster metadata, and in KRaft mode the way to
# seed them is `kafka-storage format --add-scram`), then starts the broker. Storage is reformatted
# whenever the credentials' generation changes, so users and files cannot disagree.
set -euo pipefail
CONF=/etc/tributary/broker
DATA=/var/lib/kafka/data
BIN=/opt/kafka/bin
CLUSTER_ID=ZEBnsPTsTmysrIwkF8yYDw

mkdir -p "$DATA"
if [[ ! -f "$DATA/meta.properties" || "$(cat "$DATA/tb-generation" 2>/dev/null || true)" != "$(cat "$CONF/generation")" ]]; then
  find "$DATA" -mindepth 1 -delete
  args=()
  while IFS=: read -r name password; do
    args+=(--add-scram "SCRAM-SHA-512=[name=$name,password=$password]")
  done <"$CONF/scram-users"
  "$BIN/kafka-storage.sh" format --config "$CONF/server.properties" --cluster-id "$CLUSTER_ID" \
    --ignore-formatted "${args[@]}" >/dev/null
  cp "$CONF/generation" "$DATA/tb-generation"
fi
exec "$BIN/kafka-server-start.sh" "$CONF/server.properties"
