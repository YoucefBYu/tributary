#!/bin/bash
# Broker entrypoint for the lite stack: starts the broker on the storage and server.properties that
# kafka-tls-init generated and formatted (SCRAM users seeded there, in an isolated one-shot, so no
# password is ever an argument of a process in this container). No JMX.
set -euo pipefail
unset JMX_PORT
# Not empty: an empty value makes kafka-run-class.sh install its unauthenticated JMX defaults.
export KAFKA_JMX_OPTS=-Dcom.sun.management.jmxremote=false
[[ -f /var/lib/kafka/data/meta.properties ]] || { echo "storage not formatted: run kafka-tls-init" >&2; exit 1; }
exec /opt/kafka/bin/kafka-server-start.sh /etc/tributary/broker/server.properties
