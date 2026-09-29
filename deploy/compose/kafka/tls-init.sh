#!/bin/bash
# One-shot init for the lite stack's Kafka security (ADR-008 D5, engine-kafka/v0 README section 6).
# Runs as root inside the apache/kafka image (which carries bash, openssl and keytool), writes into
# two Compose-managed volumes, and exits. Nothing it writes is committed, baked into an image, or
# printed: this script never runs with xtrace, never echoes a secret, and never puts one on a command
# line that outlives it.
#
#   /secrets     (volume tb-kafka-secrets, mounted per component by SUBPATH, read-only)
#     tb-lite-kafka-ca/ca.crt            the trust bundle every client mounts (tls.secretRef)
#     tb-lite-<component>/{username,password}   one SCRAM-SHA-512 principal per component
#     broker/...                         the broker's own material; mounted only into `kafka`
#   /ca-private  (volume tb-kafka-ca-private, mounted ONLY here)
#     ca.key                             the CA private key: no component ever sees it
#
# Idempotent: if a valid set already exists it is kept, because rotating credentials under a running
# broker would strand every client. Regeneration (first `up`, or under 7 days of validity left) writes a
# new `generation` id, and the broker wrapper reformats its storage when that id changes, so the SCRAM
# users in the cluster metadata always match the files. `docker compose down -v` resets everything.
set -euo pipefail
umask 077

SECRETS=/secrets
CAPRIV=/ca-private
CA_DAYS=30
RENEW_SECONDS=$((7 * 24 * 3600))
# uid of the non-root application images (deploy/compose/images, topic-bootstrap, channel-publish) and
# of `appuser` in the apache/kafka image.
APP_UID=10001
BROKER_UID=1000
COMPONENTS=(receiver worker loader channel-publish topic-bootstrap admin)

# A changed script (new listeners, new users, new cipher pin) must not be masked by stale output.
SCRIPT_SUM=$(sha256sum "${BASH_SOURCE[0]}" | cut -d' ' -f1)

if [[ -f "$SECRETS/generation" && "$(cat "$SECRETS/script.sha256" 2>/dev/null || true)" == "$SCRIPT_SUM" && -f "$CAPRIV/ca.key" && -f "$SECRETS/tb-lite-kafka-ca/ca.crt" ]] \
   && openssl x509 -checkend "$RENEW_SECONDS" -noout -in "$SECRETS/tb-lite-kafka-ca/ca.crt" >/dev/null 2>&1 \
   && openssl x509 -checkend "$RENEW_SECONDS" -noout -in "$SECRETS/broker/broker.pem" >/dev/null 2>&1; then
  echo "kafka-tls-init: existing credentials are valid, keeping them"
  exit 0
fi

echo "kafka-tls-init: generating dev CA, broker certificate and per-component SCRAM users"
find "$SECRETS" -mindepth 1 -delete
find "$CAPRIV" -mindepth 1 -delete
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

rand() { openssl rand -hex 32; }

# --- dev CA (EC P-256). The key stays in its own volume. -----------------------------------------
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days "$CA_DAYS" \
  -subj "/CN=tributary-lite-dev-ca" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "$CAPRIV/ca.key" -out "$tmp/ca.crt" 2>/dev/null

# --- broker certificate: SANs match every name a client (or the broker itself) dials -----------
# kafka: the Compose service name (SASL_SSL client listener). 127.0.0.1 and localhost: the
# host-published listener and the inter-broker listener, both loopback.
openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$tmp/broker.key"
openssl req -new -key "$tmp/broker.key" -subj "/CN=kafka" -out "$tmp/broker.csr"
cat >"$tmp/broker.ext" <<EXT
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1
EXT
openssl x509 -req -in "$tmp/broker.csr" -CA "$tmp/ca.crt" -CAkey "$CAPRIV/ca.key" -CAcreateserial \
  -days "$CA_DAYS" -extfile "$tmp/broker.ext" -out "$tmp/broker.crt" 2>/dev/null

# --- directories ------------------------------------------------------------------------------
install -d -m 0500 -o "$APP_UID" -g "$APP_UID" "$SECRETS/tb-lite-kafka-ca"
install -m 0444 -o "$APP_UID" -g "$APP_UID" "$tmp/ca.crt" "$SECRETS/tb-lite-kafka-ca/ca.crt"

: >"$tmp/scram-users"
declare -A PASS
for c in "${COMPONENTS[@]}"; do
  user="tb-lite-$c"
  PASS[$c]=$(rand)
  install -d -m 0500 -o "$APP_UID" -g "$APP_UID" "$SECRETS/$user"
  printf '%s' "$user" >"$SECRETS/$user/username"
  printf '%s' "${PASS[$c]}" >"$SECRETS/$user/password"
  chown "$APP_UID:$APP_UID" "$SECRETS/$user/username" "$SECRETS/$user/password"
  chmod 0400 "$SECRETS/$user/username" "$SECRETS/$user/password"
  printf '%s:%s\n' "$user" "${PASS[$c]}" >>"$tmp/scram-users"
done
BROKER_PASS=$(rand)
printf '%s:%s\n' "tb-lite-broker" "$BROKER_PASS" >>"$tmp/scram-users"

# --- broker material ---------------------------------------------------------------------------
B="$SECRETS/broker"
install -d -m 0500 -o "$BROKER_UID" -g "$BROKER_UID" "$B"
install -m 0400 "$tmp/ca.crt" "$B/ca.crt"
cat "$tmp/broker.key" "$tmp/broker.crt" >"$B/broker.pem"
install -m 0400 "$tmp/scram-users" "$B/scram-users"
openssl rand -hex 8 >"$B/generation"

# AEAD-only suites, TLS 1.2+ (Java names). Same intent as the clients' ECDHE+AESGCM:ECDHE+CHACHA20.
SUITES="TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256,\
TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,\
TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,\
TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
SCRAM=org.apache.kafka.common.security.scram.ScramLoginModule

cat >"$B/server.properties" <<PROPS
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
controller.listener.names=CONTROLLER
inter.broker.listener.name=BROKER
# No listener is PLAINTEXT. CONTROLLER and BROKER (inter-broker) are SASL_SSL and bound to 127.0.0.1
# inside this container; CLIENT is what containers dial; EXTERNAL is the host-published,
# loopback-mapped one. (engine-kafka/v0 README section 6 rule 1 would allow a loopback PLAINTEXT
# controller, but then the controller's own requests arrive as User:ANONYMOUS, and super.users must
# never contain ANONYMOUS (rule 3). Authenticating the controller link as the broker principal
# satisfies both.)
listeners=CONTROLLER://127.0.0.1:9093,BROKER://127.0.0.1:9095,CLIENT://0.0.0.0:9094,EXTERNAL://0.0.0.0:19092
advertised.listeners=BROKER://127.0.0.1:9095,CLIENT://kafka:9094,EXTERNAL://127.0.0.1:19092
listener.security.protocol.map=CONTROLLER:SASL_SSL,BROKER:SASL_SSL,CLIENT:SASL_SSL,EXTERNAL:SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
sasl.mechanism.controller.protocol=SCRAM-SHA-512
listener.name.controller.scram-sha-512.sasl.jaas.config=$SCRAM required username="tb-lite-broker" password="$BROKER_PASS";
listener.name.broker.scram-sha-512.sasl.jaas.config=$SCRAM required username="tb-lite-broker" password="$BROKER_PASS";
listener.name.client.scram-sha-512.sasl.jaas.config=$SCRAM required;
listener.name.external.scram-sha-512.sasl.jaas.config=$SCRAM required;

ssl.keystore.type=PEM
ssl.keystore.location=/etc/tributary/broker/broker.pem
ssl.truststore.type=PEM
ssl.truststore.location=/etc/tributary/broker/ca.crt
ssl.client.auth=none
ssl.endpoint.identification.algorithm=https
ssl.enabled.protocols=TLSv1.3,TLSv1.2
ssl.cipher.suites=$SUITES

# Deny by default. super.users: the broker, the topic-creation job (the control-plane stand-in) and
# the operator's admin principal. Never User:ANONYMOUS, never a data-plane component.
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:tb-lite-broker;User:tb-lite-topic-bootstrap;User:tb-lite-admin

log.dirs=/var/lib/kafka/data

# Lite profile (topic-defaults.json profiles.lite, ADR-002 D14, human-approved 2026-09-21): ONE broker,
# so RF=1 and min.insync.replicas=1. An explicit, documented exception to invariant 1's durability
# promise, not a relaxation of it. Not in must_not_hardcode: these are fixed by "one broker", not
# measured. Topic-level min.insync.replicas and unclean election are still set per topic by
# topic-bootstrap from the contract file.
# auto.create.topics stays off so nothing but topic-bootstrap creates a topic. The internal-topic
# factors default to 3 and can never be satisfied on one broker: without them __consumer_offsets is
# never created and every consumer group hangs at FIND_COORDINATOR (found by qa-engineer-2, W1-8).
default.replication.factor=1
min.insync.replicas=1
unclean.leader.election.enable=false
auto.create.topics.enable=false
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
PROPS
chmod 0400 "$B/server.properties"

# Healthcheck client: the admin principal over the real SASL_SSL listener, so "healthy" proves TLS and
# SCRAM work, not just that a port is open.
cat >"$B/healthcheck.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=$SCRAM required username="tb-lite-admin" password="${PASS[admin]}";
ssl.truststore.type=PEM
ssl.truststore.location=/etc/tributary/broker/ca.crt
ssl.endpoint.identification.algorithm=https
PROPS
chmod 0400 "$B/healthcheck.properties"
chown -R "$BROKER_UID:$BROKER_UID" "$B"

# The generation marker is written last: a half-finished run is never taken for a valid one.
printf '%s' "$SCRIPT_SUM" >"$SECRETS/script.sha256"
cp "$B/generation" "$SECRETS/generation"
echo "kafka-tls-init: done"
