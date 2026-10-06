#!/bin/bash
# One-shot init for the lite stack's Kafka security (ADR-008 D5, engine-kafka/v0 README section 6).
# Runs as root inside the apache/kafka image (which carries bash and openssl) with no network and only
# CHOWN/DAC_OVERRIDE/FOWNER (docker-compose.yml), writes ONE Compose-managed volume, and exits. Nothing
# it writes is committed, baked into an image, or printed. It never runs with xtrace and never echoes a
# secret. Secret values come from `openssl rand` and are written with shell builtins, so no password is
# an argument of any process, in any container, at any point (visible neither to `ps` in a container nor
# to a host user): the SCRAM users are created after the broker starts by `kafka-users-init`
# (topic-bootstrap/add_scram_users.py), which reads the password files.
#
#   /secrets     (volume kafka-secrets, mounted per component by SUBPATH, read-only)
#     tb-lite-kafka-ca/ca.crt                   the trust bundle every client mounts (tls.secretRef)
#     tb-lite-<component>/{username,password}   one SCRAM-SHA-512 principal per component
#     broker/          server.properties (no secret in it), broker.pem, healthcheck.pem, ca.crt, generation;
#                      mounted only into `kafka`
#     users-init/      the client certificate of the one-shot that creates the SCRAM users
#
# Identities that are certificates, not passwords (no secret to leak, nothing in a properties file):
#   CN=kafka                 the broker: controller and inter-broker links (super user)
#   CN=tb-lite-users-init    creates the SCRAM users over the broker's LOOPBACK admin listener (9096),
#                            reached only through the broker's own network namespace. NOT a super user:
#                            start-broker.sh grants it ALTER on the cluster, which is all
#                            AlterUserScramCredentials needs
#   CN=tb-lite-healthcheck   the healthcheck; no ACL at all
#
# The CA private key exists only in a temporary directory for the seconds it takes to sign those
# certificates, then is deleted: nothing can issue certificates afterwards (regeneration makes a new
# CA). LIMITATION, documented not fixed: the dev CA has no nameConstraints, so anything holding it could
# mint a certificate for any name; it is never stored or mounted anywhere.
#
# Idempotent: a valid set is kept, because rotating credentials under a running broker would strand
# every client. Regeneration (first `up`, a changed script, or under 7 days of validity left) writes a
# new `generation`; the broker wrapper then wipes and reformats its storage, so old ACLs never outlive
# the credentials they were granted to.
set -euo pipefail
umask 077

SECRETS=/secrets
CA_DAYS=30
RENEW_SECONDS=$((7 * 24 * 3600))
# uid of the non-root application images (deploy/compose/images, topic-bootstrap, channel-publish) and
# of `appuser` in the apache/kafka image.
APP_UID=10001
BROKER_UID=1000
COMPONENTS=(receiver worker loader channel-publish topic-bootstrap admin)
# TB_KAFKA_NODES=3 (docker-compose.rf3.yml, W3-33a): three KRaft nodes (broker and controller each), RF=3 and
# min.insync.replicas=2 (topic-defaults.json profiles.production). 1 (the default) is the lite stack, unchanged.
NODES="${TB_KAFKA_NODES:-1}"
case "$NODES" in 1|3) ;; *) echo "TB_KAFKA_NODES must be 1 or 3" >&2; exit 2 ;; esac

# A changed script (new listeners, new users, new cipher pin) must not be masked by stale output.
SCRIPT_SUM=$(printf '%s' "$NODES" | cat - "${BASH_SOURCE[0]}" | sha256sum | cut -d' ' -f1)

if [[ -f "$SECRETS/generation" && "$(cat "$SECRETS/script.sha256" 2>/dev/null || true)" == "$SCRIPT_SUM" && -f "$SECRETS/tb-lite-kafka-ca/ca.crt" ]] \
   && openssl x509 -checkend "$RENEW_SECONDS" -noout -in "$SECRETS/tb-lite-kafka-ca/ca.crt" >/dev/null 2>&1 \
   && openssl x509 -checkend "$RENEW_SECONDS" -noout -in "$SECRETS/broker/broker.pem" >/dev/null 2>&1; then
  echo "kafka-tls-init: existing credentials are valid, keeping them"
  exit 0
fi

echo "kafka-tls-init: generating dev CA, certificates and per-component SCRAM passwords"
find "$SECRETS" -mindepth 1 -delete
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

rand() { openssl rand -hex 32; }

# --- dev CA (EC P-256). The key lives in $tmp only, and is deleted after signing. --------------
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days "$CA_DAYS" \
  -subj "/CN=tributary-lite-dev-ca" \
  -addext "basicConstraints=critical,CA:TRUE,pathlen:0" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "$tmp/ca.key" -out "$tmp/ca.crt" 2>/dev/null

# issue <name> <CN> <subjectAltName>  ->  $tmp/<name>.key, $tmp/<name>.crt (serverAuth + clientAuth)
issue() {
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$tmp/$1.key"
  openssl req -new -key "$tmp/$1.key" -subj "/CN=$2" -out "$tmp/$1.csr"
  printf 'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=%s\n' "$3" >"$tmp/$1.ext"
  openssl x509 -req -in "$tmp/$1.csr" -CA "$tmp/ca.crt" -CAkey "$tmp/ca.key" -CAcreateserial \
    -days "$CA_DAYS" -extfile "$tmp/$1.ext" -out "$tmp/$1.crt" 2>/dev/null
}
# The broker certificate's SANs match every name a client (or the broker itself) dials: `kafka` is the
# Compose service name; 127.0.0.1 and localhost are the host-published and loopback listeners.
BROKER_SAN="DNS:kafka,DNS:localhost,IP:127.0.0.1"
[[ "$NODES" -gt 1 ]] && BROKER_SAN="$BROKER_SAN,DNS:kafka-2,DNS:kafka-3"   # one certificate shared by the three nodes
issue broker kafka "$BROKER_SAN"
issue healthcheck tb-lite-healthcheck "DNS:tb-lite-healthcheck"
issue users-init tb-lite-users-init "DNS:tb-lite-users-init"
rm -f "$tmp/ca.key"   # signed; nothing may issue certificates from this CA any more

# --- per-component directories -----------------------------------------------------------------
install -d -m 0500 -o "$APP_UID" -g "$APP_UID" "$SECRETS/tb-lite-kafka-ca"
install -m 0444 -o "$APP_UID" -g "$APP_UID" "$tmp/ca.crt" "$SECRETS/tb-lite-kafka-ca/ca.crt"

for c in "${COMPONENTS[@]}"; do
  user="tb-lite-$c"
  install -d -m 0500 -o "$APP_UID" -g "$APP_UID" "$SECRETS/$user"
  printf '%s' "$user" >"$SECRETS/$user/username"
  printf '%s' "$(rand)" >"$SECRETS/$user/password"
  chown "$APP_UID:$APP_UID" "$SECRETS/$user/username" "$SECRETS/$user/password"
  chmod 0400 "$SECRETS/$user/username" "$SECRETS/$user/password"
done

install -d -m 0500 -o "$APP_UID" -g "$APP_UID" "$SECRETS/users-init"
install -m 0400 -o "$APP_UID" -g "$APP_UID" "$tmp/users-init.crt" "$SECRETS/users-init/client.crt"
install -m 0400 -o "$APP_UID" -g "$APP_UID" "$tmp/users-init.key" "$SECRETS/users-init/client.key"

# --- broker material ---------------------------------------------------------------------------
# AEAD-only suites, TLS 1.2+ (Java names). Same intent as the clients' ECDHE+AESGCM:ECDHE+CHACHA20.
SUITES="TLS_AES_256_GCM_SHA384,TLS_AES_128_GCM_SHA256,TLS_CHACHA20_POLY1305_SHA256,\
TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,\
TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,\
TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"
SCRAM=org.apache.kafka.common.security.scram.ScramLoginModule
GEN=$(openssl rand -hex 8)
cat "$tmp/broker.key" "$tmp/broker.crt" >"$tmp/broker.pem"
cat "$tmp/healthcheck.key" "$tmp/healthcheck.crt" >"$tmp/healthcheck.pem"

# write_broker <n>: node 1 is `broker/` (the lite layout), nodes 2 and 3 are `broker-<n>/`.
write_broker() {
  local n=$1 B="$SECRETS/broker" host=kafka
  [[ "$n" -gt 1 ]] && B="$SECRETS/broker-$n" && host="kafka-$n"
  install -d -m 0500 -o "$BROKER_UID" -g "$BROKER_UID" "$B"
  install -m 0400 "$tmp/ca.crt" "$B/ca.crt"
  install -m 0400 "$tmp/broker.pem" "$B/broker.pem"
  install -m 0400 "$tmp/healthcheck.pem" "$B/healthcheck.pem"
  printf '%s' "$GEN" >"$B/generation"
  local voters listeners advertised dflt_rf=1 isr=1 txisr=1
  if [[ "$NODES" -eq 1 ]]; then
    voters="1@127.0.0.1:9093"
    listeners="CONTROLLER://127.0.0.1:9093,BROKER://127.0.0.1:9095,ADMIN://127.0.0.1:9096,CLIENT://0.0.0.0:9094,EXTERNAL://0.0.0.0:19092"
    advertised="BROKER://127.0.0.1:9095,ADMIN://127.0.0.1:9096,CLIENT://kafka:9094,EXTERNAL://127.0.0.1:19092"
  else
    # RF=3 profile: controller and inter-broker links cross containers, so they bind all interfaces and are
    # advertised by service name (the certificate carries those SANs). Still SSL with required client certs.
    voters="1@kafka:9093,2@kafka-2:9093,3@kafka-3:9093"
    listeners="CONTROLLER://0.0.0.0:9093,BROKER://0.0.0.0:9095,ADMIN://127.0.0.1:9096,CLIENT://0.0.0.0:9094,EXTERNAL://0.0.0.0:1909$((n + 1))"
    advertised="BROKER://$host:9095,ADMIN://127.0.0.1:9096,CLIENT://$host:9094,EXTERNAL://127.0.0.1:1909$((n + 1))"
    dflt_rf=3 isr=2 txisr=2
  fi
  cat >"$B/server.properties" <<PROPS
process.roles=broker,controller
node.id=$n
controller.quorum.voters=$voters
controller.listener.names=CONTROLLER
inter.broker.listener.name=BROKER
# No listener is PLAINTEXT. CONTROLLER, BROKER (inter-broker) and ADMIN are SSL with REQUIRED client
# certificates: the certificate is the identity, so no password exists to leak. CLIENT is what containers
# dial and EXTERNAL is the host-published, loopback-mapped one: both SASL_SSL. (engine-kafka/v0 README
# section 6 rule 1 would allow a loopback PLAINTEXT controller, but then the controller's own requests
# arrive as User:ANONYMOUS, and super.users must never contain ANONYMOUS, rule 3.)
listeners=$listeners
advertised.listeners=$advertised
listener.security.protocol.map=CONTROLLER:SSL,BROKER:SSL,ADMIN:SSL,CLIENT:SASL_SSL,EXTERNAL:SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.controller.ssl.client.auth=required
listener.name.broker.ssl.client.auth=required
listener.name.admin.ssl.client.auth=required
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
# ACLs are only ever ADDED by topic-bootstrap, so a narrowed contract leaves old grants in place for
# the life of the broker's storage, which is a tmpfs (docker-compose.yml): any stop starts clean.

# Deny by default. super.users: the broker (by certificate), the topic-creation job (the
# control-plane stand-in) and the operator's admin principal. Never User:ANONYMOUS, never a data-plane component.
authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:CN=kafka;User:tb-lite-topic-bootstrap;User:tb-lite-admin

log.dirs=/var/lib/kafka/data

# One node (lite profile, topic-defaults.json profiles.lite, ADR-002 D14, human-approved 2026-09-21): RF=1 and
# min.insync.replicas=1, an explicit documented exception to invariant 1's durability promise. Three nodes
# (docker-compose.rf3.yml): RF=3 and min.insync.replicas=2, topic-defaults.json profiles.production. Topic-level
# min.insync.replicas and unclean election are still set per topic by topic-bootstrap from the contract file.
# auto.create.topics stays off so nothing but topic-bootstrap creates a topic. The internal-topic
# factors default to 3 and can never be satisfied on one broker: without them __consumer_offsets is
# never created and every consumer group hangs at FIND_COORDINATOR (found by qa-engineer-2, W1-8).
default.replication.factor=$dflt_rf
min.insync.replicas=$isr
unclean.leader.election.enable=false
auto.create.topics.enable=false
offsets.topic.replication.factor=$dflt_rf
transaction.state.log.replication.factor=$dflt_rf
transaction.state.log.min.isr=$txisr
PROPS
  chmod 0400 "$B/server.properties"
  # Healthcheck client: the ACL-less certificate principal over the real (loopback) admin listener, so
  # "healthy" proves TLS and mutual authentication work, not just that a port is open.
  cat >"$B/healthcheck.properties" <<PROPS
security.protocol=SSL
ssl.keystore.type=PEM
ssl.keystore.location=/etc/tributary/broker/healthcheck.pem
ssl.truststore.type=PEM
ssl.truststore.location=/etc/tributary/broker/ca.crt
ssl.endpoint.identification.algorithm=https
PROPS
  chmod 0400 "$B/healthcheck.properties"
  # The broker's own admin client (its certificate, the super user): used once by start-broker.sh.
  cat >"$B/broker-client.properties" <<PROPS
security.protocol=SSL
ssl.keystore.type=PEM
ssl.keystore.location=/etc/tributary/broker/broker.pem
ssl.truststore.type=PEM
ssl.truststore.location=/etc/tributary/broker/ca.crt
ssl.endpoint.identification.algorithm=https
PROPS
  chmod 0400 "$B/broker-client.properties"
  chown -R "$BROKER_UID:$BROKER_UID" "$B"
}
for n in $(seq 1 "$NODES"); do write_broker "$n"; done
B="$SECRETS/broker"

# The generation marker is written last: a half-finished run is never taken for a valid one.
printf '%s' "$SCRIPT_SUM" >"$SECRETS/script.sha256"
cp "$B/generation" "$SECRETS/generation"
echo "kafka-tls-init: done"
