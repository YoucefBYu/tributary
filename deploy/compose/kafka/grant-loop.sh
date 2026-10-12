#!/bin/bash
# Grant window loop for kafka-users-init (W3-16), run in the background by start-broker.sh.
# Usage: grant-loop.sh <broker-pid>. Env: GRANT (flag dir), TB_ACLS_BIN, TB_ACL_ARGS, TB_GRANT_TTL_SECONDS,
# TB_GRANT_POLL_SECONDS. Protocol: the one-shot writes a nonce to $GRANT/request; after `kafka-acls --add`
# succeeds this loop writes the same nonce to $GRANT/granted. The ACL is removed when the request is withdrawn
# or replaced, and ALWAYS once the grant is older than the TTL. A request that already exists when the loop
# starts (left by a killed one-shot) is never granted. Deliberately NOT `set -e`: no failure may end the loop
# while ALTER is granted.
set +e
broker=$1
GRANT=${GRANT:?}
TTL=${TB_GRANT_TTL_SECONDS:-120}
POLL=${TB_GRANT_POLL_SECONDS:-1}
acls() { $TB_ACLS_BIN $TB_ACL_ARGS "$@" >/dev/null 2>&1; }

served=$(cat "$GRANT/request" 2>/dev/null)
active=0
since=0
: > "$GRANT/loop-started" 2>/dev/null  # tells a test (or an operator) the stale-request snapshot is taken
while kill -0 "$broker" 2>/dev/null; do
  req=$(cat "$GRANT/request" 2>/dev/null)
  if [[ $active -eq 1 ]] && { [[ -z "$req" || "$req" != "$served" ]] || (( SECONDS - since > TTL )); }; then
    if acls --remove --force; then rm -f "$GRANT/granted"; active=0; fi
  elif [[ $active -eq 0 && -n "$req" && "$req" != "$served" ]]; then
    if acls --add; then
      served="$req"; active=1; since=$SECONDS
      printf '%s' "$req" > "$GRANT/granted.tmp" && mv -f "$GRANT/granted.tmp" "$GRANT/granted"
    fi
  fi
  sleep "$POLL"
done
