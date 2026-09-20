#!/usr/bin/env bash
# PreToolUse hook for Write|Edit.
# 1) Blocks HL7 fixtures that are not marked synthetic (MSH-3 = TRIBUTARY-SYNTH).
# 2) Blocks obvious secrets (private keys, AWS access key ids).
# Exit 2 blocks the tool call and feeds stderr back to the agent.
# Requires: jq

INPUT=$(cat)
FILE=$(echo "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)
CONTENT=$(echo "$INPUT" | jq -r '.tool_input.content // .tool_input.new_string // empty' 2>/dev/null)

[ -z "$CONTENT" ] && exit 0

# Secrets
if echo "$CONTENT" | grep -qE -- '-----BEGIN [A-Z ]*PRIVATE KEY-----|AKIA[0-9A-Z]{16}'; then
  echo "Blocked: content looks like a private key or cloud credential. Use a secretRef or environment variable instead." >&2
  exit 2
fi

# HL7 fixtures must be synthetic
case "$FILE" in
  */fixtures/*.hl7|*/fixtures/*.er7|*/samples/*.hl7|*/samples/*.er7)
    if echo "$CONTENT" | grep -q 'MSH|'; then
      if ! echo "$CONTENT" | grep -q 'TRIBUTARY-SYNTH'; then
        echo "Blocked: HL7 fixtures must be synthetic. Set MSH-3 (sending application) to TRIBUTARY-SYNTH and use invented patient data only. Never paste real patient data." >&2
        exit 2
      fi
    fi
    ;;
esac

exit 0
