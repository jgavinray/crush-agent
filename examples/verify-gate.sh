#!/usr/bin/env bash
# verify-gate.sh — host-side (human/CI) verification of agent completion,
# reading the plain-text mailbox log directly (SPEC §13 "External
# verification": one canonical record, two readers, no second source of
# truth). The agent-side equivalent is read_my_mailbox(kind="reply") /
# get_agent; this script is the human's gate, not the agent's.
#
# Usage:
#   BUS_ROOT=/path/to/bus-root PROMPT_BACKEND=2 PROMPT_FRONTEND=1 ./verify-gate.sh
#
# Exits 0 when backend-dev replied to prompt id $PROMPT_BACKEND AND
# frontend-dev replied to prompt id $PROMPT_FRONTEND in the orchestrator's
# mailbox; 2 otherwise. Records are "field: value" blocks separated by a
# blank line, so RS="\n\n" makes one record one awk record.
set -euo pipefail

BUS_ROOT="${BUS_ROOT:-$HOME/.crush-mailbox-bus}"
PROMPT_BACKEND="${PROMPT_BACKEND:-2}"
PROMPT_FRONTEND="${PROMPT_FRONTEND:-1}"
LOG="$BUS_ROOT/mailboxes/orchestrator.log"

if [[ ! -f "$LOG" ]]; then
  echo "verify-gate: no mailbox log at $LOG (have any agents registered and sent?)" >&2
  exit 2
fi

awk 'BEGIN{RS="\n\n"}
     $0 ~ "in_reply_to: '"$PROMPT_FRONTEND"'" && /from: frontend-dev/ {f=1}
     $0 ~ "in_reply_to: '"$PROMPT_BACKEND"'" && /from: backend-dev/   {b=1}
     END{exit !(f&&b)}' "$LOG" || {
  echo "verify-gate: missing required replies in $LOG" >&2
  echo "  want: from: frontend-dev, in_reply_to: $PROMPT_FRONTEND" >&2
  echo "  want: from: backend-dev,  in_reply_to: $PROMPT_BACKEND" >&2
  echo "  (inspect with: grep -E '^(from|in_reply_to|id|kind):' \"$LOG\")" >&2
  exit 2
}

echo "verify-gate: backend-dev -> prompt $PROMPT_BACKEND and frontend-dev -> prompt $PROMPT_FRONTEND confirmed in $LOG"
