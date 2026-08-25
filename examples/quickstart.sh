#!/usr/bin/env bash
# quickstart.sh — one-command end-to-end demo of the Mailbox Bus P3 loop
# (SPEC §17 P3: push delivery + self-driving agent turn inside `crush server`).
#
#   ./examples/quickstart.sh
#
# No setup, no env vars, no permanent changes:
#   - builds .bin/mailbox-bus + .bin/crush-p3 (cached; skipped if up to date)
#   - runs EVERYTHING in a throwaway /tmp dir (scratch HOME, data dir,
#     bus_root, socket); your real crush config is COPIED, never modified
#   - starts `crush server` (client-server mode), launches two agents
#     (solo=worker, orchestrator), proves: register -> send -> push ->
#     self-driven turn -> reply -> wait_for_message
#   - verifies on the durable bus logs, prints PASS/FAIL, cleans up
#     (on failure the scratch dir is kept and its path printed)
#
# Prereqs: Go toolchain (only for the first build), and a working crush
# model config (your usual ~/.local/share/crush/... — whatever model you
# run day-to-day). Exits 0 only when the full loop is proven on this machine.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/mbus-qs.XXXXXX")"
BIN="$ROOT/.bin"
PASS=1

SOLO_PID=""
SERVER_PID=""

log()  { printf '\033[1;36m[qs]\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31m[qs] FAIL: %s\033[0m\n' "$*" >&2; exit 1; }

cleanup() {
  local rc=$?
  [ -n "$SOLO_PID" ]   && kill "$SOLO_PID"   2>/dev/null || true
  [ -n "$SERVER_PID" ] && kill "$SERVER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  if [ "$rc" -eq 0 ] && [ "$PASS" -eq 1 ]; then
    rm -rf "$SCRATCH"
  else
    log "scratch dir kept for inspection: $SCRATCH"
  fi
}
trap cleanup EXIT

# ---------------------------------------------------------------- binaries
mkdir -p "$BIN"
go_newer() { find "$1" -name '*.go' -newer "$2" 2>/dev/null | head -1; }

if [ ! -x "$BIN/mailbox-bus" ] || [ -n "$(go_newer "$ROOT/cmd/mailbox-bus" "$BIN/mailbox-bus")" ] \
   || [ -n "$(go_newer "$ROOT/internal" "$BIN/mailbox-bus")" ]; then
  log "building mailbox-bus ..."
  ( cd "$ROOT" && go build -o "$BIN/mailbox-bus" ./cmd/mailbox-bus )
fi

if [ ! -x "$BIN/crush-p3" ] || [ -n "$(go_newer "$ROOT/p3/crush/internal" "$BIN/crush-p3")" ] \
   || [ -n "$(go_newer "$ROOT/p3/crush/main.go" "$BIN/crush-p3")" ]; then
  log "building P3 crush fork (first build can take a few minutes) ..."
  ( cd "$ROOT/p3/crush" && go build -o "$BIN/crush-p3" . )
fi

CRUSH="$BIN/crush-p3"
BUS="$BIN/mailbox-bus"

# ---------------------------------------------------------------- scratch
HOME0="$SCRATCH/home"; DATA="$SCRATCH/data"; WORK="$SCRATCH/work"
BUSROOT="$SCRATCH/busroot"
SOCK="$SCRATCH/crush.sock"
mkdir -p "$HOME0/.local/share/crush" "$DATA" "$WORK" "$BUSROOT"

# copy (never modify) your global crush config into the scratch home
if [ -d "$HOME/.local/share/crush" ]; then
  cp -R "$HOME/.local/share/crush/." "$HOME0/.local/share/crush/" 2>/dev/null || true
fi
if [ ! -f "$HOME0/.local/share/crush/crush.json" ]; then
  log "no crush config found at ~/.local/share/crush/crush.json — using crush defaults"
  echo '{}' > "$HOME0/.local/share/crush/crush.json"
fi

# add the mcp.bus entry + allowlist the bus tools in the COPIED config
# (non-interactive `crush run` has no TUI to approve tool calls)
python3 - "$HOME0/.local/share/crush/crush.json" "$BUS" "$BUSROOT" <<'PY'
import json, sys
path, bus, root = sys.argv[1:4]
d = json.load(open(path))
d.setdefault("mcp", {})["bus"] = {
    "type": "stdio",
    "command": bus,
    "env": {"BUS_ROOT": root},
}
tools = ["register", "unregister", "list_agents", "send_message",
         "read_my_mailbox", "wait_for_message", "reply", "whoami",
         "get_agent", "set_my_status", "heartbeat", "compact"]
perms = d.setdefault("permissions", {})
allowed = set(perms.get("allowed_tools") or [])
allowed.update("mcp_bus_" + t for t in tools)
perms["allowed_tools"] = sorted(allowed)
json.dump(d, open(path, "w"), indent=2)
PY
log "scratch ready: $SCRATCH"

# optional model override: QS_MODEL="provider/model" (default: your crush default)
QS_MODEL="${QS_MODEL:-}"
RUNM=()
if [ -n "$QS_MODEL" ]; then RUNM=(-m "$QS_MODEL"); fi

# ---------------------------------------------------------------- server
export HOME="$HOME0"
log "starting crush server ..."
# --channels bus: opt the "bus" MCP server in as a channel (hidden flag,
# root.go) — without it the channel gate stays closed and no push ever
# reaches the router (fail-closed by design).
CRUSH_SERVER_DETACH_GRACE=3600 "$CRUSH" server \
  -H "unix://$SOCK" -D "$DATA" -c "$WORK" --channels bus > "$SCRATCH/server.out" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 30); do [ -S "$SOCK" ] && break; kill -0 "$SERVER_PID" 2>/dev/null || break; sleep 1; done
[ -S "$SOCK" ] || { tail -5 "$SCRATCH/server.out"; fail "crush server did not listen"; }
log "server up on $SOCK (pid $SERVER_PID)"

server_log() { ls "$HOME0/.cache/crush"/server-*/crush.log 2>/dev/null | head -1; }

# ---------------------------------------------------------------- solo
SOLO_PROMPT=$(cat <<EOF
You are "solo" (agent_id: solo, role: worker) on the Mailbox Bus. The bus is an MCP server named "bus" in this session.
1. Call bus.register(agent_id="solo", role="worker", description="quickstart worker", working_dir="$(pwd)", model="quickstart").
2. Call bus.read_my_mailbox(agent_id="solo", since=0). For each record:
   - kind=prompt: do exactly what the body says, then call bus.reply(in_reply_to=<the record's id>, body=<the exact result the body asked for>, dedup_id="<id>:reply").
   - kind=info / kind=reply: note it, no reply.
3. If there is no unhandled prompt, call bus.wait_for_message(agent_id="solo", since=<highest seq you have seen>, timeout=120). When a prompt arrives, go to step 2. If the wait times out and there is still nothing unhandled, print exactly "SOLO-DONE" and finish.
Use only the bus MCP tools. Do not create files. Do not use any other tools.
EOF
)

log "starting solo (worker) ..."
env CRUSH_CLIENT_SERVER=1 "$CRUSH" run -H "unix://$SOCK" -D "$DATA" -c "$WORK" --quiet --channels bus \
  ${RUNM[@]+"${RUNM[@]}"} "$SOLO_PROMPT" > "$SCRATCH/solo.out" 2>&1 &
SOLO_PID=$!

# wait for solo's durable registration (registry snapshot appears on register)
for _ in $(seq 1 90); do
  [ -f "$BUSROOT/registry/solo.json" ] && break
  kill -0 "$SOLO_PID" 2>/dev/null || break
  sleep 1
done
if [ ! -f "$BUSROOT/registry/solo.json" ]; then
  log "--- solo.out ---"; cat "$SCRATCH/solo.out"
  fail "solo never registered on the bus"
fi
log "solo registered ($(python3 -c "import json;print(json.load(open('$BUSROOT/registry/solo.json'))['role'])" 2>/dev/null || echo worker))"

# ---------------------------------------------------------------- orchestrator
ORCH_PROMPT=$(cat <<'EOF'
You are "orchestrator" (agent_id: orchestrator, role: orchestrator) on the Mailbox Bus. The bus is an MCP server named "bus" in this session.
1. Call bus.register(agent_id="orchestrator", role="orchestrator", description="quickstart orchestrator", working_dir="$(pwd)", model="quickstart").
2. Call bus.send_message(agent_id="orchestrator", to_agent="solo", kind="prompt", dedup_id="quickstart-1", body="Reply with the exact string P3-OK-solo and nothing else.").
3. Call bus.wait_for_message(agent_id="orchestrator", since=0, kind="reply", timeout=300) and block until solo's reply arrives.
4. When the reply arrives, finish with exactly one line: RECEIVED: <the reply body>
Use only the bus MCP tools. Do not create files. Do not use any other tools.
EOF
)

log "starting orchestrator (blocks until solo's reply lands) ..."
set +e
env CRUSH_CLIENT_SERVER=1 "$CRUSH" run -H "unix://$SOCK" -D "$DATA" -c "$WORK" --quiet --channels bus \
  ${RUNM[@]+"${RUNM[@]}"} "$ORCH_PROMPT" > "$SCRATCH/orch.out" 2>&1
set -e

# ---------------------------------------------------------------- verify
# The durable bus logs are the source of truth (SPEC §13: one canonical
# record, two readers). PASS requires: prompt reached solo, reply from
# solo reached the orchestrator's mailbox, and the server log shows the
# bus-channel push actually self-drove an agent turn (not a host loop).
log "verifying against the durable bus logs ..."
ORCHLOG="$BUSROOT/mailboxes/orchestrator.log"
SOLOLOG="$BUSROOT/mailboxes/solo.log"

check() { # $1=description $2=file $3=pattern
  if [ -f "$2" ] && grep -q "$3" "$2"; then
    log "  ok: $1"
  else
    log "  MISSING: $1 (pattern '$3' not in $2)"
    PASS=0
  fi
}
check "prompt record in solo mailbox"            "$SOLOLOG"  "^kind: prompt"
check "reply record in orchestrator mailbox"     "$ORCHLOG"  "^kind: reply"
check "reply is from solo"                       "$ORCHLOG"  "^from: solo"
check "reply body is P3-OK-solo"                 "$ORCHLOG"  "P3-OK-solo"
# fresh bus_root => orchestrator prompt gets id 1, solo's reply dedups on it
check "reply deduped on prompt id (1:reply)"     "$ORCHLOG"  "^dedup_id: 1:reply$"

SLOG="$(server_log || true)"
if [ -n "$SLOG" ]; then
  check "server self-drove solo's turn from the push" "$SLOG" "Starting agent turn from bus channel message"
  log "  server channel evidence:"
  grep -E "Bus channel (event published|router started)|Starting agent turn" "$SLOG" \
    | python3 -c "import sys,json
for l in sys.stdin:
    try: o=json.loads(l); print('    ', o.get('time','')[11:23], o.get('msg',''), o.get('agent',''))
    except Exception: pass" 2>/dev/null || true
  log "--- orchestrator final output ---"
  grep -v '^\s*$' "$SCRATCH/orch.out" | tail -5 | sed 's/^/    /'
else
  log "  (no server log found at $HOME0/.cache/crush/server-*/crush.log)"
fi

# ---------------------------------------------------------------- verdict
if [ "$PASS" -eq 1 ]; then
  log "PASS — full P3 loop verified on this machine:"
  log "  register -> send -> bus push -> self-driven turn -> reply -> wait_for_message"
  log "  (scratch dir removed; re-run any time with: $(basename "$0") )"
else
  echo
  echo "==================================================================="
  echo " FAIL — durable bus log evidence above is incomplete."
  echo " Scratch dir kept at: $SCRATCH"
  echo "   solo.out / orch.out / server.out / busroot/ hold the full trail."
  echo "==================================================================="
  exit 1
fi
