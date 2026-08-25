#!/bin/sh
# agent-loop — the no-fork delivery loop (SPEC §11.4 fallback).
#
# Drives one autonomous peer by re-invoking `crush run` on exit:
#
#   agent-loop <agent_id> <role-prompt-file>
#
# Each `crush run` is ONE batch: the session loads its role prompt,
# registers, drains its mailbox with read_my_mailbox, handles records by
# kind (prompt -> work + reply; info -> note; reply -> record), persists
# its cursor to .crush/last_seq, and exits. The wrapper re-invokes it.
#
# This is a HOST-SIDE LAUNCHER the human writes and runs — not agent
# tooling, not a Crush or MCP feature. It is the pulse that turns "run
# one batch and exit" into "run repeatedly". State lives in the mailbox
# and the cursor file, not in the process: a crash loses no work and no
# messages. `--data-dir` is per-agent so the Crush SQLite cold-start race
# (SPEC §11.3) cannot occur; the 3s sleep is the stagger, belt-and-
# suspenders.
while :; do
  crush run --data-dir ".crush-$1" \
            --yolo \
            "$(cat "$2")"
  sleep 3
done
