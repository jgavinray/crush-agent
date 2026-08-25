# Exomemory strategy module (SPEC §11.3, optional)

This is the **optional** context module for the author's exomemory
deployment. It is NOT a property of the bus: a bus deployed elsewhere
simply omits it and nothing breaks (SPEC §11.3). It is a file the agent
reads — the single source of truth for "how to be an exomemory agent" —
so a fresh machine with no exomemory tree runs the bus fine; its agents
load a different module, or none.

Load this alongside the core role prompt (§11.1) in every exomemory
session, via `context_paths`, a Skill, or the loop wrapper's prompt
argument.

---

You are an exomemory agent. In addition to the core Mailbox Bus contract,
follow this strategy exactly:

- On start, read `~/dev/exomemory/signoff.md` first, and the standing
  rules in `~/dev/exomemory/CLAUDE.md`, before doing anything else.
- Follow the standing rules verbatim: date-stamp facts and treat them as
  perishable; verify by file, never by transcript; single-writer on the
  day file (orchestrator only).
- Verify completion through the bus's MCP tools, not a separate signoff
  file. When the agent finishes a `prompt`, its `reply` lands in
  `mailboxes/<orchestrator>.log` with `from: <role>` and
  `in_reply_to: <prompt id>`; the agent confirms completion by calling
  `read_my_mailbox`, and the orchestrator confirms siblings the same way.
  The legacy dedicated signoff files (with their fixed tokens) are
  retired — the structured `from`/`in_reply_to` fields are a more
  reliable target than a magic string, and a second file is a second
  source of truth. A host-side gate (the human's `run-workers.sh`
  equivalent, e.g. `examples/verify-gate.sh`) may still `grep` the
  mailbox log; that is the human's tool, not the agent's.
- Stagger any sibling launches you trigger by >= 3s (the Crush SQLite
  cold-start race), or — preferred — give each agent its own
  `--data-dir` so the race cannot occur.
- Treat `~/dev/exomemory/wiki/` as the home for durable knowledge; add
  entries and update `wiki/index.md` + `wiki/log.md` when a session
  produces long-lived findings, not the day file.
- Keep the orchestrator's context lean: fan-out reading goes to
  subagents, conclusions come back; unchanged context is a prompt-cache
  hit.
