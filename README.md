# crush-agent — Mailbox Bus for Crush

Multi-agent message bus for [Crush](https://github.com/charmbracelet/crush),
implemented exactly as defined in [SPEC.md](SPEC.md) (v1.0, handoff package
§19): a **stdio MCP server** (`mailbox-bus`) over **durable plain-text
files**, spawned per Crush session, all converging on one shared
`bus_root`.

```
crush session A (agent "frontend-dev") ──spawns──> mailbox-bus (stdio MCP) ─┐
crush session B (agent "backend-dev")  ──spawns──> mailbox-bus (stdio MCP) ─┼─> bus_root (shared files,
crush session C (agent "orchestrator") ──spawns──> mailbox-bus (stdio MCP) ─┘   single advisory flock)
```

The bus holds **no in-memory correctness state**. Every fact — membership,
message ids, deliveries, last-seen — lives in plain files under `bus_root`,
so any number of bus processes (one per Crush session) can share a root, a
killed process loses nothing, and a human can `cat` the whole system state.

## Quickstart

One command, no setup, no permanent changes — proves the full P3 loop
(register → send → bus push → self-driven agent turn → reply →
`wait_for_message`) on your machine:

```sh
./examples/quickstart.sh
```

What it does:

- builds `.bin/mailbox-bus` and `.bin/crush-p3` (the P3 fork) if needed —
  cached, skipped when up to date
- creates a throwaway scratch dir under `$TMPDIR`: scratch `HOME`, data
  dir, `bus_root`, socket. Your real `~/.local/share/crush/` config is
  **copied, never modified**; the script adds the `mcp.bus` entry and
  allowlists the 12 bus tools in the copy only
- starts `crush server` (client-server mode), launches two agents —
  `solo` (worker) and `orchestrator` — with your default model
- verifies against the **durable bus logs** (the source of truth, SPEC
  §13): prompt reached solo, reply from solo reached the orchestrator
  with its dedup id, and the server log shows the push actually
  self-drove solo's turn (`Starting agent turn from bus channel message`)

Exits 0 only when every check passes, then removes the scratch dir.
On failure it keeps the scratch dir and prints its path
(`solo.out` / `orch.out` / `server.out` / `busroot/` hold the full trail).

Optional: `QS_MODEL=provider/model ./examples/quickstart.sh` to run the
demo on a specific model instead of your crush default.

> Note: P3 requires the P3-enabled Crush fork in [`p3/crush`](p3/crush)
> (branch `p3/mailbox-bus`) — the plain release build has no channel
> routing. The script builds it for you; it assumes that tree is present
> at `p3/crush` relative to the repo root.

## Status

| Phase | Spec § | Contents | State |
|-------|--------|----------|-------|
| P0 | §17 | Integration sanity: real Crush session drives the bus over stdio MCP | done |
| P1 | §17 | Bus MVP: records, registry, delivery, wait, reply; gate C1–C10 | done |
| P2 | §17 | Lifecycle + bootstrap: `whoami`, `get_agent`, `set_my_status`, `heartbeat`; prompts, examples; gate C11–C14 | done |
| P3 | §17 | Delivery via `crush server` + channel, self-driving agent turn | done (Crush fork branch `p3/mailbox-bus`; bus-side gate C19–C20 here) |
| P4 | §17 | Hardening: `index.db`, fsync-before-return, `compact`, 0700 root; gate C15–C17 | done |

The conformance gate in [`conformance/`](conformance/) is the acceptance
criterion (SPEC §18): a black-box test suite written **from the spec before
implementation**, driving the binary as a stdio MCP client and asserting on
tool results plus the on-disk logs. It is the single source of truth for
"the bus behaves as specified".

## Build

Requires Go (the module pins `go 1.25`).

```sh
go build -o bin/mailbox-bus ./cmd/mailbox-bus
```

`bin/` is gitignored; the binary is a plain static Go executable.

## Run it under Crush

Point each Crush session at the binary with a shared `BUS_ROOT` (SPEC
§19.2). In a project `crush.json`:

```json
{
  "mcp": {
    "bus": {
      "type": "stdio",
      "command": "/path/to/mailbox-bus",
      "env": { "BUS_ROOT": "$HOME/.crush-mailbox-bus" }
    }
  }
}
```

or via the CLI:

```sh
crush mcp add bus --type stdio --command /path/to/mailbox-bus --env BUS_ROOT "$HOME/.crush-mailbox-bus"
```

A ready-made example lives in [`examples/crush.json.example`](examples/crush.json.example).
The binary also accepts `--bus-root <dir>`, which overrides the environment.

Every agent passes its identity explicitly as `agent_id` on every tool call
— the server keeps no per-connection identity (SPEC §9). Each agent's role,
model, and `agent_id` are set in that session's config and flow through the
role prompt ([`prompts/agent-role.md`](prompts/agent-role.md), SPEC §11.1).

## The tools (SPEC §9)

All agent-scoped tools take an explicit `agent_id`. Errors come back as MCP
tool errors of the form `{"error": "<code>", "message": "..."}` with codes
`agent_not_registered`, `agent_removed`, `recipient_unknown`,
`in_reply_to_not_found`, `no_such_mailbox`, `invalid_argument`,
`internal_error`. A `wait_for_message` timeout is **not** an error: it
returns `{"timeout": true}`.

| Tool | Purpose |
|------|---------|
| `register` | Join the bus: `{agent_id, role, description?, working_dir?, model?}` → durable registry event + snapshot. Idempotent. |
| `unregister` | Leave the bus. Drops the snapshot, retains the mailbox. Idempotent. |
| `list_agents` | Registry rows, filterable by `role`, `membership` (default `alive`; `removed`/`any`), `liveness` (`live`/`dead`; default any). |
| `whoami` | Durable registry state for `agent_id`: identity, `status`, `last_seen`, `registered_at`. |
| `get_agent` | `whoami` plus `recent`: the last 20 complete mailbox records with `body_excerpt` (200 bytes). |
| `set_my_status` | Publish a free-form work-state string (reported verbatim; never a filter key). |
| `heartbeat` | Refresh `last_seen` (liveness = within 90 s). Observability only. |
| `send_message` | Deliver `kind ∈ {prompt, info, reply}` to `to_agent` **or** `to_role` (broadcast). Optional `in_reply_to`, `dedup_id`. → `{id, delivered:[{agent_id, seq}]}`. |
| `read_my_mailbox` | Own mailbox records with `seq > since`, optional `kind` filter, `limit` (default 256). |
| `wait_for_message` | Block (kqueue/inotify, 100 ms backoff fallback) until a record matching `since`/`from_role`/`from_agent`/`kind` exists; `timeout` seconds (default 30). |
| `reply` | Convenience: look up `in_reply_to` in the caller's mailbox (live log, then cold archive) and send `kind=reply` back to that record's `from`, with `dedup_id="<id>:reply"` recommended. |
| `compact` | Archive the consumed prefix (`seq <= up_to_seq`) verbatim to `mailboxes/<id>.log.archived` and truncate the live log. Seq counters unaffected; idempotent. |

### Delivery semantics (SPEC §7)

Under the canonical-log flock on `state/lock`:

0. `dedup_id` check — an O(1) `index.db` lookup (P4) with the full log scan
   as the durable backstop. A hit returns the original `{id, delivered}`
   and writes nothing.
1. Recipient expansion from the durable `registry.log` (never a per-process
   cache), validated **before** any id is consumed — a failed send never
   advances the counter.
2. `id = counter + 1`, persisted to `state/counter` **before** any append
   (crash ordering: gaps, never reused ids).
3. Per recipient: `seq` incremented in `state/seq.<agent>` and persisted
   **before** the record's single `O_APPEND` write to
   `mailboxes/<agent>.log` (gaps, never duplicates; monotonic per
   recipient).

All canonical writes are fsynced before the tool returns (P4), including
the containing directory entry when a file is created, so durability holds
across power loss, not just process crash.

## `bus_root` layout (SPEC §4)

```
bus_root/                     mode 0700
├── registry.log              append-only membership events (source of truth)
├── registry/<agent>.json     derived snapshots (alive agents only; rebuildable)
├── index.db                  derived SQLite index (rebuildable on every Open)
├── mailboxes/<agent>.log     append-only delivery log per agent
├── mailboxes/<agent>.log.archived   cold archive of compacted prefixes
└── state/
    ├── lock                  advisory flock (the canonical-log lock)
    ├── counter               last assigned global id
    └── seq.<agent>           last assigned per-recipient seq
```

Records are `header ---\n body \n\n`; the `len` header is authoritative, so
bodies may contain blank lines and lines equal to `---` (gate C9). All logs
are `cat`-readable — invariant V.

### Crash recovery (SPEC §12)

On every `Open`, under the lock: partial trailing records are truncated
back to the last complete record (using `len`), `state/counter` is
recomputed as the max id observed across all mailbox logs (a hand-edited
larger value gets corrected), snapshots are rebuilt from `registry.log`,
and `index.db` is rebuilt from the mailbox logs. The result is
crash-idempotent: an empty root and a populated one start up the same way.

## Conformance gate (SPEC §18)

```sh
go test ./conformance/ -v
```

Cases C1–C20 (plus the P1 `wait_for_message` cross-process tests) cover the
invariants: at-least-once delivery, cross-process dedup, per-pair ordering,
crash-order gap-never-reuse, durable recipient expansion, idempotent
consumer cursor, register/unregister/re-register, page-cache durability +
recovery, record framing, broadcast correlation, status/liveness,
`whoami`/`get_agent` semantics, compact, restart dedup via the rebuilt
index, the 0700 root, bootstrap layout, and P3 channel push (C19 single
child, C20 cross-process scoping via the durable registry). The gate is black-box: it spawns the real binary
over stdio MCP, kills it with SIGKILL, and reads the on-disk files as the
oracle.

## Bootstrap prompts and examples

- [`prompts/agent-role.md`](prompts/agent-role.md) — the §11.1 core role
  prompt, delivered to every agent verbatim with `<role>`/`<id>`
  substituted.
- [`prompts/exomemory-module.md`](prompts/exomemory-module.md) — the §11.3
  **optional** environment-specific strategy module (bus-agnostic).
- [`examples/agent-loop.sh`](examples/agent-loop.sh) — the §11.4 no-fork
  delivery loop: `crush run` one batch, re-invoke on exit; per-agent
  `--data-dir` avoids the SQLite cold-start race.
- [`examples/verify-gate.sh`](examples/verify-gate.sh) — host-side (human/CI)
  verifier that greps `mailboxes/orchestrator.log` for required replies
  (SPEC §13 "External verification"): one canonical record, two readers,
  no second source of truth.
- [`examples/crush.json.example`](examples/crush.json.example) — per-session
  MCP wiring (SPEC §19.2).

## Security model (SPEC §14)

The bus is stdio-only: no network listener, no binding, no authentication.
The trust boundary is the spawn — each bus child has exactly its parent
session's privileges. `agent_id` is self-declared; impersonation is
detectable after the fact because the canonical logs are append-only and
auditable. The shared-boundary gate is `bus_root` mode `0700`.

## Repository layout

```
cmd/mailbox-bus/     the stdio MCP server binary
internal/bus/        durable core: records, registry, delivery, wait, index, compact, recovery
internal/tools/      MCP tool surface (SPEC §9)
conformance/         the SPEC §18 gate — black-box, written pre-implementation
prompts/             §11 bootstrap role prompts (P2)
examples/            wiring + host-side verifier examples (P2)
```
