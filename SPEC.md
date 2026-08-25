# Mailbox Bus — a multi-agent message bus for Crush

**Version:** 1.0 (draft)
**Status:** design, not yet implemented
**Date:** 2026-08-24
**Scope:** the inter-agent communication subsystem only. Agent internals, model
selection, and the Crush TUI are out of scope except where they contract with the
bus.

> **Revision 0.4 note.** This revision revises three points from 0.3, verified
> against the Crush **v0.91.0** source (`github.com/charmbracelet/crush`):
> (1) **transport choice**: Crush supports stdio, Streamable HTTP, and SSE
> (`createTransport`, `init.go:999`); v1 selects **stdio** (one child per
> session, shared `bus_root` files) to remove the daemon/port/auth surface — a
> choice, not a correction (HTTP remains supported); (2) Crush **already has a
> server-initiated push mechanism** via `claude/channel`, so §16 Q9's "needs an
> upstream PR for push" missed the real mechanism — the gap is *delivery
> routing*, not the mechanism; (3) Crush's **client/server architecture
> (`crush server` + `CRUSH_CLIENT_SERVER=1`)** already provides the long-lived
> Go process where the mail-delivery loop belongs, superseding the host-side
> loop wrapper (§11.4) once push delivery is wired. Citations are `internal/...`
> paths with line numbers, pinned to the **v0.91.0** tag (commit `41cdd18a`);
> later versions may drift.
>
> **Revision 0.5 note.** Clarifications only, re-verified at v0.91.0: §8
> distinguishes blocking `wait_for_message` (loop agents) from non-blocking
> `read_my_mailbox` polling (interactive orchestrators, to avoid freezing the
> TUI); §16 Q11 flags the documented `CRUSH_CLIENT_SERVER` socket-init race
> that P3 depends on.
>
> **Revision 0.6 note.** Gap-closure, all from source review against v0.91.0:
> (1) §7 step 0 and §16 Q8 — `dedup_id` idempotency must be checked against
> durable shared state, not an in-memory set, because stdio runs N processes; a
> per-process set would miss another process's `dedup_id` and duplicate.
> (2) §8 — retracted the false "consumers do not poll" absolute; loop agents
> block, interactive orchestrators poll. (3) §9 — trust boundary is
> spawn-scoped, not "localhost". (4) §16 Q11 — the cited race-note is absent
> from the public checkout; describe the mitigation behaviorally rather than
> citing the missing file.
>
> **Revision 0.7 note.** Independent re-review of the 0.6 gap closures against
> v0.91.0; all four verified accurate and complete: (1) §7 step 0 / §16 Q8 —
> durable dedup check (scan `dedup_id` headers under flock) closes the
> N-process visibility gap; (2) §8 — "block or poll depending on session shape"
> replaces the false absolute; (3) §9 — "spawn-scoped trust boundary" replaces
> "localhost"; (4) §16 Q11 — the cited note `docs/notes/2026-05-11-...` is
> absent (v0.91.0 `docs/` ships only `config/` and `hooks/`), so the mitigation
> is now behavioral: reuse `ensureServer`'s readiness probe (`root.go:563`,
> which polls `/v1/health` via `waitForServerReady` and emits "failed to
> initialize crush server" at `root.go:618/624` on give-up). No further changes.
>
> **Revision 0.8 note.** Correctness pass (Knuth-style review) — closes two
> blocking defects and five under-specified algorithms:
> (1) **§7 — id assignment** is now `id = counter + 1` with a fresh-bus first
> id of `1` (was an off-by-one: "read counter → id" gave an ambiguous first id
> against §15's `id: 1`). (2) **§7 — crash ordering**: `counter` is persisted
> *before* the record append so a crash gaps ids rather than reusing one,
> closing a window that silently violated invariant II; §12 recovery recomputes
> `counter = max(id)` from the logs. (3) **§7 — canonical seq**: per-recipient
> `seq` now comes from a counter file `state/seq.<agent_id>`, not log length
> (log length breaks under §16 Q4 compaction). (4) **§7 — locking**: one
> statement now covers all five canonical mutations, not just `send_message`.
> (5) **§8/§12 — at-least-once hazard named** (non-idempotent work, not just
> replies). (6) **§9 — `list_agents`** splits the conflated `status` enum into
> `membership` + `liveness` + free-form `status`. (7) **§9 — `reply`** lookup
> algorithm stated (linear mailbox scan in v1, `index.db` at P4) and returned
> shape aligned to `send_message`. (8) **§5 — transport vs format**: body
> constrained to round-trippable UTF-8 over the JSON MCP transport.
>
> **Revision 0.9 note.** Closed the three remaining Knuth gaps from the v0.7
> review (gap 3, `seq`, was already closed in 0.8): (1) §3 IV / §7 atomicity /
> §12 / §17 P4 — durability stated precisely: page-cache-durable across
> process crashes; power-loss durability via `fsync` is P4, not assumed. (2)
> §6 / §7 step 2 / §9 register+unregister+list_agents / §2 — membership reads
> are durable (on-disk snapshots or `registry.log` under the flock), never a
> per-process in-memory cache, so a `to_role` send can't miss a recipient
> registered by another process (invariant I); the §2 "no in-memory
> correctness" claim is now true. (3) §5 — `to` always carries the recipient;
> broadcasts produce N copies each with its own `to`.
>
> **Revision 1.0 note.** Handoff package complete. Adds §18 Conformance (the
> gate) — a deterministic, black-box suite written from the invariants BEFORE
> implementation (per `ops/parallel-workers.md` rule #5), which the implementer
> must not modify; P1 is not done until C1–C10 are green. Adds §19 Handoff
> artifacts: scope (P0/P1/P2/P4 = the bus, no-fork; P3 = a separate Crush
> fork, out of scope), the concrete `crush.json` stdio wiring, the §11.1
> role-prompt as a loadable file, and a verified MCP Go-SDK v1.7.0 server
> skeleton (`mcp.NewServer`/`s.AddTool`/`s.Run` + `&mcp.StdioTransport{}`).
> Tests are a hard requirement, not optional.

---

## 1. Problem and scope

Crush runs one agent per session. A user may run several sessions concurrently
across different project directories, each with its own role, memory, provider,
and model. Today these sessions cannot address each other. This spec defines the
smallest mechanism by which they can: a per-agent mailbox, implemented as an
append-only log file, exposed to every session as a Model Context Protocol
server.

### Goals
- Any Crush session can send a message to any other session by role or id.
- A message can be an instruction the recipient must execute and report on
  (a *prompt*), a one-way fact (*info*), or the result of a prior prompt
  (*reply*).
- The full record of what was said is recoverable from durable files alone and
  is readable with ordinary tools (`cat`, `grep`, `tail`).
- The bus holds no durable state in memory. Crashing and restarting it loses
  nothing.
- Membership is dynamic: agents are added and removed at runtime, and every
  membership operation is idempotent (repeating it converges to the same state).
- The bus core is self-contained. It depends on no external memory, file
  layout, or verification system. Deploying it on a fresh machine and starting
  it produces a working system with no prior state required.
- The bus assumes only that each agent has file primitives (`read`/`write`/
  `edit` in its own workspace) plus the bus's MCP tools. No agent needs shell,
  network, or direct access to the bus's files (§13, Agent tooling assumption).

### Non-goals
- Process supervision. The bus does not start, stop, or restart agents. The
  human launches each Crush session; the delivery loop (§11.4, a `crush server`
  turn-driver or the no-fork wrapper) is optional.
- A wire protocol beyond MCP. The bus speaks MCP over stdio; the record format
  is the only other contract.
- Cross-host operation. v1 is per-user, per-machine (spawned stdio children
  sharing one `bus_root`); no listener, no address to bind.
- High throughput. The target is a handful of agents, a few messages per
  second. The design is correct under that load and makes no claim beyond it.
- Integration with any specific external memory or signoff system (e.g. the
  author's exomemory). Such integration is an external adapter loaded into the
  agent's bootstrap prompt (§11.3); the bus core assumes nothing about it.

---

## 2. Architecture

```
                    ┌──────────────────────────────────────┐
                    │  Mailbox Bus (Go binary)              │
                    │  stdio MCP server, spawned per session│
                    │  stateless façade over files          │
                    └─────────────────┬────────────────────┘
                                      │ MCP/stdio
        ┌─────────────────┬───────────┴───────────┬─────────────────┐
        │                 │                       │                 │
   ┌────┴──────┐    ┌──────┴─────┐          ┌──────┴──────┐   ┌──────┴──────┐
   │ orchestrator│   │ frontend-dev│         │ backend-dev  │   │ test-writer │
   │ (TUI)       │   │ (crush run) │         │ (crush run)  │   │ (crush run) │
   │ own .crush  │   │ own .crush  │         │ own .crush   │   │ own .crush  │
   │ own model   │   │ own model   │         │ own model    │   │ own model   │
   │ own role.md │   │ own role.md │         │ own role.md  │   │ own role.md │
   └─────────────┘   └─────────────┘         └─────────────┘   └─────────────┘
```

Each session is a peer. The bus is a router and registry, nothing more. Because
the bus holds no in-memory correctness state — every correctness read (dedup, recipient expansion, registry) goes to durable `bus_root` files under the flock (§7, §6) — it may be run as
**one stdio process per session** — each Crush session spawns its own bus child
via `mcp` config, and all of them converge on the same `bus_root` files, with
`flock` on `state/lock` as the cross-process critical section. No long-lived
daemon, no port, no shared process is required. Crush selects the transport via
`createTransport` (`internal/agent/tools/mcp/init.go:999`, v0.91.0): stdio is
the `mcp.CommandTransport` branch (`init.go:1001`), HTTP the
`mcp.StreamableClientTransport` branch (`init.go:1028`), SSE the
`mcp.SSEClientTransport` branch. v1 uses stdio.

Per-session `crush.json` files define each peer's provider, model, permissions,
and role, and point at the bus via `mcp add bus --command <bus-binary> --env
BUS_ROOT "$HOME/.crush-mailbox-bus"` (default MCP type is stdio).

---

## 3. Invariants

The system is correct iff these hold. Every later section is a consequence of
them.

- **(I) At-least-once delivery.** A message accepted by `send_message` is
  appended to every recipient's mailbox before the call returns. It is never
  silently dropped.
- **(II) No duplication.** Each accepted message produces exactly one record per
  recipient. Producers retry with `dedup_id`; consumers are idempotent by
  convention (§8). A redelivery after a crash is harmless on both sides.
- **(III) Per-pair ordering.** Messages from sender *S* to recipient *R* appear
  in *R*'s mailbox in the order *S* sent them. There is no global ordering
  guarantee across senders.
- **(IV) Durability.** Every fact the bus needs to be correct (assigned ids,
  delivered records, the registry) is written to files (the OS page cache)
  before any call returns that depends on it, so it survives a **process**
  crash. v1 does **not** claim power-loss durability — that requires
  `fsync`-before-return, deferred to P4 (§17). In-memory caches are
  rebuildable from files.
- **(V) Legibility.** The record of all messages is readable without the bus
  process, without a database client, and without parsing code beyond a single
  record grammar. `cat`, `grep`, and `tail` suffice for inspection.

---

## 4. Storage layout

```
<bus_root>/                         # default: $HOME/.crush-mailbox-bus
  state/
    counter            # one decimal integer: last assigned global message id
    lock               # flock target for the global write critical section
    seq.<agent_id>     # canonical per-recipient seq counter, monotonic, ~0
  registry.log         # canonical: append-only register/unregister/status/heartbeat events
  registry/
    <agent_id>.json    # derived snapshot, rebuildable from registry.log
  mailboxes/
    <agent_id>.log     # canonical: append-only messages delivered to that agent
  index.db             # derived (SQLite): indexes logs for fast wait_for_message + dedup_id
```

**Canonical state:** `state/counter`, `registry.log`, and `mailboxes/*.log`.
Everything else is derived and rebuildable. The bus may be killed at any time;
recovery (§12) reconstructs `registry/*.json` and `index.db` from the canonical
files. The bus never writes a path outside `bus_root` (§13).

---

## 5. The record format

Every mailbox record is a single document in this fixed grammar:

```ebnf
record      := header "---\n" body "\n\n"
header      := header-line { header-line }
header-line := field ":" SP value "\n"
field       := "seq" | "id" | "from" | "from_role" | "to" | "kind"
            | "in_reply_to" | "ts" | "dedup_id" | "len"
body        := (* exactly <len> bytes; content unconstrained *)
```

Concrete example:

```
seq: 0007
id: 23
from: orchestrator
from_role: orchestrator
to: test-writer
kind: prompt
in_reply_to: -
ts: 2026-08-24T19:40:12Z
dedup_id: -
len: 97
---
Write the gate tests for the auth module per spec/auth.md. Reply with the
commit hash when green.
```

The parser reads exactly `len` bytes for the body, so a body may contain any
bytes — blank lines, code, even a line equal to `---`. The `---` line and the
trailing blank line are visual separators and a cross-check; `len` is
authoritative for parsing. `cat`, `grep`, and `awk` on header fields remain
valid for inspection, since the header block (before `---`) never contains
blank lines.

The record format itself permits arbitrary body bytes, but the MCP transport
carries `body` as a JSON string, so a v1 body is in practice a valid UTF-8
string (nulls and control bytes JSON-escaped). The parser must not *require*
UTF-8 — it reads `len` bytes verbatim — but producers must not put bytes in a
body that the JSON transport cannot round-trip.

### Field semantics

| field         | type     | meaning                                                        |
|---------------|----------|----------------------------------------------------------------|
| `seq`         | int      | per-recipient mailbox offset, monotonic from 1                 |
| `id`          | int      | global message id, monotonic across the whole system           |
| `from`        | string   | sender `agent_id`                                              |
| `from_role`   | string   | sender's declared role at send time                            |
| `to`          | string   | recipient `agent_id` (the mailbox owner); a broadcast produces N copies, each with its own `to`                |
| `kind`        | enum     | `prompt` \| `info` \| `reply`                                  |
| `in_reply_to` | int\|`-` | the `id` this message replies to, or "-"                      |
| `ts`          | RFC3339  | UTC timestamp assigned by the bus                              |
| `dedup_id`    | str\|`-` | optional client key; a resend with the same key returns the original result and writes nothing (producer idempotency) |
| `len`         | int      | byte length of the body; authoritative for parsing (bodies may contain any bytes, including blank lines and `---`) |

### Kinds

- **`prompt`** — the body is an instruction. The recipient must execute it and
  send a `reply` citing this message's `id`. The sender may block on the reply
  via `wait_for_message` (§8).
- **`info`** — the body is a fact. No reply expected.
- **`reply`** — cites `in_reply_to`. Closes a `prompt`. Its `body` is the result
  (e.g., a commit hash, an answer, an error).

Three kinds is the whole grammar. A question is a `prompt` whose body happens to
be a question; nothing is gained by a fourth kind.

---

## 6. Identity and registry

- **`agent_id`** — a unique, human-chosen string declared in the agent's
  `crush.json` (e.g., `frontend-dev`, `orchestrator`, `test-writer-2`). Stable
  across restarts.
- **`role`** — a functional classification declared at registration. Multiple
  agents may share a role; `send_message(to_role=...)` broadcasts to all.
  `agent_id` is the identity; `role` is the address.

Registration is an append to `registry.log`:

```
event: register
agent_id: frontend-dev
role: frontend
description: Builds and maintains the web UI
working_dir: /Users/jgavinray/dev/frontend
model: qwen3.8-27b
registered_at: 2026-08-24T19:00:00Z
```

`registry/<agent_id>.json` is a derived snapshot of the latest event for that
`agent_id`, kept **on disk in `bus_root`** and therefore shared across the N
bus processes — not an in-memory cache. Re-registering on reconnect appends a
new event and rewrites the on-disk snapshot under the flock; the log is the
truth, the snapshot is a rebuildable index. Removing an agent appends an
`unregister` event and drops the snapshot; the mailbox is retained
(§10, §13). Membership state at any time is the latest event per `agent_id`:
`register` (alive) or `unregister` (removed).

**Membership reads are durable.** Because stdio runs N processes, `send_message`'s
recipient expansion (§7 step 2), `list_agents`, and `get_agent` compute the
current membership from `registry.log` (or the on-disk snapshots) **while
holding the flock** (step 2) / from disk (the read-only tools), never from a
per-process in-memory copy — a per-process copy would be stale to registrations
made by another process since this one started, and a `to_role` send could then
miss a live recipient (violating invariant I). This is the same durable-read
discipline as the `dedup_id` check (§7 step 0); it is what makes the
"consistent with concurrent registration" claim of the canonical-log lock
actually true.

---

## 7. Message delivery

**The canonical-log lock.** Every operation that mutates a canonical file —
`send_message`, `register`, `unregister`, `set_my_status`, `heartbeat` — runs
under the same global critical section, an `flock` on `state/lock`. There is
one lock, and all five mutators hold it for their whole write. Read-only tools
(`read_my_mailbox`, `wait_for_message`, `get_agent`, `list_agents`, `whoami`)
do not take it (they may observe a pre- or post-mutation snapshot, which is
harmless because every canonical write is a single atomic `write()`). This scoping
is what makes §7 step 2's role expansion consistent with concurrent registration.

`send_message` runs under that critical section:

0. If `dedup_id` is given and is already recorded, return the original
   `{id, delivered}` and do nothing else. (Producer idempotency: an agent that
   crashes after sending but before recording the result may resend safely.)

   **The dedup record must be disk-backed, not in-memory.** Under stdio there
   are N bus processes sharing `bus_root`; an in-memory set is per-process, so
   a resend routed through a *different* still-running process would not see a
   `dedup_id` recorded by another and would write a duplicate (violating
   invariant II). The check is therefore done against a durable, shared
   structure *inside* the flock — for v1 a scan of `dedup_id` headers in the
   mailbox logs (or a dedicated dedup index; see §16 Q8) — so a `dedup_id`
   recorded by any process is visible to all. The flock already serializes §7,
   so the check-and-append is atomic across processes.
1. Assign the next id: `id = counter + 1`, where `counter` is the value read
   from `state/counter` (`last assigned id`, §4). The first send over a fresh
   `bus_root` therefore yields `id: 1`, matching §15. Write `id` back to
   `state/counter` **before** appending any record (see the crash-order note
   below).
2. Expand the recipient set **from durable state** (read `registry.log` or
   the on-disk snapshots while holding this flock — never a per-process
   in-memory cache; §6):
   - `to_agent=A` → `[A]`.
   - `to_role=R` → all agents whose latest membership event is `register`
     (not `unregister`) and whose role at that event was `R`.
3. For each recipient `R_i`:
   a. Determine `seq_i` = the per-recipient `seq` counter incremented by 1.
      The canonical source of `seq` is a per-mailbox counter file
      `state/seq.<agent_id>` (initialized to `0`), **not** mailbox log length —
      log length is incorrect the moment `compact` truncates a prefix
      (§16 Q4), whereas a counter file is monotonic forever.
   b. Format the record (with `to: <R_i>`; for broadcasts all copies share the
      same `id` and `from`, each has its own `seq`).
   c. Append the record to `mailboxes/<R_i>.log` in a single `write()` syscall
      under the flock, then write the incremented `seq` back to
      `state/seq.<agent_id>`. (O_APPEND + flock guarantees no interleaving.)
4. Release the lock. Return `{id, delivered: [{agent_id, seq}]}`.

**Crash ordering (invariant II).** The counter and the mailbox are separate
files and cannot be written atomically together. The order is chosen so a crash
*gaps, never reuses*: persist `id` to `state/counter` (step 1) **before** the
record append (step 3c). A kill between the two leaves `counter` advanced past
an id that was never delivered — a harmless gap permitted by at-least-once
delivery (invariant I forbids loss, not gaps). The forbidden case, appending a
record and *then* failing to advance the counter, would reuse `id` on restart;
the write-before ordering rules it out. Recovery (§12) additionally recomputes
`counter = max(id observed across all mailbox logs)` on startup so that a
manually truncated or hand-edited log cannot cause reuse either.

The entire send is atomic with respect to other sends: a recipient either sees
the full record or none of it (single `write()` under flock; invariant II).
This is **process-level** atomicity (no interleaving); a large `write()` is
not atomic against power loss, which can leave a partial trailing record —
hence the `len` header (§5) lets a repair tool truncate to the last complete
record, and `fsync`-before-return (P4, §17) closes the power-loss window
(invariant IV).

**On the global lock.** One `flock` serializes all canonical writes. This is
deliberately coarse: it is correct (invariants I–III hold) and, at the stated
load of a handful of agents at a few messages per second, contention is
negligible and the reasoning is trivial. It is the first thing to revisit if
the load grows — the invariants only require per-mailbox ordering (III), so a
per-mailbox lock plus an atomic counter for `id` would restore independence
between disjoint recipients. That change is deferred as premature until
measured.

**Broadcasts** produce N mailbox records, all sharing one `id`. A `reply` to
that `id` cites it; the sender correlates all replies to the broadcast by
`in_reply_to`.

---

## 8. Synchronization

Consumers block or poll **depending on the session shape** (§8's two
primitives below).

- `read_my_mailbox(agent_id, since=K, ...)` returns records with `seq > K`,
  ordered by `seq`. Non-blocking.
- `wait_for_message(agent_id, since=K, ...)` blocks until a matching record with
  `seq > K` exists, then returns it. Long-poll, bounded by `timeout`. Over
  stdio this is simply a blocking tool call: the bus's handler blocks on the
  mailbox file (inotify/kqueue, falling back to backoff polling) until the
  condition is met or `timeout` elapses. Crush does not bound `CallTool`
  (`internal/agent/tools/mcp/tools.go:46`, the caller's context is passed straight
  through with no `WithTimeout`), so a blocked handler is safe.

**Use the right primitive for the session shape.** Blocking `wait_for_message`
is for autonomous loop agents (§11.4) that have nothing else to do — over stdio
it holds the pipe, so it freezes an interactive TUI for up to `timeout`. An
interactive orchestrator should not block: it polls `read_my_mailbox` when it
wants to check, or waits for a pushed message to self-drive a turn (P3,
§16 Q9/Q10). Reserve `wait_for_message` for loop peers.

The **agent owns its read cursor.** The bus is stateless about how far an agent
has read. Each agent persists `last_seq_read` in its own `.crush/` state (or
passes it explicitly each call). On restart, the agent resumes from its saved
cursor. This keeps all correctness state in canonical files plus the agent's own
state; none in the bus.

**Idempotency (invariant II in the consumer):** because `id` is globally unique
and `seq` is per-recipient monotonic, a consumer that tracks `last_seq_read` and
processes each `seq` once cannot process a *message* twice, even if it restarts
and re-reads from a checkpointed `since`. But "processes each message once" does
not by itself make the agent's *work* idempotent: a crash between handling a
`prompt` and checkpointing `last_seq_read` re-delivers it, and the agent re-runs
the work. The bus guarantees the *message* is delivered once per record; it
cannot guarantee the *work* runs once. Agents must make their work idempotent
against the prompt `id`, or accept re-execution (§12, at-least-once hazard). The
`dedup_id="<id>:reply"` convention (§11.1) makes the *reply* idempotent; it does
not make the underlying work idempotent.

`wait_for_message` returns one message. To drain a batch, the agent loops
`read_my_mailbox` until empty, then `wait_for_message` for the next.

---

## 9. MCP tool surface

The bus exposes these tools over stdio MCP. Every tool that acts on behalf
of an agent takes `agent_id` explicitly (self-identifying, no connection state).
v1 trusts the claim (spawn-scoped trust boundary, §14).

### `register`
```
register(agent_id: string, role: string, description: string,
         working_dir: string, model?: string)
  -> { agent_id, role, registered_at, status: "alive" }
```
Appends a `register` event to `registry.log` and rewrites the on-disk snapshot
`registry/<agent_id>.json` under the flock before returning. Idempotent:
re-registering on reconnect is the normal case.

### `unregister`
```
unregister(agent_id: string)
  -> { agent_id, status: "removed", unregistered_at: <ts> }
```
Appends an `unregister` event to `registry.log` and removes the on-disk snapshot
under the flock. The
mailbox `mailboxes/<agent_id>.log` is **retained** (append-only logs are the
audit record, invariant V); a later `register` with the same `agent_id` resumes
it. Idempotent: unregistering an unknown or already-removed agent is a no-op
that still returns success. `list_agents` no longer lists the agent; messages
addressed to its `agent_id` or `role` fail with `recipient_unknown` until it
re-registers.

### `whoami`
```
whoami(agent_id: string)
  -> { agent_id, role, description, working_dir, model, status, last_seen,
       registered_at }
```

### `list_agents`
```
list_agents(role?: string,
            membership?: "alive" | "removed" | "any",
            liveness?: "live" | "dead" | "any")
  -> [{ agent_id, role, membership, liveness, working_dir, model, last_seen,
        status }]
```
Reads the on-disk snapshots (or `registry.log`) — never a per-process in-memory
cache, so it sees registrations made by other processes (§6). Two orthogonal
dimensions are reported and filtered
independently, because they come from different sources:

- **membership** — the latest membership event: `alive` (latest is `register`)
  or `removed` (latest is `unregister`). Default filter `alive`.
- **liveness** — derived from `last_seen` (§10): `live` (`now − last_seen
  ≤ 90s`) or `dead` (`> 90s`). Default filter `any`.
- **status** — the free-form work-state string from `set_my_status` (e.g.
  `"idle"`, `"working: auth module"`); reported verbatim, never used as a
  filter key.

The old §0.3 `status` enum (`alive|dead|idle|removed`) conflated these three
axes into one; it is retired. Calls filtering on the old enum are a v0.3
compatibility concern only.

### `send_message`
```
send_message(to_agent?: string, to_role?: string,
             kind: "prompt" | "info" | "reply",
             body: string, in_reply_to?: int,
             dedup_id?: string)
  -> { id: int, delivered: [{ agent_id, seq }] }
```
Exactly one of `to_agent` / `to_role` required. Assigns `id` and per-recipient
`seq`, appends to each recipient mailbox (§7). `dedup_id` makes the call
idempotent: a resend with a `dedup_id` already recorded returns the original
result and writes nothing new (§7 step 0). Use it for any send the agent might
retry after a crash.

### `read_my_mailbox`
```
read_my_mailbox(agent_id: string, since?: int = 0,
                kind?: string, limit?: int = 256)
  -> [{ id, seq, from, from_role, kind, in_reply_to, ts, dedup_id, body }]
```
Returns records with `seq > since`, ordered by `seq`, optionally filtered by
`kind`.

### `wait_for_message`
```
wait_for_message(agent_id: string, since: int,
                 from_role?: string, from_agent?: string, kind?: string,
                 timeout: int)
  -> { message: { id, seq, from, from_role, kind, in_reply_to, ts, dedup_id, body } }
   | { timeout: true }
```
Blocks until a matching record with `seq > since` exists, or `timeout` seconds
elapse. Returns the first match. Timeout is not an error; the agent calls again.
Implementation: `kqueue`/`inotify` on `mailboxes/<agent_id>.log` — which works
across the N bus processes that share `bus_root` — falling back to backoff
polling.

### `reply`
```
reply(in_reply_to: int, agent_id: string, body: string,
      dedup_id?: string)
  -> { id: int, delivered: [{ agent_id, seq }] }
```
Convenience. Looks up message `in_reply_to`, sends a `reply` to its `from` with
`in_reply_to` set. Common path for closing a `prompt`. `dedup_id` as in
`send_message`. Return shape matches `send_message` (a `reply` has one
recipient, so `delivered` has one element).

**Lookup (v1).** The original message lives in the caller's own mailbox. v1 has
no id→record index, so the lookup is a linear scan of `mailboxes/<agent_id>.log`
for `id == in_reply_to`; on miss, return `in_reply_to_not_found` (§9 Errors).
O(mailbox size) per call — acceptable for a handful of agents; `index.db`
(§17 P4) turns it into an O(1) index lookup.

### `set_my_status`
```
set_my_status(agent_id: string, status: string)
  -> { ok: true }
```
`status` is a short free-form string (`"idle"`, `"working: auth module"`,
`"blocked: waiting on spec"`, `"done"`). Appends a `status` event to
`registry.log`, updates the snapshot.

### `heartbeat`
```
heartbeat(agent_id: string)
  -> { ok: true, last_seen: <ts> }
```
Updates `last_seen` in the snapshot. **Observability only.** No correctness
property depends on it (the invariants do not mention liveness). It exists so
`list_agents` can show which autonomous loops are still breathing.

### `get_agent`
```
get_agent(agent_id: string)
  -> { agent_id, role, description, working_dir, model, status, last_seen,
       registered_at, recent: [{ id, seq, from, kind, ts, body_excerpt }] }
```
For inspection from any session.

### Errors
Standard MCP error codes. Bus-specific:
- `agent_not_registered` — `agent_id` has no live `register` event.
- `agent_removed` — `agent_id`'s latest membership event is `unregister`.
- `recipient_unknown` — `to_agent` names no registered agent; `to_role` matches
  none.
- `in_reply_to_not_found` — `reply` cites an unknown `id`.
- `no_such_mailbox` — `agent_id` in `read_my_mailbox` / `wait_for_message` is
  not registered.
- `timeout` is **not** an error (returned as `{timeout: true}`).

---

## 10. Agent lifecycle

1. **Register.** On session start, the agent calls `register`. Its `crush.json`
   sets `agent_id` (via env or a fixed config value); every MCP call from that
   session carries it.
2. **Heartbeat.** Autonomous loop agents (§11.4) call `heartbeat` every 30s.
   Idle TUI sessions need not; `list_agents` shows their `last_seen` as stale,
   which is honest.
3. **Status.** Agents call `set_my_status` when their state changes
   meaningfully.
4. **Completion.** On completing a `prompt`, an agent calls `reply`. The reply
   is appended to the sender's and recipient's mailbox logs; the logs are the
   verifiable record. The bus writes no separate signoff file (see §13 for
   why, and for how external verification reads the logs).
5. **Unregister.** When an agent is done permanently, it (or its supervisor)
   calls `unregister`. The agent leaves `list_agents`; its mailbox is retained
   for audit and reuse if it re-registers.
6. **Death.** An agent that stops heartbeating is shown as `dead` by
   `list_agents` once `now - last_seen > 90s`. This is informational. The human
   decides whether to relaunch or `unregister`. No message is lost; the mailbox
   retains everything.

A `dead` or `removed` agent's mailbox is retained. Re-registering with the same
`agent_id` reuses it; the agent resumes from its saved cursor.

---

## 11. Agent bootstrap

Every Crush session that joins the bus is bootstrapped by a prompt assembled
from two layers: a **core role prompt** that every agent must receive to use
the bus correctly, and a set of **optional context modules** that tailor the
agent to a project or a working convention. The bus core knows nothing about
either layer; both live in files Crush already reads (CRUSH.md, AGENTS.md, a
role file, or a Skill), so adding the bus to a session changes no Crush
mechanism.

### 11.1 Core role prompt (required)

The minimum contract every agent receives, interactive or autonomous:

```
You are <role> (agent_id: <id>) on the Mailbox Bus.
- On start, call register(agent_id=<id>, role=<role>, description=...,
  working_dir=<cwd>, model=<your model id>).
- Call read_my_mailbox(agent_id=<id>, since=<last_seq_read>). For each record:
    kind=prompt: do the work in the body, then reply(in_reply_to=<id>,
      body=<result>, dedup_id="<id>:reply").
    kind=info:  note it; no reply.
    kind=reply: record the result for the cited prompt.
  Persist last_seq_read to .crush/last_seq after each handled record.
- If no messages, call wait_for_message(agent_id=<id>, since=<last_seq_read>,
  timeout=60); on {timeout:true}, you may exit (the delivery loop re-invokes
  you, §11.4).
- Never assume another agent is alive; address by role when you can.
- Verify others' work by calling read_my_mailbox / get_agent and inspecting
  the returned records, never by trusting a transcript. You do not have
  direct filesystem access to the bus's logs; the MCP tools are the only
  window onto them.
```

### 11.2 Optional context modules (pluggable)

Anything project- or environment-specific lives in a separate file loaded by
Crush's existing mechanisms, not in the core prompt:

- `context_paths` / `global_context_paths` in crush.json (CRUSH.md, AGENTS.md),
- a Crush **Skill** (a SKILL.md the agent loads on trigger),
- or an extra prompt fragment passed by the delivery loop (§11.4).

The bus core never reads these. They are how an agent learns *what* to do
beyond the protocol; the core prompt is only *how* to talk on the bus.

### 11.3 The exomemory strategy module (optional, outlined)

When the deployment is the author's exomemory environment, agents load an
exomemory module that tells them how to participate in that side-band memory.
This is the place to make the exomemory strategy explicit and reproducible; it
is NOT a property of the bus, so a bus deployed elsewhere simply omits it and
nothing breaks. The module instructs each agent to:

- On start, read `~/dev/exomemory/signoff.md` first, and the standing rules in
  `~/dev/exomemory/CLAUDE.md`, before doing anything else.
- Follow the standing rules verbatim: date-stamp facts and treat them as
  perishable; verify by file, never by transcript; single-writer on the day
  file (orchestrator only).
- Verify completion through the bus's MCP tools, not a separate signoff file.
  When the agent finishes a `prompt`, its `reply` lands in
  `mailboxes/<orchestrator>.log` with `from: <role>` and `in_reply_to: <prompt
  id>`; the agent confirms completion by calling `read_my_mailbox`, and the
  orchestrator confirms siblings the same way. The legacy dedicated signoff
  files (with their fixed tokens) are retired — the structured
  `from`/`in_reply_to` fields are a more reliable target than a magic string,
  and a second file is a second source of truth. A host-side gate (the human's
  `run-workers.sh` equivalent) may still `grep` the mailbox log; that is the
  human's tool, not the agent's.
- Stagger any sibling launches it triggers by >= 3s (the Crush SQLite
  cold-start race), or — preferred — give each agent its own `--data-dir` so
  the race cannot occur.
- Treat `~/dev/exomemory/wiki/` as the home for durable knowledge; add entries
  and update `wiki/index.md` + `wiki/log.md` when a session produces long-lived
  findings, not the day file.
- Keep the orchestrator's context lean: fan-out reading goes to subagents,
  conclusions come back; unchanged context is a prompt-cache hit.

This module is the single source of truth for "how to be an exomemory agent."
Because it is a file the agent reads, not a bus feature, a fresh machine with
no exomemory tree runs the bus fine; its agents simply load a different module
(or none).

### 11.4 The delivery loop (preferred: `crush server`; fallback: wrapper)

**Preferred (target architecture).** Run all agents as sessions hosted by a
single `crush server` process (`CRUSH_CLIENT_SERVER=1`), each a thin client
that connects over the server's Unix socket (§16 Q10). The server is the one
process that spawns the bus MCP child *and* runs agent turns
(`internal/app/app.go:145`, `internal/backend/agent.go:87`), so once push
delivery is wired (§16 Q9 a+b), an incoming `notifications/claude/channel` from
the bus routes to the owning workspace and self-drives `AgentCoordinator.Run` —
no host-side loop, no repeated `crush run` invocations. An idle agent idles in
the server; a message wakes it. This is the clean form of "the loop lives in
the Go binary," and it lives in *Crush's* binary, not the bus's.

**Fallback (no-fork, until Q9 a+b land).** Where the client/server path is not
used, or push delivery is not yet wired, drive each autonomous peer with a tiny
host-side wrapper that re-invokes `crush run` on exit:

```sh
#!/bin/sh
# agent-loop <agent_id> <role-prompt-file>
while :; do
  crush run --data-dir ".crush-$1" \
            --yolo \
            "$(cat "$2")"
  sleep 1
done
```

The wrapper is a **host-side launcher** the human writes and runs; it is not
agent tooling and not a Crush or MCP feature — it is the pulse that turns
"run one batch and exit" into "run repeatedly." The agent inside each
`crush run` needs only its file primitives (`read`/`write`/`edit` in its own
workspace) plus the bus's MCP tools — no shell, network, or direct access to
`bus_root`. Each `crush run` is one batch: read mail, handle by kind, persist
the cursor with `write` to `.crush/last_seq`, exit. The wrapper re-invokes.
State lives in the mailbox and the cursor file, not in the process, so a crash
loses no work and no messages. This generalizes `ops/parallel-workers.md` with
the addition of inter-agent messaging. The orchestrator stays an interactive
TUI; the human drives it and watches peers via `list_agents` / `get_agent`.

---

## 12. Durability and recovery

- **Bus crash.** On restart: recompute `state/counter = max(id observed across
  all mailbox logs)`, then replay `registry.log` to rebuild `registry/*.json`;
  rebuild `index.db` from `mailboxes/*.log`. (The counter recomputation closes
  the crash-order gap of §7: even a hand-truncated log cannot make the next
  `id` reuse one already delivered.) In-flight `wait_for_message` calls return
  `{timeout: true}` (the stdio connection died with the child process); agents
  simply re-call. No message is lost (invariant I) because every accepted
  message was appended before `send_message` returned (invariant IV). A
  per-session stdio deployment loses only that session's own child on crash;
  the shared `bus_root` files are untouched by the death of any one process.
- **Agent crash.** The agent's `last_seq_read` is persisted in its own `.crush`
  after each handled message. On restart it resumes from there; redelivery of
  any message handled-but-not-checkpointed is harmless **only for idempotent
  work** — see the at-least-once hazard below. A resend of a
  `send_message`/`reply` uses `dedup_id` so it does not duplicate (§7 step 0).
- **At-least-once / duplicate-work hazard (explicit).** Invariant I delivers
  at-least-once. Agent *work* (a commit, a file edit outside `bus_root`) is not
  idempotent, so a crash between handling a `prompt` and checkpointing
  `last_seq_read` re-runs that work. The bus cannot make the work itself
  idempotent — it can only make the *effects it owns* (the record append, the
  reply) idempotent via `dedup_id`. The contract is therefore: agents must make
  their work idempotent against the prompt `id` (e.g. a `git commit` recorded as
  "already done for id N" before acting), or accept re-execution. This hazard is
  named here and in §8, not silently assumed away.
- **Disk full / write error.** `send_message` returns an error and the record is
  not appended; the sender retries. No partial records (single `write()` under
  flock).
- **Corrupted log (power loss).** v1 is page-cache-durable (IV): a process
  crash loses nothing; a power-loss crash may leave a partial trailing record.
  The `len` header (§5) lets a repair tool truncate to the last complete record
  (boundary = `header + "---\n" + len bytes + "\n\n"`). `fsync` before return,
  which closes the power-loss window entirely, is P4 (§17).

---

## 13. Self-containment and idempotency

The bus core assumes nothing outside its own `bus_root`. It runs on a fresh
machine with no prior state, no external memory, and no sibling project;
starting it creates what it needs. Every operation that changes membership or
sends a message is idempotent or safely retryable, so a crash anywhere can be
recovered by re-running the last action.

The MCP server is a convenience over the files; it holds no correctness.
Correctness lives in the plain-text logs, which any reader can inspect — the
bus's own `read_my_mailbox` tool, a human with `cat`, or a CI job with
`grep`. The server exists so LLMs can do this via a standard protocol, not
because the files require it. Agents never touch `bus_root` directly; the
MCP tools are their only window onto the logs.

### Self-containment
- The only path the bus reads or writes is `bus_root` (default
  `$HOME/.crush-mailbox-bus`; override with `BUS_ROOT` or `--bus-root`). No
  path outside `bus_root` is touched, ever — no exomemory tree, no project
  repo, no `~/.config`.
- On start, the bus creates `state/`, `registry/`, `mailboxes/`, and
  `registry.log` if absent (`mkdir -p` semantics); `state/counter` is created
  with value `0` if absent. Starting on an empty `bus_root` and on a populated
  one yields the same running system; existing logs are never truncated or
  reset.
- The bus has no dependency on any exomemory, signoff directory, wiki, or
  verification convention. Those are external concerns loaded into agent
  bootstrap prompts (§11.3); the bus does not know they exist.

### Agent tooling assumption
The bus assumes each agent has only its file primitives (`read`, `write`,
`edit`, scoped to its own working directory) plus the bus's MCP tools. It
never requires an agent to run shell commands, access the network, or touch
`bus_root` directly. Every protocol action — register, send, receive, wait,
verify a sibling's completion, persist the read cursor — is expressible
through the MCP tools plus a single `write` of `last_seq_read` to the agent's
own `.crush/`. The human enforces this per agent via crush.json
`permissions.allowed_tools` (e.g., allow only `view`/`edit`/`write` plus the
`bus.*` MCP tools, disable `bash`). Verification of "agent X replied to prompt
Y" is done by an agent calling `read_my_mailbox` / `get_agent` and inspecting
the returned records; shell tools (`grep`, `awk`) on the logs are reserved for
host-side verifiers the human runs (a CI job, `run-workers.sh`), not for
agents.

### Idempotency
- `register` is idempotent: re-registering the same `agent_id` appends a new
  `register` event and refreshes the snapshot. Repeating it N times leaves the
  agent registered once with the latest field values.
- `unregister` is idempotent: unregistering an unknown or already-removed
  `agent_id` appends a no-op `unregister` event and returns success. Repeating
  it leaves the agent removed.
- `heartbeat` and `set_my_status` are idempotent (last-write-wins on the
  snapshot; the log grows but the derived state is a pure function of the
  latest event).
- `send_message` is idempotent **when `dedup_id` is supplied**: a resend with
  a `dedup_id` already recorded returns the original `{id, delivered}` and
  writes nothing (§7 step 0). Without `dedup_id` each call is a distinct event
  (correct for genuinely new messages); consumers dedup by `id` if they could
  receive a redelivery (§8).
- `read_my_mailbox`, `wait_for_message`, `get_agent`, `list_agents`, `whoami`
  are read-only; repeating them is safe.
- Deployment of the bus itself is idempotent: there is no `init` step that can
  corrupt state. The first start self-initializes; subsequent starts reuse.
  Reinstalling the binary and restarting changes no data.

### External verification (replaces a signoff side-channel)
Because the canonical logs are append-only and `cat`-readable (invariant V),
verifying "agent X completed prompt Y" is done against the bus logs, not a
side-channel file the bus writes. There are two readers of the one record:

- **Agent path (primary).** The orchestrator calls
  `read_my_mailbox(agent_id="orchestrator", kind="reply")` (or `get_agent`)
  and inspects the returned records for `from: <role>` and `in_reply_to:
  <prompt id>`. No shell, no second file — the MCP tools are the agent's only
  window onto the logs (see Agent tooling assumption above).
- **Host path (for humans/CI).** A `run-workers.sh`-style gate or a CI job run
  by the human reads the plain-text log directly:

```sh
# "Did backend-dev reply to prompt id 2, and frontend-dev to id 1?"
awk 'BEGIN{RS="\n\n"}
     /in_reply_to: 1/ && /from: frontend-dev/ {f=1}
     /in_reply_to: 2/ && /from: backend-dev/  {b=1}
     END{exit !(f&&b)}' "$BUS_ROOT/mailboxes/orchestrator.log" || exit 2
```

One canonical record, two readers, no second source of truth. The legacy
dedicated signoff files (and their magic tokens) are retired. The bus core
never writes a file outside `bus_root`.

---

## 14. Security and trust model

v1: the bus is **stdio** and therefore requires **no network listening, no
binding, and no authentication**. Each Crush session spawns its own bus child
process (the `mcp.CommandTransport` branch of `createTransport`,
`internal/agent/tools/mcp/init.go:1001`), which has exactly the parent session's
privileges. The trust boundary is the spawn, not a port.

`agent_id` is self-declared; a malicious local process could impersonate an
agent by writing to `bus_root`. This is acceptable because:

- The threat model is "cooperating agents on one machine run by one user," not
  adversarial multi-tenant.
- The canonical logs are append-only and auditable, so impersonation is
  detectable after the fact.

A v1.1 may gate writes with filesystem ownership on `bus_root` (mode `0700`)
rather than a header — there is no HTTP header channel to protect over stdio.
It is intentionally absent from v1 to keep the mechanism minimal.

---

## 15. Worked example

Three agents: `orchestrator` (TUI), `frontend-dev` (loop), `backend-dev` (loop).
All registered. Empty mailboxes to start.

**Step 1.** Orchestrator sends two prompts.

```
send_message(to_role="frontend", kind="prompt",
  body="Build /login posting to /api/auth. Reply with the commit hash.")
  -> { id: 1, delivered: [{agent_id:"frontend-dev", seq:1}] }

send_message(to_role="backend", kind="prompt",
  body="Build POST /api/auth: email+password -> JWT. Reply with the commit hash.")
  -> { id: 2, delivered: [{agent_id:"backend-dev", seq:1}] }
```

`mailboxes/frontend-dev.log` now contains:

```
seq: 1
id: 1
from: orchestrator
from_role: orchestrator
to: frontend-dev
kind: prompt
in_reply_to: -
ts: 2026-08-24T19:40:12Z
dedup_id: -
len: 62
---
Build /login posting to /api/auth. Reply with the commit hash.
```

**Step 2.** `frontend-dev` loop wakes, reads, handles, replies.

```
reply(in_reply_to=1, agent_id="frontend-dev",
  body="Done: /login -> /api/auth, commit a1b2c3.",
  dedup_id="frontend-dev:reply:1")
  -> { id: 3, seq: 1, delivered_to: [{agent_id:"orchestrator", seq:1}] }
```

`mailboxes/orchestrator.log`:

```
seq: 1
id: 3
from: frontend-dev
from_role: frontend
to: orchestrator
kind: reply
in_reply_to: 1
ts: 2026-08-24T19:41:50Z
dedup_id: frontend-dev:reply:1
len: 41
---
Done: /login -> /api/auth, commit a1b2c3.
```

**Step 3.** `backend-dev` likewise → `id: 4`, delivered to `orchestrator` `seq: 2`.

**Step 4.** Orchestrator waits for both replies.

```
wait_for_message(agent_id="orchestrator", since=0, kind="reply", timeout=600)
  -> { message: {id:3, seq:1, from:"frontend-dev", ...} }
wait_for_message(agent_id="orchestrator", since=1, kind="reply", timeout=600)
  -> { message: {id:4, seq:2, from:"backend-dev", ...} }
```

For this example the orchestrator is treated as a loop peer deliberately waiting
on both replies; an interactive orchestrator would instead poll
`read_my_mailbox` to avoid freezing the TUI (§8).

**Step 5.** Orchestrator gates.

The orchestrator (an agent) verifies by calling the bus, not by shelling out:

```
read_my_mailbox(agent_id="orchestrator", since=0, kind="reply")
  -> [{id:3, seq:1, from:"frontend-dev", in_reply_to:1, ...},
      {id:4, seq:2, from:"backend-dev",  in_reply_to:2, ...}]
```

Both expected replies present (`from` + `in_reply_to`), so the gate passes.
The human may instead run a host-side gate over the plain-text log:

```sh
awk 'BEGIN{RS="\n\n"}
     /in_reply_to: 1/ && /from: frontend-dev/ {f=1}
     /in_reply_to: 2/ && /from: backend-dev/  {b=1}
     END{exit !(f&&b)}' "$BUS_ROOT/mailboxes/orchestrator.log" || exit 2
cd repo && python -m unittest test_auth -v
```

Either way, the canonical record is the mailbox log — one source of truth, no
dedicated signoff files. The agent path uses MCP tools; the host path uses
shell; neither requires the other.

---

## 16. Open questions and risks

1. **Long-poll `wait_for_message` over stdio — RESOLVED (verified in Crush
   v0.91.0 source; `internal/agent/tools/mcp/`).** Crush implements all
   three transports: stdio (`mcp.CommandTransport`), Streamable HTTP
   (`mcp.StreamableClientTransport`, the 2025-06-18 transport, selected by
   `type:"http"`), and SSE (`mcp.SSEClientTransport`, `type:"sse"`) — see
   `createTransport` in `init.go:999`. The per-server `timeout` config
   (`mcpTimeout`, default 10s) bounds **initialization** (`createSession`'s
   `time.AfterFunc`, stopped on successful `Connect`) and the **ping**
   (`pingSession`'s `context.WithTimeout`) only. It does **not** wrap `CallTool`:
   `RunTool` calls `c.CallTool(ctx, ...)` with the caller's context and no
   `WithTimeout` wraps it anywhere in the package. The MCP spec (2025-06-18)
   imposes no protocol-level timeout and explicitly lets a server hold a
   `tools/call` indefinitely (it recommends clients implement their own; Crush
   does not bound tool calls). So a bus holding `wait_for_message` for minutes is
   bounded only by the agent's turn context, not by Crush. P0 is now an
   integration sanity check, not an unknown.
2. **MCP transport choice — RESOLVED: stdio.** The bus runs as one stdio child
   per session (v0.91.0 `createTransport`, `init.go:1001`), all converging on
   shared `bus_root` files with `flock` for the critical section. stdio removes
   the daemon-to-supervise, the port, and the auth surface entirely (§14), and
   blocking `wait_for_message` calls are natural over stdio. SSE is needed only
   if we later stream partial output; it is not used.
3. **`agent_id` collision.** Two live sessions registering the same `agent_id`
   both appear `alive` and both read the same mailbox. v1: the human guarantees
   uniqueness via crush.json. v1.1: `register` rejects an `agent_id` held by an
   agent with a recent `heartbeat`; `unregister` is the explicit release.
4. **Mailbox growth.** Logs are append-only and unbounded. v1 does not compact.
   A future `compact` tool can snapshot consumed prefixes to a cold file and
   truncate. Out of scope here.
5. **`from_role` drift.** An agent may change role between sends; `from_role`
   is recorded per message, so history is truthful. `list_agents` shows the
   latest role. Acceptable.
6. **Reply-to-broadcast correlation.** A broadcast `id` may yield many replies.
   The sender correlates by `in_reply_to == id`. No aggregate "all replies
   received" primitive in v1; the sender counts (it knows the recipient count
   from `send_message`'s `delivered` list).
7. **Time.** `ts` is bus-assigned on append, not sender-claimed, so ordering
   by `id` and by `ts` agree within a single bus.
8. **`dedup_id` lookup cost and multi-process visibility.** Step 0 of
   `send_message` checks whether a `dedup_id` already exists across all
   mailboxes. Because stdio runs N processes sharing `bus_root`, this check
   **must read durable state, not an in-memory set** (a per-process set cannot
   see a `dedup_id` recorded by another process; §7 step 0). For v1, do the
   check inside the flock by scanning `dedup_id` headers in the mailbox logs
   (handful of agents keeps this cheap); at scale it needs `index.db` (§17 P4)
   or a dedicated dedup index, so the check becomes an O(1) index lookup rather
   than a log scan.
9. **Push (server→client injection) — RESOLVED (rewritten): Crush already has a
   channel mechanism.** Crush v0.91.0 implements the `claude/channel`
   experimental capability (`internal/agent/tools/mcp/channel.go`): a server
   declares `capabilities.experimental["claude/channel"]` (`channel.go:29`) and
   pushes `notifications/claude/channel` events server→client
   (`channel.go:35`). Crush intercepts these at the transport layer
   (`channelConn.Read`, `channel.go:309`) and publishes a must-deliver
   `EventChannelMessage` (`publishChannelMessage`, `channel.go:183`) carrying a
   rendered, escaped `<channel>` element. The server is opted-in via the hidden
   `--channels` flag (`internal/cmd/root.go:63`).

   **The gap is delivery, not the mechanism.** `SubscribeEvents`
   (`init.go:232`) deliberately *excludes* `EventChannelMessage`: the MCP
   broker is process-global and the event carries no workspace/session identity,
   so routing is deferred — "Channel delivery requires workspace-scoped routing,
   which is deferred to a later PR" (`init.go:230`). And `server/events.go:43`
   drops `EventChannelMessage` for SSE ("no proto representation until session
   delivery is wired up"). So push is **received-but-not-injected**: the
   pipeline ends at the broker; nothing routes a pushed message into an active
   session's context yet.

   Consequences for v1:
   - The pull model (§8) remains the **no-fork path** and is sufficient: the
     agent LLM polls `read_my_mailbox` / `wait_for_message`. The loop wrapper
     (§11.4) is a stopgap *for the missing delivery + self-drive*, not for a
     missing transport.
   - The **targeted fork** is two small changes in Crush, both in the
     client/server path (§16 Q10): (a) route `EventChannelMessage` to the owning
     workspace (identity is available server-side), and (b) on delivery, invoke
     `AgentCoordinator.Run` so the pushed mail becomes a fresh turn — i.e. the
     agent self-drives. Both live in `crush server`, not the bus.

10. **The long-lived process already exists: `crush server`.** Crush v0.91.0
    ships a client/server architecture gated by `CRUSH_CLIENT_SERVER=1`
    (`internal/cmd/root.go:294-298`, `useClientServer`). `crush server`
    (`internal/cmd/server.go`) is a long-lived daemon over a Unix socket
    (`crush-<uid>.sock`, `internal/server/server.go:74`); `crush run --host`
    and `crush --host` connect to it and become thin clients. Critically, **the
    server is the process that both spawns the MCP clients and runs agent
    turns**: `app.New` fires `go mcp.Initialize(...)`
    (`internal/app/app.go:145`), and the backend runs turns via
    `runAgent` → `ws.AgentCoordinator.RunAccepted(...)`
    (`internal/backend/agent.go:87`, `:97`). The server embeds `*app.App`
    (`internal/backend/backend.go:171`), so MCP (which is process-global) and
    the agent loop already live in one process.

    This is the natural home for the delivery loop: the bus stays a dumb stdio
    child, and `crush server` is the thing that receives the channel push and
    drives the next turn. With Q9 (a)+(b) wired, the host-side loop wrapper
    (§11.4) disappears entirely — each agent is a session hosted by one server,
    idling until a pushed message self-drives a turn. The wrapper remains only
    as the documented no-fork fallback for deployments that do not run the
    client/server split.
11. **`crush server` socket-init race (P3 risk).** The client/server path P3
    depends on has a documented startup race: `internal/cmd/clientserverrace/
    race_test.go` is a regression test for "the CRUSH_CLIENT_SERVER=1
    socket-init race documented in `docs/notes/2026-05-11-client-server-socket-
    init-race.md` (item F5)" (v0.91.0). The test is reproducible; the note it
    cites is not present in the public checkout (`docs/` ships only `config/`
    and `hooks/`), so do not rely on the note's mitigation. P3 must instead
    ensure, behaviorally, that the server is listening before any client
    connects: observe the readiness probe that `ensureServer` performs
    (`root.go`, emitting "failed to initialize crush server" when it gives up),
    and reuse or replicate that wait rather than assume the socket is bound
    immediately after spawn.

---

## 17. Implementation phases

- **P0 — Integration sanity check.** Stand up a trivial **stdio** MCP server
  with one tool that blocks 30s then returns. Point a Crush session at it via
  `crush.json` `mcp.bus` (`--command <binary>`, default stdio type). Confirm the
  call completes end-to-end. (The long-poll question is already resolved by
  source reading in §16; this is belt-and-suspenders before the build.)
- **P1 — Bus MVP.** Single Go binary (see language note below), SQLite
  optional (files first). Tools: `register`, `unregister`, `list_agents`,
  `send_message` (with `dedup_id`), `read_my_mailbox`, `wait_for_message`,
  `reply`. No `heartbeat`. Recipient expansion and `list_agents` read membership
  from durable `registry.log` / on-disk snapshots under the flock, not a
  per-process cache (§6). Durability is page-cache only (IV); `fsync` is P4.
  Proves invariants I–V with two loop agents and one orchestrator, and proves
  `register`/`unregister`/`send_message` idempotency. **DoD: the §18 conformance
  gate (C1–C10) is green** on a clean `bus_root`; the gate is written from the
  spec before P1 starts and the implementer does not modify it (per
  `ops/parallel-workers.md` rule #5).
- **P2 — Lifecycle + bootstrap.** Add `heartbeat`, `set_my_status`,
  `get_agent`, `whoami`. Write the core role prompt (§11.1) and the exomemory
  strategy module (§11.3) as installable context files. Write the external
  adapter example that gates a build by reading `mailboxes/orchestrator.log`
  (§13). Verify `register`/`unregister` idempotency under repeat and crash.
- **P3 — Delivery via `crush server` + channel (the Crush fork).** Wire
  workspace-scoped routing for `EventChannelMessage` (§16 Q9-a) and turn it
  into an `AgentCoordinator.Run` (§16 Q9-b), so a pushed mailbox message
  self-drives the receiving agent's turn inside `crush server`. This replaces
  the loop wrapper (§11.4) with a native, server-hosted idle+wake. Dependent on
  the `CRUSH_CLIENT_SERVER` architecture; the wrapper remains the fallback
  until this lands. Mind the known socket-init race (§16 Q11) — the server
  must be listening before clients connect.
- **P4 — Hardening.** `index.db` for fast `wait_for_message` and `dedup_id`
  lookup; **`fsync`-before-return** (closes the power-loss durability window
  of invariant IV; §12); `compact` tool. Auth is moot over stdio (§14); file
  ownership on `bus_root` (mode `0700`) if a shared-boundary mode is ever
  needed.

### Implementation language: Go

The reference implementation is Go. Reasons:
- Crush is Go (charmbracelet); a future upstream `spawn_agent` builtin or an
  in-process bus mode would share the language and toolchain.
- The official MCP Go SDK (`modelcontextprotocol/go-sdk`) makes a correct
  stdio MCP server a small amount of code.
- The bus is file + SQLite glue, which is Go's strength; goroutines map
  naturally onto long-poll `wait_for_message` handlers.
- A single static binary matches Crush's distribution model.

Rust (official `rmcp` SDK) would be the pick if strict memory safety or a much
larger footprint were required; neither dominates for this service at this
scale. The protocol and record format are language-agnostic, so a Rust (or
Python, or Node) implementation remains a drop-in alternative if priorities
change.

---

## 18. Conformance (the gate)

The gate is a deterministic, **black-box** test suite written from this spec
BEFORE implementation, per the exomemory runbook (`ops/parallel-workers.md`
invariant #5: "Gate with a deterministic suite written from the spec, before
workers launch. No self-authored-tests."). It drives the bus binary as a stdio
MCP client — the same transport Crush uses — and asserts on tool results and on
the on-disk `bus_root` files, never on Go internals, so it tests the contract,
not the implementation. The implementer MUST NOT modify the gate; a failing
test is a bug in the bus, not in the test. **P1 (§17) is not done until the
whole gate is green; a skipped test is a loud failure, never silent.** Tests
are a hard requirement for handoff, not optional.

The suite uses a fresh temp `BUS_ROOT` per test, two or three bus children
(separate stdio processes) where concurrency matters, and the on-disk logs as
the oracle. Required cases, each mapped to the invariant it proves:

- **C1 — At-least-once delivery (I, §7).** `send_message(to_agent=B,
  kind=info)` from A; assert B's `read_my_mailbox` returns exactly that record
  and that the record is present in `mailboxes/B.log`.
- **C2 — No duplication across processes (II, §7 step 0).** Two bus children
  P1, P2. `send_message` with `dedup_id=K` via P1 → `{id, delivered}`; resend
  the same `dedup_id=K` via P2 → assert P2 returns the **original**
  `{id, delivered}` and writes no new record (exactly one record with
  `dedup_id: K` across all mailboxes).
- **C3 — Per-pair ordering (III, §7).** A sends m1 then m2 (both `to_agent=B`);
  assert B receives them in `seq` order 1, 2 in `mailboxes/B.log`.
- **C4 — Crash-order: gap, never reuse (II, §7 crash ordering, §12).** Kill
  the bus child between the `state/counter` write and the record append;
  restart; assert the next `id` is the killed counter (a gap), and that no
  delivered `id` is ever reused (recovery recomputes `counter = max(id)`).
- **C5 — Durable recipient expansion (I, §7 step 2, §6).** P2 starts. P1
  `register`s an agent in role R *after* P2 started. P2
  `send_message(to_role=R)` → assert the late-registered agent receives it (P2
  read durable `registry.log`, not a stale per-process cache).
- **C6 — Idempotent consumer cursor (I, §8).** Consumer handles `seq` N,
  crashes before persisting `last_seq_read`; restart, re-read from `since =
  N-1`; assert the handler runs once given an `id`-idempotent handler (no
  duplicate work).
- **C7 — Register / unregister / re-register (§9, §10).** `register` A;
  `list_agents` shows A. `unregister` A; `list_agents` excludes A,
  `send_message(to_agent=A)` fails `recipient_unknown`, `mailboxes/A.log` is
  retained. `register` A again; mailbox reused, cursor resumes.
- **C8 — Page-cache durability + recovery (IV, §12).** Accept a
  `send_message` (returned); kill the bus child (process crash); restart;
  assert recovery recomputes `counter = max(id)` and the accepted record is
  intact. (Power-loss durability is explicitly NOT asserted in v1 — it is
  P4/`fsync`.)
- **C9 — Record framing (§5).** Send a body containing a blank line and a
  line equal to `---`; assert `read_my_mailbox` returns the body verbatim and
  `mailboxes/<R>.log` parses to exactly one record (the `len` header is
  authoritative).
- **C10 — Broadcast correlation (§7, §16 Q6).** `send_message(to_role=R)` to
  two agents; assert both copies share one `id` and each has its own
  `to`/`seq`; both `reply` with `in_reply_to = id`; the sender correlates both
  replies by `in_reply_to`.

DoD: C1–C10 green on a clean `bus_root`. The suite is the single acceptance
criterion for P1; P2/P4 add cases as their features land.

---

## 19. Handoff artifacts

These remove round-trips with the implementing model. They are scaffolding,
not part of the bus contract.

### 19.1 Scope of this handoff
Implement **P0, P1, P2, P4** — the bus, no-fork, all in the bus repo. **P3 is
a separate fork of Crush itself** (the `claude/channel` delivery routing +
`AgentCoordinator.Run` self-drive), NOT part of this handoff: it is a
Crush-side effort depending on the `CRUSH_CLIENT_SERVER` architecture and its
known socket-init race (§16 Q11). The bus ships without P3; the loop wrapper
(§11.4) is the no-fork delivery path until P3 lands elsewhere.

### 19.2 `crush.json` wiring (per session)
One MCP entry per session, stdio (default), pointing each Crush session at the
bus binary with a shared `BUS_ROOT`:

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

or `crush mcp add bus --command /path/to/mailbox-bus --env BUS_ROOT
"$HOME/.crush-mailbox-bus"`. Each session's `agent_id`/`role`/`model` are set
in that session's `crush.json` (or env) and passed as the explicit `agent_id`
argument on every tool call (§9).

### 19.3 Role-prompt file (§11.1)
The §11.1 core role prompt is delivered to the agent verbatim as a file the
session loads (via `context_paths`, a Skill, or the loop-wrapper's prompt
arg), with the `<role>`/`<id>` slots substituted before load. The exomemory
module (§11.3) is an additional, OPTIONAL context file the author provides
for the author's environment; a generic deploy omits it.

### 19.4 MCP Go-SDK skeleton (v1.7.0, verified API)
A stdio MCP server with one tool; `agent_id` is an explicit per-call argument
(self-identifying, no connection state, §9). API verified in the go-sdk
source pinned by Crush v0.91.0 (`mcp.NewServer` / `s.AddTool` / `s.Run` +
`&mcp.StdioTransport{}`):

```go
s := mcp.NewServer(&mcp.Implementation{Name: "mailbox-bus", Version: version}, nil)

s.AddTool(&mcp.Tool{
    Name: "send_message",
    InputSchema: json.RawMessage(`{"type":"object","properties":{...}}`),
}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
    var args struct {
        ToAgent, ToRole, Kind, Body, DedupID string
        InReplyTo *int
        AgentID  string // explicit per-call identity (§9)
    }
    if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
        return nil, err
    }
    // §7: take the canonical-log flock; durable dedup check; assign id;
    // expand recipients from registry.log; append; return {id, delivered}.
    out, _ := json.Marshal(doSend(ctx, args))
    return &mcp.CallToolResult{
        Content: []mcp.Content{&mcp.TextContent{Text: string(out)}},
    }, nil
})
// ...one AddTool per §9 tool (register, unregister, list_agents,
//    read_my_mailbox, wait_for_message, reply, ...)...

s.Run(ctx, &mcp.StdioTransport{}) // blocks; Crush does not bound CallTool (§16 Q1)
```

All correctness state lives in `bus_root` files under the canonical-log lock
(§7); the server process holds none. The `wait_for_message` handler blocks on
the mailbox file (kqueue/inotify, fallback backoff) until the condition or
`timeout`. The conformance gate (§18) is the contract — if the skeleton's
exact wiring drifts from a future go-sdk version, the gate still defines
correct behavior.
