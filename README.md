# crush-agent — Mailbox Bus for Crush

Multi-agent message bus for [Crush](https://github.com/charmbracelet/crush),
implemented exactly as defined in [SPEC.md](SPEC.md) (v1.0, handoff package
§19): a **stdio MCP server** (`mailbox-bus`) over **durable plain-text files**,
spawned per Crush session, all converging on one shared `bus_root`.

> Status: under construction — see the commit log for the P0 → P1 → P2 → P4
> build-out. The conformance gate in `conformance/` is written from the spec
> before the implementation and is the acceptance criterion (SPEC §18).

## Quick start (once built)

```sh
go build -o bin/mailbox-bus ./cmd/mailbox-bus
```

Point a Crush session at it (SPEC §19.2):

```sh
crush mcp add bus --command "$PWD/bin/mailbox-bus" --env BUS_ROOT "$HOME/.crush-mailbox-bus"
```

or in `crush.json`:

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

Each agent registers with `register(agent_id, role, ...)` and exchanges
`prompt` / `info` / `reply` records via `send_message`, `read_my_mailbox`,
`wait_for_message`, and `reply` (SPEC §9). The full README lands with the
documentation commit.

## Layout

```
cmd/mailbox-bus/        the stdio MCP server binary
internal/bus/           durable core: records, registry, delivery, recovery
internal/tools/         MCP tool surface (SPEC §9)
conformance/            the SPEC §18 gate — black-box, written pre-implementation
prompts/                §11 bootstrap role prompts (P2)
examples/               wiring + host-side verifier examples (P2)
```
