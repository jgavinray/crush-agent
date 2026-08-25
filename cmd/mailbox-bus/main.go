// Command mailbox-bus is the Mailbox Bus (SPEC.md): a multi-agent message bus
// for Crush, implemented as a stdio MCP server over durable files.
//
// Crush spawns one mailbox-bus child per session (SPEC §2, §19.2); every
// child converges on the same durable files under $BUS_ROOT (default
// $HOME/.crush-mailbox-bus), with the advisory lock on state/lock as the
// cross-process critical section (SPEC §7). The process holds no in-memory
// correctness state and may be killed at any time (SPEC §12).
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const version = "0.1.0"

func main() {
	// P0 sanity-check shell: a trivial stdio MCP server that Crush can spawn
	// and talk to end to end (SPEC §17 P0). The bus tool surface lands in
	// internal/tools once the durable core (internal/bus) exists.
	srv := mcp.NewServer(&mcp.Implementation{Name: "mailbox-bus", Version: version}, nil)
	if err := srv.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		slog.Error("mailbox-bus: mcp server stopped", "error", err)
		os.Exit(1)
	}
}
