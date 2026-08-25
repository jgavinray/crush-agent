// Command mailbox-bus is the Mailbox Bus: a stdio MCP server over the
// durable bus_root files described in SPEC.md.
//
// Each Crush session that joins the bus spawns one mailbox-bus child (SPEC
// §2, §19.2); all children converge on the same bus_root, serialized by the
// advisory flock on state/lock. The bus root is resolved from $BUS_ROOT,
// else the --bus-root flag, else $HOME/.crush-mailbox-bus (SPEC §13, §19.2).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/jgavinray/crush-agent/internal/bus"
	"github.com/jgavinray/crush-agent/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	busRoot := flag.String("bus-root", "", "bus root directory (default: $BUS_ROOT or $HOME/.crush-mailbox-bus)")
	flag.Parse()

	root := bus.RootFromEnv()
	if *busRoot != "" {
		root = *busRoot
	}

	// Open performs the SPEC §12 startup recovery under the lock: truncate
	// partial trailing records, recompute state/counter from the logs, and
	// rebuild the derived snapshots.
	b, err := bus.Open(root, bus.Options{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "mailbox-bus: open bus root %s: %v\n", root, err)
		os.Exit(1)
	}

	s := mcp.NewServer(&mcp.Implementation{
		Name:    "mailbox-bus",
		Version: "0.1.0",
	}, nil)
	tools.Register(s, b)

	// stdio transport: stdin/stdout are the MCP pipe owned by the spawning
	// Crush session (SPEC §2). All diagnostics go to stderr.
	if err := s.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		fmt.Fprintf(os.Stderr, "mailbox-bus: server: %v\n", err)
		os.Exit(1)
	}
}
