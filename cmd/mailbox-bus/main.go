// Command mailbox-bus is the Mailbox Bus: a stdio MCP server over the
// durable bus_root files described in SPEC.md.
//
// Each Crush session that joins the bus spawns one mailbox-bus child (SPEC
// §2, §19.2); all children converge on the same bus_root, serialized by the
// advisory flock on state/lock. The bus root is resolved from $BUS_ROOT,
// else the --bus-root flag, else $HOME/.crush-mailbox-bus (SPEC §13, §19.2).
//
// P3 (SPEC §17 P3, §16 Q9): the server also declares the experimental
// "claude/channel" capability and, for every new mailbox record belonging
// to an agent this process has seen register, pushes a
// notifications/claude/channel notification to its client. The Crush P3
// fork routes that notification to the owning session and starts a fresh
// agent turn from it (the self-drive that replaces the §11.4 loop
// wrapper). Clients that did not opt in (--channels) ignore the
// notification: the go-sdk logs unknown server notifications and drops
// them without touching the connection.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/jgavinray/crush-agent/internal/bus"
	"github.com/jgavinray/crush-agent/internal/tools"
	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

func main() {
	busRoot := flag.String("bus-root", "", "bus root directory (default: $BUS_ROOT or $HOME/.crush-mailbox-bus)")
	flag.Parse()

	root := bus.RootFromEnv()
	if *busRoot != "" {
		root = *busRoot
	}

	// P4 hardening (SPEC §17 P4): fsync-before-return on every canonical
	// write (power-loss durability, invariant IV) and index.db for O(1)
	// dedup checks and the wait_for_message fast path. The index is derived
	// state — rebuilt from the logs under the lock on every Open — so these
	// are safe defaults, not opt-ins.
	b, err := bus.Open(root, bus.Options{Fsync: true, Index: true})
	if err != nil {
		fmt.Fprintf(os.Stderr, "mailbox-bus: open bus root %s: %v\n", root, err)
		os.Exit(1)
	}

	s := mcp.NewServer(&mcp.Implementation{
		Name:    "mailbox-bus",
		Version: "0.1.0",
	}, &mcp.ServerOptions{
		Capabilities: &mcp.ServerCapabilities{
			// P3: declare the claude/channel experimental capability
			// (SPEC §16 Q9). Presence of the key is what turns on the
			// client-side notification listener.
			Experimental: map[string]any{"claude/channel": map[string]any{}},
			// Keep the default logging capability: a non-nil
			// Capabilities overrides the SDK default.
			Logging: &mcp.LoggingCapabilities{},
		},
	})
	tools.Register(s, b)

	// stdio transport: stdin/stdout are the MCP pipe owned by the spawning
	// Crush session (SPEC §2). All diagnostics go to stderr.
	//
	// The capturing transport hands the established JSON-RPC connection to
	// the channel pusher: the go-sdk server has no public API for custom
	// server->client notifications, so the pusher writes raw
	// notifications/claude/channel frames on the shared connection
	// (jsonrpc.Connection.Write is safe for concurrent use — the same
	// pattern Crush's own channel test uses).
	sink := &busChannelSink{}
	pusher := bus.NewChannelPusher(b, sink)
	connCh := make(chan mcp.Connection, 1)
	transport := &captureTransport{Transport: &mcp.StdioTransport{}, connCh: connCh}
	go func() {
		conn := <-connCh
		sink.conn = conn
		pusher.Start()
	}()

	if err := s.Run(context.Background(), transport); err != nil {
		pusher.Stop()
		if cerr := b.Close(); cerr != nil {
			fmt.Fprintf(os.Stderr, "mailbox-bus: close: %v\n", cerr)
		}
		fmt.Fprintf(os.Stderr, "mailbox-bus: server: %v\n", err)
		os.Exit(1)
	}
	pusher.Stop()
	if err := b.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "mailbox-bus: close: %v\n", err)
		os.Exit(1)
	}
}

// captureTransport wraps an mcp.Transport and reports the established
// connection to connCh (at most once).
type captureTransport struct {
	mcp.Transport
	connCh chan mcp.Connection
}

func (t *captureTransport) Connect(ctx context.Context) (mcp.Connection, error) {
	c, err := t.Transport.Connect(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case t.connCh <- c:
	default:
	}
	return c, nil
}

// busChannelSink implements bus.ChannelSink: one
// notifications/claude/channel JSON-RPC notification per record. The zero
// jsonrpc.ID makes the frame a notification (no response expected), which
// is exactly the shape Crush's channel interceptor (channelConn.Read)
// consumes.
type busChannelSink struct {
	conn mcp.Connection
}

func (s *busChannelSink) PushChannel(_ context.Context, content string, meta map[string]string) error {
	if s.conn == nil {
		// No client yet (the transport has not connected). Drop the push:
		// the record is durable in the mailbox and the pull path covers it.
		return nil
	}
	params, err := json.Marshal(map[string]any{"content": content, "meta": meta})
	if err != nil {
		return err
	}
	return s.conn.Write(context.Background(), &jsonrpc.Request{
		Method: "notifications/claude/channel",
		Params: params,
	})
}
