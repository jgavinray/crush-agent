// Package tools wires the mailbox-bus MCP tool surface (SPEC §9) onto a
// go-sdk server. Every tool that acts on behalf of an agent takes
// agent_id explicitly: stdio runs N bus processes, one per session, and
// none of them may rely on per-connection state (SPEC §9, §14).
package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jgavinray/crush-agent/internal/bus"
	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Register adds all P1 bus tools to s (SPEC §17 P1: register, unregister,
// list_agents, send_message, read_my_mailbox, wait_for_message, reply).
func Register(s *mcp.Server, b *bus.Bus) {
	s.AddTool(&mcp.Tool{
		Name:        "register",
		Description: "Register this session's agent in the durable registry (SPEC §6/§9). Appends a register event to registry.log and rewrites the on-disk snapshot under the canonical-log lock. Idempotent: re-registering on reconnect is the normal case.",
		InputSchema: objectSchema(map[string]any{
			"agent_id":    map[string]any{"type": "string", "description": "Unique stable agent identity, e.g. frontend-dev."},
			"role":        map[string]any{"type": "string", "description": "Functional classification; to_role sends broadcast to all alive agents with this role."},
			"description": map[string]any{"type": "string"},
			"working_dir": map[string]any{"type": "string"},
			"model":       map[string]any{"type": "string"},
		}, "agent_id", "role", "description", "working_dir"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		role, err := reqString(a, "role")
		if err != nil {
			return nil, err
		}
		description, _ := optString(a, "description")
		workingDir, _ := optString(a, "working_dir")
		model, _ := optString(a, "model")
		ag, err := b.Register(agentID, role, description, workingDir, model)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"agent_id":      ag.AgentID,
			"role":          ag.Role,
			"registered_at": ag.RegisteredAt.UTC().Format(rfc3339),
			"status":        "alive",
		}, nil
	}))

	s.AddTool(&mcp.Tool{
		Name:        "unregister",
		Description: "Remove the agent from the registry (SPEC §9). Appends an unregister event and drops the snapshot under the lock; the mailbox log is RETAINED (audit record) and a later re-register resumes it. Idempotent: unregistering an unknown or already-removed agent succeeds.",
		InputSchema: objectSchema(map[string]any{
			"agent_id": map[string]any{"type": "string"},
		}, "agent_id"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		at, err := b.Unregister(agentID)
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"agent_id":        agentID,
			"status":          "removed",
			"unregistered_at": at.UTC().Format(rfc3339),
		}, nil
	}))

	s.AddTool(&mcp.Tool{
		Name:        "list_agents",
		Description: "List agents from the durable registry (SPEC §9). Never reads a per-process cache, so it sees registrations made by other bus processes. membership (default \"alive\") and liveness (default \"any\") are independent filters; liveness derives from last_seen (live = within 90s).",
		InputSchema: objectSchema(map[string]any{
			"role":       map[string]any{"type": "string"},
			"membership": map[string]any{"type": "string", "enum": []string{"alive", "removed", "any"}},
			"liveness":   map[string]any{"type": "string", "enum": []string{"live", "dead", "any"}},
		}),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		role, _ := optString(a, "role")
		membership, _ := optString(a, "membership")
		liveness, _ := optString(a, "liveness")
		views, err := b.ListAgents(role, membership, liveness)
		if err != nil {
			return nil, err
		}
		if views == nil {
			views = []bus.AgentView{}
		}
		return views, nil
	}))

	s.AddTool(&mcp.Tool{
		Name: "send_message",
		Description: "Send a message to an agent (to_agent) or to all alive agents of a role (to_role) under the canonical-log lock (SPEC §7). " +
			"Exactly one of to_agent / to_role is required. dedup_id makes the send idempotent across ALL bus processes: a resend with an already-recorded dedup_id returns the original {id, delivered} and writes nothing. " +
			"Returns {id, delivered:[{agent_id, seq}]}; broadcasts share one id with a per-recipient seq.",
		InputSchema: objectSchema(map[string]any{
			"agent_id":    map[string]any{"type": "string", "description": "The sending agent (must be registered)."},
			"to_agent":    map[string]any{"type": "string"},
			"to_role":     map[string]any{"type": "string"},
			"kind":        map[string]any{"type": "string", "enum": []string{"prompt", "info", "reply"}},
			"body":        map[string]any{"type": "string"},
			"in_reply_to": map[string]any{"type": "integer"},
			"dedup_id":    map[string]any{"type": "string"},
		}, "agent_id", "kind", "body"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		kind, err := reqString(a, "kind")
		if err != nil {
			return nil, err
		}
		body, err := reqString(a, "body")
		if err != nil {
			return nil, err
		}
		toAgent, _ := optString(a, "to_agent")
		toRole, _ := optString(a, "to_role")
		var inReplyTo *int
		if v, ok := optInt(a, "in_reply_to"); ok {
			inReplyTo = &v
		} else if _, present := a["in_reply_to"]; present {
			return nil, &bus.Error{Code: bus.CodeInvalidArgument, Message: "in_reply_to must be an integer"}
		}
		dedup, _ := optString(a, "dedup_id")
		return b.Send(agentID, toAgent, toRole, kind, []byte(body), inReplyTo, dedup)
	}))

	s.AddTool(&mcp.Tool{
		Name:        "read_my_mailbox",
		Description: "Non-blocking read of the agent's mailbox (SPEC §8/§9): records with seq > since, ordered by seq, optionally filtered by kind, capped at limit (default 256). The read cursor is the caller's state, not the bus's.",
		InputSchema: objectSchema(map[string]any{
			"agent_id": map[string]any{"type": "string"},
			"since":    map[string]any{"type": "integer", "minimum": 0},
			"kind":     map[string]any{"type": "string"},
			"limit":    map[string]any{"type": "integer", "minimum": 0},
		}, "agent_id"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		since, _ := optInt(a, "since")
		kind, _ := optString(a, "kind")
		limit, _ := optInt(a, "limit")
		views, err := b.ReadMailbox(agentID, since, kind, limit)
		if err != nil {
			return nil, err
		}
		if views == nil {
			views = []bus.RecordView{}
		}
		return views, nil
	}))

	s.AddTool(&mcp.Tool{
		Name: "wait_for_message",
		Description: "Blocking long-poll (SPEC §8/§9): block until a record with seq > since matching the optional from_role / from_agent / kind filters exists in the agent's mailbox, or timeout seconds elapse. " +
			"Cross-process wake via kqueue/inotify on the mailbox file, with backoff polling as fallback. Reserves this tool for autonomous loop agents: it holds the stdio pipe while blocked. " +
			"Timeout is NOT an error: it returns {timeout: true} so the agent can re-call.",
		InputSchema: objectSchema(map[string]any{
			"agent_id":   map[string]any{"type": "string"},
			"since":      map[string]any{"type": "integer", "minimum": 0},
			"from_role":  map[string]any{"type": "string"},
			"from_agent": map[string]any{"type": "string"},
			"kind":       map[string]any{"type": "string"},
			"timeout":    map[string]any{"type": "integer", "minimum": 0},
		}, "agent_id"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		since, _ := optInt(a, "since")
		fromRole, _ := optString(a, "from_role")
		fromAgent, _ := optString(a, "from_agent")
		kind, _ := optString(a, "kind")
		timeout, err := optIntDefault(a, "timeout", 30)
		if err != nil {
			return nil, err
		}
		rec, err := b.WaitForMessage(agentID, since, fromRole, fromAgent, kind, time.Duration(timeout)*time.Second, ctx)
		if err != nil {
			if errors.Is(err, bus.ErrTimeout) {
				return map[string]any{"timeout": true}, nil
			}
			return nil, err
		}
		return map[string]any{"message": rec.View()}, nil
	}))

	s.AddTool(&mcp.Tool{
		Name:        "reply",
		Description: "Convenience for closing a prompt (SPEC §9): looks up message in_reply_to in the caller's own mailbox and sends a kind=reply message to that record's from, with in_reply_to set. dedup_id is honored as in send_message (the \"<id>:reply\" convention makes the reply idempotent). Returns the send_message shape.",
		InputSchema: objectSchema(map[string]any{
			"in_reply_to": map[string]any{"type": "integer"},
			"agent_id":    map[string]any{"type": "string"},
			"body":        map[string]any{"type": "string"},
			"dedup_id":    map[string]any{"type": "string"},
		}, "in_reply_to", "agent_id", "body"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		inReplyTo, err := reqInt(a, "in_reply_to")
		if err != nil {
			return nil, err
		}
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		body, err := reqString(a, "body")
		if err != nil {
			return nil, err
		}
		dedup, _ := optString(a, "dedup_id")
		return b.Reply(agentID, inReplyTo, []byte(body), dedup)
	}))

	// --- P2 lifecycle tools (SPEC §17 P2) ---------------------------------

	s.AddTool(&mcp.Tool{
		Name:        "whoami",
		Description: "Return the durable registry state for agent_id (SPEC §9 whoami): identity, declared fields, current work-status, last_seen, registered_at. Read-only; no lock.",
		InputSchema: objectSchema(map[string]any{
			"agent_id": map[string]any{"type": "string"},
		}, "agent_id"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		ag, err := b.Whoami(agentID)
		if err != nil {
			return nil, err
		}
		return bus.Info(ag), nil
	}))

	s.AddTool(&mcp.Tool{
		Name:        "get_agent",
		Description: "Inspect any agent from any session (SPEC §9 get_agent): the whoami fields plus `recent`, the last 20 complete records of the agent's mailbox (id, seq, from, kind, ts, body_excerpt). Read-only; no lock.",
		InputSchema: objectSchema(map[string]any{
			"agent_id": map[string]any{"type": "string"},
		}, "agent_id"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		ag, err := b.Whoami(agentID)
		if err != nil {
			return nil, err
		}
		info, err := b.InfoWithRecent(ag, 20)
		if err != nil {
			return nil, err
		}
		return info, nil
	}))

	s.AddTool(&mcp.Tool{
		Name: "set_my_status",
		Description: "Publish a short free-form work-state string (\"idle\", \"working: auth module\", \"blocked: waiting on spec\", \"done\") " +
			"(SPEC §9 set_my_status). Appends a status event to registry.log and updates the on-disk snapshot under the canonical-log lock. " +
			"Status is reported verbatim by whoami/get_agent/list_agents and never used as a filter key.",
		InputSchema: objectSchema(map[string]any{
			"agent_id": map[string]any{"type": "string"},
			"status":   map[string]any{"type": "string"},
		}, "agent_id", "status"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		status, err := reqString(a, "status")
		if err != nil {
			return nil, err
		}
		if err := b.SetStatus(agentID, status); err != nil {
			return nil, err
		}
		return map[string]any{"ok": true}, nil
	}))

	s.AddTool(&mcp.Tool{
		Name: "heartbeat",
		Description: "Keep-alive: refresh the agent's last_seen in the durable registry (SPEC §9 heartbeat). " +
			"Observability only — no correctness property depends on it; it exists so list_agents can show which autonomous loops are still breathing (live = within 90s). " +
			"Autonomous loop agents call it every ~30s.",
		InputSchema: objectSchema(map[string]any{
			"agent_id": map[string]any{"type": "string"},
		}, "agent_id"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		lastSeen, err := b.Heartbeat(agentID)
		if err != nil {
			return nil, err
		}
		return map[string]any{"ok": true, "last_seen": lastSeen.UTC().Format(rfc3339)}, nil
	}))

	// --- P4 hardening tool (SPEC §17 P4) ----------------------------------

	s.AddTool(&mcp.Tool{
		Name: "compact",
		Description: "Archive the consumed prefix of agent_id's mailbox: records with seq <= up_to_seq are appended " +
			"verbatim to mailboxes/<agent_id>.log.archived and the live log is truncated to the remaining records " +
			"(SPEC §16 Q4, §17 P4). Precondition: the agent has durably consumed up to up_to_seq (its last_seq_read). " +
			"Per-recipient seq counters are unaffected — the next delivery continues from the last assigned seq. " +
			"Idempotent: a boundary at or below the smallest live seq archives nothing. The cold archive is greppable " +
			"and is the audit trail for compacted records (invariant V).",
		InputSchema: objectSchema(map[string]any{
			"agent_id":  map[string]any{"type": "string"},
			"up_to_seq": map[string]any{"type": "integer", "minimum": 0},
		}, "agent_id", "up_to_seq"),
	}, handle(b, func(ctx context.Context, a map[string]any) (any, error) {
		agentID, err := reqString(a, "agent_id")
		if err != nil {
			return nil, err
		}
		upToSeq, err := reqInt(a, "up_to_seq")
		if err != nil {
			return nil, err
		}
		return b.Compact(agentID, upToSeq)
	}))
}

const rfc3339 = "2006-01-02T15:04:05Z07:00"

// handle adapts a plain args->result function to a go-sdk ToolHandler.
// Bus errors are returned as MCP tool errors ({error: code, message},
// IsError=true) so the calling LLM can see the code and self-correct
// (SPEC §9 "Errors").
func handle(b *bus.Bus, fn func(ctx context.Context, a map[string]any) (any, error)) mcp.ToolHandler {
	return func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		a, err := parseArgs(req)
		if err != nil {
			return busErrorResult(&bus.Error{Code: bus.CodeInvalidArgument, Message: err.Error()}), nil
		}
		out, err := fn(ctx, a)
		if err != nil {
			return busErrorResult(err), nil
		}
		data, err := json.Marshal(out)
		if err != nil {
			return busErrorResult(err), nil
		}
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}, nil
	}
}

// busErrorResult renders a bus error as a tool error result.
func busErrorResult(err error) *mcp.CallToolResult {
	var be *bus.Error
	if errors.As(err, &be) {
		data, _ := json.Marshal(map[string]any{"error": be.Code, "message": be.Message})
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
		}
	}
	data, _ := json.Marshal(map[string]any{"error": "internal_error", "message": err.Error()})
	return &mcp.CallToolResult{
		IsError: true,
		Content: []mcp.Content{&mcp.TextContent{Text: string(data)}},
	}
}

// parseArgs decodes req.Params.Arguments into a map (SPEC: arguments may be
// absent for tools with no required parameters).
func parseArgs(req *mcp.CallToolRequest) (map[string]any, error) {
	a := map[string]any{}
	if req == nil || req.Params == nil || len(req.Params.Arguments) == 0 {
		return a, nil
	}
	if err := json.Unmarshal(req.Params.Arguments, &a); err != nil {
		return nil, fmt.Errorf("arguments are not a JSON object: %w", err)
	}
	return a, nil
}

// objectSchema builds a minimal 2020-12 JSON Schema object (the draft the
// go-sdk validates against).
func objectSchema(props map[string]any, required ...string) json.RawMessage {
	s := map[string]any{
		"type":                 "object",
		"properties":           props,
		"additionalProperties": false,
	}
	if len(required) > 0 {
		reqd := make([]any, len(required))
		for i, r := range required {
			reqd[i] = r
		}
		s["required"] = reqd
	}
	data, _ := json.Marshal(s)
	return data
}

// --- typed argument helpers -------------------------------------------------

func reqString(a map[string]any, key string) (string, error) {
	v, ok := a[key]
	if !ok {
		return "", fmt.Errorf("missing required argument %q", key)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("argument %q must be a string", key)
	}
	return s, nil
}

func optString(a map[string]any, key string) (string, bool) {
	v, ok := a[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func reqInt(a map[string]any, key string) (int, error) {
	v, ok := a[key]
	if !ok {
		return 0, fmt.Errorf("missing required argument %q", key)
	}
	n, ok := v.(float64)
	if !ok || n != float64(int(n)) {
		return 0, fmt.Errorf("argument %q must be an integer", key)
	}
	return int(n), nil
}

func optInt(a map[string]any, key string) (int, bool) {
	v, ok := a[key]
	if !ok {
		return 0, false
	}
	n, ok := v.(float64)
	if !ok || n != float64(int(n)) {
		return 0, false
	}
	return int(n), true
}

// optIntDefault returns the default when key is absent, or a validation
// error when present but not an integer.
func optIntDefault(a map[string]any, key string, def int) (int, error) {
	v, ok := a[key]
	if !ok {
		return def, nil
	}
	n, ok := v.(float64)
	if !ok || n != float64(int(n)) {
		return 0, fmt.Errorf("argument %q must be an integer", key)
	}
	return int(n), nil
}
