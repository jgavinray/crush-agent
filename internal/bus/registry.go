package bus

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

// LivenessWindow is the last_seen age at which list_agents reports an agent
// as dead (SPEC §9 list_agents: live = now - last_seen <= 90s).
const LivenessWindow = 90 * time.Second

// agentIDPattern is the bus-wide constraint on agent_id (and therefore on
// file names and header values derived from it): one to 64 characters,
// filename- and header-safe.
var agentIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// validateAgentID enforces agentIDPattern.
func validateAgentID(id string) error {
	if !agentIDPattern.MatchString(id) {
		return errInvalidArgument("agent_id %q must match %s", id, agentIDPattern)
	}
	return nil
}

// Agent is one agent's current registry state (SPEC §6). The registry log is
// the truth; registry/<agent_id>.json is a derived on-disk snapshot, kept
// shared across the N bus processes and rebuildable from the log at any time
// (SPEC §12).
type Agent struct {
	AgentID        string     `json:"agent_id"`
	Role           string     `json:"role"`
	Description    string     `json:"description"`
	WorkingDir     string     `json:"working_dir"`
	Model          string     `json:"model"`
	Status         string     `json:"status"`
	Membership     string     `json:"membership"` // "alive" | "removed"
	RegisteredAt   time.Time  `json:"registered_at"`
	UnregisteredAt *time.Time `json:"unregistered_at,omitempty"`
	LastSeen       time.Time  `json:"last_seen"`
}

// Liveness derives the liveness axis from last_seen (SPEC §9/§10).
func (a *Agent) Liveness(now time.Time) string {
	if now.Sub(a.LastSeen) <= LivenessWindow {
		return "live"
	}
	return "dead"
}

// AgentView is the list_agents row shape (SPEC §9).
type AgentView struct {
	AgentID    string    `json:"agent_id"`
	Role       string    `json:"role"`
	Membership string    `json:"membership"`
	Liveness   string    `json:"liveness"`
	WorkingDir string    `json:"working_dir"`
	Model      string    `json:"model"`
	LastSeen   time.Time `json:"last_seen"`
	Status     string    `json:"status"`
}

// Registry event types (append-only events in registry.log).
const (
	eventRegister   = "register"
	eventUnregister = "unregister"
	eventStatus     = "status"
	eventHeartbeat  = "heartbeat"
)

// registryEvent is one parsed event block from registry.log.
type registryEvent struct {
	kind        string
	agentID     string
	role        string
	description string
	workingDir  string
	model       string
	status      string
	ts          time.Time
}

// parseRegistryLog folds the append-only event log (SPEC §6) into the
// latest state per agent_id. The log is flat `field: value` blocks
// terminated by a blank line; a truncated trailing block (power loss) that
// fails to parse is dropped, which is harmless: the bus replays the log on
// every membership read, and the dropped block was never durably complete.
func parseRegistryLog(data []byte) []registryEvent {
	var events []registryEvent
	for _, block := range strings.Split(string(data), "\n\n") {
		block = strings.TrimRight(block, "\n")
		if block == "" {
			continue
		}
		fields := make(map[string]string, 8)
		complete := true
		for _, line := range strings.Split(block, "\n") {
			key, value, ok := strings.Cut(line, ": ")
			if !ok || key == "" {
				complete = false
				break
			}
			fields[key] = value
		}
		if !complete || fields["event"] == "" || fields["agent_id"] == "" {
			continue // partial/corrupt trailing block: drop it
		}
		ts, terr := time.Parse(time.RFC3339, fields["ts"])
		if terr != nil {
			ts = time.Time{}
		}
		events = append(events, registryEvent{
			kind:        fields["event"],
			agentID:     fields["agent_id"],
			role:        fields["role"],
			description: fields["description"],
			workingDir:  fields["working_dir"],
			model:       fields["model"],
			status:      fields["status"],
			ts:          ts,
		})
	}
	return events
}

// foldRegistry computes the latest membership state per agent_id from a
// parsed event list.
func foldRegistry(events []registryEvent) map[string]*Agent {
	out := map[string]*Agent{}
	for _, ev := range events {
		switch ev.kind {
		case eventRegister:
			out[ev.agentID] = &Agent{
				AgentID:      ev.agentID,
				Role:         ev.role,
				Description:  ev.description,
				WorkingDir:   ev.workingDir,
				Model:        ev.model,
				Status:       "idle",
				Membership:   "alive",
				RegisteredAt: ev.ts,
				LastSeen:     ev.ts,
			}
		case eventUnregister:
			prev := out[ev.agentID]
			removed := &Agent{
				AgentID:        ev.agentID,
				Membership:     "removed",
				UnregisteredAt: &ev.ts,
			}
			if prev != nil {
				removed.RegisteredAt = prev.RegisteredAt
			}
			out[ev.agentID] = removed
		case eventStatus:
			if a, ok := out[ev.agentID]; ok && a.Membership == "alive" {
				a.Status = ev.status
			}
		case eventHeartbeat:
			if a, ok := out[ev.agentID]; ok && a.Membership == "alive" {
				a.LastSeen = ev.ts
			}
		}
	}
	return out
}

// readRegistryLog reads and folds registry.log. Used by read-only tools
// (never under the lock, SPEC §7) and, via withLock, by mutators.
func (b *Bus) readRegistryLog() (map[string]*Agent, error) {
	data, err := os.ReadFile(b.registryLogPath())
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]*Agent{}, nil
		}
		return nil, err
	}
	return foldRegistry(parseRegistryLog(data)), nil
}

// requireAgentLocked looks up agentID in the durable registry. Callers hold
// the canonical-log lock when they need the read to be consistent with the
// surrounding mutation (SPEC §6/§7 step 2).
func (b *Bus) requireAgentLocked(agentID string, aliveOnly bool) (*Agent, error) {
	agents, err := b.readRegistryLog()
	if err != nil {
		return nil, err
	}
	a, ok := agents[agentID]
	if !ok {
		return nil, errAgentNotRegistered(agentID)
	}
	if a.Membership != "alive" {
		return nil, errAgentRemoved(agentID)
	}
	if aliveOnly && a.Membership != "alive" {
		return nil, errAgentRemoved(agentID)
	}
	return a, nil
}

// appendRegistryEvent appends one event block to registry.log in a single
// write under the canonical-log lock (SPEC §6: registration is an append;
// "every canonical write is a single atomic write()").
func (b *Bus) appendRegistryEventLocked(ev registryEvent) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "event: %s\n", ev.kind)
	fmt.Fprintf(&sb, "agent_id: %s\n", ev.agentID)
	switch ev.kind {
	case eventRegister:
		fmt.Fprintf(&sb, "role: %s\n", ev.role)
		fmt.Fprintf(&sb, "description: %s\n", ev.description)
		fmt.Fprintf(&sb, "working_dir: %s\n", ev.workingDir)
		if ev.model != "" {
			fmt.Fprintf(&sb, "model: %s\n", ev.model)
		}
	}
	if ev.kind == eventStatus {
		fmt.Fprintf(&sb, "status: %s\n", ev.status)
	}
	if ts := ev.ts; !ts.IsZero() {
		fmt.Fprintf(&sb, "ts: %s\n", ts.UTC().Format(time.RFC3339))
	}
	sb.WriteString("\n") // blank-line terminator
	f, err := os.OpenFile(b.registryLogPath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(sb.String()); err != nil {
		return err
	}
	return b.syncFile(f)
}

// writeSnapshotLocked rewrites registry/<agent_id>.json for an alive agent
// in a single write under the lock (SPEC §6).
func (b *Bus) writeSnapshotLocked(a *Agent) error {
	data, err := jsonMarshalIndent(a)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	f, err := os.OpenFile(b.snapshotPath(a.AgentID), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return b.syncFile(f)
}

// dropSnapshotLocked removes the on-disk snapshot (SPEC §6: removing an
// agent appends an unregister event and drops the snapshot; the mailbox is
// retained).
func (b *Bus) dropSnapshotLocked(agentID string) error {
	err := os.Remove(b.snapshotPath(agentID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// rebuildSnapshotsLocked replays registry.log and rewrites every on-disk
// snapshot (SPEC §12: "replay registry.log to rebuild registry/*.json").
func (b *Bus) rebuildSnapshotsLocked() error {
	agents, err := b.readRegistryLog()
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, a := range agents {
		seen[a.AgentID] = true
		if a.Membership == "alive" {
			if err := b.writeSnapshotLocked(a); err != nil {
				return err
			}
		} else if err := b.dropSnapshotLocked(a.AgentID); err != nil {
			return err
		}
	}
	// Stray snapshots for ids the log no longer mentions: rebuildable junk,
	// drop them so the snapshot dir is a faithful projection.
	entries, err := os.ReadDir(b.registryDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".json")
		if !seen[id] {
			if err := os.Remove(filepath.Join(b.registryDir(), e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// Register appends a register event and rewrites the snapshot under the
// flock (SPEC §9 register). Idempotent: re-registering on reconnect is the
// normal case (§13).
func (b *Bus) Register(agentID, role, description, workingDir, model string) (*Agent, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if role == "" {
		return nil, errInvalidArgument("role must be non-empty")
	}
	var agent *Agent
	err := b.withLock(func() error {
		now := busNow()
		agent = &Agent{
			AgentID:      agentID,
			Role:         sanitizeHeader(role),
			Description:  sanitizeHeader(description),
			WorkingDir:   sanitizeHeader(workingDir),
			Model:        sanitizeHeader(model),
			Status:       "idle",
			Membership:   "alive",
			RegisteredAt: now,
			LastSeen:     now,
		}
		if err := b.appendRegistryEventLocked(registryEvent{
			kind:        eventRegister,
			agentID:     agentID,
			role:        agent.Role,
			description: agent.Description,
			workingDir:  agent.WorkingDir,
			model:       agent.Model,
			ts:          now,
		}); err != nil {
			return err
		}
		return b.writeSnapshotLocked(agent)
	})
	if err != nil {
		return nil, err
	}
	return agent, nil
}

// Unregister appends an unregister event and drops the snapshot under the
// flock (SPEC §9 unregister). The mailbox is retained. Idempotent:
// unregistering an unknown or already-removed agent is a no-op that still
// returns success.
func (b *Bus) Unregister(agentID string) (unregisteredAt time.Time, err error) {
	if err = validateAgentID(agentID); err != nil {
		return time.Time{}, err
	}
	err = b.withLock(func() error {
		unregisteredAt = busNow()
		if err := b.appendRegistryEventLocked(registryEvent{
			kind:    eventUnregister,
			agentID: agentID,
			ts:      unregisteredAt,
		}); err != nil {
			return err
		}
		return b.dropSnapshotLocked(agentID)
	})
	if err != nil {
		return time.Time{}, err
	}
	return unregisteredAt, nil
}

// SetStatus appends a status event and updates the snapshot under the lock
// (SPEC §9 set_my_status). status is a short free-form work-state string;
// it is reported verbatim and never used as a filter key.
func (b *Bus) SetStatus(agentID, status string) error {
	if err := validateAgentID(agentID); err != nil {
		return err
	}
	if status == "" {
		return errInvalidArgument("status must be a non-empty string")
	}
	status = sanitizeHeader(status)
	return b.withLock(func() error {
		if _, err := b.requireAgentLocked(agentID, true); err != nil {
			return err
		}
		if err := b.appendRegistryEventLocked(registryEvent{
			kind:    eventStatus,
			agentID: agentID,
			status:  status,
			ts:      busNow(),
		}); err != nil {
			return err
		}
		return b.writeSnapshotLocked(b.statusSnapshotLocked(agentID, status))
	})
}

// Heartbeat updates the agent's last_seen in the durable registry (SPEC §9
// heartbeat). Observability only: no invariant depends on it.
func (b *Bus) Heartbeat(agentID string) (lastSeen time.Time, err error) {
	if err = validateAgentID(agentID); err != nil {
		return time.Time{}, err
	}
	err = b.withLock(func() error {
		if _, err := b.requireAgentLocked(agentID, true); err != nil {
			return err
		}
		lastSeen = busNow()
		if err := b.appendRegistryEventLocked(registryEvent{
			kind:    eventHeartbeat,
			agentID: agentID,
			ts:      lastSeen,
		}); err != nil {
			return err
		}
		return b.writeSnapshotLocked(b.heartbeatSnapshotLocked(agentID, lastSeen))
	})
	if err != nil {
		return time.Time{}, err
	}
	return lastSeen, nil
}

// statusSnapshotLocked / heartbeatSnapshotLocked return the current durable
// registry state for agentID with the just-applied status / last_seen, so
// the rewritten snapshot stays a faithful projection of registry.log.
// Callers hold the lock.
func (b *Bus) statusSnapshotLocked(agentID, status string) *Agent {
	agents, _ := b.readRegistryLog()
	a := agents[agentID]
	a.Status = status
	return a
}

func (b *Bus) heartbeatSnapshotLocked(agentID string, lastSeen time.Time) *Agent {
	agents, _ := b.readRegistryLog()
	a := agents[agentID]
	a.LastSeen = lastSeen
	return a
}

// AgentInfo is the whoami / get_agent payload (SPEC §9) plus, for
// get_agent, the agent's most recent mailbox records.
type AgentInfo struct {
	AgentID      string         `json:"agent_id"`
	Role         string         `json:"role"`
	Description  string         `json:"description"`
	WorkingDir   string         `json:"working_dir"`
	Model        string         `json:"model"`
	Status       string         `json:"status"`
	LastSeen     string         `json:"last_seen"`
	RegisteredAt string         `json:"registered_at"`
	Recent       []RecentRecord `json:"recent,omitempty"`
}

// RecentRecord is one entry of get_agent's `recent` list (SPEC §9).
type RecentRecord struct {
	ID          int    `json:"id"`
	Seq         int    `json:"seq"`
	From        string `json:"from"`
	Kind        string `json:"kind"`
	TS          string `json:"ts"`
	BodyExcerpt string `json:"body_excerpt"`
}

// recentExcerptBytes bounds body_excerpt (SPEC §9 names the field
// body_excerpt; the bound is an implementation choice).
const recentExcerptBytes = 200

// Info renders the whoami payload for a live agent.
func Info(a *Agent) AgentInfo {
	return AgentInfo{
		AgentID:      a.AgentID,
		Role:         a.Role,
		Description:  a.Description,
		WorkingDir:   a.WorkingDir,
		Model:        a.Model,
		Status:       a.Status,
		LastSeen:     a.LastSeen.UTC().Format(time.RFC3339),
		RegisteredAt: a.RegisteredAt.UTC().Format(time.RFC3339),
	}
}

// InfoWithRecent renders the get_agent payload: whoami fields plus the last
// recentLimit complete records of the agent's mailbox, oldest first.
func (b *Bus) InfoWithRecent(a *Agent, recentLimit int) (AgentInfo, error) {
	info := Info(a)
	if recentLimit > 0 {
		recs, err := b.readMailboxRecords(a.AgentID)
		if err != nil {
			return info, err
		}
		start := len(recs) - recentLimit
		if start < 0 {
			start = 0
		}
		info.Recent = make([]RecentRecord, 0, len(recs)-start)
		for i := start; i < len(recs); i++ {
			r := &recs[i]
			body := r.Body
			if len(body) > recentExcerptBytes {
				body = body[:recentExcerptBytes]
			}
			info.Recent = append(info.Recent, RecentRecord{
				ID:          r.ID,
				Seq:         r.Seq,
				From:        r.From,
				Kind:        r.Kind,
				TS:          r.TS.UTC().Format(time.RFC3339),
				BodyExcerpt: string(body),
			})
		}
	}
	return info, nil
}

// Whoami is the read-only whoami lookup (SPEC §9): the latest durable
// registry state for agentID.
func (b *Bus) Whoami(agentID string) (*Agent, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	agents, err := b.readRegistryLog()
	if err != nil {
		return nil, err
	}
	a, ok := agents[agentID]
	if !ok {
		return nil, errAgentNotRegistered(agentID)
	}
	if a.Membership != "alive" {
		return nil, errAgentRemoved(agentID)
	}
	return a, nil
}

// ListAgents returns registry rows filtered by role, membership
// (default "alive") and liveness (default "any") (SPEC §9 list_agents).
// Read-only: no lock (SPEC §7).
func (b *Bus) ListAgents(role, membership, liveness string) ([]AgentView, error) {
	if membership == "" {
		membership = "alive"
	}
	switch membership {
	case "alive", "removed", "any":
	default:
		return nil, errInvalidArgument("membership filter must be alive, removed, or any")
	}
	switch liveness {
	case "", "live", "dead", "any":
	default:
		return nil, errInvalidArgument("liveness filter must be live, dead, or any")
	}
	agents, err := b.readRegistryLog()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	views := make([]AgentView, 0, len(agents))
	for _, a := range agents {
		if a.Membership != membership && membership != "any" {
			continue
		}
		if role != "" && a.Role != role {
			continue
		}
		views = append(views, AgentView{
			AgentID:    a.AgentID,
			Role:       a.Role,
			Membership: a.Membership,
			Liveness:   a.Liveness(now),
			WorkingDir: a.WorkingDir,
			Model:      a.Model,
			LastSeen:   a.LastSeen,
			Status:     a.Status,
		})
	}
	if liveness != "" && liveness != "any" {
		kept := views[:0]
		for _, v := range views {
			if v.Liveness == liveness {
				kept = append(kept, v)
			}
		}
		views = kept
	}
	sort.Slice(views, func(i, j int) bool { return views[i].AgentID < views[j].AgentID })
	return views, nil
}

// GetAgent inspects one agent from any session (SPEC §9 get_agent).
// Read-only: no lock.
func (b *Bus) GetAgent(agentID string) (*Agent, []Record, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, nil, err
	}
	agents, err := b.readRegistryLog()
	if err != nil {
		return nil, nil, err
	}
	a, ok := agents[agentID]
	if !ok {
		return nil, nil, errAgentNotRegistered(agentID)
	}
	if a.Membership != "alive" {
		return nil, nil, errAgentRemoved(agentID)
	}
	recs, err := b.readMailboxRecords(agentID)
	if err != nil {
		return nil, nil, err
	}
	return a, recs, nil
}

// busNow returns the current UTC time truncated to whole seconds: the
// bus's own timestamps are second-granular (the spec examples are, and the
// liveness window is 90s), and using one canonical representation keeps
// registry.log, the snapshots, and tool results byte-identical.
func busNow() time.Time {
	return time.Now().UTC().Truncate(time.Second)
}

// sanitizeHeader forces a value to a single line so it stays inside one
// registry/record header line (SPEC §5 header grammar).
func sanitizeHeader(v string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\n', '\r':
			return ' '
		}
		return r
	}, v)
}
