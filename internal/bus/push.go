package bus

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ChannelSink is the wire a ChannelPusher sends
// notifications/claude/channel payloads onto. Implementations write the
// JSON-RPC notification on the session's MCP connection.
type ChannelSink interface {
	PushChannel(ctx context.Context, content string, meta map[string]string) error
}

// ChannelPusher implements the bus half of SPEC §17 P3 (§16 Q9): it
// watches the mailbox logs and, for every record that lands in the
// mailbox of an agent this bus process has seen register, pushes a
// `notifications/claude/channel` notification to the connected MCP
// client. The Crush side (the P3 fork) routes that notification to the
// session that registered `meta.agent_id` and starts a fresh agent turn
// from it — that is what turns "a message arrives" into "the agent
// wakes up", replacing the §11.4 host-side loop wrapper.
//
// Why "agents this process has seen register": in the no-fork deployment
// (N bus children, one per session) every child would otherwise re-push
// every record for every agent to its own parent; scoping the push to
// the agents observed through this process means each session is pushed
// only its own mail. In the `crush server` deployment one bus child
// serves all hosted sessions, so it observes every agent's register and
// pushes for all of them — the server-side routing picks the owner.
//
// Delivery semantics: at-least-once, matching invariant I. A record is
// pushed at most once per bus process lifetime (per-mailbox seq high
// watermark), and a record that is never pushed still reaches its
// recipient through the pull path (read_my_mailbox / wait_for_message),
// so the push is an optimization, never the source of truth.
type ChannelPusher struct {
	b        *Bus
	sink     ChannelSink
	interval time.Duration

	mu      sync.Mutex
	running bool
	// lastSeq[agentID] is the highest seq pushed for that mailbox; it is
	// seeded to the current max on Start so pre-existing mail is never
	// re-pushed (history belongs to the pull path).
	lastSeq map[string]int
	stop    chan struct{}
	done    chan struct{}
}

// NewChannelPusher returns a pusher with the default 250ms poll interval.
func NewChannelPusher(b *Bus, sink ChannelSink) *ChannelPusher {
	return &ChannelPusher{
		b:        b,
		sink:     sink,
		interval: 250 * time.Millisecond,
		lastSeq:  make(map[string]int),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start begins the watch loop. It is a no-op if already running. The
// loop exits when Stop is called or when the sink stops accepting
// writes (the MCP session ended).
func (p *ChannelPusher) Start() {
	p.mu.Lock()
	if p.running {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	go p.loop()
}

// Stop terminates the watch loop and waits for it to exit.
func (p *ChannelPusher) Stop() {
	p.mu.Lock()
	if !p.running {
		p.mu.Unlock()
		return
	}
	p.running = false
	close(p.stop)
	p.mu.Unlock()
	<-p.done
}

func (p *ChannelPusher) loop() {
	defer close(p.done)

	// Seed the per-mailbox seq watermarks to the current state so only
	// records that land AFTER this process is live get pushed.
	p.seedWatermarks()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			if !p.tick() {
				// The connection died; there is no client to push to
				// anymore.
				return
			}
		}
	}
}

func (p *ChannelPusher) seedWatermarks() {
	for _, id := range p.b.listMailboxAgents() {
		if maxSeq, ok := p.b.MaxSeq(id); ok {
			p.lastSeq[id] = maxSeq
		}
	}
}

// tick scans every mailbox log for records newer than the watermark and
// pushes them. It returns false when the sink is dead.
func (p *ChannelPusher) tick() bool {
	agents := p.b.listMailboxAgents()
	// New agents may have no mailbox yet; only agents with a log can
	// have new records.
	for _, id := range agents {
		data, err := p.b.readMailboxFile(id)
		if err != nil {
			continue
		}
		recs, _, _, perr := ParseRecords(data)
		if perr != nil {
			// A malformed log is a repair-time problem (recovery
			// truncates it); the pusher stays out of the way.
			continue
		}
		highest := p.watermark(id)
		for _, rec := range recs {
			if rec.Seq <= highest {
				continue
			}
			// Only push mail for agents this process knows — see the
			// type comment for the deployment reasoning.
			if !p.b.AgentSeen(rec.To) {
				highest = rec.Seq
				continue
			}
			if err := p.pushRecord(rec); err != nil {
				return false
			}
			highest = rec.Seq
		}
		if highest > p.watermark(id) {
			p.setWatermark(id, highest)
		}
	}
	return true
}

func (p *ChannelPusher) pushRecord(rec Record) error {
	meta := map[string]string{
		// agent_id is the routing key the Crush P3 fork maps back to the
		// owning session (SPEC §16 Q9-a). The remaining keys are plain
		// context; all of them satisfy crush's meta key validation
		// (^[A-Za-z_][A-Za-z0-9_]*$) and none is reserved.
		"agent_id": rec.To,
		"id":       strconv.Itoa(rec.ID),
		"kind":     rec.Kind,
		"from":     rec.From,
	}
	if rec.InReplyTo != nil {
		meta["in_reply_to"] = strconv.Itoa(*rec.InReplyTo)
	}

	// Keep the pushed payload light: the record is durable in the mailbox
	// and the receiving agent's role prompt tells it to call
	// read_my_mailbox, so the push is a wake-up, not a copy.
	body := string(rec.Body)
	const maxBody = 4000
	if len(body) > maxBody {
		body = body[:maxBody] + " [...truncated; call read_my_mailbox for the full record]"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Bus channel delivery: a new record just landed in your mailbox (agent_id=%s).\n", rec.To)
	fmt.Fprintf(&b, "kind=%s, id=%d, from=%s", rec.Kind, rec.ID, rec.From)
	if rec.InReplyTo != nil {
		fmt.Fprintf(&b, ", in_reply_to=%d", *rec.InReplyTo)
	}
	b.WriteString("\nBody: ")
	b.WriteString(body)
	b.WriteString("\nPer your bus role: call read_my_mailbox(agent_id=")
	b.WriteString(rec.To)
	b.WriteString(") and handle the new record.")
	return p.sink.PushChannel(context.Background(), b.String(), meta)
}

func (p *ChannelPusher) watermark(id string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastSeq[id]
}

func (p *ChannelPusher) setWatermark(id string, seq int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if seq > p.lastSeq[id] {
		p.lastSeq[id] = seq
	}
}
