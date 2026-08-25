package bus

import (
	"context"
	"fmt"
	"os"
	"sort"
	"time"
)

// Delivery names one recipient mailbox and the seq of the record placed in
// it (SPEC §9 send_message result).
type Delivery struct {
	AgentID string `json:"agent_id"`
	Seq     int    `json:"seq"`
}

// SendResult is the canonical send_message / reply result (SPEC §9).
type SendResult struct {
	ID        int        `json:"id"`
	Delivered []Delivery `json:"delivered"`
}

// ErrTimeout signals wait_for_message's non-error timeout result
// ({timeout: true}, SPEC §9 — "timeout is not an error").
var ErrTimeout = fmt.Errorf("wait_for_message: timeout elapsed")

// defaultReadLimit is the read_my_mailbox limit default (SPEC §9).
const defaultReadLimit = 256

// Send delivers a message per SPEC §7 under the canonical-log lock:
//
//  0. dedup_id check against durable shared state (the mailbox logs, scanned
//     under the flock) — a hit returns the original {id, delivered} and
//     writes nothing (invariant II);
//  1. id = counter + 1, persisted to state/counter BEFORE any append
//     (crash ordering: gap, never reuse);
//  2. recipient expansion from durable registry state (never a per-process
//     cache);
//  3. per recipient: seq = seq-counter + 1, single O_APPEND write of the
//     record (invariant II: full record or none).
func (b *Bus) Send(agentID, toAgent, toRole, kind string, body []byte, inReplyTo *int, dedupID string) (*SendResult, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if toAgent == "" && toRole == "" {
		return nil, errInvalidArgument("exactly one of to_agent or to_role is required")
	}
	if toAgent != "" && toRole != "" {
		return nil, errInvalidArgument("exactly one of to_agent or to_role is required")
	}
	if toAgent != "" {
		if err := validateAgentID(toAgent); err != nil {
			return nil, err
		}
	}
	if kind != "prompt" && kind != "info" && kind != "reply" {
		return nil, errInvalidArgument("kind must be prompt, info, or reply")
	}

	var res SendResult
	err := b.withLock(func() error {
		sender, err := b.requireAgentLocked(agentID, true)
		if err != nil {
			return err
		}
		r, e := b.sendLocked(sender, toAgent, toRole, kind, body, inReplyTo, dedupID)
		if e != nil {
			return e
		}
		res = *r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// sendLocked performs the §7 write. Callers hold the canonical-log lock.
func (b *Bus) sendLocked(sender *Agent, toAgent, toRole, kind string, body []byte, inReplyTo *int, dedupID string) (*SendResult, error) {
	// Step 0 — producer idempotency (SPEC §7, §16 Q8): the dedup record must
	// be disk-backed. v1 scans dedup_id headers in the mailbox logs under
	// the flock, so a dedup_id recorded by ANY process is visible to all.
	if dedupID != "" {
		if orig, found, err := b.findDedupLocked(dedupID); err != nil {
			return nil, err
		} else if found {
			return orig, nil
		}
	}

	// Step 2 — recipient expansion from durable state (SPEC §6: read the
	// registry while holding the flock, never a per-process cache). Expansion
	// is validated BEFORE the id is consumed: a send that fails with
	// recipient_unknown must not advance the counter, so a later retry after
	// the recipient registers does not gap ids (SPEC §18 C7).
	agents, err := b.readRegistryLog()
	if err != nil {
		return nil, err
	}
	var recipients []string
	if toAgent != "" {
		r, ok := agents[toAgent]
		if !ok || r.Membership != "alive" {
			return nil, errRecipientUnknown(toAgent)
		}
		recipients = []string{toAgent}
	} else {
		for name, a := range agents {
			if a.Membership == "alive" && a.Role == toRole {
				recipients = append(recipients, name)
			}
		}
		sort.Strings(recipients) // deterministic delivery order
		if len(recipients) == 0 {
			return nil, errRecipientUnknown("role " + toRole)
		}
	}

	// Step 1 — id = counter + 1, persisted BEFORE the appends (crash
	// ordering: a kill in between leaves a gap, never a reused id; SPEC §7,
	// §12, C4/C8).
	ctr, err := b.readCounterLocked()
	if err != nil {
		return nil, err
	}
	id := ctr + 1
	if err := b.writeCounterLocked(id); err != nil {
		return nil, err
	}

	// Step 3 — per-recipient seq + single O_APPEND write of the record.
	var dedupPtr *string
	if dedupID != "" {
		dedupPtr = &dedupID
	}
	now := time.Now().UTC()
	delivered := make([]Delivery, 0, len(recipients))
	for _, r := range recipients {
		seq, err := b.readSeqLocked(r)
		if err != nil {
			return nil, err
		}
		seq++
		// Documented deviation from §7 step 3c's write order: the seq
		// counter is persisted BEFORE the record append. A crash then gaps
		// the per-recipient seq (never duplicates it), mirroring the id
		// rule, and keeps per-recipient seq monotonic (invariant III)
		// across restarts. The record is written before the tool returns,
		// so invariant I (no silent loss) is unaffected.
		if err := b.writeSeqLocked(r, seq); err != nil {
			return nil, err
		}
		rec := Record{
			Seq:       seq,
			ID:        id,
			From:      sender.AgentID,
			FromRole:  sender.Role,
			To:        r,
			Kind:      kind,
			InReplyTo: inReplyTo,
			TS:        now,
			DedupID:   dedupPtr,
			Body:      body,
		}
		if err := b.appendMailboxRecordLocked(r, rec.Encode()); err != nil {
			return nil, err
		}
		delivered = append(delivered, Delivery{AgentID: r, Seq: seq})
	}
	return &SendResult{ID: id, Delivered: delivered}, nil
}

// appendMailboxRecordLocked appends raw encoded record bytes to
// mailboxes/<agent>.log in a single write() under the lock (SPEC §7 step 3c:
// "append the record ... in a single write() syscall under the flock").
func (b *Bus) appendMailboxRecordLocked(agentID string, encoded []byte) error {
	f, err := os.OpenFile(b.mailboxPath(agentID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(encoded); err != nil {
		return err
	}
	return b.syncFile(f)
}

// findDedupLocked scans all mailbox logs for a record carrying dedupID and
// reconstructs the original send result (SPEC §7 step 0). Callers hold the
// canonical-log lock.
func (b *Bus) findDedupLocked(dedupID string) (*SendResult, bool, error) {
	byAgent, err := b.mailboxRecordsLocked()
	if err != nil {
		return nil, false, err
	}
	var orig *Record
	var delivered []Delivery
	for agent, recs := range byAgent {
		for i := range recs {
			if recs[i].DedupID != nil && *recs[i].DedupID == dedupID {
				if orig == nil {
					orig = &recs[i]
				}
				delivered = append(delivered, Delivery{AgentID: agent, Seq: recs[i].Seq})
			}
		}
	}
	if orig == nil {
		return nil, false, nil
	}
	sort.Slice(delivered, func(i, j int) bool { return delivered[i].AgentID < delivered[j].AgentID })
	return &SendResult{ID: orig.ID, Delivered: delivered}, true, nil
}

// Reply is the §9 reply convenience: it looks up message in_reply_to in the
// caller's OWN mailbox and sends a kind=reply message to that record's from
// with in_reply_to set (SPEC §9 "Lookup (v1)": linear scan, no index).
func (b *Bus) Reply(agentID string, inReplyTo int, body []byte, dedupID string) (*SendResult, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if inReplyTo <= 0 {
		return nil, errInvalidArgument("in_reply_to must be a positive message id")
	}
	var res SendResult
	err := b.withLock(func() error {
		sender, err := b.requireAgentLocked(agentID, true)
		if err != nil {
			return err
		}
		// A dedup hit returns the original result without any lookup.
		if dedupID != "" {
			orig, found, derr := b.findDedupLocked(dedupID)
			if derr != nil {
				return derr
			}
			if found {
				res = *orig
				return nil
			}
		}
		// v1 lookup (SPEC §9 "Lookup (v1)"): linear scan of the caller's
		// own mailbox; on miss, in_reply_to_not_found.
		origRec, found, err := b.mailboxRecordByID(agentID, inReplyTo)
		if err != nil {
			return err
		}
		if !found {
			return errInReplyToNotFound(inReplyTo, agentID)
		}
		r, e := b.sendLocked(sender, origRec.From, "", "reply", body, &inReplyTo, dedupID)
		if e != nil {
			return e
		}
		res = *r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// mailboxRecordByID finds one complete record by global id in a mailbox log
// (read-only; a partial trailing record is ignored, as on any mailbox read).
func (b *Bus) mailboxRecordByID(agentID string, id int) (*Record, bool, error) {
	recs, err := b.readMailboxRecords(agentID)
	if err != nil {
		return nil, false, err
	}
	for i := range recs {
		if recs[i].ID == id {
			return &recs[i], true, nil
		}
	}
	return nil, false, nil
}

// readMailboxRecords parses one mailbox log (complete records only). The
// mailbox file may not exist yet (no message delivered to the agent yet):
// that is an empty mailbox, not an error.
func (b *Bus) readMailboxRecords(agentID string) ([]Record, error) {
	data, err := os.ReadFile(b.mailboxPath(agentID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	recs, _, _, perr := ParseRecords(data)
	if perr != nil {
		return nil, perr
	}
	return recs, nil
}

// ReadMailbox returns records with seq > since, ordered by seq, optionally
// filtered by kind, capped at limit (default 256) (SPEC §9
// read_my_mailbox). The read cursor is the caller's own state (SPEC §8);
// the bus stores none of it. Read-only: no lock (SPEC §7).
func (b *Bus) ReadMailbox(agentID string, since int, kind string, limit int) ([]RecordView, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if since < 0 {
		return nil, errInvalidArgument("since must be >= 0")
	}
	if limit < 0 {
		return nil, errInvalidArgument("limit must be >= 0")
	}
	if limit == 0 {
		limit = defaultReadLimit
	}
	if err := b.requireAgentForMailbox(agentID); err != nil {
		return nil, err
	}
	recs, err := b.readMailboxRecords(agentID)
	if err != nil {
		return nil, err
	}
	out := make([]RecordView, 0, len(recs))
	for i := range recs {
		if recs[i].Seq <= since {
			continue
		}
		if kind != "" && recs[i].Kind != kind {
			continue
		}
		out = append(out, recs[i].View())
		if len(out) >= limit {
			break
		}
	}
	// Mailbox logs are append-only in seq order, so parse order is seq
	// order; the stable sort keeps that guarantee explicit.
	sort.SliceStable(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, nil
}

// requireAgentForMailbox maps "not a live agent" to the no_such_mailbox code
// for the mailbox-scoped read tools (SPEC §9 "Errors").
func (b *Bus) requireAgentForMailbox(agentID string) error {
	agents, err := b.readRegistryLog()
	if err != nil {
		return err
	}
	a, ok := agents[agentID]
	if !ok || a.Membership != "alive" {
		return errNoSuchMailbox(agentID)
	}
	return nil
}

// waitChunk is the maximum blocking slice before the wait loop re-checks the
// mailbox (SPEC §8: kqueue/inotify "falling back to backoff polling").
const waitChunk = 100 * time.Millisecond

// WaitForMessage blocks until a record with seq > since matching the filters
// exists in the agent's mailbox, or the timeout elapses (SPEC §8/§9). It
// returns ErrTimeout — NOT a bus.Error; the tool layer renders it as
// {timeout: true}, because "timeout is not an error". Read-only: no lock.
func (b *Bus) WaitForMessage(agentID string, since int, fromRole, fromAgent, kind string, timeout time.Duration, ctx context.Context) (*Record, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if since < 0 {
		return nil, errInvalidArgument("since must be >= 0")
	}
	if timeout < 0 {
		return nil, errInvalidArgument("timeout must be >= 0 seconds")
	}
	if err := b.requireAgentForMailbox(agentID); err != nil {
		return nil, err
	}

	w, err := newMailboxWatcher(b.mailboxesDir(), b.mailboxPath(agentID))
	if err != nil {
		// Platform watcher unavailable: degrade to pure backoff polling
		// (SPEC §8 fallback) rather than failing the call.
		w = newSleepWaiter()
	}
	defer w.close()

	deadline := time.Now().Add(timeout)
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if rec, ok := b.matchMailbox(agentID, since, fromRole, fromAgent, kind); ok {
			return rec, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, ErrTimeout
		}
		chunk := waitChunk
		if remaining < chunk {
			chunk = remaining
		}
		if _, werr := w.wait(chunk); werr != nil {
			// A broken watcher must not kill the wait: just poll.
			time.Sleep(min(chunk, waitChunk))
		}
	}
}

// matchMailbox returns the first record with seq > since matching the
// filters, or nil (SPEC §9 wait_for_message: "returns the first match").
func (b *Bus) matchMailbox(agentID string, since int, fromRole, fromAgent, kind string) (*Record, bool) {
	recs, err := b.readMailboxRecords(agentID)
	if err != nil {
		return nil, false
	}
	for i := range recs {
		r := &recs[i]
		if r.Seq <= since {
			continue
		}
		if fromRole != "" && r.FromRole != fromRole {
			continue
		}
		if fromAgent != "" && r.From != fromAgent {
			continue
		}
		if kind != "" && r.Kind != kind {
			continue
		}
		return r, true
	}
	return nil, false
}
