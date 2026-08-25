package bus

import (
	"fmt"
	"os"
)

// CompactResult is the compact tool result (SPEC §17 P4, §16 Q4).
type CompactResult struct {
	AgentID     string `json:"agent_id"`
	UpToSeq     int    `json:"up_to_seq"`
	Archived    int    `json:"archived"`
	ArchivePath string `json:"archive_path"`
}

// archivePath is the cold file that holds compacted mailbox prefixes
// (SPEC §16 Q4: "snapshot the consumed prefix to a cold file and truncate
// it"). It lives next to the live log so bus_root stays the only tree.
func (b *Bus) archivePath(agentID string) string {
	return b.mailboxPath(agentID) + ".archived"
}

// Compact archives the consumed prefix (records with seq <= upToSeq) of
// agentID's mailbox to the cold archive and truncates the live log to the
// remaining records, in one locked critical section (SPEC §17 P4):
//
//   - the prefix bytes are appended VERBATIM to mailboxes/<id>.log.archived
//     (record framing preserved; the archive is itself a valid mailbox log
//     fragment, invariant V);
//   - the live log is rewritten with the surviving suffix in a single write
//     under the flock (SPEC §7 single-write rule);
//   - per-recipient seq counters are UNTOUCHED (SPEC §16 Q4: "seq counters
//     unaffected"), so the next delivery continues from the last assigned
//     seq — the log's byte length is deliberately wrong after a compact;
//   - index rows for archived records are kept: the dedup_id contract must
//     hold for the whole system lifetime, and the index is the shared
//     dedup registry.
//
// The caller is responsible for the precondition: the agent must have
// durably consumed up to upToSeq (its last_seq_read). Compact is idempotent:
// a boundary at or below the smallest live seq archives nothing.
func (b *Bus) Compact(agentID string, upToSeq int) (*CompactResult, error) {
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if upToSeq < 0 {
		return nil, errInvalidArgument("up_to_seq must be >= 0")
	}
	var res CompactResult
	err := b.withLock(func() error {
		if _, err := b.requireAgentLocked(agentID, true); err != nil {
			return err
		}
		archived, err := b.compactLocked(agentID, upToSeq)
		if err != nil {
			return err
		}
		res = CompactResult{
			AgentID:     agentID,
			UpToSeq:     upToSeq,
			Archived:    archived,
			ArchivePath: b.archivePath(agentID),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &res, nil
}

// compactLocked performs the archive+truncate. Callers hold the lock.
// Returns the number of records moved to the archive.
func (b *Bus) compactLocked(agentID string, upToSeq int) (int, error) {
	data, err := os.ReadFile(b.mailboxPath(agentID))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	// Walk complete records; the log is in ascending seq order (invariant
	// III), so the archived prefix is the leading run with seq <= upToSeq.
	// A partial trailing record (crashed writer) is never archived: it stays
	// in the live log for the next startup recovery to truncate.
	pos := 0
	boundary := 0
	archived := 0
	for pos < len(data) {
		rec, next, complete, perr := parseOneRecord(data, pos)
		if perr != nil {
			return 0, fmt.Errorf("mailbox %s: %w", agentID, perr)
		}
		if !complete {
			break
		}
		if rec.Seq > upToSeq {
			break
		}
		pos = next
		boundary = next
		archived++
	}
	if boundary == 0 {
		return 0, nil
	}
	prefix := data[:boundary]
	suffix := data[boundary:]

	// 1. Append the prefix to the cold archive (single write under the lock).
	archiveNew := b.fileAbsent(b.archivePath(agentID))
	af, err := os.OpenFile(b.archivePath(agentID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	if _, err := af.Write(prefix); err != nil {
		af.Close()
		return 0, err
	}
	if err := b.syncFile(af); err != nil {
		af.Close()
		return 0, err
	}
	if err := af.Close(); err != nil {
		return 0, err
	}
	if archiveNew {
		if err := b.syncDirFor(b.mailboxesDir()); err != nil {
			return 0, err
		}
	}

	// 2. Rewrite the live log with the surviving suffix, single write under
	// the lock (SPEC §7). An empty suffix truncates the log to zero bytes.
	if err := b.writeMailboxFileLocked(agentID, suffix); err != nil {
		return 0, err
	}
	return archived, nil
}

// writeMailboxFileLocked replaces a live mailbox log with raw bytes in a
// single write under the canonical-log lock (the only rewrite the system
// ever performs on a live log; SPEC §7 single-write rule).
func (b *Bus) writeMailboxFileLocked(agentID string, data []byte) error {
	f, err := os.OpenFile(b.mailboxPath(agentID), os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return b.syncFile(f)
}

// fileAbsent reports whether path does not exist yet.
func (b *Bus) fileAbsent(path string) bool {
	_, err := os.Stat(path)
	return os.IsNotExist(err)
}
