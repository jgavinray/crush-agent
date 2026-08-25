package bus

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"sort"

	_ "modernc.org/sqlite" // pure-Go SQLite driver, driver name "sqlite"
)

// index.db is a DERIVED, rebuildable lookup structure (SPEC §17 P4, §16 Q4):
// it accelerates the dedup_id check (§7 step 0), the reply lookup, and the
// wait_for_message fast-path. The mailbox logs remain the source of truth;
// every Open rebuilds the index from the logs under the canonical-log lock,
// so a lost or corrupt index.db is self-healing and can never lose data.
//
// Compact (§16 Q4) deliberately does NOT drop index rows for archived
// records: the dedup_id contract ("a resend with the same key returns the
// original result and writes nothing") must hold for the whole lifetime of
// the system, and the index is the shared dedup registry.
type indexDB struct {
	db *sql.DB
}

const indexSchema = `
CREATE TABLE IF NOT EXISTS message (
    mailbox    TEXT    NOT NULL,
    seq        INTEGER NOT NULL,
    id         INTEGER NOT NULL,
    from_agent TEXT    NOT NULL,
    dedup_id   TEXT,
    PRIMARY KEY (mailbox, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS message_dedup_idx
    ON message (dedup_id) WHERE dedup_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS message_id_idx ON message (id);
`

// openIndex opens (creating if needed) the index for this bus root. Returns
// nil without error when Options.Index is off.
func (b *Bus) openIndex() (*indexDB, error) {
	if !b.opts.Index {
		return nil, nil
	}
	path := b.indexPath()
	// busy_timeout: the flock already serializes writers across processes;
	// a brief reader may transiently hold the db file.
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open index.db: %w", err)
	}
	if _, err := db.Exec(indexSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("init index.db schema: %w", err)
	}
	return &indexDB{db: db}, nil
}

func (b *Bus) indexPath() string { return filepath.Join(b.root, "index.db") }

// rebuildIndexLocked replaces the entire index from the mailbox logs.
// Called on every Open under the canonical-log lock (SPEC §12: "rebuild
// index.db from mailboxes/*.log").
func (b *Bus) rebuildIndexLocked() error {
	if b.index == nil {
		return nil
	}
	tx, err := b.index.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("DELETE FROM message"); err != nil {
		return err
	}
	byAgent, err := b.mailboxRecordsLocked()
	if err != nil {
		return err
	}
	insert, err := tx.Prepare(
		"INSERT OR REPLACE INTO message (mailbox, seq, id, from_agent, dedup_id) VALUES (?, ?, ?, ?, ?)")
	if err != nil {
		return err
	}
	defer insert.Close()
	for agent, recs := range byAgent {
		for i := range recs {
			var dedup any
			if recs[i].DedupID != nil {
				dedup = *recs[i].DedupID
			}
			if _, err := insert.Exec(agent, recs[i].Seq, recs[i].ID, recs[i].From, dedup); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// recordIndexLocked inserts one delivered record into the index. Callers
// hold the canonical-log lock; the insert happens after the log append in
// the same critical section, so the log and the index can never diverge for
// a returned send.
func (b *Bus) recordIndexLocked(rec *Record, owner string) error {
	if b.index == nil {
		return nil
	}
	var dedup any
	if rec.DedupID != nil {
		dedup = *rec.DedupID
	}
	_, err := b.index.db.Exec(
		"INSERT OR REPLACE INTO message (mailbox, seq, id, from_agent, dedup_id) VALUES (?, ?, ?, ?, ?)",
		owner, rec.Seq, rec.ID, rec.From, dedup,
	)
	return err
}

// findDedupLocked is the P4 dedup check (SPEC §7 step 0, §16 Q8): an O(1)
// index lookup; on any index failure the durable log scan is the backstop
// (the index is derived, the logs are the source of truth).
func (b *Bus) findDedupLocked(dedupID string) (*SendResult, bool, error) {
	if b.index != nil {
		rows, err := b.index.db.Query(
			"SELECT mailbox, seq, id FROM message WHERE dedup_id = ?", dedupID,
		)
		if err == nil {
			var result *SendResult
			for rows.Next() {
				var mailbox string
				var seq, id int
				if err := rows.Scan(&mailbox, &seq, &id); err != nil {
					rows.Close()
					return nil, false, err
				}
				if result == nil {
					result = &SendResult{ID: id}
				}
				result.Delivered = append(result.Delivered, Delivery{AgentID: mailbox, Seq: seq})
			}
			if err := rows.Err(); err != nil {
				rows.Close()
				return nil, false, err
			}
			rows.Close()
			if result != nil {
				sortDelivered(result)
				return result, true, nil
			}
		}
		// Index query failed (or the index is empty of this key): fall
		// through to the authoritative log scan.
	}
	return b.findDedupByLogScanLocked(dedupID)
}

// waitIndexHasMatch reports whether the index believes any record with
// seq > since exists for the mailbox (optionally from one agent). Used by
// wait_for_message to SKIP the log parse in the common idle case. A false
// positive is harmless (the log scan is authoritative); a false negative is
// impossible: every append inserts its row before the send returns.
func (b *Bus) waitIndexHasMatch(agentID string, since int, fromAgent string) (bool, bool) {
	// (found, known): known=false means "index unavailable, do the scan".
	if b.index == nil {
		return false, false
	}
	q := "SELECT COUNT(*) FROM message WHERE mailbox = ? AND seq > ?"
	args := []any{agentID, since}
	if fromAgent != "" {
		q += " AND from_agent = ?"
		args = append(args, fromAgent)
	}
	var n int
	if err := b.index.db.QueryRow(q, args...).Scan(&n); err != nil {
		return false, false
	}
	return n > 0, true
}

// sortDelivered keeps a reconstructed delivered list deterministic.
func sortDelivered(r *SendResult) {
	sort.Slice(r.Delivered, func(i, j int) bool {
		return r.Delivered[i].AgentID < r.Delivered[j].AgentID
	})
}
