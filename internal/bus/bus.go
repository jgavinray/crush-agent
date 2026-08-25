package bus

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// DefaultRootName is where the bus root lives when BUS_ROOT is unset
// (SPEC §19.2).
const DefaultRootName = ".crush-mailbox-bus"

// RootFromEnv resolves the bus root: $BUS_ROOT if set and non-empty, else
// $HOME/.crush-mailbox-bus (SPEC §13, §19.2).
func RootFromEnv() string {
	if v := os.Getenv("BUS_ROOT"); v != "" {
		return filepath.Clean(v)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, DefaultRootName)
	}
	return DefaultRootName
}

// Options configures Open. P1 runs with zeroed options (page-cache
// durability, log-scan lookups); P4 enables both (SPEC §17 P4).
type Options struct {
	// Fsync enables fsync-before-return on every canonical write, upgrading
	// page-cache durability to power-loss durability (SPEC §3 IV, §17 P4).
	Fsync bool
}

// Bus is a stateless façade over the durable files under one root directory
// (SPEC §2, §4, §13). All correctness state lives in those files; any
// number of bus processes may share one root, serialized by the advisory
// lock on state/lock (SPEC §7 "The canonical-log lock"). A Bus holds no
// in-memory correctness state and may be killed at any time (SPEC §12).
type Bus struct {
	root string
	opts Options
}

// NewBus returns a Bus for root without touching the filesystem.
func NewBus(root string, opts Options) *Bus {
	return &Bus{root: filepath.Clean(root), opts: opts}
}

// Root returns the bus root directory.
func (b *Bus) Root() string { return b.root }

// Filesystem layout (SPEC §4).
func (b *Bus) stateDir() string     { return filepath.Join(b.root, "state") }
func (b *Bus) registryDir() string  { return filepath.Join(b.root, "registry") }
func (b *Bus) mailboxesDir() string { return filepath.Join(b.root, "mailboxes") }

func (b *Bus) lockPath() string    { return filepath.Join(b.stateDir(), "lock") }
func (b *Bus) counterPath() string { return filepath.Join(b.stateDir(), "counter") }
func (b *Bus) seqPath(id string) string {
	return filepath.Join(b.stateDir(), "seq."+id)
}
func (b *Bus) registryLogPath() string { return filepath.Join(b.root, "registry.log") }
func (b *Bus) snapshotPath(id string) string {
	return filepath.Join(b.registryDir(), id+".json")
}
func (b *Bus) mailboxPath(id string) string {
	return filepath.Join(b.mailboxesDir(), id+".log")
}

// Open prepares root (creating the SPEC §4 layout if needed) and performs
// the SPEC §12 startup recovery, then returns a ready Bus. It is idempotent
// and crash-safe: an empty root and a populated one produce the same running
// system, and existing logs are never truncated or reset except to remove a
// partial trailing record left by a power loss.
func Open(root string, opts Options) (*Bus, error) {
	b := NewBus(root, opts)
	for _, dir := range []string{b.root, b.stateDir(), b.registryDir(), b.mailboxesDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	if err := b.withLock(b.recover); err != nil {
		return nil, err
	}
	return b, nil
}

// recover performs the SPEC §12 startup reconciliation, under the lock:
//
//  1. every mailbox log with a truncated trailing record is truncated back
//     to its last complete record — the `len` header makes the truncation
//     point unambiguous;
//  2. state/counter is recomputed as the max id observed across all mailbox
//     logs, so neither a crash between the counter write and the record
//     append, nor a hand-truncated or hand-edited log, can ever cause an id
//     to be reused (SPEC §7 crash ordering, invariant II).
func (b *Bus) recover() error {
	for _, id := range b.listMailboxAgents() {
		path := b.mailboxPath(id)
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, consumed, complete, perr := ParseRecords(data)
		if perr != nil {
			return fmt.Errorf("mailbox %s: %w", id, perr)
		}
		if !complete && consumed < len(data) {
			if err := os.Truncate(path, int64(consumed)); err != nil {
				return fmt.Errorf("mailbox %s: truncate partial tail: %w", id, err)
			}
		}
	}
	maxID, err := b.maxObservedID()
	if err != nil {
		return err
	}
	return b.writeCounterLocked(maxID)
}

// listMailboxAgents lists the agents that have a mailbox log.
func (b *Bus) listMailboxAgents() []string {
	entries, err := os.ReadDir(b.mailboxesDir())
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		out = append(out, strings.TrimSuffix(e.Name(), ".log"))
	}
	return out
}

// maxObservedID returns the highest id present in any complete mailbox
// record (0 for an empty bus).
func (b *Bus) maxObservedID() (int, error) {
	maxID := 0
	for _, id := range b.listMailboxAgents() {
		data, err := os.ReadFile(b.mailboxPath(id))
		if err != nil {
			return 0, err
		}
		recs, _, _, perr := ParseRecords(data)
		if perr != nil {
			return 0, perr
		}
		for i := range recs {
			if recs[i].ID > maxID {
				maxID = recs[i].ID
			}
		}
	}
	return maxID, nil
}

// withLock serializes a canonical mutation on the shared advisory lock
// (SPEC §7). The lock is acquired per open-file-description, so it
// serializes other bus processes as well as concurrent tool calls within
// this process. Read-only tools deliberately do not take it (SPEC §7).
func (b *Bus) withLock(fn func() error) (err error) {
	f, lerr := lockFile(b.lockPath())
	if lerr != nil {
		return fmt.Errorf("lock %s: %w", b.lockPath(), lerr)
	}
	defer func() {
		if uerr := unlockFile(f); uerr != nil && err == nil {
			err = uerr
		}
	}()
	return fn()
}

// --- counters (SPEC §4, §7) ------------------------------------------------

// readCounterLocked returns the last assigned global id (0 when the file is
// absent). Caller must hold the canonical-log lock.
func (b *Bus) readCounterLocked() (int, error) {
	return b.readIntCounter(b.counterPath())
}

// readSeqLocked returns the recipient's last assigned seq (0 for a fresh
// mailbox). Per-recipient counters live in state/seq.<agent_id>, NOT in
// mailbox log length: log length is wrong the moment compact truncates a
// prefix (SPEC §7 step 3a). Caller must hold the canonical-log lock.
func (b *Bus) readSeqLocked(agentID string) (int, error) {
	return b.readIntCounter(b.seqPath(agentID))
}

func (b *Bus) readIntCounter(path string) (int, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	v, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return 0, fmt.Errorf("%s: unparseable counter %q", path, string(data))
	}
	if v < 0 {
		return 0, fmt.Errorf("%s: negative counter %d", path, v)
	}
	return v, nil
}

// writeCounterLocked persists the last assigned global id. Callers must hold
// the canonical-log lock. SPEC §7 crash ordering: the counter is persisted
// BEFORE any record append, so a crash gaps ids rather than reusing one.
func (b *Bus) writeCounterLocked(v int) error {
	return b.writeIntCounter(b.counterPath(), v)
}

func (b *Bus) writeSeqLocked(agentID string, v int) error {
	return b.writeIntCounter(b.seqPath(agentID), v)
}

func (b *Bus) writeIntCounter(path string, v int) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%d\n", v); err != nil {
		return err
	}
	return b.syncFile(f)
}

// mailboxRecordsLocked parses every mailbox log (complete records only).
// Used under the lock for dedup and id-reconstruction scans (SPEC §7 step 0,
// §16 Q8). A malformed record is a hard error; a partial trailing record
// (crashed writer) yields no error from ParseRecords and is simply not
// included — it will be truncated by the next startup recovery.
func (b *Bus) mailboxRecordsLocked() (map[string][]Record, error) {
	out := map[string][]Record{}
	for _, id := range b.listMailboxAgents() {
		data, err := os.ReadFile(b.mailboxPath(id))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		recs, _, _, perr := ParseRecords(data)
		if perr != nil {
			return nil, fmt.Errorf("mailbox %s: %w", id, perr)
		}
		out[id] = recs
	}
	return out, nil
}

// mailboxRecordsNoLock is the read-only, lock-free variant for the same
// scans (SPEC §7: read-only tools may observe a pre- or post-mutation
// snapshot; that is harmless here).
func (b *Bus) mailboxRecordsNoLock() map[string][]Record {
	out := map[string][]Record{}
	entries, err := os.ReadDir(b.mailboxesDir())
	if err != nil {
		return out
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		id := strings.TrimSuffix(e.Name(), ".log")
		data, err := os.ReadFile(filepath.Join(b.mailboxesDir(), e.Name()))
		if err != nil {
			continue
		}
		recs, _, _, _ := ParseRecords(data)
		out[id] = recs
	}
	return out
}
