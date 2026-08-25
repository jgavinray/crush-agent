package bus

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
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

// Options configures Open. The P1 reference mode is zeroed (page-cache
// durability, log-scan lookups); P4 enables both fields (SPEC §17 P4).
type Options struct {
	// Fsync enables fsync-before-return on every canonical write, upgrading
	// page-cache durability to power-loss durability (SPEC §3 IV, §17 P4).
	Fsync bool
	// Index maintains index.db, the derived lookup structure that makes
	// dedup_id checks and the wait_for_message fast path O(1) (SPEC §16 Q4,
	// §17 P4). The mailbox logs stay the source of truth; the index is
	// rebuilt from them on every Open under the lock.
	Index bool
}

// Bus is a stateless façade over the durable files under one root directory
// (SPEC §2, §4, §13). All correctness state lives in those files; any
// number of bus processes may share one root, serialized by the advisory
// lock on state/lock (SPEC §7 "The canonical-log lock"). A Bus holds no
// in-memory correctness state and may be killed at any time (SPEC §12).
type Bus struct {
	root  string
	opts  Options
	index *indexDB

	// seenAgents is the set of agent_ids that called register/whoami
	// through THIS bus process. It scopes the P3 channel pusher: only
	// mail for agents this process has seen is pushed to its own MCP
	// client (see ChannelPusher, push.go). In-memory by design — the
	// durable registry lives in registry.log; this is a per-process
	// routing hint, and every agent re-registers on each session start
	// (SPEC §11.1), so no state is lost across restarts.
	seenMu     sync.Mutex
	seenAgents map[string]struct{}
}

// NewBus returns a Bus for root without touching the filesystem.
func NewBus(root string, opts Options) *Bus {
	return &Bus{root: filepath.Clean(root), opts: opts}
}

// NoteAgent records that agentID interacted with this bus process
// (SPEC §17 P3 channel push scoping). Called by the tool layer on
// register/whoami.
func (b *Bus) NoteAgent(agentID string) {
	if agentID == "" {
		return
	}
	b.seenMu.Lock()
	defer b.seenMu.Unlock()
	if b.seenAgents == nil {
		b.seenAgents = make(map[string]struct{})
	}
	b.seenAgents[agentID] = struct{}{}
}

// AgentSeen reports whether this bus process has seen agentID register or
// call whoami. Used by the P3 channel pusher to scope pushes to the agents
// this process serves (push.go).
func (b *Bus) AgentSeen(agentID string) bool {
	b.seenMu.Lock()
	defer b.seenMu.Unlock()
	_, ok := b.seenAgents[agentID]
	return ok
}

// readMailboxFile reads a mailbox log; a missing mailbox yields no records.
func (b *Bus) readMailboxFile(agentID string) ([]byte, error) {
	data, err := os.ReadFile(b.mailboxPath(agentID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

// MaxSeq returns the highest seq in the agent's mailbox log (the last
// record's seq; logs are append-only with monotonic per-recipient seq,
// SPEC §5/§7). ok=false when the mailbox has no complete records.
func (b *Bus) MaxSeq(agentID string) (int, bool) {
	data, err := b.readMailboxFile(agentID)
	if err != nil || len(data) == 0 {
		return 0, false
	}
	recs, _, _, perr := ParseRecords(data)
	if perr != nil || len(recs) == 0 {
		return 0, false
	}
	return recs[len(recs)-1].Seq, true
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
	for _, dir := range []string{b.stateDir(), b.registryDir(), b.mailboxesDir()} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create %s: %w", dir, err)
		}
	}
	// bus_root itself is 0700 (SPEC §17 P4: file ownership on bus_root as
	// the shared-boundary gate; auth is moot over stdio, §14). Chmod as
	// well as MkdirAll so pre-existing roots tighten on first P4 open.
	if err := os.MkdirAll(b.root, 0o700); err != nil {
		return nil, fmt.Errorf("create %s: %w", b.root, err)
	}
	if err := os.Chmod(b.root, 0o700); err != nil {
		return nil, fmt.Errorf("chmod %s: %w", b.root, err)
	}
	ix, err := b.openIndex()
	if err != nil {
		return nil, err
	}
	b.index = ix
	if err := b.withLock(b.recover); err != nil {
		b.Close()
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
//     to be reused (SPEC §7 crash ordering, invariant II);
//  3. index.db is rebuilt from the (now-truncated) mailbox logs — the
//     derived index can never lead the durable logs (SPEC §17 P4).
//
// It also guarantees the SPEC §13 bootstrap layout: registry.log exists
// from the first start (created empty), so the canonical log is a stable
// path a human or host-side verifier can always `cat`.
func (b *Bus) recover() error {
	if err := b.ensureRegistryLogLocked(); err != nil {
		return err
	}
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
	if err := b.writeCounterLocked(maxID); err != nil {
		return err
	}
	return b.rebuildIndexLocked()
}

// ensureRegistryLogLocked creates registry.log if absent (SPEC §13: on
// start the bus creates it "if absent"). Callers hold the lock.
func (b *Bus) ensureRegistryLogLocked() error {
	f, err := os.OpenFile(b.registryLogPath(), os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("create registry.log: %w", err)
	}
	return f.Close()
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
	preExisting := !b.fileAbsent(path)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := fmt.Fprintf(f, "%d\n", v); err != nil {
		return err
	}
	if err := b.syncFile(f); err != nil {
		return err
	}
	// P4: durable-ify the directory entry when the file is brand new, so a
	// power loss cannot leave the counter "lost" behind an unsynced dirent
	// (SPEC §17 P4 fsync-before-return).
	if !preExisting {
		return b.syncDirFor(filepath.Dir(path))
	}
	return nil
}

// Close releases derived resources (the index.db handle). Safe to call more
// than once and after a failed Open; it never touches durable state — the
// bus holds no correctness state in memory (SPEC §2).
func (b *Bus) Close() error {
	if b.index != nil {
		if err := b.index.db.Close(); err != nil {
			return err
		}
		b.index = nil
	}
	return nil
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
