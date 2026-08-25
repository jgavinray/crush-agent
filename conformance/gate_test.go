// Package conformance is the deterministic black-box gate for the
// mailbox-bus binary (SPEC §18). It was written from SPEC.md BEFORE the
// implementation, and the implementer MUST NOT modify it to make a failing
// test pass: a failing test is a bug in the bus, not in the test. A skipped
// test is a loud failure, never silent.
//
// The suite drives the bus binary as a stdio MCP client — the same transport
// Crush uses — and asserts on tool results and on the on-disk bus_root files
// (the oracle), never on Go internals.
//
// P1 cases (SPEC §18 C1–C10) plus P1 wait_for_message behavior are covered
// here. P2/P4 cases (lifecycle, compact, index, fsync) are added by the
// commits that land those features.
package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

var binPath string

func TestMain(m *testing.M) {
	wd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "gate: getwd: %v\n", err)
		os.Exit(1)
	}
	root := findModuleRoot(wd)
	tmp, err := os.MkdirTemp("", "mbus-gate-*")
	if err != nil {
		fmt.Fprintf(os.Stderr, "gate: mkdtemp: %v\n", err)
		os.Exit(1)
	}
	binPath = filepath.Join(tmp, "mailbox-bus")
	build := exec.Command("go", "build", "-o", binPath, filepath.Join(root, "cmd", "mailbox-bus"))
	build.Dir = root
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "gate: cannot build bus binary: %v\n%s\n", err, out)
		os.RemoveAll(tmp)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(tmp)
	os.Exit(code)
}

func findModuleRoot(start string) string {
	dir := start
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return start
		}
		dir = parent
	}
}

// ---------------------------------------------------------------------------
// Bus process wrapper
// ---------------------------------------------------------------------------

type busProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	sess   *mcp.ClientSession
	root   string
	errb   *bytes.Buffer
	reaped bool
}

// startBus spawns a fresh bus child process against the given bus_root and
// connects to it over stdio MCP (the same transport Crush uses).
func startBus(t *testing.T, root string) *busProc {
	t.Helper()
	cmd := exec.Command(binPath)
	cmd.Env = append(os.Environ(), "BUS_ROOT="+root)
	errb := &bytes.Buffer{}
	cmd.Stderr = errb

	client := mcp.NewClient(&mcp.Implementation{Name: "conformance-gate", Version: "0"}, nil)
	sess, err := client.Connect(context.Background(), &mcp.CommandTransport{Command: cmd}, nil)
	if err != nil {
		t.Fatalf("bus connect: %v (stderr: %s)", err, errb.String())
	}
	p := &busProc{t: t, cmd: cmd, sess: sess, root: root, errb: errb}
	t.Cleanup(func() {
		if p.reaped {
			return
		}
		_ = p.sess.Close()
		_ = p.cmd.Wait()
	})
	return p
}

// kill SIGKILLs the bus child process to simulate a crash (SPEC §12, C4/C8).
// The kernel releases the flock on process death, so a restarted bus can
// proceed immediately.
func (p *busProc) kill(t *testing.T) {
	t.Helper()
	p.reaped = true
	if err := p.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill bus process: %v", err)
	}
	_ = p.sess.Close()
	_, _ = p.cmd.Process.Wait()
}

func (p *busProc) call(t *testing.T, tool string, args map[string]any) *mcp.CallToolResult {
	t.Helper()
	res, err := p.sess.CallTool(context.Background(), &mcp.CallToolParams{Name: tool, Arguments: args})
	if err != nil {
		t.Fatalf("%s: transport error: %v (bus stderr: %s)", tool, err, p.errb.String())
	}
	return res
}

func contentText(res *mcp.CallToolResult) string {
	if len(res.Content) > 0 {
		if tc, ok := res.Content[0].(*mcp.TextContent); ok {
			return tc.Text
		}
	}
	return ""
}

// callObj invokes a tool and returns its JSON object payload.
func (p *busProc) callObj(t *testing.T, tool string, args map[string]any) map[string]any {
	t.Helper()
	res := p.call(t, tool, args)
	if res.IsError {
		t.Fatalf("%s: unexpected error result: %s", tool, contentText(res))
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(contentText(res)), &out); err != nil {
		t.Fatalf("%s: result is not a JSON object: %v (raw: %q)", tool, err, contentText(res))
	}
	return out
}

// callList invokes a tool whose result is a JSON array of objects.
func (p *busProc) callList(t *testing.T, tool string, args map[string]any) []map[string]any {
	t.Helper()
	res := p.call(t, tool, args)
	if res.IsError {
		t.Fatalf("%s: unexpected error result: %s", tool, contentText(res))
	}
	var out []map[string]any
	if err := json.Unmarshal([]byte(contentText(res)), &out); err != nil {
		t.Fatalf("%s: result is not a JSON array of objects: %v (raw: %q)", tool, err, contentText(res))
	}
	return out
}

// callErr invokes a tool and asserts a bus error result with the exact code.
func (p *busProc) callErr(t *testing.T, tool string, args map[string]any, code string) {
	t.Helper()
	res := p.call(t, tool, args)
	if !res.IsError {
		t.Fatalf("%s: expected bus error %q, got success: %s", tool, code, contentText(res))
	}
	var out map[string]any
	if err := json.Unmarshal([]byte(contentText(res)), &out); err != nil {
		t.Fatalf("%s: error result is not a JSON object: %v", tool, err)
	}
	if out["error"] != code {
		t.Fatalf("%s: error code = %v, want %q (raw: %s)", tool, out["error"], code, contentText(res))
	}
}

// ---------------------------------------------------------------------------
// Helpers for the on-disk oracle
// ---------------------------------------------------------------------------

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func mailboxPath(root, agent string) string { return filepath.Join(root, "mailboxes", agent+".log") }

// parseMailboxLog is an independent, spec-derived parser (SPEC §5 record
// grammar): it reads `len` bytes of body verbatim, so bodies containing blank
// lines or a line equal to "---" are handled. It hard-fails on malformed or
// truncated records.
func parseMailboxLog(t *testing.T, data []byte) []map[string]string {
	t.Helper()
	out := []map[string]string{}
	pos := 0
	for pos < len(data) {
		rest := data[pos:]
		// Locate the standalone "---" line terminating the header. Because
		// the header precedes the body and a body may contain "---", the
		// first standalone "---" line from the record start is the separator.
		i := 0
		found := false
		for i < len(rest) {
			lineEnd := bytes.IndexByte(rest[i:], '\n')
			if lineEnd < 0 {
				break
			}
			if string(rest[i:i+lineEnd]) == "---" {
				found = true
				break
			}
			i += lineEnd + 1
		}
		if !found {
			t.Fatalf("incomplete record header at offset %d: %q", pos, rest)
		}
		fields := map[string]string{}
		// rest[:i] ends with the "\n" that closes the last header-line
		// (SPEC §5: header-line := field ":" SP value "\n"), so the split
		// yields one trailing empty element that is NOT a header line —
		// stop at the first blank element, and require every real line to
		// be "field: value".
		for _, line := range strings.Split(string(rest[:i]), "\n") {
			if line == "" {
				break
			}
			k, v, ok := strings.Cut(line, ": ")
			if !ok {
				t.Fatalf("malformed record header line %q", line)
			}
			fields[k] = v
		}
		ln, err := strconv.Atoi(fields["len"])
		if err != nil {
			t.Fatalf("bad len field %q: %v", fields["len"], err)
		}
		bodyStart := i + len("---\n")
		if bodyStart+ln+2 > len(rest) {
			t.Fatalf("truncated record at offset %d", pos)
		}
		if string(rest[bodyStart+ln:bodyStart+ln+2]) != "\n\n" {
			t.Fatalf("missing record terminator at offset %d", pos+bodyStart)
		}
		rec := map[string]string{"body": string(rest[bodyStart : bodyStart+ln])}
		for _, k := range []string{"seq", "id", "from", "from_role", "to", "kind", "in_reply_to", "ts", "dedup_id"} {
			if v, ok := fields[k]; !ok {
				t.Fatalf("record at offset %d missing header field %q", pos, k)
			} else {
				rec[k] = v
			}
		}
		out = append(out, rec)
		pos += bodyStart + ln + 2
	}
	return out
}

func readMailboxRecords(t *testing.T, root, agent string) []map[string]string {
	t.Helper()
	return parseMailboxLog(t, mustReadFile(t, mailboxPath(root, agent)))
}

func readCounter(t *testing.T, root string) int {
	t.Helper()
	raw := strings.TrimSpace(string(mustReadFile(t, filepath.Join(root, "state", "counter"))))
	v, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("unparseable counter %q: %v", raw, err)
	}
	return v
}

// countDedup counts records carrying dedup_id across ALL mailbox logs.
func countDedup(t *testing.T, root, dedupID string) int {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "mailboxes"))
	if err != nil {
		t.Fatalf("read mailboxes dir: %v", err)
	}
	n := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") {
			continue
		}
		recs := parseMailboxLog(t, mustReadFile(t, filepath.Join(root, "mailboxes", e.Name())))
		for _, r := range recs {
			if r["dedup_id"] == dedupID {
				n++
			}
		}
	}
	return n
}

func (p *busProc) register(t *testing.T, id, role string) {
	t.Helper()
	p.callObj(t, "register", map[string]any{
		"agent_id":    id,
		"role":        role,
		"description": "gate agent " + id,
		"working_dir": "/tmp/" + id,
	})
}

func intField(t *testing.T, v any, field string) int {
	t.Helper()
	f, ok := v.(float64)
	if !ok {
		t.Fatalf("field %q is not a number: %v", field, v)
	}
	return int(f)
}

func (p *busProc) send(t *testing.T, args map[string]any) map[string]any {
	t.Helper()
	return p.callObj(t, "send_message", args)
}

func assertDelivered(t *testing.T, out map[string]any, wantID int, want ...[2]any) {
	t.Helper()
	if got := intField(t, out["id"], "id"); got != wantID {
		t.Fatalf("id = %d, want %d (result: %v)", got, wantID, out)
	}
	dlv, ok := out["delivered"].([]any)
	if !ok {
		t.Fatalf("delivered is not an array: %v", out)
	}
	if len(dlv) != len(want) {
		t.Fatalf("delivered has %d entries, want %d (result: %v)", len(dlv), len(want), out)
	}
	for i, w := range want {
		entry, ok := dlv[i].(map[string]any)
		if !ok {
			t.Fatalf("delivered[%d] is not an object: %v", i, dlv[i])
		}
		if entry["agent_id"] != w[0] {
			t.Fatalf("delivered[%d].agent_id = %v, want %v", i, entry["agent_id"], w[0])
		}
		if got := intField(t, entry["seq"], "delivered.seq"); got != w[1].(int) {
			t.Fatalf("delivered[%d].seq = %d, want %d", i, got, w[1])
		}
	}
}

// ---------------------------------------------------------------------------
// C1 — At-least-once delivery (invariant I, SPEC §7)
// ---------------------------------------------------------------------------

func TestC1_AtLeastOnceDelivery(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "ra")
	p.register(t, "b", "rb")

	out := p.send(t, map[string]any{
		"agent_id": "a", "to_agent": "b", "kind": "info", "body": "hello C1",
	})
	assertDelivered(t, out, 1, [2]any{"b", 1})

	// Via MCP: exactly that record, with the correct fields.
	recs := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 0})
	if len(recs) != 1 {
		t.Fatalf("read_my_mailbox returned %d records, want 1: %v", len(recs), recs)
	}
	r := recs[0]
	if intField(t, r["id"], "id") != 1 {
		t.Fatalf("record id = %v, want 1", r["id"])
	}
	if intField(t, r["seq"], "seq") != 1 {
		t.Fatalf("record seq = %v, want 1", r["seq"])
	}
	if r["from"] != "a" || r["from_role"] != "ra" || r["to"] != "b" || r["kind"] != "info" {
		t.Fatalf("record addressing wrong: %v", r)
	}
	if r["in_reply_to"] != nil {
		t.Fatalf("in_reply_to = %v, want null", r["in_reply_to"])
	}
	if r["dedup_id"] != nil {
		t.Fatalf("dedup_id = %v, want null", r["dedup_id"])
	}
	if r["body"] != "hello C1" {
		t.Fatalf("body = %q, want %q", r["body"], "hello C1")
	}

	// On-disk oracle: the record is present in mailboxes/b.log.
	disk := readMailboxRecords(t, root, "b")
	if len(disk) != 1 {
		t.Fatalf("b.log has %d records, want 1", len(disk))
	}
	if disk[0]["id"] != "1" || disk[0]["seq"] != "1" || disk[0]["body"] != "hello C1" {
		t.Fatalf("b.log record mismatch: %v", disk[0])
	}
}

// ---------------------------------------------------------------------------
// C2 — No duplication across processes (invariant II, SPEC §7 step 0)
// ---------------------------------------------------------------------------

func TestC2_NoDuplicationAcrossProcesses(t *testing.T) {
	root := t.TempDir()
	p1 := startBus(t, root)
	p2 := startBus(t, root) // second bus child, same bus_root

	p1.register(t, "a", "ra")
	p1.register(t, "b", "rb")

	one := map[string]any{
		"agent_id": "a", "to_agent": "b", "kind": "info",
		"body": "c2", "dedup_id": "K1",
	}
	out1 := p1.send(t, one)
	out2 := p2.send(t, one) // resend through a DIFFERENT process

	if intField(t, out2["id"], "id") != intField(t, out1["id"], "id") {
		t.Fatalf("replay returned id %v, want original id %v", out2["id"], out1["id"])
	}
	assertDelivered(t, out2, intField(t, out1["id"], "id"), [2]any{"b", 1})

	if n := countDedup(t, root, "K1"); n != 1 {
		t.Fatalf("found %d records with dedup_id K1 across all mailboxes, want exactly 1", n)
	}
	if recs := readMailboxRecords(t, root, "b"); len(recs) != 1 {
		t.Fatalf("b.log has %d records after replay, want 1", len(recs))
	}

	// A replay must not consume a new id.
	out3 := p2.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "c2-next"})
	if got, want := intField(t, out3["id"], "id"), intField(t, out1["id"], "id")+1; got != want {
		t.Fatalf("post-replay id = %d, want %d (replay must not advance the counter)", got, want)
	}
}

// ---------------------------------------------------------------------------
// C3 — Per-pair ordering (invariant III, SPEC §7)
// ---------------------------------------------------------------------------

func TestC3_PerPairOrdering(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "ra")
	p.register(t, "b", "rb")

	out1 := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "first"})
	out2 := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "second"})
	id1 := intField(t, out1["id"], "id")
	id2 := intField(t, out2["id"], "id")
	if id2 <= id1 {
		t.Fatalf("global ids not monotonic: %d then %d", id1, id2)
	}

	// On-disk order in b's mailbox.
	disk := readMailboxRecords(t, root, "b")
	if len(disk) != 2 {
		t.Fatalf("b.log has %d records, want 2", len(disk))
	}
	if disk[0]["seq"] != "1" || disk[0]["body"] != "first" {
		t.Fatalf("first record wrong: %v", disk[0])
	}
	if disk[1]["seq"] != "2" || disk[1]["body"] != "second" {
		t.Fatalf("second record wrong: %v", disk[1])
	}

	// MCP read order.
	recs := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 0})
	if len(recs) != 2 || recs[0]["body"] != "first" || recs[1]["body"] != "second" {
		t.Fatalf("read_my_mailbox order wrong: %v", recs)
	}
}

// ---------------------------------------------------------------------------
// C4 — Crash ordering: gap, never reuse (invariant II, SPEC §7, §12)
// ---------------------------------------------------------------------------

func TestC4_CrashOrderGapNeverReuse(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "ra")
	p.register(t, "b", "rb")
	for i := 1; i <= 4; i++ {
		p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": fmt.Sprintf("m%d", i)})
	}
	// Simulate a crash in the §7 crash window: the counter had been advanced
	// to 5 (id assigned and persisted) but the record append never happened.
	p.kill(t)
	if err := os.WriteFile(filepath.Join(root, "state", "counter"), []byte("5\n"), 0o644); err != nil {
		t.Fatalf("write counter: %v", err)
	}

	p2 := startBus(t, root)

	// Recovery must recompute counter = max(id observed across logs) = 4.
	if got := readCounter(t, root); got != 4 {
		t.Fatalf("counter after recovery = %d, want 4 (recomputed from logs)", got)
	}

	// The next id must be the killed counter value 5 — a gap-free fill of the
	// undelivered id, but never a reuse of a delivered id.
	out5 := p2.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "m5"})
	if got := intField(t, out5["id"], "id"); got != 5 {
		t.Fatalf("next id = %d, want 5 (the killed counter value)", got)
	}
	out6 := p2.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "m6"})
	if got := intField(t, out6["id"], "id"); got != 6 {
		t.Fatalf("next id = %d, want 6", got)
	}

	// No delivered id is ever reused.
	seen := map[string]bool{}
	for _, rec := range readMailboxRecords(t, root, "b") {
		if seen[rec["id"]] {
			t.Fatalf("id %s delivered twice in b.log", rec["id"])
		}
		seen[rec["id"]] = true
	}
	if len(seen) != 6 {
		t.Fatalf("b.log has %d distinct ids, want 6", len(seen))
	}
}

// ---------------------------------------------------------------------------
// C5 — Durable recipient expansion (invariant I, SPEC §7 step 2, §6)
// ---------------------------------------------------------------------------

func TestC5_DurableRecipientExpansion(t *testing.T) {
	root := t.TempDir()
	p2 := startBus(t, root) // starts first; its in-process view (if any) has no agents
	p1 := startBus(t, root)

	p1.register(t, "x", "worker") // registered AFTER p2 started
	p2.register(t, "a", "sender")

	out := p2.send(t, map[string]any{"agent_id": "a", "to_role": "worker", "kind": "info", "body": "c5"})
	assertDelivered(t, out, 1, [2]any{"x", 1})

	recs := readMailboxRecords(t, root, "x")
	if len(recs) != 1 || recs[0]["body"] != "c5" {
		t.Fatalf("late-registered agent did not receive the broadcast: %v", recs)
	}
}

// ---------------------------------------------------------------------------
// C6 — Idempotent consumer cursor (invariant I, SPEC §8)
// ---------------------------------------------------------------------------

func TestC6_IdempotentConsumerCursor(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "ra")
	p.register(t, "b", "rb")
	out1 := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "prompt", "body": "work-1"})
	out2 := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "prompt", "body": "work-2"})
	id1 := intField(t, out1["id"], "id")
	id2 := intField(t, out2["id"], "id")

	// Consumer b "handles" seq 1 and seq 2 but crashes before persisting the
	// cursor past seq 1. Restart → re-read from since = N-1 = 1.
	first := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 0})
	if len(first) != 2 {
		t.Fatalf("initial read returned %d records, want 2", len(first))
	}
	processed := map[int]bool{id1: true, id2: true}
	workCount := 2 // work done before the "crash"

	re := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 1})
	if len(re) != 1 {
		t.Fatalf("re-read with since=1 returned %d records, want exactly 1 (seq 2)", len(re))
	}
	for _, r := range re {
		id := intField(t, r["id"], "id")
		if processed[id] {
			// Idempotent handler: recognize the already-handled message by id
			// and do NOT re-run the work.
			continue
		}
		workCount++
		processed[id] = true
	}
	if workCount != 2 {
		t.Fatalf("work executed %d times total, want 2 (no duplicate work)", workCount)
	}
}

// ---------------------------------------------------------------------------
// C7 — Register / unregister / re-register (SPEC §9, §10)
// ---------------------------------------------------------------------------

func TestC7_RegisterUnregisterReregister(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "r")
	p.register(t, "b", "s")

	// b sends to a so a's mailbox exists on disk.
	p.send(t, map[string]any{"agent_id": "b", "to_agent": "a", "kind": "info", "body": "c7-1"})

	// Unregister a: it leaves list_agents and is no longer addressable.
	out := p.callObj(t, "unregister", map[string]any{"agent_id": "a"})
	if out["status"] != "removed" {
		t.Fatalf("unregister status = %v, want \"removed\"", out["status"])
	}
	if out["unregistered_at"] == nil || out["unregistered_at"] == "" {
		t.Fatalf("unregister missing unregistered_at: %v", out)
	}
	agents := p.callList(t, "list_agents", map[string]any{})
	for _, a := range agents {
		if a["agent_id"] == "a" {
			t.Fatalf("unregistered agent still listed: %v", a)
		}
	}
	p.callErr(t, "send_message", map[string]any{
		"agent_id": "b", "to_agent": "a", "kind": "info", "body": "to removed",
	}, "recipient_unknown")

	// The mailbox is retained on disk (append-only audit, SPEC §9).
	if _, err := os.Stat(mailboxPath(root, "a")); err != nil {
		t.Fatalf("mailbox log for unregistered agent missing: %v", err)
	}

	// Re-register: back alive, and the existing mailbox is reused.
	p.register(t, "a", "r")
	agents = p.callList(t, "list_agents", map[string]any{})
	found := false
	for _, a := range agents {
		if a["agent_id"] == "a" && a["membership"] == "alive" {
			found = true
		}
	}
	if !found {
		t.Fatalf("re-registered agent not listed as alive: %v", agents)
	}
	out2 := p.send(t, map[string]any{"agent_id": "b", "to_agent": "a", "kind": "info", "body": "c7-2"})
	assertDelivered(t, out2, 2, [2]any{"a", 2}) // seq continues, same log file
	recs := readMailboxRecords(t, root, "a")
	if len(recs) != 2 {
		t.Fatalf("reused mailbox has %d records, want 2", len(recs))
	}
}

// ---------------------------------------------------------------------------
// C8 — Page-cache durability + recovery (invariant IV, SPEC §12)
// ---------------------------------------------------------------------------

func TestC8_PageCacheDurabilityAndRecovery(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "ra")
	p.register(t, "b", "rb")
	out := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "durable"})
	id1 := intField(t, out["id"], "id")

	// Process crash (SIGKILL) — the accepted record must survive.
	p.kill(t)

	p2 := startBus(t, root)
	if got := readCounter(t, root); got != id1 {
		t.Fatalf("counter after recovery = %d, want %d (max id observed)", got, id1)
	}
	recs := readMailboxRecords(t, root, "b")
	if len(recs) != 1 || recs[0]["body"] != "durable" || recs[0]["id"] != strconv.Itoa(id1) {
		t.Fatalf("accepted record not intact after restart: %v", recs)
	}

	// The bus is fully functional after recovery.
	out2 := p2.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "next"})
	if got, want := intField(t, out2["id"], "id"), id1+1; got != want {
		t.Fatalf("post-recovery id = %d, want %d", got, want)
	}
}

// ---------------------------------------------------------------------------
// C9 — Record framing (SPEC §5): `len` is authoritative
// ---------------------------------------------------------------------------

func TestC9_RecordFraming(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "ra")
	p.register(t, "b", "rb")

	body := "line one\n\n---\nline three"
	p.callObj(t, "send_message", map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": body})

	// On disk: exactly ONE record, body verbatim.
	disk := readMailboxRecords(t, root, "b")
	if len(disk) != 1 {
		t.Fatalf("b.log parsed to %d records, want exactly 1 (len must be authoritative)", len(disk))
	}
	if disk[0]["body"] != body {
		t.Fatalf("body = %q, want %q", disk[0]["body"], body)
	}

	// Via MCP: body verbatim.
	recs := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 0})
	if len(recs) != 1 || recs[0]["body"] != body {
		t.Fatalf("read_my_mailbox body mismatch: %v", recs)
	}
}

// ---------------------------------------------------------------------------
// C10 — Broadcast correlation (SPEC §7, §16 Q6)
// ---------------------------------------------------------------------------

func TestC10_BroadcastCorrelation(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "a", "sender")
	p.register(t, "b", "team")
	p.register(t, "c", "team")

	out := p.send(t, map[string]any{"agent_id": "a", "to_role": "team", "kind": "prompt", "body": "do the thing"})
	id := intField(t, out["id"], "id")
	assertDelivered(t, out, id, [2]any{"b", 1}, [2]any{"c", 1})

	// Both copies share one id; each has its own `to` and its own `seq`.
	brecs := readMailboxRecords(t, root, "b")
	crecs := readMailboxRecords(t, root, "c")
	if len(brecs) != 1 || len(crecs) != 1 {
		t.Fatalf("broadcast copies missing: b=%d c=%d", len(brecs), len(crecs))
	}
	if brecs[0]["id"] != strconv.Itoa(id) || crecs[0]["id"] != strconv.Itoa(id) {
		t.Fatalf("broadcast copies do not share id %d: b=%q c=%q", id, brecs[0]["id"], crecs[0]["id"])
	}
	if brecs[0]["to"] != "b" || crecs[0]["to"] != "c" {
		t.Fatalf("each copy must carry its own `to`: b=%q c=%q", brecs[0]["to"], crecs[0]["to"])
	}
	if brecs[0]["seq"] != "1" || crecs[0]["seq"] != "1" {
		t.Fatalf("each copy must carry its own per-recipient seq: b=%q c=%q", brecs[0]["seq"], crecs[0]["seq"])
	}

	// Both recipients reply citing the shared broadcast id; the sender
	// correlates both by in_reply_to.
	r1 := p.callObj(t, "reply", map[string]any{
		"in_reply_to": id, "agent_id": "b", "body": "b done",
	})
	assertDelivered(t, r1, id+1, [2]any{"a", 1})
	r2 := p.callObj(t, "reply", map[string]any{
		"in_reply_to": id, "agent_id": "c", "body": "c done",
	})
	assertDelivered(t, r2, id+2, [2]any{"a", 2})

	replies := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "a", "since": 0, "kind": "reply"})
	if len(replies) != 2 {
		t.Fatalf("sender has %d replies, want 2", len(replies))
	}
	byFrom := map[string]int{}
	for _, r := range replies {
		if intField(t, r["in_reply_to"], "in_reply_to") != id {
			t.Fatalf("reply %v does not cite broadcast id %d", r["id"], id)
		}
		byFrom[r["from"].(string)]++
	}
	if byFrom["b"] != 1 || byFrom["c"] != 1 {
		t.Fatalf("replies not correlated to both senders: %v", byFrom)
	}
}

// ---------------------------------------------------------------------------
// P1 — wait_for_message (blocking long-poll, SPEC §8, §9)
// ---------------------------------------------------------------------------

func TestP1_WaitForMessageBlocksCrossProcess(t *testing.T) {
	root := t.TempDir()
	p1 := startBus(t, root) // will block in wait_for_message
	p2 := startBus(t, root) // will deliver the wake-up message

	p1.register(t, "a", "ra")
	p1.register(t, "b", "rb")

	done := make(chan error, 1)
	go func() {
		// Give the waiter time to enter its blocking loop, then deliver from
		// a DIFFERENT bus process: the wake-up must cross processes (file
		// event / re-check), not in-memory state.
		time.Sleep(500 * time.Millisecond)
		_, err := p2.sess.CallTool(context.Background(), &mcp.CallToolParams{
			Name: "send_message",
			Arguments: map[string]any{
				"agent_id": "a", "to_agent": "b", "kind": "info", "body": "wake",
			},
		})
		done <- err
	}()

	start := time.Now()
	out := p1.callObj(t, "wait_for_message", map[string]any{
		"agent_id": "b", "since": 0, "timeout": 30,
	})
	if out["timeout"] != nil {
		t.Fatalf("wait returned timeout instead of the message: %v", out)
	}
	msg, ok := out["message"].(map[string]any)
	if !ok {
		t.Fatalf("wait_for_message missing message: %v", out)
	}
	if msg["body"] != "wake" || msg["from"] != "a" {
		t.Fatalf("wrong message returned: %v", msg)
	}
	if intField(t, msg["seq"], "seq") != 1 {
		t.Fatalf("wait returned seq %v, want 1", msg["seq"])
	}
	if elapsed := time.Since(start); elapsed > 20*time.Second {
		t.Fatalf("wait took %v; expected near-instant wake-up", elapsed)
	}
	if err := <-done; err != nil {
		t.Fatalf("sender CallTool failed: %v", err)
	}
}

func TestP1_WaitForMessageTimeout(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)
	p.register(t, "a", "ra")
	p.register(t, "b", "rb")

	start := time.Now()
	out := p.callObj(t, "wait_for_message", map[string]any{
		"agent_id": "b", "since": 0, "timeout": 1,
	})
	if out["timeout"] != true {
		t.Fatalf("expected {timeout: true}, got %v", out)
	}
	if out["message"] != nil {
		t.Fatalf("timeout result must not contain a message: %v", out)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("returned after %v; did not wait out the 1s timeout", elapsed)
	}

	// A message arriving after the timeout is still retrievable via since=0.
	p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "late"})
	recs := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 0})
	if len(recs) != 1 || recs[0]["body"] != "late" {
		t.Fatalf("late message not readable after timeout: %v", recs)
	}
}

func TestP1_WaitForMessageFilters(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)
	p.register(t, "a", "ra")
	p.register(t, "b", "rb")
	p.register(t, "c", "rc")

	p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "noise"})
	p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "prompt", "body": "real"})
	p.send(t, map[string]any{"agent_id": "c", "to_agent": "b", "kind": "info", "body": "from-c"})

	// kind filter: skip the info record, return the prompt.
	out := p.callObj(t, "wait_for_message", map[string]any{
		"agent_id": "b", "since": 0, "kind": "prompt", "timeout": 5,
	})
	if out["timeout"] != nil {
		t.Fatalf("kind-filtered wait timed out: %v", out)
	}
	if msg := out["message"].(map[string]any); msg["body"] != "real" {
		t.Fatalf("kind filter returned %v", msg)
	}

	// from_agent filter: skip a's records, return c's.
	out = p.callObj(t, "wait_for_message", map[string]any{
		"agent_id": "b", "since": 0, "from_agent": "c", "timeout": 5,
	})
	if out["timeout"] != nil {
		t.Fatalf("from_agent-filtered wait timed out: %v", out)
	}
	if msg := out["message"].(map[string]any); msg["body"] != "from-c" {
		t.Fatalf("from_agent filter returned %v", msg)
	}

	// since filter: nothing new since seq 3 → timeout.
	out = p.callObj(t, "wait_for_message", map[string]any{
		"agent_id": "b", "since": 3, "timeout": 1,
	})
	if out["timeout"] != true {
		t.Fatalf("expected timeout with since=3, got %v", out)
	}
}

// ---------------------------------------------------------------------------
// P2 — lifecycle & inspection (SPEC §9, §10, §17 P2)
// ---------------------------------------------------------------------------

// C11 — Status and liveness: set_my_status is reported verbatim, heartbeat
// refreshes the durable last_seen, and liveness derives from it (live within
// the 90s window, dead beyond it).
func TestC11_StatusAndLiveness(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "o", "orchestrator")
	p.register(t, "w", "worker")

	out := p.callObj(t, "set_my_status", map[string]any{
		"agent_id": "w", "status": "working: auth module",
	})
	if out["ok"] != true {
		t.Fatalf("set_my_status = %v, want {ok: true}", out)
	}

	agents := p.callList(t, "list_agents", map[string]any{})
	var w, o map[string]any
	for _, a := range agents {
		switch a["agent_id"] {
		case "w":
			w = a
		case "o":
			o = a
		}
	}
	if w == nil || o == nil {
		t.Fatalf("list_agents missing agents: %v", agents)
	}
	if w["status"] != "working: auth module" {
		t.Fatalf("status = %v, want \"working: auth module\"", w["status"])
	}
	if w["membership"] != "alive" {
		t.Fatalf("membership = %v, want alive", w["membership"])
	}
	if w["liveness"] != "live" {
		t.Fatalf("liveness = %v, want live (freshly registered)", w["liveness"])
	}

	hb := p.callObj(t, "heartbeat", map[string]any{"agent_id": "w"})
	if hb["ok"] != true {
		t.Fatalf("heartbeat = %v, want ok", hb)
	}
	ls, _ := hb["last_seen"].(string)
	if ls == "" {
		t.Fatalf("heartbeat missing last_seen: %v", hb)
	}

	// Durable: the on-disk snapshot projects the heartbeat (SPEC §6).
	var snap map[string]any
	snapData := mustReadFile(t, filepath.Join(root, "registry", "w.json"))
	if err := json.Unmarshal(snapData, &snap); err != nil {
		t.Fatalf("snapshot not JSON: %v", err)
	}
	if snap["last_seen"] != ls {
		t.Fatalf("snapshot last_seen = %v, want %v", snap["last_seen"], ls)
	}

	who := p.callObj(t, "whoami", map[string]any{"agent_id": "w"})
	if who["status"] != "working: auth module" || who["last_seen"] != ls {
		t.Fatalf("whoami = %v, want status and last_seen %q", who, ls)
	}

	// Make w stale: append a heartbeat event with an old timestamp to the
	// durable log (black-box state manipulation, as in C4). The running bus
	// must observe it without a restart — membership reads are durable.
	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC).Format(time.RFC3339)
	block := "event: heartbeat\nagent_id: w\nts: " + old + "\n\n"
	f, err := os.OpenFile(filepath.Join(root, "registry.log"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("append registry.log: %v", err)
	}
	if _, err := f.WriteString(block); err != nil {
		t.Fatalf("write registry.log: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close registry.log: %v", err)
	}

	agents = p.callList(t, "list_agents", map[string]any{})
	for _, a := range agents {
		if a["agent_id"] == "w" && a["liveness"] != "dead" {
			t.Fatalf("stale agent liveness = %v, want dead: %v", a["liveness"], a)
		}
	}
	// The liveness filter must exclude the dead agent.
	live := p.callList(t, "list_agents", map[string]any{"liveness": "live"})
	for _, a := range live {
		if a["agent_id"] == "w" {
			t.Fatalf("liveness=live filter still lists dead agent w: %v", a)
		}
	}
}

// C12 — whoami: identity fields and error codes.
func TestC12_WhoamiIdentityAndErrors(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)

	p.register(t, "fe", "frontend")
	out := p.callObj(t, "whoami", map[string]any{"agent_id": "fe"})
	if out["agent_id"] != "fe" || out["role"] != "frontend" {
		t.Fatalf("whoami identity wrong: %v", out)
	}
	if out["description"] != "gate agent fe" {
		t.Fatalf("whoami description = %v, want \"gate agent fe\"", out["description"])
	}
	if out["working_dir"] != "/tmp/fe" {
		t.Fatalf("whoami working_dir = %v, want /tmp/fe", out["working_dir"])
	}
	if out["registered_at"] == nil || out["registered_at"] == "" {
		t.Fatalf("whoami missing registered_at: %v", out)
	}

	p.callErr(t, "whoami", map[string]any{"agent_id": "ghost"}, "agent_not_registered")

	p.register(t, "gone", "g")
	p.callObj(t, "unregister", map[string]any{"agent_id": "gone"})
	p.callErr(t, "whoami", map[string]any{"agent_id": "gone"}, "agent_removed")
}

// C13 — get_agent: inspection from any session, recent records with
// body_excerpt.
func TestC13_GetAgent(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)
	p.register(t, "s", "sender")
	p.register(t, "r", "receiver")

	body1 := strings.Repeat("x", 300)
	out1 := p.send(t, map[string]any{"agent_id": "s", "to_agent": "r", "kind": "prompt", "body": body1})
	p.send(t, map[string]any{"agent_id": "s", "to_agent": "r", "kind": "info", "body": "second"})

	out := p.callObj(t, "get_agent", map[string]any{"agent_id": "r"})
	if out["agent_id"] != "r" || out["role"] != "receiver" {
		t.Fatalf("get_agent identity wrong: %v", out)
	}
	recent, ok := out["recent"].([]any)
	if !ok || len(recent) != 2 {
		t.Fatalf("get_agent recent = %v, want 2 records", out["recent"])
	}
	r0, r1 := recent[0].(map[string]any), recent[1].(map[string]any)
	if intField(t, r0["id"], "recent.id") != intField(t, out1["id"], "id") {
		t.Fatalf("recent[0].id = %v, want %v", r0["id"], out1["id"])
	}
	if r0["from"] != "s" || r0["kind"] != "prompt" {
		t.Fatalf("recent[0] = %v", r0)
	}
	if r1["kind"] != "info" {
		t.Fatalf("recent[1] = %v", r1)
	}
	if intField(t, r0["seq"], "recent.seq") != 1 {
		t.Fatalf("recent[0].seq = %v, want 1", r0["seq"])
	}
	// body_excerpt must be a strict prefix of the 300-byte body.
	ex, _ := r0["body_excerpt"].(string)
	if !strings.HasPrefix(body1, ex) || len(ex) >= len(body1) {
		t.Fatalf("body_excerpt = %q, want a strict prefix of the 300-byte body", ex)
	}

	p.callErr(t, "get_agent", map[string]any{"agent_id": "ghost"}, "agent_not_registered")
}

// C14 — heartbeat/set_my_status error codes, and status durability across a
// process restart (SPEC §6 durable registry, §10 lifecycle).
func TestC14_HeartbeatAndStatusErrors(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)
	p.register(t, "h", "heartbeat-test")

	first := p.callObj(t, "heartbeat", map[string]any{"agent_id": "h"})
	if first["ok"] != true {
		t.Fatalf("heartbeat = %v, want ok", first)
	}
	if ls, _ := first["last_seen"].(string); ls == "" {
		t.Fatalf("heartbeat missing last_seen: %v", first)
	}

	p.callErr(t, "heartbeat", map[string]any{"agent_id": "ghost"}, "agent_not_registered")
	p.callErr(t, "set_my_status", map[string]any{"agent_id": "ghost", "status": "idle"}, "agent_not_registered")
	p.callErr(t, "set_my_status", map[string]any{"agent_id": "h", "status": ""}, "invalid_argument")

	// Status must survive a process restart: it lives in registry.log, not
	// in the process.
	p.callObj(t, "set_my_status", map[string]any{"agent_id": "h", "status": "blocked: waiting on spec"})
	p.kill(t)
	p2 := startBus(t, root)
	who := p2.callObj(t, "whoami", map[string]any{"agent_id": "h"})
	if who["status"] != "blocked: waiting on spec" {
		t.Fatalf("status lost across restart: %v", who)
	}
}

// ---------------------------------------------------------------------------
// P4 — hardening (SPEC §17 P4)
// ---------------------------------------------------------------------------

// C15 — compact: the consumed prefix is archived verbatim, the live log is
// truncated in place, seq counters are unaffected, and it is idempotent
// (SPEC §16 Q4, §17 P4).
func TestC15_Compact(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)
	p.register(t, "a", "sender")
	p.register(t, "b", "receiver")

	out1 := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "prompt", "body": "msg 1"})
	p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "prompt", "body": "msg 2"})
	p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "msg 3"})

	out := p.callObj(t, "compact", map[string]any{"agent_id": "b", "up_to_seq": 2})
	if intField(t, out["archived"], "archived") != 2 {
		t.Fatalf("compact = %v, want archived 2", out)
	}

	// Live log retains only seq 3, and parses to exactly one record.
	live := readMailboxRecords(t, root, "b")
	if len(live) != 1 || live[0]["seq"] != "3" || live[0]["body"] != "msg 3" {
		t.Fatalf("live log after compact = %v, want single record seq 3", live)
	}

	// The cold archive holds the consumed prefix in order, verbatim.
	archivedPath := filepath.Join(root, "mailboxes", "b.log.archived")
	arch := parseMailboxLog(t, mustReadFile(t, archivedPath))
	if len(arch) != 2 || arch[0]["seq"] != "1" || arch[1]["seq"] != "2" {
		t.Fatalf("archive = %v, want records seq 1,2", arch)
	}
	if arch[0]["body"] != "msg 1" || arch[1]["body"] != "msg 2" {
		t.Fatalf("archive bodies = %q, %q", arch[0]["body"], arch[1]["body"])
	}

	// seq counters are UNaffected by compact: the next delivery continues
	// from seq 4 (SPEC §16 Q4).
	out4 := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "msg 4"})
	assertDelivered(t, out4, 4, [2]any{"b", 4})

	// Reads now return only the surviving records.
	recs := p.callList(t, "read_my_mailbox", map[string]any{"agent_id": "b", "since": 2})
	if len(recs) != 2 {
		t.Fatalf("read_my_mailbox since=2 returned %d records, want 2 (seq 3,4)", len(recs))
	}

	// Idempotent: compacting the same boundary again archives nothing.
	out2 := p.callObj(t, "compact", map[string]any{"agent_id": "b", "up_to_seq": 4})
	if intField(t, out2["archived"], "archived") != 2 {
		t.Fatalf("second compact = %v, want archived 2 (seq 3,4)", out2)
	}
	out3 := p.callObj(t, "compact", map[string]any{"agent_id": "b", "up_to_seq": 4})
	if intField(t, out3["archived"], "archived") != 0 {
		t.Fatalf("repeat compact = %v, want archived 0", out3)
	}

	// Errors: unknown agent, negative boundary.
	p.callErr(t, "compact", map[string]any{"agent_id": "ghost", "up_to_seq": 0}, "agent_not_registered")
	p.callErr(t, "compact", map[string]any{"agent_id": "b", "up_to_seq": -1}, "invalid_argument")

	// A reply to a compacted-away record still resolves through the cold
	// archive (SPEC §9 reply; §16 Q4).
	id1 := intField(t, out1["id"], "id")
	rep := p.callObj(t, "reply", map[string]any{"agent_id": "b", "in_reply_to": id1, "body": "done 1"})
	assertDelivered(t, rep, 5, [2]any{"a", 1})
}

// C16 — dedup survives a process restart via the derived index: index.db
// is rebuilt from the logs under the lock on every Open (SPEC §12, §17 P4,
// §16 Q8), so a kill between two processes cannot make a dedup_id
// "forget" its original.
func TestC16_RestartDedupViaIndex(t *testing.T) {
	root := t.TempDir()
	p := startBus(t, root)
	p.register(t, "a", "ra")
	p.register(t, "b", "rb")

	orig := p.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "k", "dedup_id": "K16"})
	origID := intField(t, orig["id"], "id")

	// P4: the derived index is on disk inside the root.
	if fi, err := os.Stat(filepath.Join(root, "index.db")); err != nil {
		t.Fatalf("index.db missing: %v", err)
	} else if fi.Size() == 0 {
		t.Fatalf("index.db is empty after a send")
	}

	// Kill the bus and restart: Open must rebuild index.db from the logs.
	p.kill(t)
	p2 := startBus(t, root)

	again := p2.send(t, map[string]any{"agent_id": "a", "to_agent": "b", "kind": "info", "body": "k", "dedup_id": "K16"})
	assertDelivered(t, again, origID, [2]any{"b", 1})
	if n := countDedup(t, root, "K16"); n != 1 {
		t.Fatalf("dedup_id K16 appears in %d records, want 1 (restart must not re-deliver)", n)
	}
}

// C17 — bus_root is 0700 (SPEC §17 P4: file ownership on bus_root as the
// shared-boundary gate; auth is moot over stdio, §14).
func TestC17_RootPermissions(t *testing.T) {
	// t.TempDir() is 0700 and would mask the check: use a 0755 root.
	root, err := os.MkdirTemp("", "mbus-p4-root-*")
	if err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatalf("chmod root 0755: %v", err)
	}

	p := startBus(t, root)
	_ = p

	fi, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat root: %v", err)
	}
	if perm := fi.Mode() & 0o777; perm != 0o700 {
		t.Fatalf("bus_root mode = %o, want 700", perm)
	}
}
