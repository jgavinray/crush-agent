package bus

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Record is one mailbox message in the canonical on-disk format defined in
// SPEC §5:
//
//	record      := header "---\n" body "\n\n"
//	header      := header-line { header-line }
//	header-line := field ":" SP value "\n"
//
// The body is exactly `len` bytes; `len` is authoritative, so a body may
// contain blank lines, a line equal to "---", or (in principle) any bytes.
// In practice the MCP/JSON transport carries bodies as UTF-8 strings, but
// the parser does not require UTF-8.
type Record struct {
	Seq       int
	ID        int
	From      string
	FromRole  string
	To        string
	Kind      string
	InReplyTo *int
	TS        time.Time
	DedupID   *string
	Body      []byte
}

// recordFields is the fixed header field order written by Encode.
var recordFields = []string{"seq", "id", "from", "from_role", "to", "kind", "in_reply_to", "ts", "dedup_id", "len"}

// Encode renders the record in canonical form. Header string values are
// single-line by construction (validated at write time).
func (r Record) Encode() []byte {
	var b bytes.Buffer
	fmt.Fprintf(&b, "seq: %d\n", r.Seq)
	fmt.Fprintf(&b, "id: %d\n", r.ID)
	b.WriteString("from: " + r.From + "\n")
	b.WriteString("from_role: " + r.FromRole + "\n")
	b.WriteString("to: " + r.To + "\n")
	b.WriteString("kind: " + r.Kind + "\n")
	if r.InReplyTo != nil {
		fmt.Fprintf(&b, "in_reply_to: %d\n", *r.InReplyTo)
	} else {
		b.WriteString("in_reply_to: -\n")
	}
	b.WriteString("ts: " + r.TS.UTC().Format(time.RFC3339) + "\n")
	if r.DedupID != nil && *r.DedupID != "" {
		b.WriteString("dedup_id: " + *r.DedupID + "\n")
	} else {
		b.WriteString("dedup_id: -\n")
	}
	fmt.Fprintf(&b, "len: %d\n", len(r.Body))
	b.WriteString("---\n")
	b.Write(r.Body)
	b.WriteString("\n\n")
	return b.Bytes()
}

// ParseRecords parses all complete records in data, in file order. It
// returns the records, the number of bytes they consumed, and whether data
// ends exactly on a record boundary (tailComplete). A truncated trailing
// record (the power-loss window of SPEC §12) yields tailComplete=false and
// is not an error; a malformed record anywhere else is.
func ParseRecords(data []byte) (records []Record, consumed int, tailComplete bool, err error) {
	pos := 0
	for pos < len(data) {
		rec, next, complete, perr := parseOneRecord(data, pos)
		if perr != nil {
			return nil, 0, false, perr
		}
		if !complete {
			return records, pos, false, nil
		}
		records = append(records, rec)
		pos = next
	}
	return records, pos, true, nil
}

// parseOneRecord parses the single record starting at data[pos].
func parseOneRecord(data []byte, pos int) (rec Record, next int, complete bool, err error) {
	rest := data[pos:]
	// Locate the standalone "---" line that terminates the header. The
	// header precedes the body, and a body may itself contain a "---" line,
	// so the first standalone "---" line from the record start is the
	// separator (SPEC §5).
	i := 0
	for i < len(rest) {
		lineEnd := bytes.IndexByte(rest[i:], '\n')
		if lineEnd < 0 {
			break
		}
		if string(rest[i:i+lineEnd]) == "---" {
			break
		}
		i += lineEnd + 1
	}
	if i+4 > len(rest) || string(rest[i:i+4]) != "---\n" {
		return rec, pos, false, nil // header (or separator newline) not written yet
	}
	header := rest[:i]
	fields := make(map[string]string, len(recordFields))
	// header ends with the "\n" that closes its last header-line (SPEC §5:
	// header-line := field ":" SP value "\n"), so the split yields one
	// trailing empty element that is not a header line: stop there.
	for _, line := range strings.Split(string(header), "\n") {
		if line == "" {
			break
		}
		key, value, ok := strings.Cut(line, ": ")
		if !ok || key == "" {
			return rec, pos, false, fmt.Errorf("malformed record header line %q at byte offset %d", line, pos)
		}
		fields[key] = value
	}
	for _, f := range recordFields {
		if _, ok := fields[f]; !ok {
			return rec, pos, false, fmt.Errorf("record at byte offset %d missing header field %q", pos, f)
		}
	}
	seq, aerr := strconv.Atoi(fields["seq"])
	if aerr != nil {
		return rec, pos, false, fmt.Errorf("record at byte offset %d: bad seq %q", pos, fields["seq"])
	}
	id, aerr := strconv.Atoi(fields["id"])
	if aerr != nil {
		return rec, pos, false, fmt.Errorf("record at byte offset %d: bad id %q", pos, fields["id"])
	}
	length, aerr := strconv.Atoi(fields["len"])
	if aerr != nil || length < 0 {
		return rec, pos, false, fmt.Errorf("record at byte offset %d: bad len %q", pos, fields["len"])
	}
	bodyStart := i + len("---\n")
	if bodyStart+length+2 > len(rest) {
		return rec, pos, false, nil // body or trailing blank line not fully written yet
	}
	if string(rest[bodyStart+length:bodyStart+length+2]) != "\n\n" {
		return rec, pos, false, fmt.Errorf("record at byte offset %d: missing trailing blank line", pos)
	}
	ts, terr := time.Parse(time.RFC3339, fields["ts"])
	if terr != nil {
		return rec, pos, false, fmt.Errorf("record at byte offset %d: bad ts %q", pos, fields["ts"])
	}
	rec = Record{
		Seq:      seq,
		ID:       id,
		From:     fields["from"],
		FromRole: fields["from_role"],
		To:       fields["to"],
		Kind:     fields["kind"],
		TS:       ts,
		Body:     append([]byte(nil), rest[bodyStart:bodyStart+length]...),
	}
	if v := fields["in_reply_to"]; v != "-" {
		n, aerr := strconv.Atoi(v)
		if aerr != nil {
			return rec, pos, false, fmt.Errorf("record at byte offset %d: bad in_reply_to %q", pos, v)
		}
		rec.InReplyTo = &n
	}
	if v := fields["dedup_id"]; v != "-" {
		rec.DedupID = &v
	}
	return rec, pos + bodyStart + length + 2, true, nil
}

// RecordView is the JSON shape of a record as returned to MCP callers
// (SPEC §9: read_my_mailbox / wait_for_message results).
type RecordView struct {
	ID        int     `json:"id"`
	Seq       int     `json:"seq"`
	From      string  `json:"from"`
	FromRole  string  `json:"from_role"`
	To        string  `json:"to"`
	Kind      string  `json:"kind"`
	InReplyTo *int    `json:"in_reply_to"`
	TS        string  `json:"ts"`
	DedupID   *string `json:"dedup_id"`
	Body      string  `json:"body"`
}

// View renders the record for JSON transport.
func (r Record) View() RecordView {
	return RecordView{
		ID:        r.ID,
		Seq:       r.Seq,
		From:      r.From,
		FromRole:  r.FromRole,
		To:        r.To,
		Kind:      r.Kind,
		InReplyTo: r.InReplyTo,
		TS:        r.TS.UTC().Format(time.RFC3339),
		DedupID:   r.DedupID,
		Body:      string(r.Body),
	}
}
