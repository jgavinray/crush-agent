package bus

import (
	"encoding/json"
	"os"
)

// jsonMarshalIndent renders a value as 2-space-indented JSON for the
// derived snapshot files (SPEC §6: registry/<agent_id>.json).
func jsonMarshalIndent(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}

// syncFile fsyncs f when Fsync is enabled (P4, SPEC §17: fsync-before-return
// closes the power-loss window of invariant IV). In P1 the bus is
// page-cache-durable (SPEC §12), so this is a no-op and file data is flushed
// to the kernel on close.
func (b *Bus) syncFile(f *os.File) error {
	if !b.opts.Fsync {
		return nil
	}
	return f.Sync()
}

// syncDirFor fsyncs the directory containing path when Fsync is enabled, so
// a newly created file's dirent is durable too (P4 hardening; P1 no-op).
func (b *Bus) syncDirFor(path string) error {
	if !b.opts.Fsync {
		return nil
	}
	d, err := os.Open(dirOf(path))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// dirOf is a small local helper to avoid an import cycle with filepath users.
func dirOf(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' || path[i] == '\\' {
			return path[:i]
		}
	}
	return "."
}
