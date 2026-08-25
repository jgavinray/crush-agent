//go:build windows

package bus

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/windows"
)

// Windows has no flock(2). The closest faithful approximation is a
// non-blocking CreateFile claim with FILE_FLAG_POSIX_SEMANTICS plus a
// retry loop: each process that holds the "lock" keeps state/lock open with
// a write share mode of zero, so any other process's exclusive open fails
// while the holder lives. The kernel releases the handle on process death,
// matching the flock semantics the rest of the bus relies on (SPEC §7, §12).
//
// This is a best-effort port; the canonical target for the reference
// implementation is Unix (SPEC §17).
func lockFile(path string) (*os.File, error) {
	deadline := time.Now().Add(5 * time.Minute)
	var lastErr error
	for {
		h, err := windows.CreateFile(path,
			windows.GENERIC_WRITE,
			0, // exclusive: no sharing
			nil,
			windows.CREATE_ALWAYS,
			windows.FILE_FLAG_POSIX_SEMANTICS,
			0)
		if err == nil {
			f := os.NewFile(uintptr(h), path)
			return f, nil
		}
		lastErr = err
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("lock %s: timed out: %w", path, lastErr)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// unlockFile closes the claim handle, releasing the lock.
func unlockFile(f *os.File) error {
	return f.Close()
}
