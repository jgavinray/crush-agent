//go:build !windows

package bus

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// lockFile acquires the canonical-log lock (SPEC §7 "The canonical-log
// lock"): a global advisory flock on state/lock. It is acquired per
// open-file-description, so it serializes other bus PROCESSES as well as
// concurrent tool calls within this process. The kernel releases the lock
// on process death (SIGKILL included), which is what lets a restarted bus
// proceed after a crash (SPEC §12).
func lockFile(path string) (*os.File, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX); err != nil {
		f.Close()
		return nil, fmt.Errorf("flock: %w", err)
	}
	return f, nil
}

// unlockFile releases the canonical-log lock and closes the descriptor.
func unlockFile(f *os.File) error {
	if err := unix.Flock(int(f.Fd()), unix.LOCK_UN); err != nil {
		f.Close()
		return fmt.Errorf("flock unlock: %w", err)
	}
	return f.Close()
}
