//go:build !darwin && !linux

package bus

import "time"

// newPlatformWatcher is the polling fallback for platforms without
// kqueue/inotify (SPEC §8 "falling back to backoff polling"): each chunk is
// a sleep, and the wait loop re-checks the mailbox after every chunk.
func newPlatformWatcher(_, _ string) (mailboxWatcher, error) {
	return newSleepWaiter(), nil
}

var _ = time.Second
