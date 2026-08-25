package bus

import "time"

// mailboxWatcher is a platform hook (kqueue on darwin, inotify on linux,
// plain sleep elsewhere) that wakes the wait_for_message loop when the
// agent's mailbox file is written to or created. Because the watch targets
// files in the SHARED bus_root, a message appended by ANY of the N bus
// processes wakes this one (SPEC §8/§9: "kqueue/inotify on
// mailboxes/<agent_id>.log — which works across the N bus processes that
// share bus_root — falling back to backoff polling").
type mailboxWatcher interface {
	// wait blocks for up to d. It returns true if the target mailbox was
	// written or created in that window, false on timeout.
	wait(d time.Duration) (changed bool, err error)
	close()
}

// newMailboxWatcher watches the mailboxes directory (for mailbox creation)
// and, when it already exists, the mailbox file itself (for appends).
func newMailboxWatcher(mailboxesDir, mailboxPath string) (mailboxWatcher, error) {
	w, err := newPlatformWatcher(mailboxesDir, mailboxPath)
	if err != nil {
		return nil, err
	}
	return w, nil
}

// sleepWatcher is the pure backoff-poll fallback (SPEC §8): every chunk is
// a sleep; the wait loop re-checks the mailbox after each one.
type sleepWatcher struct{}

func newSleepWaiter() mailboxWatcher { return &sleepWatcher{} }

func (w *sleepWatcher) wait(d time.Duration) (bool, error) {
	time.Sleep(d)
	return false, nil
}

func (w *sleepWatcher) close() {}
