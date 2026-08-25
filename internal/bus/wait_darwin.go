//go:build darwin

package bus

import (
	"fmt"
	"time"

	"golang.org/x/sys/unix"
)

// notone is kevent(2)'s Kevflag NOTONE: the filter entry is disabled after
// it is reported once. x/sys/unix does not export the constant.
const notone = 0x0001

// darwinWatcher watches the mailboxes directory (mailbox creation) and, when
// the mailbox file exists, the file itself (appends) via EVFILT_VNODE. Both
// entries are one-shots, re-armed on every wait, so the watcher works across
// many 100ms chunks. NOTE_EXTEND on the file covers O_APPEND writes from any
// bus process; NOTE_WRITE on the directory covers creation of a mailbox
// that did not exist yet.
type darwinWatcher struct {
	kq     int
	dirFd  int
	fileFd int // -1 until the mailbox file exists
	path   string
}

func newPlatformWatcher(mailboxesDir, mailboxPath string) (mailboxWatcher, error) {
	kq, err := unix.Kqueue()
	if err != nil {
		return nil, fmt.Errorf("kqueue: %w", err)
	}
	dirFd, err := unix.Open(mailboxesDir, unix.O_RDONLY, 0)
	if err != nil {
		unix.Close(kq)
		return nil, fmt.Errorf("open mailboxes dir: %w", err)
	}
	w := &darwinWatcher{kq: kq, dirFd: dirFd, fileFd: -1, path: mailboxPath}
	if err := w.arm(); err != nil {
		w.close()
		return nil, err
	}
	return w, nil
}

// arm (re)registers the one-shot vnode watches.
func (w *darwinWatcher) arm() error {
	var changes []unix.Kevent_t
	changes = append(changes, unix.Kevent_t{
		Ident:  uint64(w.dirFd),
		Filter: int16(unix.EVFILT_VNODE),
		Flags:  notone,
		Data:   int64(unix.NOTE_WRITE | unix.NOTE_DELETE),
	})
	if _, err := unix.Kevent(w.kq, changes, nil, nil); err != nil {
		return fmt.Errorf("kqueue dir watch: %w", err)
	}
	if w.fileFd >= 0 {
		changes = []unix.Kevent_t{{
			Ident:  uint64(w.fileFd),
			Filter: int16(unix.EVFILT_VNODE),
			Flags:  notone,
			Data:   int64(unix.NOTE_WRITE | unix.NOTE_EXTEND),
		}}
		if _, err := unix.Kevent(w.kq, changes, nil, nil); err != nil {
			return fmt.Errorf("kqueue file watch: %w", err)
		}
	} else if fd, err := unix.Open(w.path, unix.O_RDONLY, 0); err == nil {
		// The mailbox appeared since the last arm: watch it directly too.
		w.fileFd = fd
		changes = []unix.Kevent_t{{
			Ident:  uint64(fd),
			Filter: int16(unix.EVFILT_VNODE),
			Flags:  notone,
			Data:   int64(unix.NOTE_WRITE | unix.NOTE_EXTEND),
		}}
		if _, err := unix.Kevent(w.kq, changes, nil, nil); err != nil {
			return fmt.Errorf("kqueue file watch: %w", err)
		}
	}
	return nil
}

// wait blocks on Kevent until an event or d elapses.
func (w *darwinWatcher) wait(d time.Duration) (bool, error) {
	if err := w.arm(); err != nil {
		return false, err
	}
	ts := unix.NsecToTimespec(d.Nanoseconds())
	n, err := unix.Kevent(w.kq, nil, make([]unix.Kevent_t, 8), &ts)
	if err != nil {
		return false, fmt.Errorf("kevent: %w", err)
	}
	return n > 0, nil
}

func (w *darwinWatcher) close() {
	if w.fileFd >= 0 {
		unix.Close(w.fileFd)
		w.fileFd = -1
	}
	if w.dirFd >= 0 {
		unix.Close(w.dirFd)
		w.dirFd = -1
	}
	if w.kq >= 0 {
		unix.Close(w.kq)
		w.kq = -1
	}
}
