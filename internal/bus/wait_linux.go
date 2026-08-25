//go:build linux

package bus

import (
	"fmt"
	"os"
	"time"

	"golang.org/x/sys/unix"
)

// linuxWatcher watches the mailboxes directory (mailbox creation) and, when
// the mailbox file exists, the file itself (appends), via inotify. Unlike
// the darwin one-shots, inotify watches persist, so arm() only adds the
// file watch once the file exists.
type linuxWatcher struct {
	fd     int
	dirWD  int
	fileWD int
	path   string
}

func newPlatformWatcher(mailboxesDir, mailboxPath string) (mailboxWatcher, error) {
	fd, err := unix.InotifyInit1(0)
	if err != nil {
		return nil, fmt.Errorf("inotify_init1: %w", err)
	}
	w := &linuxWatcher{fd: fd, dirWD: -1, fileWD: -1, path: mailboxPath}
	if wd, err := unix.InotifyAddWatch(fd, mailboxesDir,
		uint32(unix.IN_CREATE|unix.IN_MODIFY|unix.IN_MOVED_TO)); err != nil {
		w.close()
		return nil, fmt.Errorf("inotify watch dir: %w", err)
	} else {
		w.dirWD = wd
	}
	if err := w.armFile(); err != nil {
		w.close()
		return nil, err
	}
	return w, nil
}

// armFile adds the file watch if the mailbox exists and is not yet watched.
func (w *linuxWatcher) armFile() error {
	if w.fileWD >= 0 {
		return nil
	}
	if _, err := os.Stat(w.path); err != nil {
		return nil // no mailbox yet; the directory watch catches its creation
	}
	wd, err := unix.InotifyAddWatch(w.fd, w.path, uint32(unix.IN_MODIFY))
	if err != nil {
		return fmt.Errorf("inotify watch file: %w", err)
	}
	w.fileWD = wd
	return nil
}

// wait blocks until an inotify event or d elapses (unix.Poll bounds the
// read), then re-checks.
func (w *linuxWatcher) wait(d time.Duration) (bool, error) {
	if err := w.armFile(); err != nil {
		return false, err
	}
	_, err := unix.Poll([]unix.PollFd{{Fd: int32(w.fd), Events: unix.POLLIN}}, int(d.Milliseconds()))
	if err != nil {
		return false, fmt.Errorf("poll: %w", err)
	}
	// Consume pending events so the next Poll blocks fresh.
	buf := make([]byte, 64*1024)
	n, rerr := unix.Read(w.fd, buf)
	_ = rerr // EAGAIN is fine: we just need to clear what is there
	_ = n
	return true, nil
}

func (w *linuxWatcher) close() {
	if w.fd >= 0 {
		unix.Close(w.fd)
		w.fd = -1
	}
}
