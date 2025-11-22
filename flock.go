//go:build !windows

package lgdb

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// Locker is an interface for a file-based lock.
type Locker interface {
	// Lock acquires the lock, blocking until it's available.
	Lock() error
	// Unlock releases the lock.
	Unlock() error
}

// fileLocker implements the Locker interface using syscall.Flock.
type fileLocker struct {
	file *os.File
}

// newFileLocker creates a new file lock for the given database directory.
// The lock file will be named "LOCK" and placed inside the directory.
func newFileLocker(dir string) (Locker, error) {
	lockPath := filepath.Join(dir, "LOCK")

	// Open the lock file. Create it if it doesn't exist.
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file %s: %w", lockPath, err)
	}

	return &fileLocker{file: file}, nil
}

// Lock acquires an exclusive lock on the file descriptor.
// It will not block if the lock is unavailable.
func (l *fileLocker) Lock() error {
	// Attempt to acquire an exclusive lock (LOCK_EX) without blocking (LOCK_NB).
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err == syscall.EWOULDBLOCK {
		return ErrDBAlreadyOpen // Specific error for lock already held
	}
	if err != nil {
		return fmt.Errorf("failed to acquire file lock: %w", err)
	}
	return nil
}

// Unlock releases the file lock and closes the file.
func (l *fileLocker) Unlock() error {
	// Release the exclusive lock (LOCK_UN).
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("failed to release file lock: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("failed to close lock file: %w", err)
	}
	return nil
}
