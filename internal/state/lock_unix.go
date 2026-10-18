//go:build linux || darwin || freebsd

package state

import (
	"os"
	"syscall"
)

// lockFile takes an exclusive, non-blocking advisory lock.
func lockFile(f *os.File) error {
	return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
}
