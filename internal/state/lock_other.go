//go:build !(linux || darwin || freebsd)

package state

import "os"

// lockFile is a no-op on platforms without flock.
func lockFile(f *os.File) error { return nil }
