// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package profile

import (
	"os"
	"path/filepath"
	"syscall"
)

// FixMode sets the file to 0600, and its directory to 0700 if the current user owns it.
func FixMode(path string) error {
	if err := os.Chmod(path, 0o600); err != nil {
		return err
	}
	dir := filepath.Dir(path)
	fi, err := os.Stat(dir)
	if err != nil {
		return nil
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) == os.Getuid() && fi.Mode().Perm()&0o077 != 0 {
		return os.Chmod(dir, 0o700)
	}
	return nil
}
