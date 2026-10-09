// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package testkit

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// Serial holds a cross-process lock for the rest of the test, for tests that
// change cluster-wide state (broker configs, <default> quotas, health).
func Serial(t testing.TB) {
	t.Helper()
	f, err := os.OpenFile(filepath.Join(os.TempDir(), "ntk-test-serial.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
	})
}

// Disruptive skips unless NTK_TEST_DISRUPTIVE=1, then runs the test serially.
func Disruptive(t testing.TB) {
	t.Helper()
	if os.Getenv("NTK_TEST_DISRUPTIVE") != "1" {
		t.Skip("disruptive test (stops brokers): set NTK_TEST_DISRUPTIVE=1")
	}
	Serial(t)
}
