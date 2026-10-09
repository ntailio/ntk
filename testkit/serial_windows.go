// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package testkit

import (
	"os"
	"sync"
	"testing"
)

var serial sync.Mutex

func Serial(t testing.TB) {
	serial.Lock()
	t.Cleanup(serial.Unlock)
}

func Disruptive(t testing.TB) {
	t.Helper()
	if os.Getenv("NTK_TEST_DISRUPTIVE") != "1" {
		t.Skip("disruptive test (stops brokers): set NTK_TEST_DISRUPTIVE=1")
	}
	Serial(t)
}
