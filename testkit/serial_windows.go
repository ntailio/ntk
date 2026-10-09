// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package testkit

import (
	"sync"
	"testing"
)

var serial sync.Mutex

func Serial(t testing.TB) {
	serial.Lock()
	t.Cleanup(serial.Unlock)
}
