// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package consume

import (
	"testing"
	"time"
)

// Vectors from Kafka's UtilsTest.testMurmur2.
func TestMurmur2MatchesKafka(t *testing.T) {
	tests := map[string]int32{
		"21":                         -973932308,
		"foobar":                     -790332482,
		"a-little-bit-long-string":   -985981536,
		"a-little-bit-longer-string": -1486304829,
		"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8": -58897971,
		"abc": 479470107,
	}
	for in, want := range tests {
		if got := int32(murmur2([]byte(in))); got != want {
			t.Errorf("murmur2(%q) = %d, want %d", in, got, want)
		}
	}
}

func TestKeyPartitionInRange(t *testing.T) {
	for _, k := range []string{"a", "order-981", "", "zzz"} {
		if p := KeyPartition([]byte(k), 12); p < 0 || p >= 12 {
			t.Errorf("KeyPartition(%q) = %d", k, p)
		}
	}
}

func TestParsePosition(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	tests := map[string]Position{
		"":         {Kind: Latest},
		"latest":   {Kind: Latest},
		"earliest": {Kind: Earliest},
		"-5":       {Kind: LastN, N: 5},
		"@42":      {Kind: AtOffset, Offset: 42},
		"-1h":      {Kind: AtTime, Time: now.Add(-time.Hour)},
	}
	for in, want := range tests {
		got, err := ParsePosition(in, now)
		if err != nil || got.Kind != want.Kind || got.N != want.N || got.Offset != want.Offset || !got.Time.Equal(want.Time) {
			t.Errorf("%q: got %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"@x", "@-1", "soon"} {
		if _, err := ParsePosition(bad, now); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}
