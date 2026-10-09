// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package units

import (
	"slices"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	tests := map[string]time.Duration{
		"7d": 7 * 24 * time.Hour, "1d12h": 36 * time.Hour, "-2h": -2 * time.Hour, "90s": 90 * time.Second,
		"1w": 7 * 24 * time.Hour, "1h30m": 90 * time.Minute, "500ms": 500 * time.Millisecond,
	}
	for in, want := range tests {
		got, err := ParseDuration(in)
		if err != nil || got != want {
			t.Errorf("%s: got %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"", "d", "7x", "abc"} {
		if _, err := ParseDuration(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestParseSize(t *testing.T) {
	tests := map[string]int64{"1024": 1024, "10GiB": 10 << 30, "2MiB": 2 << 20, "1.5GB": 1_500_000_000, "4mib": 4 << 20}
	for in, want := range tests {
		got, err := ParseSize(in)
		if err != nil || got != want {
			t.Errorf("%s: got %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := ParseSize("10XB"); err == nil {
		t.Error("expected an error")
	}
	if r, _ := ParseRate("10MiB/s"); r != 10<<20 {
		t.Errorf("rate: %d", r)
	}
}

func TestParseTime(t *testing.T) {
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
	if got, _ := ParseTime("-2h", now); !got.Equal(now.Add(-2 * time.Hour)) {
		t.Errorf("relative: %v", got)
	}
	if got, _ := ParseTime("2026-09-30T08:00:00Z", now); !got.Equal(time.Date(2026, 9, 30, 8, 0, 0, 0, time.UTC)) {
		t.Errorf("rfc3339: %v", got)
	}
	if got, _ := ParseTime("1790000000000", now); got.UnixMilli() != 1790000000000 {
		t.Errorf("epoch: %v", got)
	}
}

func TestFormat(t *testing.T) {
	if Bytes(2048) != "2.0 KiB" || Bytes(5) != "5 B" || Bytes(3<<30) != "3.0 GiB" {
		t.Errorf("bytes: %s %s %s", Bytes(2048), Bytes(5), Bytes(3<<30))
	}
	if Duration(7*24*time.Hour) != "1w" || Duration(48*time.Hour) != "2d" || Duration(-1) != "∞" {
		t.Errorf("duration: %s %s", Duration(7*24*time.Hour), Duration(48*time.Hour))
	}
	if Count(1234567) != "1,234,567" || Count(-1204) != "-1,204" || Count(12) != "12" {
		t.Errorf("count: %s %s", Count(1234567), Count(-1204))
	}
}

func TestParseIDList(t *testing.T) {
	for in, want := range map[string][]int32{
		"":          nil,
		"3":         {3},
		"5,0,3-4,4": {0, 3, 4, 5},
		"0, 3-5":    {0, 3, 4, 5},
		"3-5":       {3, 4, 5},
		"7-7":       {7},
	} {
		got, err := ParseIDList(in)
		if err != nil || !slices.Equal(got, want) {
			t.Errorf("ParseIDList(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, bad := range []string{"x", "3-", "5-3", "1,,2", "-1", "a-b"} {
		if _, err := ParseIDList(bad); err == nil {
			t.Errorf("ParseIDList(%q): expected an error", bad)
		}
	}
}
