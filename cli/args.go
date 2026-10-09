// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/units"
)

// parseIDs parses "0,3-5" into [0 3 4 5].
func parseIDs(s string) ([]int32, error) {
	if s == "" {
		return nil, nil
	}
	var out []int32
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil {
			return nil, exitcode.With(exitcode.Usage, fmt.Errorf("invalid id list %q", s))
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil || b < a {
				return nil, exitcode.With(exitcode.Usage, fmt.Errorf("invalid range %q", part))
			}
		}
		for i := a; i <= b; i++ {
			out = append(out, int32(i))
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}

// parseTopicPartitions parses "orders:0,3-5" into ("orders", [0 3 4 5]).
func parseTopicPartitions(s string) (string, []int32, error) {
	topic, parts, ok := strings.Cut(s, ":")
	if !ok {
		return s, nil, nil
	}
	ids, err := parseIDs(parts)
	return topic, ids, err
}

func parseTime(s string) (time.Time, error) {
	t, err := units.ParseTime(s, time.Now())
	if err != nil {
		return time.Time{}, exitcode.With(exitcode.Usage, err)
	}
	return t, nil
}

func usageErr(format string, args ...any) error {
	return exitcode.With(exitcode.Usage, fmt.Errorf(format, args...))
}

func joinInts(v []int32) string {
	if len(v) == 0 {
		return "-"
	}
	s := make([]string, len(v))
	for i, n := range v {
		s[i] = strconv.Itoa(int(n))
	}
	return strings.Join(s, ",")
}

func itoa[T ~int | ~int16 | ~int32 | ~int64](n T) string { return strconv.FormatInt(int64(n), 10) }
