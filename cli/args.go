// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/units"
)

func parseIDs(s string) ([]int32, error) {
	ids, err := units.ParseIDList(s)
	if err != nil {
		return nil, exitcode.With(exitcode.Usage, err)
	}
	return ids, nil
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
