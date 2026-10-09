// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package units

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ParseDuration extends time.ParseDuration with d (days) and w (weeks), e.g. "7d", "1d12h", "-2h".
func ParseDuration(s string) (time.Duration, error) {
	orig := s
	if s == "" {
		return 0, errors.New("empty duration")
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(strings.TrimPrefix(s, "-"), "+")
	var total time.Duration
	for s != "" {
		i := strings.IndexFunc(s, func(r rune) bool { return !unicode.IsDigit(r) && r != '.' })
		if i <= 0 {
			return 0, fmt.Errorf("invalid duration %q", orig)
		}
		j := strings.IndexFunc(s[i:], func(r rune) bool { return unicode.IsDigit(r) || r == '.' })
		if j < 0 {
			j = len(s) - i
		}
		num, unit := s[:i], s[i:i+j]
		s = s[i+j:]
		switch unit {
		case "d", "w":
			n, err := strconv.ParseFloat(num, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q", orig)
			}
			mult := 24 * time.Hour
			if unit == "w" {
				mult *= 7
			}
			total += time.Duration(n * float64(mult))
		default:
			d, err := time.ParseDuration(num + unit)
			if err != nil {
				return 0, fmt.Errorf("invalid duration %q", orig)
			}
			total += d
		}
	}
	if neg {
		total = -total
	}
	return total, nil
}

var sizeUnits = map[string]int64{
	"": 1, "b": 1,
	"k": 1000, "kb": 1000, "m": 1000 * 1000, "mb": 1000 * 1000, "g": 1e9, "gb": 1e9, "t": 1e12, "tb": 1e12,
	"ki": 1 << 10, "kib": 1 << 10, "mi": 1 << 20, "mib": 1 << 20, "gi": 1 << 30, "gib": 1 << 30,
	"ti": 1 << 40, "tib": 1 << 40,
}

// ParseSize parses byte sizes like "1024", "10GiB", "2MiB", "1.5GB".
func ParseSize(s string) (int64, error) {
	t := strings.TrimSpace(s)
	i := strings.IndexFunc(t, func(r rune) bool { return !unicode.IsDigit(r) && r != '.' && r != '-' })
	num, unit := t, ""
	if i >= 0 {
		num, unit = t[:i], strings.ToLower(strings.TrimSpace(t[i:]))
	}
	mult, ok := sizeUnits[unit]
	if !ok || num == "" {
		return 0, fmt.Errorf("invalid size %q (use e.g. 1024, 10MiB, 2GiB)", s)
	}
	n, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return int64(math.Round(n * float64(mult))), nil
}

// ParseRate parses byte rates like "10MiB/s" or "10MiB".
func ParseRate(s string) (int64, error) { return ParseSize(strings.TrimSuffix(s, "/s")) }

// ParseTime parses RFC 3339, a relative time ("-2h", "-1d"), or epoch milliseconds.
func ParseTime(s string, now time.Time) (time.Time, error) {
	if strings.HasPrefix(s, "-") || strings.HasPrefix(s, "+") {
		d, err := ParseDuration(s)
		if err != nil {
			return time.Time{}, err
		}
		return now.Add(d), nil
	}
	if ms, err := strconv.ParseInt(s, 10, 64); err == nil {
		return time.UnixMilli(ms), nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02 15:04:05", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
			return t, nil
		}
	}
	return time.Time{}, errors.New("invalid time " + strconv.Quote(s) + " (use RFC 3339, -2h, or epoch ms)")
}

func Bytes(n int64) string {
	if n < 0 {
		return "-"
	}
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit && exp < 4; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTP"[exp])
}

func Duration(d time.Duration) string {
	if d < 0 {
		return "∞"
	}
	switch {
	case d == 0:
		return "0s"
	case d%(7*24*time.Hour) == 0:
		return fmt.Sprintf("%dw", d/(7*24*time.Hour))
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", d/(24*time.Hour))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", d/time.Hour)
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", d/time.Minute)
	case d%time.Second == 0:
		return fmt.Sprintf("%ds", d/time.Second)
	}
	return d.String()
}

func Count(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}

func Rate(perSec float64) string {
	switch {
	case perSec >= 1e6:
		return fmt.Sprintf("%.1fM", perSec/1e6)
	case perSec >= 1e3:
		return fmt.Sprintf("%.1fk", perSec/1e3)
	case perSec >= 10 || perSec == 0:
		return fmt.Sprintf("%.0f", perSec)
	}
	return fmt.Sprintf("%.1f", perSec)
}

func Ago(t time.Time, now time.Time) string {
	if t.IsZero() {
		return "-"
	}
	d := now.Sub(t).Round(time.Second)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh", int(d.Hours()))
	}
	return fmt.Sprintf("%dd", int(d.Hours()/24))
}

// Plural formats n with Count and the word, adding "s" unless n is 1.
func Plural(n int64, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return Count(n) + " " + word + "s"
}

// ParseIDList parses an id list like "0,3-5" into [0 3 4 5], sorted and without duplicates.
func ParseIDList(s string) ([]int32, error) {
	if s == "" {
		return nil, nil
	}
	var out []int32
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		lo, hi, isRange := strings.Cut(part, "-")
		a, err := strconv.Atoi(lo)
		if err != nil || a < 0 {
			return nil, fmt.Errorf("invalid id list %q", s)
		}
		b := a
		if isRange {
			if b, err = strconv.Atoi(hi); err != nil || b < a {
				return nil, fmt.Errorf("invalid range %q", part)
			}
		}
		for i := a; i <= b; i++ {
			out = append(out, int32(i))
		}
	}
	slices.Sort(out)
	return slices.Compact(out), nil
}
