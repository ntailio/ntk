// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"strings"

	"charm.land/lipgloss/v2"
)

var (
	styleTitle  = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	styleDim    = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleKey    = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	styleAccent = lipgloss.NewStyle().Foreground(lipgloss.Color("5"))
	styleWarn   = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("3"))
	styleError  = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	styleOK     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	styleTab    = lipgloss.NewStyle().Padding(0, 1)
	styleTabOn  = lipgloss.NewStyle().Padding(0, 1).Bold(true).Foreground(lipgloss.Color("0")).Background(lipgloss.Color("6"))
)

var badgeColors = map[string]string{
	"red": "1", "green": "2", "yellow": "3", "blue": "4", "magenta": "5", "cyan": "6",
}

func styleBadge(c string) lipgloss.Style {
	s := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("0"))
	if code, ok := badgeColors[c]; ok {
		return s.Background(lipgloss.Color(code))
	}
	if strings.HasPrefix(c, "#") {
		return s.Background(lipgloss.Color(c))
	}
	return s.Background(lipgloss.Color("7"))
}

var sparkChars = []rune("▁▂▃▄▅▆▇█")

func spark[T int64 | float64](v []T, width int) string {
	if len(v) > width {
		v = v[len(v)-width:]
	}
	if len(v) == 0 {
		return ""
	}
	lo, hi := v[0], v[0]
	for _, x := range v {
		lo, hi = min(lo, x), max(hi, x)
	}
	var b strings.Builder
	for _, x := range v {
		i := 0
		if hi > lo {
			i = int(float64(x-lo) / float64(hi-lo) * float64(len(sparkChars)-1))
		}
		b.WriteRune(sparkChars[i])
	}
	return b.String()
}

func bar(pct float64, width int) string {
	if pct < 0 {
		return strings.Repeat("·", width)
	}
	n := int(pct / 100 * float64(width))
	n = min(max(n, 0), width)
	return strings.Repeat("▇", n) + strings.Repeat("▁", width-n)
}

// applyTheme overrides colors from config.json tui.colors (names: title, key, accent, warn, error, ok, dim).
func applyTheme(colors map[string]string) {
	for name, c := range colors {
		col := lipgloss.Color(c)
		switch name {
		case "title":
			styleTitle = styleTitle.Foreground(col)
		case "key":
			styleKey = styleKey.Foreground(col)
		case "accent":
			styleAccent = styleAccent.Foreground(col)
		case "warn":
			styleWarn = styleWarn.Foreground(col)
		case "error":
			styleError = styleError.Foreground(col)
		case "ok":
			styleOK = styleOK.Foreground(col)
		case "dim":
			styleDim = styleDim.Foreground(col)
		}
	}
}
