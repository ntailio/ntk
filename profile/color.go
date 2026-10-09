// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"regexp"
	"slices"
	"strings"
)

// Palette holds the colours new profiles get. The most distinct ones come first.
var Palette = []string{
	"#f87171", "#fb923c", "#fbbf24", "#facc15", "#a3e635", "#4ade80", "#34d399", "#2dd4bf", "#22d3ee",
	"#38bdf8", "#60a5fa", "#818cf8", "#a78bfa", "#c084fc", "#e879f9", "#f472b6", "#fb7185",
	"#fca5a5", "#fdba74", "#fde047", "#bef264", "#86efac", "#5eead4", "#67e8f9", "#7dd3fc",
	"#93c5fd", "#a5b4fc", "#c4b5fd", "#f0abfc", "#f9a8d4",
}

// NamedColors are the colour names a profile may use instead of a hex value.
var NamedColors = []string{"red", "green", "yellow", "blue", "magenta", "cyan"}

var hexColor = regexp.MustCompile(`^#[0-9a-fA-F]{6}$`)

func ValidColor(c string) bool {
	return hexColor.MatchString(c) || slices.Contains(NamedColors, c)
}

// NextColor returns the palette colour the fewest profiles use, the first one on a tie.
func (f *File) NextColor() string {
	used := map[string]int{}
	for _, p := range f.Profiles {
		used[strings.ToLower(p.Color)]++
	}
	best := Palette[0]
	for _, c := range Palette {
		if used[c] < used[best] {
			best = c
		}
	}
	return best
}
