// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"cmp"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

type row struct {
	cells []string
	raw   any
	id    string // unique key when the first cell is not unique
}

func (r row) key() string {
	if r.id != "" {
		return r.id
	}
	if len(r.cells) == 0 {
		return ""
	}
	return r.cells[0]
}

type tableView struct {
	table    table.Model
	headers  []string
	all      []row
	shown    []row
	filter   string
	width    int
	height   int
	err      error
	loaded   bool
	sortCol  int
	sortDesc bool
	sorted   bool
	wide     bool
	wideFrom int
	multi    bool
	noMarks  bool
	selected map[string]bool
	prev     map[string]string
	changed  map[string]bool
}

func newTableView() tableView {
	st := table.DefaultStyles()
	st.Header = st.Header.Bold(true).Foreground(lipgloss.Color("6")).
		BorderStyle(lipgloss.NormalBorder()).BorderBottom(true).BorderForeground(lipgloss.Color("8"))
	st.Selected = st.Selected.Bold(true).Foreground(lipgloss.Color("0")).Background(lipgloss.Color("6"))
	return tableView{table: table.New(table.WithStyles(st), table.WithFocused(true)), selected: map[string]bool{}}
}

func (t *tableView) setRows(headers []string, rows []row) {
	changed := map[string]bool{}
	next := map[string]string{}
	for _, r := range rows {
		k, v := r.key(), strings.Join(r.cells, "\x00")
		next[k] = v
		if old, ok := t.prev[k]; ok && old != v {
			changed[k] = true
		}
	}
	if t.prev != nil && !t.noMarks {
		t.changed = changed
	}
	t.prev = next
	t.headers, t.all, t.loaded, t.err = headers, rows, true, nil
	t.refill()
}

func (t *tableView) setErr(err error) { t.err, t.loaded = err, true }

func (t *tableView) matcher() func(row) bool {
	f := t.filter
	if f == "" {
		return func(row) bool { return true }
	}
	if rest, ok := strings.CutPrefix(f, "re:"); ok {
		re, err := regexp.Compile(rest)
		if err != nil {
			return func(row) bool { return true }
		}
		return func(r row) bool {
			for _, c := range r.cells {
				if re.MatchString(c) {
					return true
				}
			}
			return false
		}
	}
	f = strings.ToLower(f)
	return func(r row) bool {
		for _, c := range r.cells {
			if strings.Contains(strings.ToLower(c), f) {
				return true
			}
		}
		return false
	}
}

func numeric(s string) (float64, bool) {
	s = strings.TrimSpace(strings.ReplaceAll(s, ",", ""))
	for _, suf := range []string{" B", " KiB", " MiB", " GiB", " TiB"} {
		if n, ok := strings.CutSuffix(s, suf); ok {
			f, err := strconv.ParseFloat(n, 64)
			mult := map[string]float64{" B": 1, " KiB": 1 << 10, " MiB": 1 << 20, " GiB": 1 << 30, " TiB": 1 << 40}[suf]
			return f * mult, err == nil
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil
}

func (t *tableView) refill() {
	match := t.matcher()
	t.shown = t.shown[:0]
	for _, r := range t.all {
		if match(r) {
			t.shown = append(t.shown, r)
		}
	}
	if t.sorted && t.sortCol < len(t.headers) {
		c := t.sortCol
		slices.SortStableFunc(t.shown, func(a, b row) int {
			var x int
			av, bv := cell(a, c), cell(b, c)
			if fa, ok := numeric(av); ok {
				if fb, ok := numeric(bv); ok {
					x = cmp.Compare(fa, fb)
				}
			}
			if x == 0 {
				x = strings.Compare(av, bv)
			}
			if t.sortDesc {
				return -x
			}
			return x
		})
	}
	headers := t.headers
	ncols := len(headers)
	if !t.wide && t.wideFrom > 0 && t.wideFrom < ncols {
		ncols = t.wideFrom
	}
	display := make([][]string, len(t.shown))
	for i, r := range t.shown {
		cells := append([]string(nil), r.cells[:min(ncols, len(r.cells))]...)
		for len(cells) < ncols {
			cells = append(cells, "")
		}
		mark := " "
		switch {
		case t.selected[r.key()]:
			mark = "✓"
		case t.changed[r.key()]:
			mark = "•"
		}
		display[i] = append([]string{mark}, cells...)
	}
	hs := append([]string{""}, headers[:ncols]...)
	if t.sorted && t.sortCol < ncols {
		arrow := " ▲"
		if t.sortDesc {
			arrow = " ▼"
		}
		hs[t.sortCol+1] += arrow
	}
	fillTable(&t.table, hs, display, t.width)
}

func cell(r row, i int) string {
	if i < len(r.cells) {
		return r.cells[i]
	}
	return ""
}

// fillTable sizes columns to their content, drops rightmost columns that don't
// fit, and lets the last visible column take the remaining width.
func fillTable(t *table.Model, headers []string, rows [][]string, width int) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = lipgloss.Width(h)
	}
	for _, r := range rows {
		for i, c := range r {
			if i < len(widths) {
				widths[i] = max(widths[i], lipgloss.Width(c))
			}
		}
	}
	for i := range widths {
		widths[i] = min(widths[i], 60)
	}
	visible := len(widths)
	for visible > 2 {
		used := 0
		for _, w := range widths[:visible] {
			used += w + 2
		}
		if used <= width || width <= 0 {
			break
		}
		visible--
	}
	used := 0
	for _, w := range widths[:visible] {
		used += w + 2
	}
	if visible > 0 && used < width {
		widths[visible-1] += width - used
	}
	cols := make([]table.Column, visible)
	for i := range visible {
		cols[i] = table.Column{Title: headers[i], Width: widths[i]}
	}
	trows := make([]table.Row, len(rows))
	for i, r := range rows {
		trows[i] = table.Row(r[:min(visible, len(r))])
	}
	cursor := t.Cursor()
	t.SetRows(nil)
	t.SetColumns(cols)
	t.SetRows(trows)
	t.SetCursor(min(max(cursor, 0), max(len(trows)-1, 0)))
}

func (t *tableView) setFilter(f string) { t.filter = f; t.refill() }

func (t *tableView) setWide(w bool) { t.wide = w; t.refill() }

func (t *tableView) setSize(w, h int) {
	t.width, t.height = w, h
	t.table.SetWidth(w)
	t.table.SetHeight(max(h, 3))
	t.refill()
}

func (t *tableView) current() *row {
	c := t.table.Cursor()
	if c < 0 || c >= len(t.shown) {
		return nil
	}
	return &t.shown[c]
}

func (t *tableView) selectedRows() []row {
	if !t.multi || len(t.selected) == 0 {
		if r := t.current(); r != nil {
			return []row{*r}
		}
		return nil
	}
	var out []row
	for _, r := range t.all {
		if t.selected[r.key()] {
			out = append(out, r)
		}
	}
	return out
}

// handleKey handles navigation, sorting, and selection keys.
func (t *tableView) handleKey(msg tea.KeyPressMsg) (bool, tea.Cmd) {
	switch msg.String() {
	case "<":
		t.sorted = true
		t.sortCol = (t.sortCol - 1 + len(t.headers)) % max(len(t.headers), 1)
		t.refill()
		return true, nil
	case ">":
		t.sorted = true
		t.sortCol = (t.sortCol + 1) % max(len(t.headers), 1)
		t.refill()
		return true, nil
	case "s":
		t.sorted = true
		t.sortDesc = !t.sortDesc
		t.refill()
		return true, nil
	case "space":
		if t.multi {
			if r := t.current(); r != nil {
				t.selected[r.key()] = !t.selected[r.key()]
				if !t.selected[r.key()] {
					delete(t.selected, r.key())
				}
				t.refill()
				t.table.MoveDown(1)
			}
			return true, nil
		}
	case "up", "down", "k", "j", "pgup", "pgdown", "home", "end", "g", "G", "ctrl+f", "ctrl+b", "ctrl+u", "ctrl+d":
		if msg.String() == "ctrl+d" {
			return false, nil
		}
		var cmd tea.Cmd
		switch msg.String() {
		case "ctrl+f":
			t.table.MoveDown(max(t.height-2, 1))
		case "ctrl+b":
			t.table.MoveUp(max(t.height-2, 1))
		default:
			t.table, cmd = t.table.Update(msg)
		}
		return true, cmd
	}
	return false, nil
}

func (t *tableView) render(empty string) string {
	switch {
	case t.err != nil:
		return styleError.Render("error: " + t.err.Error())
	case !t.loaded:
		return styleDim.Render("loading…")
	case len(t.shown) == 0 && t.filter != "":
		return styleDim.Render("no matches for /" + t.filter)
	case len(t.all) == 0:
		return styleDim.Render(empty)
	}
	return t.table.View()
}

func (t *tableView) selectedJSON() (string, bool) {
	r := t.current()
	if r == nil {
		return "", false
	}
	v := r.raw
	if v == nil {
		m := map[string]string{}
		for i, h := range t.headers {
			m[strings.ToLower(h)] = cell(*r, i)
		}
		v = m
	}
	b, err := json.Marshal(v)
	return string(b), err == nil
}
