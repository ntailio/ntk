// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"strings"

	tea "charm.land/bubbletea/v2"
)

type action struct {
	key, desc string
	write     bool
	run       func(m *model, r *row) tea.Cmd
}

type loadedMsg struct {
	vid     int
	headers []string
	rows    []row
	err     error
}

type resourceView struct {
	tableView
	vid     int
	name    string
	fetch   func(ctx context.Context, m *model) ([]string, []row, error)
	actions []action
	enter   func(m *model, r *row) tea.Cmd
	cli     func(m *model) string
	summary func(m *model) string
	empty   string
	loading bool
	again   bool
}

func newResource(name string, fetch func(ctx context.Context, m *model) ([]string, []row, error)) *resourceView {
	return &resourceView{tableView: newTableView(), vid: newID(), name: name, fetch: fetch, empty: "nothing here"}
}

func (v *resourceView) id() int       { return v.vid }
func (v *resourceView) title() string { return v.name }

func (v *resourceView) load(m *model) tea.Cmd {
	if v.fetch == nil {
		return nil
	}
	if v.loading {
		v.again = true
		return nil
	}
	v.loading = true
	id := v.vid
	return func() tea.Msg {
		ctx, cancel := m.timeout()
		defer cancel()
		h, rows, err := v.fetch(ctx, m)
		return loadedMsg{vid: id, headers: h, rows: rows, err: err}
	}
}

func (v *resourceView) update(m *model, msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case loadedMsg:
		if msg.vid != v.vid {
			return nil
		}
		v.loading = false
		if msg.err != nil {
			v.setErr(msg.err)
		} else {
			v.setRows(msg.headers, msg.rows)
		}
		if v.again {
			v.again = false
			return v.load(m)
		}
		return nil
	case tea.KeyPressMsg:
		if m.top() != view(v) && !v.isActiveTab(m) {
			return nil
		}
		key := msg.String()
		for _, a := range v.actions {
			if a.key != key {
				continue
			}
			if a.write && m.readOnly() {
				m.flash("refused: profile \""+m.profileName+"\" is read-only", true)
				return nil
			}
			return a.run(m, v.current())
		}
		if ok, cmd := v.handleKey(msg); ok {
			return cmd
		}
		if key == "enter" && v.enter != nil {
			if r := v.current(); r != nil {
				return v.enter(m, r)
			}
			return nil
		}
	}
	return nil
}

// isActiveTab reports whether v is the visible tab of the top tabbed view.
func (v *resourceView) isActiveTab(m *model) bool {
	t, ok := m.top().(interface{ activeTab() view })
	return ok && t.activeTab() == view(v)
}

func (v *resourceView) setSize(w, h int) {
	if v.summary != nil {
		h -= 2
	}
	v.tableView.setSize(w, h)
}

func (v *resourceView) render() string {
	body := v.tableView.render(v.empty)
	if v.summary != nil && v.err == nil {
		return v.summary(nil) + "\n\n" + body
	}
	return body
}

func (v *resourceView) binds(m *model, key string) bool {
	for _, a := range v.actions {
		if a.key == key && !(a.write && m.readOnly()) {
			return true
		}
	}
	return false
}

func (v *resourceView) hints(m *model) []hint {
	var hs []hint
	if v.enter != nil {
		hs = append(hs, hint{"enter", "open"})
	}
	for _, a := range v.actions {
		if a.write && m.readOnly() {
			continue
		}
		hs = append(hs, hint{a.key, a.desc})
	}
	return hs
}

func (v *resourceView) cliCommand(m *model) string {
	if v.cli == nil {
		return "ntk"
	}
	c := v.cli(m)
	if m.profileName != m.store.File.Active {
		c = strings.Replace(c, "ntk ", "ntk -p "+m.profileName+" ", 1)
	}
	return c
}

type tabbedView struct {
	vid    int
	name   string
	names  []string
	tabs   []view
	active int
	header func() string
	cli    func(m *model) string
	w, h   int
}

func newTabbed(name string, names []string, tabs ...view) *tabbedView {
	return &tabbedView{vid: newID(), name: name, names: names, tabs: tabs}
}

func (t *tabbedView) id() int       { return t.vid }
func (t *tabbedView) title() string { return t.name }

func (t *tabbedView) load(m *model) tea.Cmd {
	var cmds []tea.Cmd
	for _, v := range t.tabs {
		cmds = append(cmds, v.load(m))
	}
	return tea.Batch(cmds...)
}

func (t *tabbedView) update(m *model, msg tea.Msg) tea.Cmd {
	if k, ok := msg.(tea.KeyPressMsg); ok && m.top() == view(t) {
		switch k.String() {
		case "tab":
			t.active = (t.active + 1) % len(t.tabs)
			return nil
		case "shift+tab":
			t.active = (t.active - 1 + len(t.tabs)) % len(t.tabs)
			return nil
		}
		return t.tabs[t.active].update(m, msg)
	}
	var cmds []tea.Cmd
	for _, v := range t.tabs {
		cmds = append(cmds, v.update(m, msg))
	}
	return tea.Batch(cmds...)
}

func (t *tabbedView) setFilter(f string) { t.tabs[t.active].setFilter(f) }

func (t *tabbedView) setWide(w bool) {
	for _, v := range t.tabs {
		if x, ok := v.(interface{ setWide(bool) }); ok {
			x.setWide(w)
		}
	}
}

func (t *tabbedView) setSize(w, h int) {
	t.w, t.h = w, h
	extra := 2
	if t.header != nil {
		extra += strings.Count(t.header(), "\n") + 2
	}
	for _, v := range t.tabs {
		v.setSize(w, h-extra)
	}
}

func (t *tabbedView) render() string {
	var bar []string
	for i, n := range t.names {
		if i == t.active {
			bar = append(bar, styleTabOn.Render(n))
		} else {
			bar = append(bar, styleTab.Render(n))
		}
	}
	out := ""
	if t.header != nil {
		out = t.header() + "\n\n"
	}
	return out + strings.Join(bar, " ") + "\n\n" + t.tabs[t.active].render()
}

func (t *tabbedView) activeTab() view { return t.tabs[t.active] }

func (t *tabbedView) binds(m *model, key string) bool {
	b, ok := t.tabs[t.active].(interface{ binds(*model, string) bool })
	return ok && b.binds(m, key)
}

func (t *tabbedView) hints(m *model) []hint {
	return append([]hint{{"tab", "next tab"}}, t.tabs[t.active].hints(m)...)
}

func (t *tabbedView) cliCommand(m *model) string {
	if t.cli != nil {
		return t.cli(m)
	}
	return t.tabs[t.active].cliCommand(m)
}

func (t *tabbedView) selectedJSON() (string, bool) { return t.tabs[t.active].selectedJSON() }

func (t *tabbedView) close() {
	for _, v := range t.tabs {
		if c, ok := v.(interface{ close() }); ok {
			c.close()
		}
	}
}
