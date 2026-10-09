// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"

	"github.com/factualtech/ntk/acls"
	"github.com/factualtech/ntk/groups"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/units"
)

func newGroupsView(m *model) view {
	state, typ := "", ""
	v := newResource("groups", nil)
	v.fetch = func(ctx context.Context, m *model) ([]string, []row, error) {
		o := groups.ListOptions{Lag: true}
		if state != "" {
			o.States = []string{state}
		}
		if typ != "" {
			o.Types = []string{typ}
		}
		gs, err := groups.List(ctx, m.client.Client, m.client.Admin, o)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, g := range gs {
			lag := "-"
			if g.Lag >= 0 {
				lag = units.Count(g.Lag)
			}
			rows = append(rows, row{cells: []string{g.Name, g.Type, g.State, itoa(g.Members), dash(strings.Join(g.Topics, ", ")), lag, itoa(g.Coordinator)}, raw: g})
		}
		return []string{"GROUP", "TYPE", "STATE", "MEMBERS", "TOPICS", "LAG", "COORDINATOR"}, rows, nil
	}
	v.empty = "no consumer groups"
	v.enter = func(m *model, r *row) tea.Cmd { return m.push(newGroupDetailView(m, r.key())) }
	v.cli = func(*model) string {
		c := "ntk group list --lag"
		if state != "" {
			c += " --state " + state
		}
		if typ != "" {
			c += " --type " + typ
		}
		return c
	}
	v.actions = []action{
		{key: "m", desc: "monitor", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return m.push(newGroupMonitorView(m, r.key()))
		}},
		{key: "R", desc: "reset offsets", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return resetForm(m, r.key(), "")
		}},
		{key: "ctrl+d", desc: "delete", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			name := r.key()
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return groups.PlanDelete(ctx, m.client.Client, m.client.Admin, []string{name})
			}, nil)
		}},
		{key: "f", desc: "filter state/type", run: func(m *model, _ *row) tea.Cmd {
			f := huh.NewForm(huh.NewGroup(
				huh.NewSelect[string]().Title("State").Options(huh.NewOptions("", "Stable", "Empty", "PreparingRebalance", "CompletingRebalance", "Assigning", "Reconciling", "Dead")...).Value(&state),
				huh.NewSelect[string]().Title("Type").Options(huh.NewOptions("", "classic", "consumer", "share")...).Value(&typ),
			))
			return m.openForm("Filter groups", f, func(m *model) tea.Cmd { return v.load(m) })
		}},
		{key: "t", desc: "topic", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			g, _ := r.raw.(groups.Group)
			if len(g.Topics) == 0 {
				m.flash("group has no topics", true)
				return nil
			}
			return m.push(newTopicDetailView(m, g.Topics[0]))
		}},
	}
	return v
}

func resetForm(m *model, group, topic string) tea.Cmd {
	mode, value := "earliest", ""
	f := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Topic").Description("topic[:partitions], empty = all topics with offsets").Value(&topic),
		huh.NewSelect[string]().Title("Reset to").Options(
			huh.NewOption("earliest", "earliest"), huh.NewOption("latest", "latest"), huh.NewOption("offset", "offset"),
			huh.NewOption("time", "time"), huh.NewOption("shift by ±N", "shift-by"),
		).Value(&mode),
		huh.NewInput().Title("Value").Description("offset, time (RFC 3339 / -2h), or ±N").Value(&value),
	))
	return m.openForm("Reset offsets of "+group, f, func(m *model) tea.Cmd {
		o := groups.ResetOptions{Topics: map[string][]int32{}, Mode: groups.ResetMode(mode)}
		if topic == "" {
			o.AllTopics = true
		} else {
			name, parts, _ := strings.Cut(topic, ":")
			o.Topics[name] = nil
			if parts != "" {
				for _, p := range strings.Split(parts, ",") {
					if n, err := strconv.Atoi(strings.TrimSpace(p)); err == nil {
						o.Topics[name] = append(o.Topics[name], int32(n))
					}
				}
			}
		}
		switch o.Mode {
		case groups.ToOffset:
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				m.flash("not an offset: "+value, true)
				return nil
			}
			o.Offset = n
		case groups.ShiftBy:
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				m.flash("not a number: "+value, true)
				return nil
			}
			o.Shift = n
		case groups.ToTime:
			t, err := parseTimeNow(value)
			if err != nil {
				m.flash(err.Error(), true)
				return nil
			}
			o.Time = t
		}
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return groups.PlanReset(ctx, m.client.Client, m.client.Admin, group, o)
		}, nil)
	})
}

func newGroupDetailView(m *model, name string) view {
	var detail groups.Detail
	offsets := newResource("offsets", func(ctx context.Context, m *model) ([]string, []row, error) {
		d, err := groups.Describe(ctx, m.client.Client, m.client.Admin, name)
		if err != nil {
			return nil, nil, err
		}
		detail = d
		var rows []row
		for _, o := range d.Offsets {
			committed := "-"
			if o.Committed >= 0 {
				committed = units.Count(o.Committed)
			}
			lag := "-"
			if o.Lag >= 0 {
				lag = units.Count(o.Lag)
				if th, ok := m.prefs.LagThresholds[name]; ok && o.Lag > th {
					lag += " !"
				}
			}
			rows = append(rows, row{cells: []string{o.Topic + "/" + itoa(o.Partition), o.Topic, itoa(o.Partition), committed, units.Count(o.End), lag,
				dash(o.ClientID), dash(o.Host)}, raw: o})
		}
		return []string{"KEY", "TOPIC", "PART", "COMMITTED", "END", "LAG", "CLIENT-ID", "HOST"}, rows, nil
	})
	offsets.cli = func(*model) string { return "ntk group describe " + name }
	offsets.actions = []action{
		{key: "c", desc: "consume at committed", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			o := r.raw.(groups.PartitionLag)
			from := "earliest"
			if o.Committed >= 0 {
				from = "@" + itoa(o.Committed)
			}
			return m.push(newConsumeView(m, o.Topic, o.Partition, from))
		}},
		{key: "R", desc: "reset offsets", write: true, run: func(m *model, r *row) tea.Cmd {
			t := ""
			if r != nil {
				t = r.raw.(groups.PartitionLag).Topic
			}
			return resetForm(m, name, t)
		}},
		{key: "m", desc: "monitor", run: func(m *model, _ *row) tea.Cmd { return m.push(newGroupMonitorView(m, name)) }},
	}

	members := newResource("members", func(ctx context.Context, m *model) ([]string, []row, error) {
		var rows []row
		for _, mb := range detail.Members_ {
			var parts []string
			for _, tp := range mb.Assigned {
				parts = append(parts, tp.Topic+":"+ints(tp.Partitions))
			}
			rows = append(rows, row{cells: []string{mb.ID, mb.ClientID, mb.Host, dash(mb.InstanceID), dash(strings.Join(parts, " "))}, raw: mb})
		}
		return []string{"MEMBER", "CLIENT-ID", "HOST", "INSTANCE", "ASSIGNED"}, rows, nil
	})
	members.empty = "no active members"

	topicsTab := newResource("topics", func(ctx context.Context, m *model) ([]string, []row, error) {
		lag := map[string]int64{}
		for _, o := range detail.Offsets {
			lag[o.Topic] += max(o.Lag, 0)
		}
		var rows []row
		for _, t := range detail.Topics {
			rows = append(rows, row{cells: []string{t, units.Count(lag[t])}})
		}
		return []string{"TOPIC", "LAG"}, rows, nil
	})
	topicsTab.enter = func(m *model, r *row) tea.Cmd { return m.push(newTopicDetailView(m, r.key())) }

	aclsTab := newACLResource(m, acls.Filter{ResourceType: "GROUP", Name: name, Pattern: "MATCH"})
	aclsTab.cli = func(*model) string { return "ntk acl list --group " + name + " --pattern match" }
	t := newTabbed(name, []string{"Offsets & lag", "Members", "Topics", "ACLs"}, offsets, members, topicsTab, aclsTab)
	t.header = func() string {
		if detail.Name == "" {
			return styleTitle.Render(name)
		}
		d := detail
		extra := ""
		if d.Epoch > 0 {
			extra += fmt.Sprintf(" · epoch %d", d.Epoch)
		}
		if d.Assignor != "" {
			extra += " · assignor " + d.Assignor
		}
		return fmt.Sprintf("%s  %s · %s · %d members · coordinator %d%s · total lag %s", styleTitle.Render(d.Name), d.Type, d.State, d.Members,
			d.Coordinator, extra, units.Count(d.TotalLag()))
	}
	return &groupDetail{tabbedView: t, members: members, topics: topicsTab}
}

// groupDetail reloads the members and topics tabs after the offsets tab has loaded the group.
type groupDetail struct {
	*tabbedView
	members, topics *resourceView
}

func (g *groupDetail) update(m *model, msg tea.Msg) tea.Cmd {
	cmd := g.tabbedView.update(m, msg)
	if l, ok := msg.(loadedMsg); ok && l.vid == g.tabs[0].id() {
		return tea.Batch(cmd, g.members.load(m), g.topics.load(m))
	}
	return cmd
}

func (g *groupDetail) load(m *model) tea.Cmd { return tea.Batch(g.tabs[0].load(m), g.tabs[3].load(m)) }
