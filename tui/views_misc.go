// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/ntailio/ntk/brokers"
	"github.com/ntailio/ntk/health"
	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/topics"
	"github.com/ntailio/ntk/tx"
	"github.com/ntailio/ntk/units"
)

func newTxView(m *model) view {
	list := newResource("transactions", func(ctx context.Context, m *model) ([]string, []row, error) {
		ts, err := tx.List(ctx, m.client.Admin, nil, nil, 0)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, t := range ts {
			dur := "-"
			if d := t.Duration(); d > 0 {
				dur = roundDur(d)
			}
			rows = append(rows, row{cells: []string{t.TxnID, itoa(t.ProducerID), itoa(t.Epoch), t.State, dur, itoa(len(t.Partitions)), itoa(t.Coordinator)}, raw: t})
		}
		return []string{"TRANSACTIONAL ID", "PRODUCER ID", "EPOCH", "STATE", "DURATION", "PARTITIONS", "COORDINATOR"}, rows, nil
	})
	list.empty = "no transactions"
	list.enter = func(m *model, r *row) tea.Cmd {
		t := r.raw.(tx.Transaction)
		m.flash(fmt.Sprintf("%s: %s · timeout %dms · partitions %s", t.TxnID, t.State, t.TimeoutMs, dash(strings.Join(t.Partitions, ", "))), false)
		return nil
	}
	list.cli = func(*model) string { return "ntk tx list" }

	hanging := newResource("hanging", func(ctx context.Context, m *model) ([]string, []row, error) {
		hs, err := tx.FindHanging(ctx, m.client.Admin, nil, -1, 15*time.Minute)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, h := range hs {
			rows = append(rows, row{cells: []string{fmt.Sprintf("%s/%d", h.Topic, h.Partition), itoa(h.ProducerID), itoa(h.Epoch), roundDur(h.OpenSince),
				units.Count(h.TxnStartOffset), dash(h.TxnID), h.Reason}, raw: h})
		}
		return []string{"PARTITION", "PRODUCER ID", "EPOCH", "OPEN SINCE", "START OFFSET", "TXN ID", "REASON"}, rows, nil
	})
	hanging.empty = "no hanging transactions"
	hanging.cli = func(*model) string { return "ntk tx hanging" }
	abort := func(m *model, r *row) tea.Cmd {
		if r == nil {
			return nil
		}
		h := r.raw.(tx.Hanging)
		start := h.TxnStartOffset
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			pl, err := tx.PlanAbort(ctx, m.client.Admin, tx.AbortTarget{Topic: h.Topic, Partition: h.Partition, StartOffset: &start})
			if pl != nil {
				pl.Typed = true
			}
			return pl, err
		}, nil)
	}
	hanging.actions = []action{
		{key: "A", desc: "abort", write: true, run: abort},
		{key: "t", desc: "topic", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return m.push(newTopicDetailView(m, r.raw.(tx.Hanging).Topic))
		}},
	}

	producers := newResource("producers", func(ctx context.Context, m *model) ([]string, []row, error) {
		ps, err := tx.Producers(ctx, m.client.Admin, nil, nil)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, p := range ps {
			open := "-"
			if p.Open() {
				open = units.Count(p.TxnStartOffset) + " open"
			}
			rows = append(rows, row{cells: []string{fmt.Sprintf("%s/%d", p.Topic, p.Partition), itoa(p.ProducerID), itoa(p.Epoch), itoa(p.LastSequence),
				units.Ago(time.UnixMilli(p.LastTimestamp), time.Now()), open}, raw: p})
		}
		return []string{"PARTITION", "PRODUCER ID", "EPOCH", "LAST SEQ", "LAST WRITE", "OPEN TXN"}, rows, nil
	})
	producers.empty = "no active producers"
	producers.actions = []action{{key: "t", desc: "topic", run: func(m *model, r *row) tea.Cmd {
		if r == nil {
			return nil
		}
		return m.push(newTopicDetailView(m, r.raw.(tx.Producer).Topic))
	}}}

	t := newTabbed("transactions", []string{"Transactions", "Hanging", "Producers"}, list, hanging, producers)
	return t
}

type healthEvent struct {
	at   time.Time
	text string
}

func newHealthView(m *model) view {
	var r health.Report
	var disks []brokers.LogDir
	prev := map[string]health.Level{}
	var events []healthEvent
	showEvents := false
	v := newResource("health", func(ctx context.Context, m *model) ([]string, []row, error) {
		var err error
		if r, err = health.Run(ctx, m.client.Client, m.client.Admin, health.Options{}); err != nil {
			return nil, nil, err
		}
		disks, _, _ = brokers.LogDirs(ctx, m.client.Admin, -1, "")
		for _, c := range r.Checks {
			if old, ok := prev[c.Name]; ok && old != c.Level {
				events = append(events, healthEvent{r.At, fmt.Sprintf("%s %s → %s (%s)", c.Name, old, c.Level, c.Message)})
			}
			prev[c.Name] = c.Level
		}
		var rows []row
		if showEvents {
			for i := len(events) - 1; i >= 0; i-- {
				rows = append(rows, row{cells: []string{events[i].at.Format("15:04:05"), events[i].text}})
			}
			return []string{"TIME", "EVENT"}, rows, nil
		}
		for _, c := range r.Checks {
			if c.Level == health.OK {
				continue
			}
			mark := "!"
			if c.Level == health.Critical {
				mark = "✗"
			}
			if len(c.Items) == 0 {
				rows = append(rows, row{cells: []string{mark, c.Name, c.Message}, raw: c})
			}
			for _, it := range c.Items {
				rows = append(rows, row{cells: []string{mark, c.Name, it}, raw: c})
			}
		}
		return []string{"", "CHECK", "ISSUE"}, rows, nil
	})
	v.empty = "no issues"
	v.summary = func(*model) string {
		if r.ClusterID == "" {
			return styleDim.Render("checking…")
		}
		status := styleOK.Render("OK")
		switch r.Status {
		case health.Warn:
			status = styleWarn.Render("WARN")
		case health.Critical:
			status = styleError.Bold(true).Render("CRITICAL")
		}
		var checks []string
		for _, c := range r.Checks {
			mark := styleOK.Render("✓")
			switch c.Level {
			case health.Warn:
				mark = styleWarn.Render("!")
			case health.Critical:
				mark = styleError.Render("✗")
			}
			checks = append(checks, mark+" "+c.Name+" "+styleDim.Render(c.Message))
		}
		var disk []string
		for _, d := range disks {
			pct := -1.0
			if d.TotalBytes > 0 {
				pct = float64(d.TotalBytes-d.Usable) / float64(d.TotalBytes) * 100
			}
			disk = append(disk, fmt.Sprintf("b%d %s %.0f%%", d.Broker, bar(pct, 10), pct))
		}
		lines := []string{fmt.Sprintf("%s  %s · Kafka %s · %d brokers · %d voters", status, r.ClusterID, dash(r.Version), r.Brokers, r.Voters)}
		for i := 0; i < len(checks); i += 3 {
			lines = append(lines, strings.Join(checks[i:min(i+3, len(checks))], "   "))
		}
		lines = append(lines, "disk  "+strings.Join(disk, "  "))
		return strings.Join(lines, "\n")
	}
	v.cli = func(*model) string { return "ntk health" }
	v.enter = func(m *model, row *row) tea.Cmd {
		if row == nil || len(row.cells) < 3 {
			return nil
		}
		item := row.cells[2]
		c, _ := row.raw.(health.Check)
		switch c.Name {
		case "offline", "under-replicated", "under-min-isr", "at-min-isr":
			t, _, _ := strings.Cut(item, "/")
			return m.push(newTopicDetailView(m, t))
		case "transactions":
			return m.push(newTxView(m))
		case "disk", "log-dirs", "brokers", "quorum":
			var id int
			if _, err := fmt.Sscanf(item, "broker %d", &id); err == nil {
				return m.push(newBrokerDetailView(m, itoa(id)))
			}
			return m.push(newBrokersView(m))
		case "leader-imbalance":
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return topics.PlanElect(ctx, m.client.Admin, nil, nil, false)
			}, nil)
		case "reassignments":
			m.flash("see: ntk topic reassign status", false)
		}
		return nil
	}
	v.actions = []action{{key: "E", desc: "events", run: func(m *model, _ *row) tea.Cmd { showEvents = !showEvents; return v.load(m) }}}
	return v
}

func newProfilesView(m *model) view {
	v := newResource("profiles", func(ctx context.Context, m *model) ([]string, []row, error) {
		var rows []row
		for _, name := range m.store.Names() {
			p := m.store.File.Profiles[name]
			marker := ""
			if name == m.profileName {
				marker = "*"
			}
			flags := strings.Join(p.Labels, ",")
			if p.ReadOnly {
				flags = strings.Trim("read-only,"+flags, ",")
			}
			rows = append(rows, row{cells: []string{name, marker, p.BootstrapServers[0], p.Auth.Mechanism, p.SecurityProtocol(), dash(flags), p.Description}})
		}
		return []string{"NAME", "", "BOOTSTRAP", "AUTH", "SECURITY", "FLAGS", "DESCRIPTION"}, rows, nil
	})
	v.enter = func(m *model, r *row) tea.Cmd {
		name := r.key()
		return func() tea.Msg { return switchProfileMsg{name} }
	}
	v.cli = func(*model) string { return "ntk profile list" }
	return v
}
