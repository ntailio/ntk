// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/factualtech/ntk/monitor"
	"github.com/factualtech/ntk/units"
)

func rate(f float64) string { return units.Rate(f) }

func roundDur(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()+0.5))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()+0.5))
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

func newTopicMonitorView(m *model, topic string) view {
	windows := []time.Duration{time.Minute, 5 * time.Minute, 15 * time.Minute}
	wi := 0
	w := monitor.NewTopicWatcher(m.client.Admin, []string{topic}, windows[0])
	var last monitor.TopicStats
	v := newResource("monitor "+topic, func(ctx context.Context, m *model) ([]string, []row, error) {
		stats, err := w.Sample(ctx)
		if err != nil {
			return nil, nil, err
		}
		if len(stats) == 0 {
			return nil, nil, fmt.Errorf("topic %q not found", topic)
		}
		last = stats[0]
		var rows []row
		for _, p := range last.PartitionStats {
			rows = append(rows, row{cells: []string{itoa(p.Partition), itoa(p.Leader), ints(p.ISR), rate(p.MsgRate), spark(p.Trend, 12),
				units.Bytes(p.SizeBytes), fmt.Sprintf("%.1f×", p.Skew), p.Status}, raw: p})
		}
		return []string{"PART", "LEADER", "ISR", "MSG/S", "TREND", "SIZE", "SKEW", "STATUS"}, rows, nil
	})
	v.noMarks = true
	v.summary = func(*model) string {
		if last.Topic == "" {
			return styleTitle.Render(topic) + styleDim.Render("  sampling…")
		}
		var leaders []string
		for _, b := range slices.Sorted(maps.Keys(last.LeadersByBroker)) {
			leaders = append(leaders, fmt.Sprintf("b%d %d", b, last.LeadersByBroker[b]))
		}
		return fmt.Sprintf("%s  msg/s %s  %s now · %s avg · %s max\nsize %s (%+s in %s) · URP %d · offline %d · leaders: %s",
			styleTitle.Render(topic), spark(last.Trend, 30), rate(last.MsgRate), rate(last.AvgRate), rate(last.MaxRate),
			units.Bytes(last.SizeBytes), units.Bytes(last.WindowSizeDelta), units.Duration(windows[wi]), last.URP, last.Offline, strings.Join(leaders, " · "))
	}
	v.cli = func(*model) string { return "ntk topic watch " + topic + " --by-partition" }
	v.actions = []action{
		{key: "c", desc: "consume partition", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			p := r.raw.(monitor.PartitionStats)
			return m.push(newConsumeView(m, topic, p.Partition, "-100"))
		}},
		{key: "g", desc: "consumer groups", run: func(m *model, _ *row) tea.Cmd {
			d := newTopicDetailView(m, topic).(*tabbedView)
			d.active = 2
			return m.push(d)
		}},
		{key: "W", desc: "window", run: func(m *model, _ *row) tea.Cmd {
			wi = (wi + 1) % len(windows)
			w.Window = windows[wi]
			m.flash("window "+windows[wi].String(), false)
			return nil
		}},
		{key: "+", desc: "faster", run: func(m *model, _ *row) tea.Cmd { return stepRefresh(m, -1) }},
		{key: "-", desc: "slower", run: func(m *model, _ *row) tea.Cmd { return stepRefresh(m, 1) }},
	}
	return v
}

func stepRefresh(m *model, dir int) tea.Cmd {
	steps := []time.Duration{time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	i := slices.Index(steps, m.refresh)
	if i < 0 {
		i = 2
	}
	i = min(max(i+dir, 0), len(steps)-1)
	m.refresh = steps[i]
	m.flash("sampling every "+m.refresh.String(), false)
	return m.tick()
}

func newGroupMonitorView(m *model, group string) view {
	w := monitor.NewGroupWatcher(m.client.Client, m.client.Admin, []string{group}, "", time.Minute)
	var last monitor.GroupStats
	var events []string
	byMember := false
	v := newResource("monitor "+group, func(ctx context.Context, m *model) ([]string, []row, error) {
		stats, err := w.Sample(ctx)
		if err != nil {
			return nil, nil, err
		}
		if len(stats) == 0 {
			return nil, nil, fmt.Errorf("group %q not found", group)
		}
		last = stats[0]
		events = append(events, last.Events...)
		if len(events) > 4 {
			events = events[len(events)-4:]
		}
		var rows []row
		if byMember {
			type agg struct {
				parts int
				lag   int64
				rate  float64
			}
			ms := map[string]*agg{}
			for _, p := range last.Partitions {
				k := dash(p.ClientID)
				if ms[k] == nil {
					ms[k] = &agg{}
				}
				ms[k].parts++
				ms[k].lag += p.Lag
				ms[k].rate += p.ConsumeRate
			}
			for _, k := range slices.Sorted(maps.Keys(ms)) {
				rows = append(rows, row{cells: []string{k, itoa(ms[k].parts), units.Count(ms[k].lag), rate(ms[k].rate)}})
			}
			return []string{"MEMBER", "PARTITIONS", "LAG", "CONS/S"}, rows, nil
		}
		for _, p := range last.Partitions {
			tl := "-"
			if p.TimeLagOK {
				tl = "~" + roundDur(p.TimeLag)
			}
			rows = append(rows, row{cells: []string{fmt.Sprintf("%s/%d", p.Topic, p.Partition), dash(p.ClientID), units.Count(p.Lag), spark(p.Trend, 8),
				rate(p.ConsumeRate), tl, string(p.Status)}, raw: p})
		}
		return []string{"TOPIC/PART", "MEMBER", "LAG", "TREND", "CONS/S", "TIME LAG", "STATUS"}, rows, nil
	})
	v.noMarks = true
	v.summary = func(*model) string {
		if last.Group == "" {
			return styleTitle.Render(group) + styleDim.Render("  sampling…")
		}
		var cons, prod float64
		eta := "-"
		for _, t := range last.Topics {
			cons += t.ConsumeRate
			prod += t.ProduceRate
			if t.ETAOK {
				eta = "~" + roundDur(t.ETA)
			}
		}
		status := string(last.Status)
		switch last.Status {
		case monitor.Stuck, monitor.FallingBehind:
			status = styleError.Render(status)
		case monitor.OK:
			status = styleOK.Render(status)
		default:
			status = styleWarn.Render(status)
		}
		s := fmt.Sprintf("%s  %s · %d members · epoch %d\nlag %s  %s (%+d in 1m) · %s · consume %s/s · produce %s/s · ETA %s",
			styleTitle.Render(group), last.State, last.Members, last.Epoch, spark(last.Trend, 30), units.Count(last.Lag), last.LagDelta,
			status, rate(cons), rate(prod), eta)
		if len(events) > 0 {
			s += "\n" + styleDim.Render(strings.Join(events, " · "))
		}
		return s
	}
	v.cli = func(*model) string { return "ntk group watch " + group + " --by-partition" }
	v.enter = func(m *model, r *row) tea.Cmd {
		p, ok := r.raw.(monitor.PartitionLag)
		if !ok {
			return nil
		}
		committed := int64(-1)
		d, err := groupsDescribeCommitted(m, group, p.Topic, p.Partition)
		if err == nil {
			committed = d
		}
		from := "earliest"
		if committed >= 0 {
			from = "@" + itoa(committed)
		}
		return m.push(newConsumeView(m, p.Topic, p.Partition, from))
	}
	v.actions = []action{
		{key: "M", desc: "by member", run: func(m *model, _ *row) tea.Cmd { byMember = !byMember; return v.load(m) }},
		{key: "R", desc: "reset offsets", write: true, run: func(m *model, _ *row) tea.Cmd { return resetForm(m, group, "") }},
		{key: "+", desc: "faster", run: func(m *model, _ *row) tea.Cmd { return stepRefresh(m, -1) }},
		{key: "-", desc: "slower", run: func(m *model, _ *row) tea.Cmd { return stepRefresh(m, 1) }},
	}
	return v
}

func groupsDescribeCommitted(m *model, group, topic string, partition int32) (int64, error) {
	ctx, cancel := m.timeout()
	defer cancel()
	offs, err := m.client.Admin.FetchOffsets(ctx, group)
	if err != nil {
		return -1, err
	}
	if o, ok := offs.Lookup(topic, partition); ok && o.Err == nil {
		return o.At, nil
	}
	return -1, fmt.Errorf("no commit")
}

func newTopView(m *model) view {
	w := monitor.NewTopicWatcher(m.client.Admin, nil, time.Minute)
	v := newResource("top topics", func(ctx context.Context, m *model) ([]string, []row, error) {
		stats, err := w.Sample(ctx)
		if err != nil {
			return nil, nil, err
		}
		monitor.SortTopics(stats, "msgs")
		var rows []row
		for _, s := range stats {
			rows = append(rows, row{cells: []string{s.Topic, rate(s.MsgRate), spark(s.Trend, 12), units.Bytes(s.SizeBytes), itoa(s.Partitions), itoa(s.URP)}, raw: s})
		}
		return []string{"TOPIC", "MSG/S", "TREND", "SIZE", "PARTS", "URP"}, rows, nil
	})
	v.noMarks = true
	v.enter = func(m *model, r *row) tea.Cmd { return m.push(newTopicMonitorView(m, r.key())) }
	v.cli = func(*model) string { return "ntk topic top" }
	return v
}

func newGroupTopView(m *model) view {
	w := monitor.NewGroupWatcher(m.client.Client, m.client.Admin, nil, "", time.Minute)
	v := newResource("top groups", func(ctx context.Context, m *model) ([]string, []row, error) {
		stats, err := w.Sample(ctx)
		if err != nil {
			return nil, nil, err
		}
		monitor.SortGroups(stats, "lag")
		var rows []row
		for _, s := range stats {
			tl, ok := monitor.MaxTimeLag(s)
			tls := "-"
			if ok {
				tls = "~" + roundDur(tl)
			}
			var cons float64
			for _, t := range s.Topics {
				cons += t.ConsumeRate
			}
			rows = append(rows, row{cells: []string{s.Group, s.State, units.Count(s.Lag), spark(s.Trend, 12), tls, rate(cons), string(s.Status)}, raw: s})
		}
		return []string{"GROUP", "STATE", "LAG", "TREND", "TIME LAG", "CONS/S", "STATUS"}, rows, nil
	})
	v.noMarks = true
	v.enter = func(m *model, r *row) tea.Cmd { return m.push(newGroupMonitorView(m, r.key())) }
	v.cli = func(*model) string { return "ntk group top" }
	return v
}
