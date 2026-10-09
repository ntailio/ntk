// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"

	"github.com/factualtech/ntk/acls"
	"github.com/factualtech/ntk/configs"
	"github.com/factualtech/ntk/groups"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/topics"
	"github.com/factualtech/ntk/units"
)

func ints(v []int32) string {
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

func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func newTopicsView(m *model) view {
	internal, problems := false, false
	v := newResource("topics", nil)
	v.fetch = func(ctx context.Context, m *model) ([]string, []row, error) {
		ts, err := topics.List(ctx, m.client.Admin, topics.ListOptions{Internal: internal})
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, t := range ts {
			if problems && t.UnderReplicated == 0 && t.Offline == 0 && t.UnderMinISR == 0 {
				continue
			}
			retention := "-"
			if t.RetentionMs != nil {
				retention = configs.Humanize("retention.ms", itoa(*t.RetentionMs))
			}
			var leaders []string
			for _, b := range slices.Sorted(maps.Keys(t.LeadersByBroker)) {
				leaders = append(leaders, fmt.Sprintf("b%d:%d", b, t.LeadersByBroker[b]))
			}
			rows = append(rows, row{cells: []string{t.Name, itoa(t.Partitions), itoa(t.ReplicationFactor), itoa(t.UnderReplicated),
				units.Bytes(t.SizeBytes), retention, dash(t.CleanupPolicy), itoa(t.MinInsyncReplicas), strings.Join(leaders, " "), t.ID}, raw: t})
		}
		return []string{"NAME", "PARTITIONS", "RF", "URP", "SIZE", "RETENTION", "CLEANUP", "MIN-ISR", "LEADERS", "ID"}, rows, nil
	}
	v.wideFrom = 7
	v.empty = "no topics"
	v.enter = func(m *model, r *row) tea.Cmd { return m.push(newTopicDetailView(m, r.key())) }
	v.cli = func(m *model) string {
		c := "ntk topic list"
		if internal {
			c += " -a"
		}
		if problems {
			c += " --under-replicated"
		}
		return c
	}
	v.actions = []action{
		{key: "c", desc: "consume", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return m.push(newConsumeView(m, r.key(), -1, "-100"))
		}},
		{key: "p", desc: "produce", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return m.push(newComposerView(m, r.key(), nil))
		}},
		{key: "m", desc: "monitor", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return m.push(newTopicMonitorView(m, r.key()))
		}},
		{key: "e", desc: "config", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			d := newTopicDetailView(m, r.key()).(*tabbedView)
			d.active = 1
			return m.push(d)
		}},
		{key: "g", desc: "groups", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			d := newTopicDetailView(m, r.key()).(*tabbedView)
			d.active = 2
			return m.push(d)
		}},
		{key: "n", desc: "new", write: true, run: func(m *model, _ *row) tea.Cmd { return topicCreateForm(m) }},
		{key: "+", desc: "add partitions", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return addPartitionsForm(m, r.key())
		}},
		{key: "ctrl+d", desc: "delete", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			name := r.key()
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return topics.PlanDelete(ctx, m.client.Admin, []string{name})
			}, nil)
		}},
		{key: "i", desc: "internal", run: func(m *model, _ *row) tea.Cmd { internal = !internal; return v.load(m) }},
		{key: "u", desc: "problems only", run: func(m *model, _ *row) tea.Cmd { problems = !problems; return v.load(m) }},
	}
	return v
}

func topicCreateForm(m *model) tea.Cmd {
	var name, parts, rf, preset, extra string
	opts := []huh.Option[string]{huh.NewOption("none", "")}
	for _, n := range m.prefs.PresetNames() {
		opts = append(opts, huh.NewOption(n, n))
	}
	num := func(v string) error {
		if v == "" {
			return nil
		}
		_, err := strconv.Atoi(v)
		return err
	}
	f := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Topic name").Value(&name).Validate(huh.ValidateNotEmpty()),
		huh.NewInput().Title("Partitions").Description("empty = broker default").Value(&parts).Validate(num),
		huh.NewInput().Title("Replication factor").Description("empty = broker default").Value(&rf).Validate(num),
		huh.NewSelect[string]().Title("Preset").Options(opts...).Value(&preset),
		huh.NewInput().Title("Configs").Description("optional, comma-separated key=value (7d, 10GiB, ...)").Value(&extra),
	))
	return m.openForm("New topic", f, func(m *model) tea.Cmd {
		spec := topics.CreateSpec{Topics: []string{name}, Partitions: -1, ReplicationFactor: -1, Configs: map[string]string{}}
		if n, err := strconv.Atoi(parts); err == nil {
			spec.Partitions = int32(n)
		}
		if n, err := strconv.Atoi(rf); err == nil {
			spec.ReplicationFactor = int16(n)
		}
		if preset != "" {
			vals, _ := m.prefs.Preset(preset)
			maps.Copy(spec.Configs, vals)
		}
		for _, kv := range strings.Split(extra, ",") {
			if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
				spec.Configs[k] = v
			}
		}
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) { return topics.PlanCreate(ctx, m.client.Admin, spec) }, nil)
	})
}

func addPartitionsForm(m *model, topic string) tea.Cmd {
	var total string
	f := huh.NewForm(huh.NewGroup(huh.NewInput().Title("New total partitions for " + topic).Value(&total).Validate(func(s string) error {
		_, err := strconv.Atoi(s)
		return err
	})))
	return m.openForm("Add partitions", f, func(m *model) tea.Cmd {
		n, _ := strconv.Atoi(total)
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return topics.PlanAddPartitions(ctx, m.client.Admin, topic, n)
		}, nil)
	})
}

func newTopicDetailView(m *model, name string) view {
	var detail topics.Detail
	parts := newResource("partitions", func(ctx context.Context, m *model) ([]string, []row, error) {
		d, err := topics.Describe(ctx, m.client.Admin, name)
		if err != nil {
			return nil, nil, err
		}
		detail = d
		var rows []row
		for _, p := range d.PartitionDetails {
			rows = append(rows, row{cells: []string{itoa(p.Partition), itoa(p.Leader), ints(p.Replicas), ints(p.ISR), units.Count(p.LogStart),
				units.Count(p.HighWatermark), units.Count(p.Messages()), units.Bytes(p.SizeBytes), p.Status()}, raw: p})
		}
		return []string{"PART", "LEADER", "REPLICAS", "ISR", "LOW", "HIGH", "MSGS", "SIZE", "STATUS"}, rows, nil
	})
	parts.multi = true
	parts.cli = func(*model) string { return "ntk topic describe " + name }
	parts.actions = []action{
		{key: "c", desc: "consume partition", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			p, _ := strconv.Atoi(r.key())
			return m.push(newConsumeView(m, name, int32(p), "-100"))
		}},
		{key: "T", desc: "truncate selected", write: true, run: func(m *model, _ *row) tea.Cmd {
			var ids []int32
			for _, r := range parts.selectedRows() {
				p, _ := strconv.Atoi(r.key())
				ids = append(ids, int32(p))
			}
			return truncateForm(m, name, ids)
		}},
	}

	cfg := newTopicConfigView(m, name)

	consumers := newResource("consumers", func(ctx context.Context, m *model) ([]string, []row, error) {
		lags, err := groups.ForTopic(ctx, m.client.Client, m.client.Admin, name)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, g := range slices.Sorted(maps.Keys(lags)) {
			rows = append(rows, row{cells: []string{g, units.Count(lags[g])}})
		}
		return []string{"GROUP", "LAG ON " + strings.ToUpper(name)}, rows, nil
	})
	consumers.empty = "no consumer groups have offsets on this topic"
	consumers.enter = func(m *model, r *row) tea.Cmd { return m.push(newGroupDetailView(m, r.key())) }
	consumers.cli = func(*model) string { return "ntk group list --topic " + name + " --lag" }

	aclsTab := newACLResource(m, acls.Filter{ResourceType: "TOPIC", Name: name, Pattern: "MATCH"})
	aclsTab.cli = func(*model) string { return "ntk acl list --topic " + name + " --pattern match" }

	t := newTabbed(name, []string{"Partitions", "Config", "Consumers", "ACLs"}, parts, cfg, consumers, aclsTab)
	t.header = func() string {
		if detail.Name == "" {
			return styleTitle.Render(name)
		}
		d := detail
		retention := "-"
		if d.RetentionMs != nil {
			retention = configs.Humanize("retention.ms", itoa(*d.RetentionMs))
		}
		return fmt.Sprintf("%s  id %s · %d partitions · RF %d · %s · %s messages · retention %s · %s · %d URP · %d offline",
			styleTitle.Render(d.Name), d.ID, d.Partitions, d.ReplicationFactor, units.Bytes(d.SizeBytes), units.Count(d.Messages),
			retention, dash(d.CleanupPolicy), d.UnderReplicated, d.Offline)
	}
	return t
}

func truncateForm(m *model, topic string, parts []int32) tea.Cmd {
	mode, value := "all", ""
	f := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Delete records from "+topic+" partitions "+ints(parts)).Options(
			huh.NewOption("all records", "all"), huh.NewOption("before an offset", "offset"), huh.NewOption("before a time", "time"),
		).Value(&mode),
		huh.NewInput().Title("Offset or time").Description("offset number, RFC 3339, or -2h (ignored for all)").Value(&value),
	))
	return m.openForm("Truncate", f, func(m *model) tea.Cmd {
		o := topics.TruncateOptions{Partitions: parts}
		switch mode {
		case "all":
			o.All = true
		case "offset":
			n, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				m.flash("not an offset: "+value, true)
				return nil
			}
			o.BeforeOffset = &n
		case "time":
			t, err := units.ParseTime(value, timeNow())
			if err != nil {
				m.flash(err.Error(), true)
				return nil
			}
			o.BeforeTime = &t
		}
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return topics.PlanTruncate(ctx, m.client.Admin, topic, o)
		}, nil)
	})
}

func newTopicConfigView(m *model, topic string) view {
	all := false
	staged := map[string]*configs.Op{}
	var entries []configs.Entry
	v := newResource("config", nil)
	v.fetch = func(ctx context.Context, m *model) ([]string, []row, error) {
		es, err := configs.Describe(ctx, m.client.Client, configs.Topic, topic)
		if err != nil {
			return nil, nil, err
		}
		entries = es
		var rows []row
		for _, e := range es {
			if !all && !e.Overridden(configs.Topic) && !slices.Contains(configs.Important, e.Key) {
				if _, s := staged[e.Key]; !s {
					continue
				}
			}
			value := e.Human()
			if op := staged[e.Key]; op != nil {
				if op.Op == "delete" {
					value = "(unset) ← staged"
				} else {
					value = configs.Humanize(e.Key, op.Value) + " ← staged"
				}
			}
			rows = append(rows, row{cells: []string{e.Key, value, e.Source, e.Default()}, raw: e})
		}
		return []string{"KEY", "VALUE", "SOURCE", "DEFAULT"}, rows, nil
	}
	v.cli = func(*model) string { return "ntk topic config " + topic }
	edit := func(m *model, r *row) tea.Cmd {
		if r == nil {
			return nil
		}
		key := r.key()
		e, _ := configs.Find(entries, key)
		value := e.String()
		f := huh.NewForm(huh.NewGroup(huh.NewInput().Title(key).Description(firstSentence(e.Documentation)).Value(&value)))
		return m.openForm("Edit "+topic, f, func(m *model) tea.Cmd {
			staged[key] = &configs.Op{Key: key, Op: "set", Value: value}
			m.flash(fmt.Sprintf("staged %s=%s (ctrl+s reviews and applies)", key, value), false)
			return v.load(m)
		})
	}
	v.enter = edit
	v.actions = []action{
		{key: "e", desc: "edit", write: true, run: edit},
		{key: "u", desc: "unset", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			staged[r.key()] = &configs.Op{Key: r.key(), Op: "delete"}
			return v.load(m)
		}},
		{key: "a", desc: "all keys", run: func(m *model, _ *row) tea.Cmd { all = !all; return v.load(m) }},
		{key: "D", desc: "docs", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			e, _ := configs.Find(entries, r.key())
			m.flash(r.key()+": "+firstSentence(e.Documentation), false)
			return nil
		}},
		{key: "ctrl+s", desc: "apply staged", write: true, run: func(m *model, _ *row) tea.Cmd {
			if len(staged) == 0 {
				m.flash("nothing staged: edit values with e/enter or unset with u", false)
				return nil
			}
			var ops []configs.Op
			for _, k := range slices.Sorted(maps.Keys(staged)) {
				ops = append(ops, *staged[k])
			}
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return configs.PlanAlter(ctx, m.client.Client, m.client.Admin, configs.Topic, topic, ops)
			}, func(m *model) tea.Cmd {
				clear(staged)
				return v.load(m)
			})
		}},
	}
	return v
}

func firstSentence(s string) string {
	s = strings.NewReplacer("<code>", "", "</code>", "", "<i>", "", "</i>", "", "<br>", " ", "<p>", " ", "</p>", "").Replace(s)
	if i := strings.Index(s, ". "); i > 0 {
		return s[:i+1]
	}
	return s
}
