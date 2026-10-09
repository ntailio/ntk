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
	"github.com/factualtech/ntk/brokers"
	"github.com/factualtech/ntk/configs"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/principals"
	"github.com/factualtech/ntk/quotas"
	"github.com/factualtech/ntk/units"
)

func newBrokersView(m *model) view {
	v := newResource("brokers", func(ctx context.Context, m *model) ([]string, []row, error) {
		bs, err := brokers.List(ctx, m.client.Client, m.client.Admin)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, b := range bs {
			role := b.Roles
			if b.Controller {
				role += " (active controller)"
			}
			pct := "-"
			if p := b.DiskPercent(); p >= 0 {
				pct = fmt.Sprintf("%.0f%%", p)
			}
			rows = append(rows, row{cells: []string{itoa(b.ID), fmt.Sprintf("%s:%d", b.Host, b.Port), dash(b.Rack), role, itoa(b.Leaders), itoa(b.Replicas),
				units.Bytes(b.DiskUsed), pct, dash(b.Version)}, raw: b})
		}
		return []string{"ID", "ADDRESS", "RACK", "ROLE", "LEADERS", "REPLICAS", "DISK USED", "DISK %", "VERSION"}, rows, nil
	})
	v.enter = func(m *model, r *row) tea.Cmd { return m.push(newBrokerDetailView(m, r.key())) }
	v.cli = func(*model) string { return "ntk broker list" }
	v.actions = []action{
		{key: "e", desc: "edit config", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return brokerConfigForm(m, r.key())
		}},
		{key: "D", desc: "cluster default config", run: func(m *model, _ *row) tea.Cmd { return m.push(newBrokerConfigView(m, "")) }},
		{key: "p", desc: "partitions", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			d := newBrokerDetailView(m, r.key()).(*tabbedView)
			d.active = 1
			return m.push(d)
		}},
	}
	return v
}

func brokerConfigForm(m *model, id string) tea.Cmd {
	var kv string
	f := huh.NewForm(huh.NewGroup(huh.NewInput().Title("key=value").Description("one or more, comma-separated; an empty value unsets").Value(&kv)))
	label := "broker " + id
	if id == "" {
		label = "cluster default"
	}
	return m.openForm("Edit "+label+" config", f, func(m *model) tea.Cmd {
		var ops []configs.Op
		for _, a := range strings.Split(kv, ",") {
			k, val, ok := strings.Cut(strings.TrimSpace(a), "=")
			if !ok {
				m.flash(a+" is not key=value", true)
				return nil
			}
			if val == "" {
				ops = append(ops, configs.Op{Key: k, Op: "delete"})
			} else {
				ops = append(ops, configs.Op{Key: k, Op: "set", Value: val})
			}
		}
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return configs.PlanAlter(ctx, m.client.Client, m.client.Admin, configs.Broker, id, ops)
		}, nil)
	})
}

func newBrokerConfigView(m *model, id string) view {
	all := false
	title := "config broker " + id
	if id == "" {
		title = "cluster default config"
	}
	v := newResource(title, nil)
	v.fetch = func(ctx context.Context, m *model) ([]string, []row, error) {
		es, err := configs.Describe(ctx, m.client.Client, configs.Broker, id)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, e := range es {
			if !all && !e.Overridden(configs.Broker) {
				continue
			}
			src := e.Source
			if e.ReadOnly {
				src += " (read-only)"
			}
			rows = append(rows, row{cells: []string{e.Key, e.Human(), src, e.Default()}, raw: e})
		}
		return []string{"KEY", "VALUE", "SOURCE", "DEFAULT"}, rows, nil
	}
	v.empty = "no dynamic overrides (a shows every key)"
	v.cli = func(*model) string {
		if id == "" {
			return "ntk broker config --cluster-default"
		}
		return "ntk broker config " + id
	}
	v.actions = []action{
		{key: "a", desc: "all keys", run: func(m *model, _ *row) tea.Cmd { all = !all; return v.load(m) }},
		{key: "e", desc: "edit", write: true, run: func(m *model, _ *row) tea.Cmd { return brokerConfigForm(m, id) }},
	}
	return v
}

func newBrokerDetailView(m *model, id string) view {
	bid, _ := strconv.Atoi(id)
	var d brokers.Detail
	overview := newResource("overview", func(ctx context.Context, m *model) ([]string, []row, error) {
		var err error
		d, err = brokers.Describe(ctx, m.client.Client, m.client.Admin, int32(bid))
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, p := range d.OutOfISR {
			rows = append(rows, row{cells: []string{p, "out of ISR on this broker"}})
		}
		return []string{"PARTITION", "PROBLEM"}, rows, nil
	})
	overview.empty = "no partitions out of ISR on this broker"
	partitions := newResource("partitions", func(ctx context.Context, m *model) ([]string, []row, error) {
		md, err := m.client.Admin.Metadata(ctx)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, t := range md.Topics.Sorted() {
			for _, p := range t.Partitions.Sorted() {
				if !slices.Contains(p.Replicas, int32(bid)) {
					continue
				}
				role := "follower"
				if p.Leader == int32(bid) {
					role = "leader"
				}
				insync := "yes"
				if !slices.Contains(p.ISR, int32(bid)) {
					insync = "NO"
				}
				rows = append(rows, row{cells: []string{fmt.Sprintf("%s/%d", t.Topic, p.Partition), role, ints(p.Replicas), insync}})
			}
		}
		return []string{"PARTITION", "ROLE", "REPLICAS", "IN SYNC"}, rows, nil
	})
	partitions.enter = func(m *model, r *row) tea.Cmd {
		t, _, _ := strings.Cut(r.key(), "/")
		return m.push(newTopicDetailView(m, t))
	}
	logdirs := newResource("log dirs", func(ctx context.Context, m *model) ([]string, []row, error) {
		dirs, _, err := brokers.LogDirs(ctx, m.client.Admin, int32(bid), "")
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, ld := range dirs {
			pct := -1.0
			if ld.TotalBytes > 0 {
				pct = float64(ld.TotalBytes-ld.Usable) / float64(ld.TotalBytes) * 100
			}
			rows = append(rows, row{cells: []string{ld.Dir, units.Bytes(ld.SizeBytes), bar(pct, 20), fmt.Sprintf("%.0f%%", pct), dash(ld.Error)}, raw: ld})
		}
		return []string{"DIR", "DATA", "VOLUME", "USED", "ERROR"}, rows, nil
	})
	cfg := newBrokerConfigView(m, id)
	t := newTabbed("broker "+id, []string{"Overview", "Partitions", "Log dirs", "Config"}, overview, partitions, logdirs, cfg)
	t.header = func() string {
		if d.ID == 0 && d.Host == "" {
			return styleTitle.Render("broker " + id)
		}
		ctrl := ""
		if d.Controller {
			ctrl = " · " + styleAccent.Render("active controller")
		}
		var ov []string
		for _, k := range slices.Sorted(maps.Keys(d.Overrides)) {
			ov = append(ov, k+"="+configs.Humanize(k, d.Overrides[k]))
		}
		return fmt.Sprintf("%s  %s:%d · rack %s · %s%s · %d leaders · %d replicas · %d out of ISR\noverrides: %s",
			styleTitle.Render("broker "+id), d.Host, d.Port, dash(d.Rack), d.Roles, ctrl, d.Leaders, d.Replicas, len(d.OutOfISR), dash(strings.Join(ov, ", ")))
	}
	t.cli = func(*model) string { return "ntk broker describe " + id }
	return t
}

func newClusterView(m *model) view {
	var c brokers.Cluster
	v := newResource("cluster", func(ctx context.Context, m *model) ([]string, []row, error) {
		var err error
		if c, err = brokers.DescribeCluster(ctx, m.client.Client, m.client.Admin); err != nil {
			return nil, nil, err
		}
		var rows []row
		if c.Quorum != nil {
			for _, r := range c.Quorum.Voters {
				role := "voter"
				if r.ID == c.Quorum.LeaderID {
					role = "leader"
				}
				rows = append(rows, row{cells: []string{itoa(r.ID), role, units.Count(r.LogEndOffset), units.Count(r.Lag), brokers.Ago(r.LastFetch)}})
			}
			for _, r := range c.Quorum.Observers {
				rows = append(rows, row{cells: []string{itoa(r.ID), "observer", units.Count(r.LogEndOffset), units.Count(r.Lag), brokers.Ago(r.LastFetch)}})
			}
		}
		return []string{"NODE", "QUORUM ROLE", "LOG END", "LAG", "LAST FETCH"}, rows, nil
	})
	v.summary = func(*model) string {
		if c.ID == "" {
			return styleTitle.Render("cluster")
		}
		var fs []string
		for _, f := range c.Features {
			if f.FinalizedMax >= 0 {
				fs = append(fs, fmt.Sprintf("%s=%d", f.Name, f.FinalizedMax))
			}
		}
		return fmt.Sprintf("%s  Kafka %s · controller %d · %d brokers · %d topics · %d partitions\nfeatures: %s",
			styleTitle.Render(c.ID), dash(c.Version), c.Controller, len(c.Brokers), c.Topics, c.Partitions, strings.Join(fs, " · "))
	}
	v.cli = func(*model) string { return "ntk cluster describe" }
	return v
}

func newACLResource(m *model, f acls.Filter) *resourceView {
	return newACLResourceP(m, &f)
}

func newACLResourceP(m *model, f *acls.Filter) *resourceView {
	v := newResource("acls", func(ctx context.Context, m *model) ([]string, []row, error) {
		list, err := acls.List(ctx, m.client.Client, *f)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, a := range list {
			rows = append(rows, row{id: a.String(), cells: []string{a.Principal, a.Permission, a.Operation, a.ResourceType, a.Pattern, a.Name, a.Host}, raw: a})
		}
		return []string{"PRINCIPAL", "PERM", "OPERATION", "RESOURCE", "PATTERN", "NAME", "HOST"}, rows, nil
	})
	v.empty = "no ACLs"
	return v
}

func newACLsView(m *model, principal string) view {
	f := &acls.Filter{Principal: principal}
	v := newACLResourceP(m, f)
	v.multi = true
	v.cli = func(*model) string {
		if principal != "" {
			return "ntk acl list --principal " + principal
		}
		return "ntk acl list"
	}
	v.enter = func(m *model, r *row) tea.Cmd {
		a := r.raw.(acls.ACL)
		switch a.ResourceType {
		case "TOPIC":
			if a.Pattern == "LITERAL" && a.Name != "*" {
				return m.push(newTopicDetailView(m, a.Name))
			}
		case "GROUP":
			if a.Pattern == "LITERAL" && a.Name != "*" {
				return m.push(newGroupDetailView(m, a.Name))
			}
		}
		return nil
	}
	v.actions = []action{
		{key: "n", desc: "new", write: true, run: func(m *model, _ *row) tea.Cmd { return aclCreateForm(m, principal) }},
		{key: "G", desc: "grant recipe", write: true, run: func(m *model, _ *row) tea.Cmd { return recipeForm(m, principal) }},
		{key: "ctrl+d", desc: "delete selected", write: true, run: func(m *model, _ *row) tea.Cmd {
			var list []acls.ACL
			for _, r := range v.selectedRows() {
				list = append(list, r.raw.(acls.ACL))
			}
			if len(list) == 0 {
				return nil
			}
			self, _ := m.prof.ExpectedPrincipal()
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return acls.PlanDelete(ctx, m.client.Client, list, "ACLs", self)
			}, func(m *model) tea.Cmd { clear(v.selected); return nil })
		}},
		{key: "P", desc: "principal", run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			return m.push(newPrincipalDetailView(m, r.raw.(acls.ACL).Principal))
		}},
		{key: "C", desc: "check access", run: func(m *model, _ *row) tea.Cmd { return aclCheckForm(m) }},
		{key: "f", desc: "filter", run: func(m *model, _ *row) tea.Cmd {
			rt, op := strings.ToLower(f.ResourceType), strings.ToLower(f.Operation)
			fp, fn := f.Principal, f.Name
			types := append([]string{""}, acls.ResourceTypes()...)
			form := huh.NewForm(huh.NewGroup(
				huh.NewInput().Title("Principal").Value(&fp),
				huh.NewSelect[string]().Title("Resource type").Options(huh.NewOptions(types...)...).Value(&rt),
				huh.NewInput().Title("Resource name").Description("with a type: every ACL that applies to it").Value(&fn),
				huh.NewInput().Title("Operation").Value(&op),
			))
			return m.openForm("Filter ACLs", form, func(m *model) tea.Cmd {
				nf := acls.Filter{Principal: acls.NormalizePrincipal(fp), Name: fn}
				if rt != "" {
					nf.ResourceType, _ = acls.ParseResourceType(rt)
					if fn != "" {
						nf.Pattern = "MATCH"
					}
				}
				if op != "" {
					o, err := acls.ParseOperation(op)
					if err != nil {
						m.flash(err.Error(), true)
						return nil
					}
					nf.Operation = o
				}
				*f = nf
				return v.load(m)
			})
		}},
	}
	return v
}

func aclCreateForm(m *model, principal string) tea.Cmd {
	var ops, rt, name, perm, pattern, host = "read,describe", "topic", "", "allow", "literal", "*"
	f := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Principal").Value(&principal).Validate(huh.ValidateNotEmpty()),
		huh.NewInput().Title("Operations").Description("comma-separated, e.g. read,describe").Value(&ops),
		huh.NewSelect[string]().Title("Resource type").Options(huh.NewOptions(acls.ResourceTypes()...)...).Value(&rt),
		huh.NewInput().Title("Resource name").Description("ignored for cluster").Value(&name),
		huh.NewSelect[string]().Title("Pattern").Options(huh.NewOptions("literal", "prefixed")...).Value(&pattern),
		huh.NewSelect[string]().Title("Permission").Options(huh.NewOptions("allow", "deny")...).Value(&perm),
		huh.NewInput().Title("Host").Value(&host),
	))
	return m.openForm("New ACL", f, func(m *model) tea.Cmd {
		rtype, err := acls.ParseResourceType(rt)
		if err != nil {
			m.flash(err.Error(), true)
			return nil
		}
		if rtype == "CLUSTER" {
			name = "kafka-cluster"
		}
		var list []acls.ACL
		for _, o := range strings.Split(ops, ",") {
			op, err := acls.ParseOperation(o)
			if err != nil {
				m.flash(err.Error(), true)
				return nil
			}
			list = append(list, acls.ACL{Principal: acls.NormalizePrincipal(principal), Host: host, Permission: strings.ToUpper(perm), Operation: op,
				ResourceType: rtype, Pattern: strings.ToUpper(pattern), Name: name})
		}
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return acls.PlanCreate(ctx, m.client.Client, list, "ACLs")
		}, nil)
	})
}

func recipeForm(m *model, principal string) tea.Cmd {
	var recipe = "consumer"
	var topics, groupsS, txn, appID, in, out string
	var prefixed bool
	f := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Recipe").Options(huh.NewOptions(acls.Recipes...)...).Value(&recipe),
		huh.NewInput().Title("Principal").Value(&principal).Validate(huh.ValidateNotEmpty()),
		huh.NewInput().Title("Topics").Description("comma-separated (producer, consumer, transactional-producer, topic-admin)").Value(&topics),
		huh.NewInput().Title("Groups").Description("consumer").Value(&groupsS),
		huh.NewInput().Title("Transactional ids").Description("transactional-producer").Value(&txn),
		huh.NewInput().Title("application.id").Description("streams-app").Value(&appID),
		huh.NewInput().Title("Input topics").Description("streams-app").Value(&in),
		huh.NewInput().Title("Output topics").Description("streams-app").Value(&out),
		huh.NewConfirm().Title("Prefixed patterns?").Value(&prefixed),
	))
	return m.openForm("Grant ACL recipe", f, func(m *model) tea.Cmd {
		split := func(s string) []string {
			var o []string
			for _, x := range strings.Split(s, ",") {
				if x = strings.TrimSpace(x); x != "" {
					o = append(o, x)
				}
			}
			return o
		}
		list, err := acls.Recipe(recipe, acls.RecipeArgs{Principal: acls.NormalizePrincipal(principal), Topics: split(topics), Groups: split(groupsS),
			TxnIDs: split(txn), AppID: appID, TopicsIn: split(in), TopicsOut: split(out), Prefixed: prefixed})
		if err != nil {
			m.flash(err.Error(), true)
			return nil
		}
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return acls.PlanCreate(ctx, m.client.Client, list, "Grant "+recipe+" for "+acls.NormalizePrincipal(principal))
		}, nil)
	})
}

func aclCheckForm(m *model) tea.Cmd {
	var principal, op, rt, name, host = "", "read", "topic", "", "*"
	f := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Principal").Value(&principal).Validate(huh.ValidateNotEmpty()),
		huh.NewInput().Title("Operation").Value(&op),
		huh.NewSelect[string]().Title("Resource type").Options(huh.NewOptions(acls.ResourceTypes()...)...).Value(&rt),
		huh.NewInput().Title("Resource name").Value(&name),
		huh.NewInput().Title("Client host").Value(&host),
	))
	return m.openForm("Check access", f, func(m *model) tea.Cmd {
		o, err := acls.ParseOperation(op)
		if err != nil {
			m.flash(err.Error(), true)
			return nil
		}
		rtype, _ := acls.ParseResourceType(rt)
		if rtype == "CLUSTER" {
			name = "kafka-cluster"
		}
		return func() tea.Msg {
			ctx, cancel := m.timeout()
			defer cancel()
			all, err := acls.List(ctx, m.client.Client, acls.Filter{ResourceType: rtype, Name: name, Pattern: "MATCH"})
			if err != nil {
				return flashMsg{err.Error(), true}
			}
			d := acls.Check(all, acls.NormalizePrincipal(principal), host, o, rtype, name)
			msg := d.Reason
			for _, a := range d.By {
				msg += " " + a.String()
			}
			return flashMsg{msg, !d.Allowed}
		}
	})
}

func newPrincipalsView(m *model, usersOnly bool) view {
	name := "principals"
	if usersOnly {
		name = "users"
	}
	v := newResource(name, func(ctx context.Context, m *model) ([]string, []row, error) {
		var sources []string
		if usersOnly {
			sources = []string{"scram"}
		}
		ps, err := principals.List(ctx, m.client.Client, m.client.Admin, sources)
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, p := range ps {
			rows = append(rows, row{cells: []string{p.Name, dash(strings.Join(p.SCRAM, ",")), itoa(p.ACLs), dash(strings.Join(p.Quotas, "; "))}, raw: p})
		}
		return []string{"PRINCIPAL", "SCRAM", "ACLS", "QUOTAS"}, rows, nil
	})
	v.cli = func(*model) string {
		if usersOnly {
			return "ntk user list"
		}
		return "ntk principal list"
	}
	v.enter = func(m *model, r *row) tea.Cmd { return m.push(newPrincipalDetailView(m, r.key())) }
	v.actions = principalActions("", v)
	return v
}

func principalActions(fixed string, v *resourceView) []action {
	who := func(r *row) string {
		if fixed != "" {
			return fixed
		}
		if r == nil {
			return ""
		}
		return r.key()
	}
	user := func(r *row) (string, bool) {
		u, ok := strings.CutPrefix(who(r), "User:")
		return u, ok && u != ""
	}
	return []action{
		{key: "n", desc: "new SCRAM user", write: true, run: func(m *model, _ *row) tea.Cmd { return scramForm(m, "", false) }},
		{key: "ctrl+r", desc: "rotate password", write: true, run: func(m *model, r *row) tea.Cmd {
			u, ok := user(r)
			if !ok {
				return nil
			}
			return scramForm(m, u, true)
		}},
		{key: "ctrl+d", desc: "delete SCRAM", write: true, run: func(m *model, r *row) tea.Cmd {
			u, ok := user(r)
			if !ok {
				return nil
			}
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return principals.PlanDelete(ctx, m.client.Client, m.client.Admin, u, nil, false)
			}, nil)
		}},
		{key: "G", desc: "grant recipe", write: true, run: func(m *model, r *row) tea.Cmd { return recipeForm(m, who(r)) }},
		{key: "Q", desc: "set quota", write: true, run: func(m *model, r *row) tea.Cmd {
			u, _ := user(r)
			return quotaForm(m, "user", u, "")
		}},
	}
}

func scramForm(m *model, user string, rotate bool) tea.Cmd {
	mech, pw := "scram-sha-512", ""
	fields := []huh.Field{}
	if !rotate {
		fields = append(fields, huh.NewInput().Title("User name").Value(&user).Validate(huh.ValidateNotEmpty()))
	}
	fields = append(fields,
		huh.NewSelect[string]().Title("Mechanism").Options(huh.NewOptions("scram-sha-512", "scram-sha-256")...).Value(&mech),
		huh.NewInput().Title("Password").EchoMode(huh.EchoModePassword).Value(&pw).Validate(huh.ValidateNotEmpty()))
	title := "New SCRAM user"
	if rotate {
		title = "Rotate password of " + user
	}
	return m.openForm(title, huh.NewForm(huh.NewGroup(fields...)), func(m *model) tea.Cmd {
		mm, _ := principals.ParseMechanism(mech)
		return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
			return principals.PlanUpsert(ctx, m.client.Admin, user, mm, 8192, pw, rotate)
		}, nil)
	})
}

func newPrincipalDetailView(m *model, principal string) view {
	principal = acls.NormalizePrincipal(principal)
	var d principals.Detail
	overview := newResource("overview", func(ctx context.Context, m *model) ([]string, []row, error) {
		var err error
		if d, err = principals.Describe(ctx, m.client.Client, m.client.Admin, principal, nil); err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, a := range d.Access {
			rows = append(rows, row{cells: []string{a}})
		}
		return []string{"EFFECTIVE ACCESS"}, rows, nil
	})
	overview.empty = "no access granted by ACLs"
	aclTab := newACLsView(m, principal).(*resourceView)
	quotaTab := newResource("quotas", func(ctx context.Context, m *model) ([]string, []row, error) {
		var rows []row
		for _, e := range d.Effective {
			rows = append(rows, row{cells: []string{e.Key, quotas.Format(e.Key, e.Value), e.From}})
		}
		return []string{"KEY", "EFFECTIVE", "FROM"}, rows, nil
	})
	quotaTab.empty = "no quotas apply"
	scram := newResource("scram", func(ctx context.Context, m *model) ([]string, []row, error) {
		var rows []row
		for _, c := range d.SCRAM {
			rows = append(rows, row{cells: []string{c.Mechanism, itoa(c.Iterations)}})
		}
		return []string{"MECHANISM", "ITERATIONS"}, rows, nil
	})
	scram.empty = "no SCRAM credentials"
	overview.actions = principalActions(principal, overview)
	scram.actions = principalActions(principal, scram)
	t := newTabbed(principal, []string{"Overview", "ACLs", "Quotas", "SCRAM"}, overview, aclTab, quotaTab, scram)
	t.header = func() string {
		return fmt.Sprintf("%s  %d ACLs · %d quota entities · %d SCRAM mechanisms", styleTitle.Render(principal), len(d.ACLs), len(d.Quotas), len(d.SCRAM))
	}
	t.cli = func(*model) string { return "ntk principal describe " + principal }
	return &principalDetail{t, quotaTab, scram}
}

type principalDetail struct {
	*tabbedView
	quotas, scram *resourceView
}

func (p *principalDetail) update(m *model, msg tea.Msg) tea.Cmd {
	cmd := p.tabbedView.update(m, msg)
	if l, ok := msg.(loadedMsg); ok && l.vid == p.tabs[0].id() {
		return tea.Batch(cmd, p.quotas.load(m), p.scram.load(m))
	}
	return cmd
}

func (p *principalDetail) load(m *model) tea.Cmd {
	return tea.Batch(p.tabs[0].load(m), p.tabs[1].load(m))
}

func newQuotasView(m *model) view {
	v := newResource("quotas", func(ctx context.Context, m *model) ([]string, []row, error) {
		qs, err := quotas.List(ctx, m.client.Admin, quotas.Filter{})
		if err != nil {
			return nil, nil, err
		}
		var rows []row
		for _, q := range qs {
			cells := []string{q.Entity.String()}
			for _, k := range quotas.Keys {
				if val, ok := q.Values[k]; ok {
					cells = append(cells, quotas.Format(k, val))
				} else {
					cells = append(cells, "-")
				}
			}
			rows = append(rows, row{cells: cells, raw: q})
		}
		return []string{"ENTITY", "PRODUCE", "CONSUME", "REQUEST", "MUTATIONS", "CONNECTIONS"}, rows, nil
	})
	v.empty = "no quotas"
	v.cli = func(*model) string { return "ntk quota list" }
	entityOf := func(r *row) quotas.Entity {
		if r == nil {
			return nil
		}
		return r.raw.(quotas.Quota).Entity
	}
	v.actions = []action{
		{key: "n", desc: "new", write: true, run: func(m *model, _ *row) tea.Cmd { return quotaForm(m, "user", "", "") }},
		{key: "e", desc: "edit", write: true, run: func(m *model, r *row) tea.Cmd {
			e := entityOf(r)
			if e == nil {
				return nil
			}
			return quotaEditForm(m, e)
		}},
		{key: "ctrl+d", desc: "remove", write: true, run: func(m *model, r *row) tea.Cmd {
			if r == nil {
				return nil
			}
			q := r.raw.(quotas.Quota)
			var ops []quotas.Op
			for _, k := range slices.Sorted(maps.Keys(q.Values)) {
				ops = append(ops, quotas.Op{Key: k, Remove: true})
			}
			return m.planThen(func(ctx context.Context) (*plan.Plan, error) {
				return quotas.PlanAlter(ctx, m.client.Admin, q.Entity, ops)
			}, nil)
		}},
		{key: "E", desc: "effective", run: func(m *model, _ *row) tea.Cmd { return effectiveForm(m) }},
	}
	return v
}

func quotaForm(m *model, typ, name, client string) tea.Cmd {
	var kv string
	f := huh.NewForm(huh.NewGroup(
		huh.NewSelect[string]().Title("Entity type").Options(huh.NewOptions("user", "client-id", "user+client-id", "ip")...).Value(&typ),
		huh.NewInput().Title("Name").Description("empty = <default>").Value(&name),
		huh.NewInput().Title("Client id").Description("for user+client-id; empty = <default>").Value(&client),
		huh.NewInput().Title("Quotas").Description("produce=10MiB/s, consume=50MiB/s, request=200, mutations=10, connections=20").Value(&kv),
	))
	return m.openForm("Set quota", f, func(m *model) tea.Cmd {
		ptr := func(s string) *string {
			if s == "" {
				return nil
			}
			return &s
		}
		var e quotas.Entity
		switch typ {
		case "user":
			e = quotas.Entity{{Type: "user", Name: ptr(name)}}
		case "client-id":
			e = quotas.Entity{{Type: "client-id", Name: ptr(name)}}
		case "ip":
			e = quotas.Entity{{Type: "ip", Name: ptr(name)}}
		default:
			e = quotas.Entity{{Type: "user", Name: ptr(name)}, {Type: "client-id", Name: ptr(client)}}
		}
		return applyQuotaKV(m, e, kv)
	})
}

func quotaEditForm(m *model, e quotas.Entity) tea.Cmd {
	var kv string
	f := huh.NewForm(huh.NewGroup(huh.NewInput().Title("Quotas for " + e.String()).Description("key=value, comma-separated; key= removes").Value(&kv)))
	return m.openForm("Edit quota", f, func(m *model) tea.Cmd { return applyQuotaKV(m, e, kv) })
}

func applyQuotaKV(m *model, e quotas.Entity, kv string) tea.Cmd {
	var ops []quotas.Op
	for _, a := range strings.Split(kv, ",") {
		k, val, ok := strings.Cut(strings.TrimSpace(a), "=")
		if !ok {
			continue
		}
		key, err := quotas.Key(k)
		if err != nil {
			m.flash(err.Error(), true)
			return nil
		}
		if val == "" {
			ops = append(ops, quotas.Op{Key: key, Remove: true})
			continue
		}
		f, err := quotas.ParseValue(key, val)
		if err != nil {
			m.flash(err.Error(), true)
			return nil
		}
		ops = append(ops, quotas.Op{Key: key, Value: f})
	}
	return m.planThen(func(ctx context.Context) (*plan.Plan, error) { return quotas.PlanAlter(ctx, m.client.Admin, e, ops) }, nil)
}

func effectiveForm(m *model) tea.Cmd {
	var user, client string
	f := huh.NewForm(huh.NewGroup(huh.NewInput().Title("User").Value(&user), huh.NewInput().Title("Client id").Value(&client)))
	return m.openForm("Effective quota", f, func(m *model) tea.Cmd {
		return func() tea.Msg {
			ctx, cancel := m.timeout()
			defer cancel()
			eff, err := quotas.Resolve(ctx, m.client.Admin, user, client)
			if err != nil {
				return flashMsg{err.Error(), true}
			}
			var parts []string
			for _, e := range eff {
				parts = append(parts, fmt.Sprintf("%s=%s (from %s)", e.Key, quotas.Format(e.Key, e.Value), e.From))
			}
			if len(parts) == 0 {
				return flashMsg{"no quotas apply", false}
			}
			return flashMsg{strings.Join(parts, " · "), false}
		}
	})
}
