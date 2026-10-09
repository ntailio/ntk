// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"maps"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/factualtech/ntk/quotas"
)

type entityFlags struct {
	user, client, ip                      string
	userDefault, clientDefault, ipDefault bool
}

func (f *entityFlags) add(cmd *cobra.Command, a *app) {
	fl := cmd.Flags()
	defer func() {
		_ = cmd.RegisterFlagCompletionFunc("user", a.completeQuotaEntity("user"))
		_ = cmd.RegisterFlagCompletionFunc("client-id", a.completeQuotaEntity("client-id"))
	}()
	fl.StringVar(&f.user, "user", "", "user entity")
	fl.StringVar(&f.client, "client-id", "", "client-id entity")
	fl.StringVar(&f.ip, "ip", "", "IP entity (connection_creation_rate only)")
	fl.BoolVar(&f.userDefault, "user-default", false, "the <default> user entity")
	fl.BoolVar(&f.clientDefault, "client-default", false, "the <default> client-id entity")
	fl.BoolVar(&f.ipDefault, "ip-default", false, "the <default> IP entity")
}

func (f *entityFlags) filter() quotas.Filter {
	var q quotas.Filter
	if f.user != "" {
		q.User = &f.user
	}
	if f.client != "" {
		q.ClientID = &f.client
	}
	if f.ip != "" {
		q.IP = &f.ip
	}
	q.UserDefault, q.ClientDefault, q.IPDef = f.userDefault, f.clientDefault, f.ipDefault
	return q
}

func (f *entityFlags) entity() (quotas.Entity, error) {
	var e quotas.Entity
	add := func(typ, name string, def bool) error {
		switch {
		case name != "" && def:
			return usageErr("use --%s or --%s-default, not both", typ, typ)
		case def:
			e = append(e, quotas.Component{Type: typ})
		case name != "":
			n := name
			e = append(e, quotas.Component{Type: typ, Name: &n})
		}
		return nil
	}
	if err := add("user", f.user, f.userDefault); err != nil {
		return nil, err
	}
	if err := add("client-id", f.client, f.clientDefault); err != nil {
		return nil, err
	}
	if err := add("ip", f.ip, f.ipDefault); err != nil {
		return nil, err
	}
	if len(e) == 0 {
		return nil, usageErr("an entity is required: --user, --client-id, --ip (or --*-default)")
	}
	return e, nil
}

type quotaRows []quotas.Quota

func (r quotaRows) Header(bool) []string {
	return []string{"ENTITY", "PRODUCE", "CONSUME", "REQUEST", "MUTATIONS", "CONNECTIONS"}
}

func (r quotaRows) Rows(bool) [][]string {
	var rows [][]string
	for _, q := range r {
		row := []string{q.Entity.String()}
		for _, k := range quotas.Keys {
			if v, ok := q.Values[k]; ok {
				row = append(row, quotas.Format(k, v))
			} else {
				row = append(row, "-")
			}
		}
		rows = append(rows, row)
	}
	return rows
}

type effectiveRows []quotas.Effective

func (r effectiveRows) Header(bool) []string { return []string{"KEY", "VALUE", "FROM"} }

func (r effectiveRows) Rows(bool) [][]string {
	var rows [][]string
	for _, e := range r {
		rows = append(rows, []string{e.Key, quotas.Format(e.Key, e.Value), e.From})
	}
	return rows
}

func (a *app) newQuotaCmd() *cobra.Command {
	cmd := groupCmd("quota", "Manage client quotas", "quotas")

	var lf entityFlags
	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List quota entities",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			qs, err := quotas.List(cmd.Context(), s.cl.Admin, lf.filter())
			if err != nil {
				return err
			}
			return a.render(quotaRows(qs))
		},
	}
	lf.add(list, a)

	var sf entityFlags
	set := &cobra.Command{
		Use:     "set key=value...",
		Short:   "Set quotas (produce=10MiB/s consume=50MiB/s request=200 mutations=10 connections=20)",
		Example: "  ntk quota set --user svc-orders produce=10MiB/s\n  ntk quota set --user svc-orders --client-id backfill produce=2MiB/s consume=5MiB/s",
		Args:    usageArgs(cobra.MinimumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := sf.entity()
			if err != nil {
				return err
			}
			var ops []quotas.Op
			for _, kv := range args {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					return usageErr("%q is not key=value", kv)
				}
				key, err := quotas.Key(k)
				if err != nil {
					return usageErr("%v", err)
				}
				f, err := quotas.ParseValue(key, v)
				if err != nil {
					return usageErr("%v", err)
				}
				ops = append(ops, quotas.Op{Key: key, Value: f})
			}
			return a.alterQuota(cmd, e, ops)
		},
		ValidArgsFunction: quotaKeyCompletion("="),
	}
	sf.add(set, a)

	var uf entityFlags
	unset := &cobra.Command{
		Use:               "unset key...",
		Short:             "Remove quotas",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: quotaKeyCompletion(""),
		RunE: func(cmd *cobra.Command, args []string) error {
			e, err := uf.entity()
			if err != nil {
				return err
			}
			var ops []quotas.Op
			for _, k := range args {
				key, err := quotas.Key(k)
				if err != nil {
					return usageErr("%v", err)
				}
				ops = append(ops, quotas.Op{Key: key, Remove: true})
			}
			return a.alterQuota(cmd, e, ops)
		},
	}
	uf.add(unset, a)

	var user, client string
	effective := &cobra.Command{
		Use:   "effective --user U [--client-id C]",
		Short: "Resolve which quota applies, following Kafka's precedence",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if user == "" && client == "" {
				return usageErr("--user and/or --client-id is required")
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			eff, err := quotas.Resolve(cmd.Context(), s.cl.Admin, user, client)
			if err != nil {
				return err
			}
			return a.render(effectiveRows(eff))
		},
	}
	effective.Flags().StringVar(&user, "user", "", "user")
	effective.Flags().StringVar(&client, "client-id", "", "client id")
	_ = effective.RegisterFlagCompletionFunc("user", a.completeQuotaEntity("user"))
	_ = effective.RegisterFlagCompletionFunc("client-id", a.completeQuotaEntity("client-id"))

	cmd.AddCommand(list, set, unset, effective)
	return cmd
}

func quotaKeyCompletion(suffix string) cobra.CompletionFunc {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
		var out []string
		for _, k := range append(slices.Sorted(maps.Keys(map[string]bool{"produce": true, "consume": true, "request": true, "mutations": true, "connections": true})), quotas.Keys...) {
			if strings.HasPrefix(k, toComplete) {
				out = append(out, k+suffix)
			}
		}
		return out, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	}
}

func (a *app) alterQuota(cmd *cobra.Command, e quotas.Entity, ops []quotas.Op) error {
	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	pl, err := quotas.PlanAlter(cmd.Context(), s.cl.Admin, e, ops)
	if err != nil {
		return err
	}
	return a.run(cmd.Context(), s, pl)
}

func (a *app) completeQuotaEntity(typ string) cobra.CompletionFunc {
	return func(cmd *cobra.Command, _ []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
		items := a.cached(cmd.Context(), "quota-"+typ, func(ctx context.Context, s *session) ([]string, error) {
			qs, err := quotas.List(ctx, s.cl.Admin, quotas.Filter{})
			if err != nil {
				return nil, err
			}
			var out []string
			for _, q := range qs {
				for _, c := range q.Entity {
					if c.Type == typ && c.Name != nil && !slices.Contains(out, *c.Name) {
						out = append(out, *c.Name)
					}
				}
			}
			return out, nil
		})
		return filterPrefix(items, toComplete, nil), cobra.ShellCompDirectiveNoFileComp
	}
}
