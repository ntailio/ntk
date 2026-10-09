// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/acls"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/plan"
)

type aclRows []acls.ACL

func (r aclRows) Header(bool) []string {
	return []string{"PRINCIPAL", "PERM", "OPERATION", "RESOURCE", "PATTERN", "NAME", "HOST"}
}

func (r aclRows) Rows(bool) [][]string {
	var rows [][]string
	for _, a := range r {
		rows = append(rows, []string{a.Principal, a.Permission, a.Operation, a.ResourceType, a.Pattern, a.Name, a.Host})
	}
	return rows
}

type resourceFlags struct {
	topic, group, txnID, token, user string
	cluster, prefixed                bool
}

func (f *resourceFlags) add(cmd *cobra.Command, a *app) {
	fl := cmd.Flags()
	fl.StringVar(&f.topic, "topic", "", "topic resource")
	fl.StringVar(&f.group, "group", "", "group resource")
	fl.BoolVar(&f.cluster, "cluster", false, "the cluster resource")
	fl.StringVar(&f.txnID, "transactional-id", "", "transactional id resource")
	fl.StringVar(&f.token, "delegation-token", "", "delegation token resource")
	fl.StringVar(&f.user, "user", "", "user resource")
	fl.BoolVar(&f.prefixed, "prefixed", false, "prefixed resource pattern")
	_ = cmd.RegisterFlagCompletionFunc("topic", a.completeTopicArg)
	_ = cmd.RegisterFlagCompletionFunc("group", a.completeGroupArg)
}

func (f *resourceFlags) resolve(required bool) (rt, name, pattern string, err error) {
	set := 0
	for _, c := range []struct {
		on        bool
		rt, value string
	}{
		{f.topic != "", "TOPIC", f.topic}, {f.group != "", "GROUP", f.group}, {f.cluster, "CLUSTER", "kafka-cluster"},
		{f.txnID != "", "TRANSACTIONAL_ID", f.txnID}, {f.token != "", "DELEGATION_TOKEN", f.token}, {f.user != "", "USER", f.user},
	} {
		if c.on {
			rt, name = c.rt, c.value
			set++
		}
	}
	switch {
	case set > 1:
		return "", "", "", usageErr("use one of --topic, --group, --cluster, --transactional-id, --delegation-token, --user")
	case set == 0 && required:
		return "", "", "", usageErr("a resource is required: --topic, --group, --cluster, --transactional-id, --delegation-token, or --user")
	}
	pattern = "LITERAL"
	if f.prefixed {
		pattern = "PREFIXED"
	}
	return rt, name, pattern, nil
}

func (a *app) newACLCmd() *cobra.Command {
	cmd := groupCmd("acl", "Manage ACLs", "acls")
	cmd.AddCommand(a.newACLListCmd(), a.newACLCreateCmd(), a.newACLDeleteCmd(), a.newACLRecipeCmd(true), a.newACLRecipeCmd(false),
		a.newACLCheckCmd(), a.newACLExportCmd(), a.newACLImportCmd())
	return cmd
}

type aclFilterFlags struct {
	res                                        resourceFlags
	principal, resourceType, pattern, op, host string
	permission                                 string
}

func (f *aclFilterFlags) add(cmd *cobra.Command, a *app) {
	f.res.add(cmd, a)
	fl := cmd.Flags()
	fl.StringVar(&f.principal, "principal", "", "principal, e.g. User:bob")
	fl.StringVar(&f.resourceType, "resource-type", "", "topic, group, cluster, transactional_id, delegation_token, user")
	fl.StringVar(&f.pattern, "pattern", "", "literal, prefixed, or match (every ACL that applies to the resource)")
	fl.StringVar(&f.op, "operation", "", "operation, e.g. read")
	fl.StringVar(&f.host, "host", "", "host")
	fl.StringVar(&f.permission, "permission", "", "allow or deny")
	_ = cmd.RegisterFlagCompletionFunc("principal", a.completePrincipals)
	_ = cmd.RegisterFlagCompletionFunc("operation", cobra.FixedCompletions(lower(acls.Operations()), cobra.ShellCompDirectiveNoFileComp))
	_ = cmd.RegisterFlagCompletionFunc("resource-type", cobra.FixedCompletions(acls.ResourceTypes(), cobra.ShellCompDirectiveNoFileComp))
	_ = cmd.RegisterFlagCompletionFunc("pattern", cobra.FixedCompletions([]string{"literal", "prefixed", "match"}, cobra.ShellCompDirectiveNoFileComp))
}

func lower(s []string) []string {
	out := make([]string, len(s))
	for i, v := range s {
		out[i] = strings.ToLower(v)
	}
	return out
}

func (f *aclFilterFlags) filter() (acls.Filter, error) {
	rt, name, pattern, err := f.res.resolve(false)
	if err != nil {
		return acls.Filter{}, err
	}
	flt := acls.Filter{Principal: acls.NormalizePrincipal(f.principal), Host: f.host, ResourceType: rt, Name: name}
	if rt != "" && f.res.prefixed {
		flt.Pattern = pattern
	}
	if f.resourceType != "" {
		if flt.ResourceType, err = acls.ParseResourceType(f.resourceType); err != nil {
			return acls.Filter{}, usageErr("%v", err)
		}
	}
	if f.pattern != "" {
		if flt.Pattern, err = acls.ParsePattern(f.pattern); err != nil {
			return acls.Filter{}, usageErr("%v", err)
		}
	}
	if f.op != "" {
		if flt.Operation, err = acls.ParseOperation(f.op); err != nil {
			return acls.Filter{}, usageErr("%v", err)
		}
	}
	if f.permission != "" {
		flt.Permission = strings.ToUpper(f.permission)
		if flt.Permission != "ALLOW" && flt.Permission != "DENY" {
			return acls.Filter{}, usageErr("--permission must be allow or deny")
		}
	}
	return flt, nil
}

func (a *app) newACLListCmd() *cobra.Command {
	var f aclFilterFlags
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List ACLs (--topic t --pattern match shows every ACL that applies to t)",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			flt, err := f.filter()
			if err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			list, err := acls.List(cmd.Context(), s.cl.Client, flt)
			if err != nil {
				return err
			}
			return a.render(aclRows(list))
		},
	}
	f.add(cmd, a)
	return cmd
}

func (a *app) newACLCreateCmd() *cobra.Command {
	var res resourceFlags
	var principal, ops, host string
	var deny bool
	cmd := &cobra.Command{
		Use:     "create",
		Short:   "Create ACLs",
		Example: "  ntk acl create --principal User:svc-orders --operation write,describe --topic orders",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if principal == "" || ops == "" {
				return usageErr("--principal and --operation are required")
			}
			rt, name, pattern, err := res.resolve(true)
			if err != nil {
				return err
			}
			perm := "ALLOW"
			if deny {
				perm = "DENY"
			}
			var list []acls.ACL
			for _, o := range splitList(ops) {
				op, err := acls.ParseOperation(o)
				if err != nil {
					return usageErr("%v", err)
				}
				list = append(list, acls.ACL{Principal: acls.NormalizePrincipal(principal), Host: host, Permission: perm, Operation: op, ResourceType: rt, Pattern: pattern, Name: name})
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := acls.PlanCreate(cmd.Context(), s.cl.Client, list, "ACLs")
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	res.add(cmd, a)
	cmd.Flags().StringVar(&principal, "principal", "", "principal, e.g. User:bob")
	cmd.Flags().StringVar(&ops, "operation", "", "comma-separated operations")
	cmd.Flags().StringVar(&host, "host", "*", "host")
	cmd.Flags().BoolVar(&deny, "deny", false, "create DENY ACLs")
	_ = cmd.RegisterFlagCompletionFunc("principal", a.completePrincipals)
	_ = cmd.RegisterFlagCompletionFunc("operation", cobra.FixedCompletions(lower(acls.Operations()), cobra.ShellCompDirectiveNoFileComp))
	return cmd
}

func (a *app) selfPrincipal(s *session) string {
	p, _ := s.prof.ExpectedPrincipal()
	return p
}

func (a *app) newACLDeleteCmd() *cobra.Command {
	var f aclFilterFlags
	cmd := &cobra.Command{
		Use:     "delete",
		Aliases: []string{"rm"},
		Short:   "Delete every ACL matching the filters (shown first)",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			flt, err := f.filter()
			if err != nil {
				return err
			}
			if !flt.Targeted() {
				return usageErr("refusing to delete ACLs across the whole cluster: name a --principal or a resource (--topic, --group, --cluster, ...)")
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			list, err := acls.List(cmd.Context(), s.cl.Client, flt)
			if err != nil {
				return err
			}
			pl, err := acls.PlanDelete(cmd.Context(), s.cl.Client, list, "ACLs", a.selfPrincipal(s))
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	f.add(cmd, a)
	return cmd
}

func (a *app) newACLRecipeCmd(grant bool) *cobra.Command {
	var r acls.RecipeArgs
	var topicList, groupList, txnList, inList, outList []string
	use, short := "grant <recipe>", "Grant the ACLs for a common role"
	if !grant {
		use, short = "revoke <recipe>", "Remove exactly the ACLs a recipe grants"
	}
	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  "Recipes: " + strings.Join(acls.Recipes, ", "),
		Example: "  ntk acl grant consumer --principal User:svc-reporting --topic orders --group svc-reporting\n" +
			"  ntk acl grant streams-app --principal User:svc-agg --app-id agg --topic-in orders --topic-out totals",
		Args:      usageArgs(cobra.ExactArgs(1)),
		ValidArgs: acls.Recipes,
		RunE: func(cmd *cobra.Command, args []string) error {
			if r.Principal == "" {
				return usageErr("--principal is required")
			}
			r.Principal = acls.NormalizePrincipal(r.Principal)
			expand := func(v []string) []string {
				var out []string
				for _, x := range v {
					out = append(out, splitList(x)...)
				}
				return out
			}
			r.Topics, r.Groups, r.TxnIDs, r.TopicsIn, r.TopicsOut = expand(topicList), expand(groupList), expand(txnList), expand(inList), expand(outList)
			want, err := acls.Recipe(args[0], r)
			if err != nil {
				return usageErr("%v", err)
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			label := fmt.Sprintf("%s for %s", args[0], r.Principal)
			var pl *plan.Plan
			if grant {
				pl, err = acls.PlanCreate(cmd.Context(), s.cl.Client, want, "Grant "+label)
			} else {
				existing, lerr := acls.List(cmd.Context(), s.cl.Client, acls.Filter{Principal: r.Principal})
				if lerr != nil {
					return lerr
				}
				var del []acls.ACL
				for _, w := range want {
					if slices.Contains(existing, w) {
						del = append(del, w)
					}
				}
				pl, err = acls.PlanDelete(cmd.Context(), s.cl.Client, del, "Revoke "+label, a.selfPrincipal(s))
			}
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&r.Principal, "principal", "", "principal, e.g. User:svc-orders")
	fl.StringArrayVar(&topicList, "topic", nil, "topic (repeatable or comma-separated)")
	fl.StringArrayVar(&groupList, "group", nil, "consumer group (consumer recipe)")
	fl.StringArrayVar(&txnList, "txn-id", nil, "transactional id (transactional-producer recipe)")
	fl.StringVar(&r.AppID, "app-id", "", "application.id (streams-app recipe)")
	fl.StringArrayVar(&inList, "topic-in", nil, "input topics (streams-app recipe)")
	fl.StringArrayVar(&outList, "topic-out", nil, "output topics (streams-app recipe)")
	fl.BoolVar(&r.Prefixed, "prefixed", false, "use prefixed patterns for --topic/--group/--txn-id")
	fl.StringVar(&r.Host, "host", "*", "host")
	_ = cmd.RegisterFlagCompletionFunc("principal", a.completePrincipals)
	_ = cmd.RegisterFlagCompletionFunc("topic", a.completeTopicArg)
	_ = cmd.RegisterFlagCompletionFunc("group", a.completeGroupArg)
	return cmd
}

func (a *app) newACLCheckCmd() *cobra.Command {
	var res resourceFlags
	var principal, op, host string
	cmd := &cobra.Command{
		Use:     "check",
		Short:   "Can a principal do an operation on a resource? (evaluates ACLs locally)",
		Example: "  ntk acl check --principal User:svc-reporting --operation read --topic orders",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if principal == "" || op == "" {
				return usageErr("--principal and --operation are required")
			}
			rt, name, _, err := res.resolve(true)
			if err != nil {
				return err
			}
			o, err := acls.ParseOperation(op)
			if err != nil {
				return usageErr("%v", err)
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			all, err := acls.List(cmd.Context(), s.cl.Client, acls.Filter{ResourceType: rt, Name: name, Pattern: "MATCH"})
			if err != nil {
				return err
			}
			d := acls.Check(all, acls.NormalizePrincipal(principal), host, o, rt, name)
			if a.flags.output == "json" {
				if err := a.render(d); err != nil {
					return err
				}
			} else {
				fmt.Fprintln(a.stdout, d.Reason)
				for _, m := range d.By {
					fmt.Fprintf(a.stdout, "  %s\n", m)
				}
				if d.Allowed || len(d.By) > 0 {
					fmt.Fprintln(a.stdout, "(super.users and custom authorizers are not visible to ntk)")
				}
			}
			if !d.Allowed {
				return exitcode.With(exitcode.CheckFailed, errors.New("not allowed"))
			}
			return nil
		},
	}
	res.add(cmd, a)
	cmd.Flags().StringVar(&principal, "principal", "", "principal, e.g. User:bob")
	cmd.Flags().StringVar(&op, "operation", "", "operation, e.g. read")
	cmd.Flags().StringVar(&host, "host", "*", "client host")
	_ = cmd.RegisterFlagCompletionFunc("principal", a.completePrincipals)
	return cmd
}

func (a *app) newACLExportCmd() *cobra.Command {
	var f aclFilterFlags
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export ACLs as JSON, grouped by principal",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			flt, err := f.filter()
			if err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			list, err := acls.List(cmd.Context(), s.cl.Client, flt)
			if err != nil {
				return err
			}
			enc := json.NewEncoder(a.stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(acls.Export(list))
		},
	}
	f.add(cmd, a)
	return cmd
}

func (a *app) newACLImportCmd() *cobra.Command {
	var file, scope string
	var prune bool
	cmd := &cobra.Command{
		Use:   "import -f acls.json",
		Short: "Create missing ACLs from an export; --prune --scope also removes extra ones",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if prune && scope == "" {
				return usageErr("--prune needs --scope (e.g. 'User:svc-*') so unrelated ACLs are never deleted")
			}
			var b []byte
			var err error
			if file == "-" {
				b, err = readAll(a.stdin)
			} else {
				b, err = os.ReadFile(file)
			}
			if err != nil {
				return err
			}
			var entries []acls.ExportEntry
			if err := json.Unmarshal(b, &entries); err != nil {
				return usageErr("%s: not an ACL export: %v", file, err)
			}
			desired, err := acls.Import(entries)
			if err != nil {
				return usageErr("%v", err)
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := acls.PlanImport(cmd.Context(), s.cl.Client, desired, prune, scope)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "ACL export file (- for stdin)")
	cmd.Flags().BoolVar(&prune, "prune", false, "delete ACLs within --scope that are not in the file")
	cmd.Flags().StringVar(&scope, "scope", "", "principal glob limiting --prune, e.g. 'User:svc-*'")
	_ = cmd.MarkFlagRequired("file")
	return cmd
}

func fetchPrincipals(ctx context.Context, s *session) ([]string, error) {
	list, err := acls.List(ctx, s.cl.Client, acls.Filter{})
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range list {
		if !slices.Contains(out, a.Principal) {
			out = append(out, a.Principal)
		}
	}
	if users, err := s.cl.Admin.DescribeUserSCRAMs(ctx); err == nil {
		for u := range users {
			if p := "User:" + u; !slices.Contains(out, p) {
				out = append(out, p)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

func (a *app) completePrincipals(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return filterPrefix(a.cached(cmd.Context(), "principals", fetchPrincipals), toComplete, nil), cobra.ShellCompDirectiveNoFileComp
}
