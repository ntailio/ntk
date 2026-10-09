// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"charm.land/huh/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/factualtech/ntk/exitcode"
	"github.com/factualtech/ntk/groups"
	"github.com/factualtech/ntk/kafka"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/principals"
	"github.com/factualtech/ntk/profile"
	"github.com/factualtech/ntk/quotas"
)

type userRows []principals.User

func (r userRows) Header(bool) []string { return []string{"USER", "MECHANISMS"} }

func (r userRows) Rows(bool) [][]string {
	var rows [][]string
	for _, u := range r {
		var m []string
		for _, c := range u.Credentials {
			m = append(m, fmt.Sprintf("%s (%d)", c.Mechanism, c.Iterations))
		}
		rows = append(rows, []string{u.Name, strings.Join(m, ", ")})
	}
	return rows
}

func (r userRows) Names() []string {
	var n []string
	for _, u := range r {
		n = append(n, u.Name)
	}
	return n
}

func fetchUsers(ctx context.Context, s *session) ([]string, error) {
	us, err := principals.ListUsers(ctx, s.cl.Admin)
	if err != nil {
		return nil, err
	}
	return userRows(us).Names(), nil
}

func (a *app) completeUserArg(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return a.complete("users", fetchUsers, false)(cmd, args, toComplete)
}

func (a *app) newUserCmd() *cobra.Command {
	cmd := groupCmd("user", "Manage SCRAM users", "users")
	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List SCRAM users and their mechanisms",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			us, err := principals.ListUsers(cmd.Context(), s.cl.Admin)
			if err != nil {
				return err
			}
			return a.render(userRows(us))
		},
	}
	cmd.AddCommand(list, a.newUserUpsertCmd(false), a.newUserUpsertCmd(true), a.newUserDeleteCmd())
	return cmd
}

func (a *app) readNewPassword(ctx context.Context, stdin, generate bool) (string, bool, error) {
	switch {
	case generate:
		b := make([]byte, 24)
		if _, err := rand.Read(b); err != nil {
			return "", false, err
		}
		return base64.RawURLEncoding.EncodeToString(b), true, nil
	case stdin:
		p, err := readSecret(a.stdin)
		if err == nil && p == "" {
			err = errors.New("empty password on stdin")
		}
		return p, false, err
	}
	if !a.canPrompt() {
		return "", false, usageErr("no password: use --password-stdin or --generate when not in a terminal")
	}
	var pw, confirm string
	form := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Password").EchoMode(huh.EchoModePassword).Value(&pw).Validate(huh.ValidateNotEmpty()),
		huh.NewInput().Title("Confirm").EchoMode(huh.EchoModePassword).Value(&confirm).Validate(func(s string) error {
			if s != pw {
				return errors.New("passwords do not match")
			}
			return nil
		}),
	))
	if err := a.runForm(ctx, form); err != nil {
		return "", false, err
	}
	return pw, false, nil
}

func (a *app) newUserUpsertCmd(rotate bool) *cobra.Command {
	var mech, profileFrom string
	var iterations int32
	var stdin, generate bool
	use, short := "create <name>", "Create a SCRAM credential"
	if rotate {
		use, short = "rotate <name>", "Set a new password for an existing SCRAM credential"
	}
	cmd := &cobra.Command{
		Use:               use,
		Short:             short,
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeUserArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			if a.flags.dryRun && a.flags.output == "json" {
				return usageErr("plans for SCRAM credentials can't be saved: they would contain the password")
			}
			m, err := principals.ParseMechanism(mech)
			if err != nil {
				return usageErr("%v", err)
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pw, generated := "", false
			if !a.flags.dryRun {
				if pw, generated, err = a.readNewPassword(cmd.Context(), stdin, generate); err != nil {
					return err
				}
			}
			pl, err := principals.PlanUpsert(cmd.Context(), s.cl.Admin, args[0], m, iterations, pw, rotate)
			if err != nil {
				return err
			}
			own := s.prof.Auth.Username == args[0] && strings.HasPrefix(s.prof.Auth.Mechanism, "scram")
			if rotate && own {
				pl.Warn("this is the credential profile %q uses", s.name)
			}
			if err := a.run(cmd.Context(), s, pl); err != nil || a.flags.dryRun {
				return err
			}
			if generated {
				fmt.Fprintln(a.stdout, pw)
				fmt.Fprintln(a.stderr, "(generated password printed to stdout; it is not shown again)")
			}
			if rotate && own && a.canPrompt() && a.confirm(cmd.Context(), fmt.Sprintf("Update the password in profile %q?", s.name), true) {
				st, err := a.loadStore()
				if err != nil {
					return err
				}
				st.File.Profiles[s.name].Auth.Password = pw
				if err := st.Save(); err != nil {
					return err
				}
				fmt.Fprintf(a.stderr, "Updated profile %q\n", s.name)
			}
			if !rotate {
				if profileFrom != "" {
					return a.saveUserProfile(profileFrom, args[0], mech, pw)
				}
				fmt.Fprintf(a.stderr, "Next: ntk acl grant consumer --principal User:%s --topic ... --group ...\n", args[0])
			}
			return nil
		},
	}
	fl := cmd.Flags()
	fl.StringVar(&mech, "mechanism", "scram-sha-512", "scram-sha-256 or scram-sha-512")
	fl.Int32Var(&iterations, "iterations", 8192, "SCRAM iterations (4096-16384)")
	fl.BoolVar(&stdin, "password-stdin", false, "read the password from stdin")
	fl.BoolVar(&generate, "generate", false, "generate a strong password and print it once to stdout")
	if !rotate {
		fl.StringVar(&profileFrom, "profile-from", "", "also save a profile <base>-<name> that copies <base> with these credentials")
		_ = cmd.RegisterFlagCompletionFunc("profile-from", a.completeProfiles)
	}
	_ = cmd.RegisterFlagCompletionFunc("mechanism", cobra.FixedCompletions([]string{"scram-sha-256", "scram-sha-512"}, cobra.ShellCompDirectiveNoFileComp))
	return cmd
}

func (a *app) saveUserProfile(base, user, mech, pw string) error {
	st, err := a.loadStore()
	if err != nil {
		return err
	}
	b, ok := st.File.Profiles[base]
	if !ok {
		return exitcode.With(exitcode.NotFound, fmt.Errorf("%w: %q", profile.ErrNotFound, base))
	}
	p := *b
	p.Auth = profile.Auth{Mechanism: strings.ToLower(mech), Username: user, Password: pw}
	p.Principal, p.Description = "", fmt.Sprintf("%s as User:%s", base, user)
	name := base + "-" + user
	st.File.Profiles[name] = &p
	if err := st.Save(); err != nil {
		return err
	}
	fmt.Fprintf(a.stderr, "Saved profile %q (try: ntk -p %s profile test)\n", name, name)
	return nil
}

func (a *app) newUserDeleteCmd() *cobra.Command {
	var mech string
	var withACLs bool
	cmd := &cobra.Command{
		Use:               "delete <name>",
		Aliases:           []string{"rm"},
		Short:             "Delete SCRAM credentials (all mechanisms unless --mechanism)",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeUserArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			var mechs []kadm.ScramMechanism
			if mech != "" {
				m, err := principals.ParseMechanism(mech)
				if err != nil {
					return usageErr("%v", err)
				}
				mechs = append(mechs, m)
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := principals.PlanDelete(cmd.Context(), s.cl.Client, s.cl.Admin, args[0], mechs, withACLs)
			if err != nil {
				return err
			}
			if s.prof.Auth.Username == args[0] {
				pl.Warn("this is the credential profile %q uses: it will stop working", s.name)
				pl.Class, pl.Confirm = plan.Destructive, args[0]
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	cmd.Flags().StringVar(&mech, "mechanism", "", "only this mechanism")
	cmd.Flags().BoolVar(&withACLs, "with-acls", false, "also delete the principal's ACLs")
	return cmd
}

type principalRows []principals.Principal

func (r principalRows) Header(bool) []string { return []string{"PRINCIPAL", "SCRAM", "ACLS", "QUOTAS"} }

func (r principalRows) Rows(bool) [][]string {
	var rows [][]string
	for _, p := range r {
		rows = append(rows, []string{p.Name, orDash(strings.Join(p.SCRAM, ",")), itoa(p.ACLs), orDash(strings.Join(p.Quotas, "; "))})
	}
	return rows
}

func (r principalRows) Names() []string {
	var n []string
	for _, p := range r {
		n = append(n, p.Name)
	}
	return n
}

func (a *app) newPrincipalCmd() *cobra.Command {
	cmd := groupCmd("principal", "Principal-centric view: SCRAM, ACLs, quotas", "principals")
	var sources string
	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "Principals found in ACLs, quotas, and SCRAM users",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			ps, err := principals.List(cmd.Context(), s.cl.Client, s.cl.Admin, splitList(sources))
			if err != nil {
				return err
			}
			return a.render(principalRows(ps))
		},
	}
	list.Flags().StringVar(&sources, "source", "", "only principals from: acl, quota, scram")
	describe := &cobra.Command{
		Use:               "describe <principal>",
		Aliases:           []string{"get"},
		Short:             "Everything about one principal: SCRAM, quotas, ACLs, effective access",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completePrincipals,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			d, err := principals.Describe(cmd.Context(), s.cl.Client, s.cl.Admin, args[0], func(ctx context.Context, prefix string) []string {
				return groupsWithClientPrefix(ctx, s, prefix)
			})
			if err != nil {
				return err
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				return a.render(d)
			}
			w := a.stdout
			fmt.Fprintf(w, "Principal:  %s\n", d.Principal)
			var creds []string
			for _, c := range d.SCRAM {
				creds = append(creds, fmt.Sprintf("%s (%d)", c.Mechanism, c.Iterations))
			}
			fmt.Fprintf(w, "SCRAM:      %s\n", orDash(strings.Join(creds, ", ")))
			if len(d.Quotas) == 0 {
				fmt.Fprintln(w, "Quotas:     -")
			}
			for i, q := range d.Quotas {
				label := "Quotas:    "
				if i > 0 {
					label = "           "
				}
				fmt.Fprintf(w, "%s %s: %s\n", label, q.Entity, q.ValueString())
			}
			if len(d.Effective) > 0 {
				var eff []string
				for _, e := range d.Effective {
					eff = append(eff, e.Key+"="+quotas.Format(e.Key, e.Value))
				}
				fmt.Fprintf(w, "            (effective: %s)\n", strings.Join(eff, ", "))
			}
			fmt.Fprintf(w, "ACLs (%d):\n", len(d.ACLs))
			for _, acl := range d.ACLs {
				fmt.Fprintf(w, "  %s %-16s %s host=%s\n", acl.Permission, acl.Operation, acl.Resource(), acl.Host)
			}
			if len(d.Access) > 0 {
				fmt.Fprintln(w, "Effective access (summary):")
				for _, x := range d.Access {
					fmt.Fprintf(w, "  %s\n", x)
				}
			}
			if len(d.Groups) > 0 {
				fmt.Fprintf(w, "Active groups with matching client ids: %s\n", strings.Join(d.Groups, ", "))
			}
			return nil
		},
	}
	cmd.AddCommand(list, describe)
	return cmd
}

func groupsWithClientPrefix(ctx context.Context, s *session, prefix string) []string {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	gs, err := groups.List(ctx, s.cl.Client, s.cl.Admin, groups.ListOptions{})
	if err != nil {
		return nil
	}
	var out []string
	for _, g := range gs {
		if g.Members == 0 {
			continue
		}
		d, err := groups.Describe(ctx, s.cl.Client, s.cl.Admin, g.Name)
		if err != nil {
			continue
		}
		for _, m := range d.Members_ {
			if strings.HasPrefix(m.ClientID, prefix) {
				out = append(out, fmt.Sprintf("%s (%d members)", g.Name, d.Members))
				break
			}
		}
	}
	return out
}

func (a *app) newWhoamiCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "whoami",
		Short: "Show who the active profile authenticates as, and what the cluster allows it",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			req := kmsg.NewPtrDescribeClusterRequest()
			req.IncludeClusterAuthorizedOperations = true
			md, err := req.RequestWith(cmd.Context(), s.cl.Client)
			if err != nil {
				return err
			}
			principal, derived := s.prof.ExpectedPrincipal()
			how := "configured"
			if derived {
				how = "derived"
				if s.prof.Auth.Username != "" {
					how += "; SASL username " + s.prof.Auth.Username
				}
			}
			if principal == "" {
				switch {
				case s.prof.TLS.HasClientCert():
					principal, how = "User:<client cert subject>", "mapped by the broker's ssl.principal.mapping.rules"
					if p, err := kafka.CertPrincipal(s.prof.TLS, s.resolve); err == nil {
						principal, how = p, "client certificate subject; the broker's ssl.principal.mapping.rules may change it"
					}
				default:
					principal, how = "User:ANONYMOUS", "no authentication"
				}
			}
			var ops []string
			for _, o := range kadm.DecodeACLOperations(md.ClusterAuthorizedOperations) {
				ops = append(ops, strings.ToUpper(o.String()))
			}
			slices.Sort(ops)
			info := struct {
				Profile    string   `json:"profile"`
				Principal  string   `json:"principal"`
				How        string   `json:"principal_source"`
				Auth       string   `json:"auth"`
				Cluster    string   `json:"cluster_id"`
				ClusterOps []string `json:"authorized_cluster_operations"`
			}{s.name, principal, how, s.prof.SecurityProtocol() + " " + s.prof.Auth.Mechanism, md.ClusterID, ops}
			if a.flags.output == "json" {
				return a.render(info)
			}
			auth := info.Auth
			if s.prof.TLS.HasClientCert() {
				auth += ", client certificate"
			}
			fmt.Fprintf(a.stdout, "Profile:    %s\nPrincipal:  %s (%s)\nAuth:       %s\nCluster:    %s · authorized cluster operations: %s\n",
				s.name, principal, how, auth, md.ClusterID, orDash(strings.Join(ops, ", ")))
			return nil
		},
	}
}
