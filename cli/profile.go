// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/profile"
)

func (a *app) newProfileCmd() *cobra.Command {
	cmd := groupCmd("profile", "Manage connection profiles", "profiles")
	cmd.AddCommand(
		a.newProfileCreateCmd(),
		a.newProfileEditCmd(),
		a.newProfileDeleteCmd(),
		a.newProfileRenameCmd(false),
		a.newProfileRenameCmd(true),
		a.newProfileSwitchCmd(),
		a.newProfileListCmd(),
		a.newProfileCurrentCmd(),
		a.newProfilePathCmd(),
		a.newProfileShowCmd(),
		a.newProfileSetCmd(),
		a.newProfileTestCmd(),
		a.newProfileFixPermsCmd(),
	)
	return cmd
}

type profileRow struct {
	Name             string   `json:"name"`
	Active           bool     `json:"active"`
	Description      string   `json:"description,omitempty"`
	BootstrapServers []string `json:"bootstrap_servers"`
	Auth             string   `json:"auth"`
	SecurityProtocol string   `json:"security_protocol"`
	MTLS             bool     `json:"mtls"`
	ReadOnly         bool     `json:"read_only"`
	Labels           []string `json:"labels,omitempty"`
}

type profileRows []profileRow

func (r profileRows) Header(wide bool) []string {
	h := []string{"", "NAME", "BOOTSTRAP", "AUTH", "TLS", "FLAGS"}
	if wide {
		h = append(h, "DESCRIPTION")
	}
	return h
}

func (r profileRows) Rows(wide bool) [][]string {
	rows := make([][]string, 0, len(r))
	for _, p := range r {
		marker := ""
		if p.Active {
			marker = "*"
		}
		bootstrap := strings.Join(p.BootstrapServers, ",")
		if !wide && len(p.BootstrapServers) > 1 {
			bootstrap = fmt.Sprintf("%s (+%d)", p.BootstrapServers[0], len(p.BootstrapServers)-1)
		}
		tls := "-"
		switch {
		case p.MTLS:
			tls = "mtls"
		case p.SecurityProtocol == "SSL" || p.SecurityProtocol == "SASL_SSL":
			tls = "tls"
		}
		var flags []string
		if p.ReadOnly {
			flags = append(flags, "read-only")
		}
		flags = append(flags, p.Labels...)
		row := []string{marker, p.Name, bootstrap, p.Auth, tls, strings.Join(flags, ", ")}
		if wide {
			row = append(row, p.Description)
		}
		rows = append(rows, row)
	}
	return rows
}

func (r profileRows) Names() []string {
	names := make([]string, len(r))
	for i, p := range r {
		names[i] = p.Name
	}
	return names
}

func (a *app) newProfileListCmd() *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List profiles; * marks the active one",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			rows := profileRows{}
			for _, name := range s.Names() {
				p := s.File.Profiles[name]
				rows = append(rows, profileRow{
					Name:             name,
					Active:           name == s.File.Active,
					Description:      p.Description,
					BootstrapServers: p.BootstrapServers,
					Auth:             p.Auth.Mechanism,
					SecurityProtocol: p.SecurityProtocol(),
					MTLS:             p.TLS.HasClientCert(),
					ReadOnly:         p.ReadOnly,
					Labels:           p.Labels,
				})
			}
			return a.render(rows)
		},
	}
}

func (a *app) newProfileCurrentCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "current",
		Short: "Print the name of the profile in use",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			name, _, err := a.selectProfile(s, a.flags.profile)
			if err != nil {
				return err
			}
			fmt.Fprintln(a.stdout, name)
			return nil
		},
	}
}

func (a *app) newProfilePathCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "path",
		Short: "Print the profile file path",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			path, err := profile.ResolvePath(a.flags.profilePath)
			if err != nil {
				return err
			}
			fmt.Fprintln(a.stdout, path)
			return nil
		},
	}
}

func (a *app) newProfileShowCmd() *cobra.Command {
	var reveal bool
	cmd := &cobra.Command{
		Use:               "show [name]",
		Short:             "Show a profile (secrets masked unless --reveal)",
		Args:              usageArgs(cobra.MaximumNArgs(1)),
		ValidArgsFunction: a.completeProfileArg,
		RunE: func(_ *cobra.Command, args []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			sel := a.flags.profile
			if len(args) == 1 {
				sel = args[0]
			}
			name, p, err := a.selectProfile(s, sel)
			if err != nil {
				return err
			}
			if !reveal {
				p = p.Redacted()
			}
			f := a.flags.output
			if f == "table" || f == "wide" {
				a.flags.output = "json"
			}
			defer func() { a.flags.output = f }()
			return a.render(struct {
				Name string `json:"name"`
				*profile.Profile
			}{name, p})
		},
	}
	cmd.Flags().BoolVar(&reveal, "reveal", false, "show secrets in clear text")
	return cmd
}

func (a *app) newProfileSetCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "set <name>",
		Short:             "Set the active profile",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeProfileArg,
		RunE: func(_ *cobra.Command, args []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			if _, ok := s.File.Profiles[args[0]]; !ok {
				return exitcode.With(exitcode.NotFound, fmt.Errorf("%w: %q (have: %s)", profile.ErrNotFound, args[0], strings.Join(s.Names(), ", ")))
			}
			s.File.Active = args[0]
			if err := s.Save(); err != nil {
				return err
			}
			fmt.Fprintf(a.stderr, "Active profile: %s\n", args[0])
			return nil
		},
	}
}

func (a *app) newProfileFixPermsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "fix-perms",
		Short: "Set the profile file mode to 0600",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			path, err := profile.ResolvePath(a.flags.profilePath)
			if err != nil {
				return err
			}
			if err := profile.FixMode(path); err != nil {
				return err
			}
			fmt.Fprintf(a.stderr, "%s: mode set to 0600\n", path)
			return nil
		},
	}
}

func (a *app) completeProfileArg(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) > 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	return a.completeProfiles(cmd, args, toComplete)
}

func (a *app) completeProfiles(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	s, err := a.loadStore()
	if err != nil {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	var out []string
	for _, n := range s.Names() {
		if strings.HasPrefix(n, toComplete) {
			out = append(out, n+"\t"+s.File.Profiles[n].Description)
		}
	}
	return out, cobra.ShellCompDirectiveNoFileComp
}
