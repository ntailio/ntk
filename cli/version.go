// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/factualtech/ntk/buildinfo"
)

type versionInfo buildinfo.Info

func (v versionInfo) Header(bool) []string { return []string{"VERSION", "COMMIT", "GO"} }

func (v versionInfo) Rows(bool) [][]string {
	commit := v.Commit
	if len(commit) > 12 {
		commit = commit[:12]
	}
	if v.Modified {
		commit += " (modified)"
	}
	return [][]string{{v.Version, commit, v.GoVersion}}
}

func (a *app) newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the ntk version",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(*cobra.Command, []string) error {
			if a.flags.output == "table" {
				info := buildinfo.Get()
				fmt.Fprintf(a.stdout, "ntk %s\n", info.Version)
				return nil
			}
			return a.render(versionInfo(buildinfo.Get()))
		},
	}
}
