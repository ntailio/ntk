// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/profile"
	"github.com/ntailio/ntk/tui"
)

func (a *app) runTUI(ctx context.Context, view, arg string) error {
	s, err := a.loadStore()
	if errors.Is(err, profile.ErrNoProfiles) && a.canPick() {
		fmt.Fprintln(a.stderr, "No profiles yet. Let's create one.")
		create := a.newProfileCreateCmd()
		create.SetArgs([]string{})
		create.SetIn(a.stdin)
		create.SetOut(a.stdout)
		create.SetErr(a.stderr)
		if err := create.ExecuteContext(ctx); err != nil {
			return err
		}
		a.store = nil
		s, err = a.loadStore()
	}
	if err != nil {
		return err
	}
	name, _, err := a.selectProfile(s, a.flags.profile)
	if err != nil {
		return err
	}
	return tui.Run(ctx, tui.Options{Store: s, Profile: name, StartView: view, StartArg: arg})
}

var tuiViews = []string{"topics", "groups", "brokers", "cluster", "acls", "principals", "users", "quotas", "tx", "health",
	"consume", "produce", "topmon", "grpmon", "top", "gtop", "profiles"}

func (a *app) newTUICmd() *cobra.Command {
	return &cobra.Command{
		Use:       "tui [view] [name]",
		Short:     "Open the terminal UI, optionally at a view (e.g. `ntk tui groups svc-orders`)",
		Args:      usageArgs(cobra.MaximumNArgs(2)),
		ValidArgs: tuiViews,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !a.interactive() {
				return usageErr("the TUI needs a terminal")
			}
			view, arg := "", ""
			if len(args) > 0 {
				view = args[0]
			}
			if len(args) > 1 {
				arg = args[1]
			}
			return a.runTUI(cmd.Context(), view, arg)
		},
	}
}
