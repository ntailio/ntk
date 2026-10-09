// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/health"
)

func (a *app) newHealthCmd() *cobra.Command {
	var o health.Options
	var checks, failOn string
	var watch bool
	var interval time.Duration
	cmd := &cobra.Command{
		Use:   "health",
		Short: "Is the cluster OK? Exits 5 when a check reaches --fail-on",
		Long:  "Checks: " + strings.Join(health.AllChecks, ", ") + " (groups only with --groups).",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			level, err := health.ParseLevel(failOn)
			if err != nil {
				return usageErr("%v", err)
			}
			o.Checks = splitList(checks)
			for _, c := range o.Checks {
				if c != "all" && !slices.Contains(health.AllChecks, c) {
					return usageErr("unknown check %q (use all, or: %s)", c, strings.Join(health.AllChecks, ", "))
				}
				if c == "groups" && o.Groups == "" {
					return usageErr("the groups check needs --groups <glob>")
				}
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			if !watch {
				r, err := health.Run(cmd.Context(), s.cl.Client, s.cl.Admin, o)
				if err != nil {
					return exitcode.With(exitcode.Connection, err)
				}
				if a.streaming() {
					if err := a.render(r); err != nil {
						return err
					}
				} else {
					a.printHealth(r)
				}
				if r.Status >= level {
					return exitcode.With(exitcode.CheckFailed, fmt.Errorf("cluster status %s", r.Status))
				}
				return nil
			}
			prev := map[string]health.Check{}
			return a.loop(cmd.Context(), interval, func(ctx context.Context, first bool) error {
				r, err := health.Run(ctx, s.cl.Client, s.cl.Admin, o)
				if err != nil {
					fmt.Fprintf(a.stdout, "%s error: %v\n", time.Now().Format("15:04:05"), err)
					return nil
				}
				if first && !a.streaming() {
					a.printHealth(r)
				}
				for _, c := range r.Checks {
					p, seen := prev[c.Name]
					if seen && (p.Level != c.Level || p.Message != c.Message) {
						if a.streaming() {
							_ = a.jsonLine(map[string]any{"at": r.At, "check": c.Name, "from": p, "to": c})
						} else {
							fmt.Fprintf(a.stdout, "%s %s %s → %s (%s)\n", r.At.Format("15:04:05"), c.Name, p.Message, c.Message, c.Level)
						}
					}
					prev[c.Name] = c
				}
				return nil
			})
		},
	}
	f := cmd.Flags()
	f.StringVar(&checks, "checks", "all", "comma-separated checks to run")
	f.IntVar(&o.ExpectBrokers, "expect-brokers", 0, "critical if fewer brokers are registered")
	f.StringVar(&o.Groups, "groups", "", "also check consumer groups matching this glob")
	f.Float64Var(&o.DiskWarn, "disk-warn", 80, "disk usage warning threshold (%)")
	f.Float64Var(&o.DiskCritical, "disk-critical", 90, "disk usage critical threshold (%)")
	f.Float64Var(&o.ImbalanceWarn, "imbalance-warn", 10, "leader imbalance warning threshold (%)")
	f.DurationVar(&o.TxnTimeout, "txn-timeout", 15*time.Minute, "transactions open longer than this are hanging")
	f.DurationVar(&o.GroupSampleWait, "group-sample", 2*time.Second, "time between the two group samples")
	f.StringVar(&failOn, "fail-on", "critical", "exit 5 at this level: warn or critical")
	f.BoolVar(&watch, "watch", false, "re-run every --interval and print changes")
	f.DurationVar(&interval, "interval", 10*time.Second, "with --watch: how often to check")
	_ = cmd.RegisterFlagCompletionFunc("checks", cobra.FixedCompletions(append([]string{"all"}, health.AllChecks...), cobra.ShellCompDirectiveNoFileComp))
	_ = cmd.RegisterFlagCompletionFunc("fail-on", cobra.FixedCompletions([]string{"warn", "critical"}, cobra.ShellCompDirectiveNoFileComp))
	return cmd
}

func (a *app) printHealth(r health.Report) {
	w := a.stdout
	kraft := ""
	if r.Voters > 0 {
		kraft = fmt.Sprintf(" · KRaft (%d voters)", r.Voters)
	}
	fmt.Fprintf(w, "Cluster %s · Kafka %s · %d brokers%s\n", r.ClusterID, orDash(r.Version), r.Brokers, kraft)
	for _, c := range r.Checks {
		mark := "✓"
		switch c.Level {
		case health.Warn:
			mark = "!"
		case health.Critical:
			mark = "✗"
		}
		fmt.Fprintf(w, "%s %-18s %s\n", mark, c.Name, c.Message)
		for i, it := range c.Items {
			if i == 5 && len(c.Items) > 6 {
				fmt.Fprintf(w, "    … %d more\n", len(c.Items)-5)
				break
			}
			fmt.Fprintf(w, "    %s\n", it)
		}
	}
	fmt.Fprintf(w, "Status: %s\n", strings.ToUpper(r.Status.String()))
}
