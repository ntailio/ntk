// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/brokers"
	"github.com/ntailio/ntk/configs"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/units"
)

type brokerRows []brokers.Broker

func (r brokerRows) Header(wide bool) []string {
	h := []string{"ID", "HOST", "PORT", "RACK", "ROLE", "LEADERS", "REPLICAS", "DISK USED", "VERSION"}
	if wide {
		h = append(h, "DISK %")
	}
	return h
}

func (r brokerRows) Rows(wide bool) [][]string {
	var rows [][]string
	for _, b := range r {
		role := strings.ReplaceAll(b.Roles, ",", ", ")
		if b.Controller {
			role = strings.Replace(role, "controller", "controller*", 1)
			if !strings.Contains(role, "controller") {
				role += " (controller*)"
			}
		}
		row := []string{itoa(b.ID), b.Host, itoa(b.Port), orDash(b.Rack), role, itoa(b.Leaders), itoa(b.Replicas), units.Bytes(b.DiskUsed), orDash(b.Version)}
		if wide {
			pct := "-"
			if p := b.DiskPercent(); p >= 0 {
				pct = fmt.Sprintf("%.0f%%", p)
			}
			row = append(row, pct)
		}
		rows = append(rows, row)
	}
	return rows
}

func (r brokerRows) Names() []string {
	var n []string
	for _, b := range r {
		n = append(n, itoa(b.ID))
	}
	return n
}

func fetchBrokers(ctx context.Context, s *session) ([]string, error) {
	md, err := s.cl.Admin.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, b := range md.Brokers {
		out = append(out, fmt.Sprintf("%d\t%s:%d", b.NodeID, b.Host, b.Port))
	}
	return out, nil
}

func (a *app) completeBrokerArg(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return a.complete("brokers", fetchBrokers, false)(cmd, args, toComplete)
}

func parseBroker(s string) (int32, error) {
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, usageErr("broker id %q is not a number", s)
	}
	return int32(n), nil
}

func (a *app) newBrokerCmd() *cobra.Command {
	cmd := groupCmd("broker", "Inspect brokers and their configuration", "brokers", "b")

	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List brokers (controller* marks the active controller)",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			bs, err := brokers.List(cmd.Context(), s.cl.Client, s.cl.Admin)
			if err != nil {
				return err
			}
			return a.render(brokerRows(bs))
		},
	}

	describe := &cobra.Command{
		Use:               "describe <id>",
		Aliases:           []string{"get"},
		Short:             "Show a broker's partitions, log dirs, and dynamic config overrides",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeBrokerArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseBroker(args[0])
			if err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			d, err := brokers.Describe(cmd.Context(), s.cl.Client, s.cl.Admin, id)
			if err != nil {
				return exitcode.With(exitcode.NotFound, err)
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				return a.render(d)
			}
			w := a.stdout
			ctrl := ""
			if d.Controller {
				ctrl = " · active controller"
			}
			fmt.Fprintf(w, "Broker:     %d  %s:%d  rack %s  %s%s\n", d.ID, d.Host, d.Port, orDash(d.Rack), d.Roles, ctrl)
			fmt.Fprintf(w, "Partitions: %d leaders · %d replicas · %d out of ISR\n", d.Leaders, d.Replicas, len(d.OutOfISR))
			for _, p := range d.OutOfISR {
				fmt.Fprintf(w, "            ! %s\n", p)
			}
			fmt.Fprintln(w, "Log dirs:")
			for _, ld := range d.LogDirs {
				usage := ""
				if ld.TotalBytes > 0 {
					usage = fmt.Sprintf(" · volume %s / %s (%.0f%% used)", units.Bytes(ld.TotalBytes-ld.Usable), units.Bytes(ld.TotalBytes),
						float64(ld.TotalBytes-ld.Usable)/float64(ld.TotalBytes)*100)
				}
				errs := ""
				if ld.Error != "" {
					errs = " · ERROR " + ld.Error
				}
				fmt.Fprintf(w, "  %s  %s%s%s\n", ld.Dir, units.Bytes(ld.SizeBytes), usage, errs)
			}
			fmt.Fprintln(w, "Dynamic overrides:")
			if len(d.Overrides) == 0 {
				fmt.Fprintln(w, "  -")
			}
			for _, k := range slices.Sorted(maps.Keys(d.Overrides)) {
				fmt.Fprintf(w, "  %s=%s\n", k, configs.Humanize(k, d.Overrides[k]))
			}
			return nil
		},
	}

	var broker int32
	var topic string
	logDirs := &cobra.Command{
		Use:   "log-dirs",
		Short: "Log directories, their usage, and partition sizes",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			dirs, parts, err := brokers.LogDirs(cmd.Context(), s.cl.Admin, broker, topic)
			if err != nil {
				return err
			}
			if a.flags.output == "json" {
				return a.render(map[string]any{"log_dirs": dirs, "partitions": parts})
			}
			if topic != "" {
				return a.render(logDirPartRows(parts))
			}
			return a.render(logDirRows(dirs))
		},
	}
	logDirs.Flags().Int32Var(&broker, "broker", -1, "only this broker")
	logDirs.Flags().StringVar(&topic, "topic", "", "show partitions of this topic")
	_ = logDirs.RegisterFlagCompletionFunc("topic", a.completeTopicArg)
	_ = logDirs.RegisterFlagCompletionFunc("broker", a.completeBrokerArg)

	var apiBroker int32
	apiVersions := &cobra.Command{
		Use:   "api-versions",
		Short: "Supported Kafka API versions per broker",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			vs, err := brokers.APIVersions(cmd.Context(), s.cl.Admin, apiBroker)
			if err != nil {
				return err
			}
			return a.render(apiRows(vs))
		},
	}
	apiVersions.Flags().Int32Var(&apiBroker, "broker", -1, "only this broker")
	_ = apiVersions.RegisterFlagCompletionFunc("broker", a.completeBrokerArg)

	cmd.AddCommand(list, describe, a.newBrokerConfigCmd(), logDirs, apiVersions)
	return cmd
}

func (a *app) newBrokerConfigCmd() *cobra.Command {
	var view configView
	var clusterDefault bool
	target := func(args []string) (string, error) {
		switch {
		case clusterDefault && len(args) > 0:
			return "", usageErr("use a broker id or --cluster-default, not both")
		case clusterDefault:
			return "", nil
		case len(args) == 0:
			return "", usageErr("a broker id or --cluster-default is required")
		}
		if _, err := parseBroker(args[0]); err != nil {
			return "", err
		}
		return args[0], nil
	}
	cmd := &cobra.Command{
		Use:               "config <id|--cluster-default>",
		Short:             "Show broker configuration (overrides by default; --all for everything)",
		Args:              usageArgs(cobra.MaximumNArgs(1)),
		ValidArgsFunction: a.completeBrokerArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			name, err := target(args)
			if err != nil {
				return err
			}
			return a.showConfig(cmd, configs.Broker, name, view)
		},
	}
	view.flags(cmd)
	cmd.PersistentFlags().BoolVar(&clusterDefault, "cluster-default", false, "the cluster-wide default broker config")

	set := &cobra.Command{
		Use:     "set <id|--cluster-default> key=value...",
		Short:   "Set dynamic broker configs",
		Example: "  ntk broker config set --cluster-default log.retention.hours=168\n  ntk broker config set 1 log.cleaner.threads=2",
		Args:    usageArgs(cobra.MinimumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var id []string
			if !clusterDefault {
				id, args = args[:1], args[1:]
			}
			name, err := target(id)
			if err != nil {
				return err
			}
			if len(args) == 0 {
				return usageErr("no key=value given")
			}
			ops, err := configs.ParseAssignments(args)
			if err != nil {
				return usageErr("%v", err)
			}
			return a.alterConfig(cmd, configs.Broker, name, ops)
		},
	}
	unset := &cobra.Command{
		Use:   "unset <id|--cluster-default> key...",
		Short: "Remove dynamic broker config overrides",
		Args:  usageArgs(cobra.MinimumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var id []string
			if !clusterDefault {
				id, args = args[:1], args[1:]
			}
			name, err := target(id)
			if err != nil {
				return err
			}
			if len(args) == 0 {
				return usageErr("no key given")
			}
			return a.alterConfig(cmd, configs.Broker, name, deleteOps(args))
		},
	}
	get := &cobra.Command{
		Use:   "get <id|--cluster-default> <key>",
		Short: "Print one broker config value",
		Args:  usageArgs(cobra.RangeArgs(1, 2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			var id []string
			if !clusterDefault {
				if len(args) != 2 {
					return usageErr("need <id> <key>")
				}
				id, args = args[:1], args[1:]
			}
			name, err := target(id)
			if err != nil {
				return err
			}
			return a.getConfig(cmd, configs.Broker, name, args[0])
		},
	}
	cmd.AddCommand(get, set, unset)
	return cmd
}

type logDirRows []brokers.LogDir

func (r logDirRows) Header(bool) []string {
	return []string{"BROKER", "DIR", "SIZE", "VOLUME USED", "VOLUME TOTAL", "ERROR"}
}

func (r logDirRows) Rows(bool) [][]string {
	var rows [][]string
	for _, d := range r {
		used, total := "-", "-"
		if d.TotalBytes > 0 {
			used, total = units.Bytes(d.TotalBytes-d.Usable), units.Bytes(d.TotalBytes)
		}
		rows = append(rows, []string{itoa(d.Broker), d.Dir, units.Bytes(d.SizeBytes), used, total, orDash(d.Error)})
	}
	return rows
}

type logDirPartRows []brokers.LogDirPartition

func (r logDirPartRows) Header(bool) []string {
	return []string{"BROKER", "DIR", "TOPIC", "PART", "SIZE", "OFFSET-LAG", "FUTURE"}
}

func (r logDirPartRows) Rows(bool) [][]string {
	var rows [][]string
	for _, p := range r {
		future := "-"
		if p.Future {
			future = "yes"
		}
		rows = append(rows, []string{itoa(p.Broker), p.Dir, p.Topic, itoa(p.Partition), units.Bytes(p.SizeBytes), units.Count(p.OffsetLag), future})
	}
	return rows
}

type apiRows []brokers.APIVersion

func (r apiRows) Header(bool) []string { return []string{"BROKER", "KEY", "API", "MIN", "MAX"} }

func (r apiRows) Rows(bool) [][]string {
	var rows [][]string
	for _, v := range r {
		rows = append(rows, []string{itoa(v.Broker), itoa(v.Key), v.Name, itoa(v.Min), itoa(v.Max)})
	}
	return rows
}

type quorumRows []brokers.Replica

func (r quorumRows) Header(bool) []string {
	return []string{"REPLICA", "LOG END", "LAG", "LAST FETCH", "LAST CAUGHT UP"}
}

func (r quorumRows) Rows(bool) [][]string {
	var rows [][]string
	for _, x := range r {
		rows = append(rows, []string{itoa(x.ID), units.Count(x.LogEndOffset), units.Count(x.Lag), brokers.Ago(x.LastFetch), brokers.Ago(x.LastCaughtUp)})
	}
	return rows
}

type featureRows []brokers.Feature

func (r featureRows) Header(bool) []string { return []string{"FEATURE", "FINALIZED", "SUPPORTED"} }

func (r featureRows) Rows(bool) [][]string {
	var rows [][]string
	for _, f := range r {
		fin := "-"
		if f.FinalizedMax >= 0 {
			fin = fmt.Sprintf("%d", f.FinalizedMax)
		}
		sup := "-"
		if f.SupportedMax >= 0 {
			sup = fmt.Sprintf("%d-%d", f.SupportedMin, f.SupportedMax)
		}
		rows = append(rows, []string{f.Name, fin, sup})
	}
	return rows
}

func (a *app) newClusterCmd() *cobra.Command {
	cmd := groupCmd("cluster", "Cluster overview, KRaft quorum, and feature levels")
	describe := &cobra.Command{
		Use:   "describe",
		Short: "Cluster id, controller, brokers, quorum, and features",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			c, err := brokers.DescribeCluster(cmd.Context(), s.cl.Client, s.cl.Admin)
			if err != nil {
				return err
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				return a.render(c)
			}
			w := a.stdout
			fmt.Fprintf(w, "Cluster:     %s · Kafka %s\n", c.ID, orDash(c.Version))
			fmt.Fprintf(w, "Controller:  %d\n", c.Controller)
			if c.Quorum != nil {
				var voters []string
				for _, v := range c.Quorum.Voters {
					voters = append(voters, itoa(v.ID))
				}
				fmt.Fprintf(w, "KRaft:       leader %d · epoch %d · voters %s · %d observers\n", c.Quorum.LeaderID, c.Quorum.LeaderEpoch,
					strings.Join(voters, ","), len(c.Quorum.Observers))
			}
			fmt.Fprintf(w, "Topics:      %d · %d partitions\n", c.Topics, c.Partitions)
			for _, f := range c.Features {
				if f.Name == "metadata.version" {
					fmt.Fprintf(w, "Metadata:    metadata.version %d\n", f.FinalizedMax)
				}
			}
			fmt.Fprintln(w)
			return a.render(brokerRows(c.Brokers))
		},
	}
	quorum := &cobra.Command{
		Use:   "quorum",
		Short: "KRaft quorum status (DescribeQuorum)",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			q, err := brokers.DescribeQuorum(cmd.Context(), s.cl.Client)
			if err != nil {
				return err
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				return a.render(q)
			}
			fmt.Fprintf(a.stdout, "Leader: %d · Epoch %d · High watermark %s\n\nVOTERS\n", q.LeaderID, q.LeaderEpoch, units.Count(q.HighWatermark))
			if err := a.render(quorumRows(q.Voters)); err != nil {
				return err
			}
			if len(q.Observers) > 0 {
				fmt.Fprintln(a.stdout, "\nOBSERVERS")
				return a.render(quorumRows(q.Observers))
			}
			return nil
		},
	}
	features := &cobra.Command{
		Use:   "features",
		Short: "Finalized and supported feature levels (metadata.version, ...)",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			fs, _, err := brokers.Features(cmd.Context(), s.cl.Client)
			if err != nil {
				return err
			}
			return a.render(featureRows(fs))
		},
	}
	cmd.AddCommand(describe, quorum, features)
	return cmd
}
