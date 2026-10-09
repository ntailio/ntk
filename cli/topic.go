// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"charm.land/huh/v2"
	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/configs"
	"github.com/ntailio/ntk/prefs"
	"github.com/ntailio/ntk/topics"
	"github.com/ntailio/ntk/units"
)

func (a *app) newTopicCmd() *cobra.Command {
	cmd := groupCmd("topic", "Manage topics and partitions", "topics", "t")
	cmd.AddCommand(
		a.newTopicListCmd(),
		a.newTopicDescribeCmd(),
		a.newTopicCreateCmd(),
		a.newTopicDeleteCmd(),
		a.newTopicAddPartitionsCmd(),
		a.newTopicTruncateCmd(),
		a.newTopicOffsetsCmd(),
		a.newTopicReassignCmd(),
		a.newTopicElectCmd(),
		a.newTopicConfigCmd(),
		a.newTopicWatchCmd(),
		a.newTopicStatsCmd(),
		a.newTopicTopCmd(),
	)
	return cmd
}

type topicRows []topics.Topic

func (r topicRows) Header(wide bool) []string {
	h := []string{"NAME", "PARTITIONS", "RF", "URP", "SIZE", "RETENTION", "CLEANUP"}
	if wide {
		h = append(h, "MIN-ISR", "MESSAGES", "LEADERS", "ID")
	}
	return h
}

func (r topicRows) Rows(wide bool) [][]string {
	rows := make([][]string, 0, len(r))
	for _, t := range r {
		retention := "-"
		if t.RetentionMs != nil {
			retention = configs.Humanize("retention.ms", itoa(*t.RetentionMs))
		}
		size := "-"
		if t.SizeBytes >= 0 {
			size = units.Bytes(t.SizeBytes)
		}
		row := []string{t.Name, itoa(t.Partitions), itoa(t.ReplicationFactor), itoa(t.UnderReplicated), size, retention, orDash(t.CleanupPolicy)}
		if wide {
			var leaders []string
			for _, b := range slices.Sorted(maps.Keys(t.LeadersByBroker)) {
				leaders = append(leaders, fmt.Sprintf("b%d:%d", b, t.LeadersByBroker[b]))
			}
			msgs := "-"
			if t.Messages >= 0 {
				msgs = units.Count(t.Messages)
			}
			row = append(row, itoa(t.MinInsyncReplicas), msgs, strings.Join(leaders, " "), t.ID)
		}
		rows = append(rows, row)
	}
	return rows
}

func (r topicRows) Names() []string {
	names := make([]string, len(r))
	for i, t := range r {
		names[i] = t.Name
	}
	return names
}

func (a *app) newTopicListCmd() *cobra.Command {
	var opts topics.ListOptions
	cmd := &cobra.Command{
		Use:               "list [pattern]",
		Aliases:           []string{"ls"},
		Short:             "List topics, optionally filtered by a glob (or --regex) pattern",
		Args:              usageArgs(cobra.MaximumNArgs(1)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				opts.Pattern = args[0]
			}
			opts.Messages = a.flags.output != "table" && a.flags.output != "name"
			if a.flags.output == "name" {
				opts.Fast = true
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			ts, err := topics.List(cmd.Context(), s.cl.Admin, opts)
			if err != nil {
				return err
			}
			return a.render(topicRows(ts))
		},
	}
	f := cmd.Flags()
	f.BoolVarP(&opts.Internal, "internal", "a", false, "include internal topics")
	f.BoolVar(&opts.Regex, "regex", false, "treat the pattern as a regular expression")
	f.BoolVar(&opts.Fast, "fast", false, "skip sizes and configs")
	f.BoolVar(&opts.UnderReplicated, "under-replicated", false, "only topics with under-replicated partitions")
	f.BoolVar(&opts.NoLeader, "no-leader", false, "only topics with offline partitions")
	f.BoolVar(&opts.UnderMinISR, "under-min-isr", false, "only topics with partitions below min.insync.replicas")
	return cmd
}

type partitionRows []topics.Partition

func (r partitionRows) Header(bool) []string {
	return []string{"PART", "LEADER", "REPLICAS", "ISR", "LOW", "HIGH", "MSGS", "SIZE", "STATUS"}
}

func (r partitionRows) Rows(bool) [][]string {
	rows := make([][]string, 0, len(r))
	for _, p := range r {
		rows = append(rows, []string{
			itoa(p.Partition), itoa(p.Leader), joinInts(p.Replicas), joinInts(p.ISR),
			units.Count(p.LogStart), units.Count(p.HighWatermark), units.Count(p.Messages()), units.Bytes(p.SizeBytes), p.Status(),
		})
	}
	return rows
}

func (a *app) newTopicDescribeCmd() *cobra.Command {
	var partitions string
	cmd := &cobra.Command{
		Use:               "describe <topic>...",
		Aliases:           []string{"get"},
		Short:             "Show a topic's partitions, replicas, offsets, and consumers",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			ids, err := parseIDs(partitions)
			if err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			var details []topics.Detail
			for _, name := range args {
				d, err := topics.Describe(cmd.Context(), s.cl.Admin, name)
				if err != nil {
					return err
				}
				if len(ids) > 0 {
					d.PartitionDetails = slices.DeleteFunc(d.PartitionDetails, func(p topics.Partition) bool { return !slices.Contains(ids, p.Partition) })
				}
				details = append(details, d)
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				if len(details) == 1 {
					return a.render(details[0])
				}
				return a.render(details)
			}
			for i, d := range details {
				if i > 0 {
					fmt.Fprintln(a.stdout)
				}
				a.printTopicDetail(cmd.Context(), s, d)
				if err := a.render(partitionRows(d.PartitionDetails)); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&partitions, "partitions", "", "only these partitions, e.g. 0,3-5")
	return cmd
}

func (a *app) printTopicDetail(ctx context.Context, s *session, d topics.Detail) {
	w := a.stdout
	fmt.Fprintf(w, "Topic:        %s (id %s)\n", d.Name, d.ID)
	fmt.Fprintf(w, "Partitions:   %d   Replication: %d   min.insync.replicas: %s\n", d.Partitions, d.ReplicationFactor, orDash(itoa(d.MinInsyncReplicas)))
	retention := "-"
	if d.RetentionMs != nil {
		retention = configs.Humanize("retention.ms", itoa(*d.RetentionMs))
	}
	fmt.Fprintf(w, "Cleanup:      %s   Retention: %s   Size: %s   Messages: %s\n", orDash(d.CleanupPolicy), retention, units.Bytes(d.SizeBytes), units.Count(d.Messages))
	if len(d.Overrides) > 0 {
		var parts []string
		for _, k := range slices.Sorted(maps.Keys(d.Overrides)) {
			parts = append(parts, k+"="+configs.Humanize(k, d.Overrides[k]))
		}
		fmt.Fprintf(w, "Overrides:    %s   (see: ntk topic config %s)\n", strings.Join(parts, ", "), d.Name)
	}
	if consumers := a.topicConsumers(ctx, s, d.Name); consumers != "" {
		fmt.Fprintf(w, "Consumers:    %s\n", consumers)
	}
	fmt.Fprintln(w)
}

func (a *app) newTopicCreateCmd() *cobra.Command {
	var (
		spec   topics.CreateSpec
		cfgs   []string
		preset string
		parts  int32
		rf     int16
	)
	cmd := &cobra.Command{
		Use:   "create <topic>...",
		Short: "Create topics",
		Example: "  ntk topic create payments -P 6 -r 3 --config retention.ms=7d --config min.insync.replicas=2\n" +
			"  ntk topic create orders.state --preset compacted --config segment.ms=1h",
		Args: usageArgs(cobra.ArbitraryArgs),
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := prefs.Load()
			if err != nil {
				return err
			}
			if len(args) == 0 {
				if !a.interactive() && !accessible() {
					return usageErr("topic name required")
				}
				if args, parts, rf, preset, cfgs, err = a.topicCreateForm(cmd.Context(), p); err != nil {
					return err
				}
			}
			spec.Topics, spec.Partitions, spec.ReplicationFactor = args, parts, rf
			spec.Configs = map[string]string{}
			if preset != "" {
				values, ok := p.Preset(preset)
				if !ok {
					return usageErr("unknown preset %q (have: %s)", preset, strings.Join(p.PresetNames(), ", "))
				}
				maps.Copy(spec.Configs, values)
			}
			for _, kv := range cfgs {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					return usageErr("--config %q is not key=value", kv)
				}
				spec.Configs[k] = v
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := topics.PlanCreate(cmd.Context(), s.cl.Admin, spec)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	f := cmd.Flags()
	f.Int32VarP(&parts, "partitions", "P", -1, "number of partitions (default: broker num.partitions)")
	f.Int16VarP(&rf, "replication-factor", "r", -1, "replication factor (default: broker default.replication.factor)")
	f.StringArrayVar(&cfgs, "config", nil, "topic config key=value (repeatable; accepts 7d, 10GiB, ...)")
	f.StringVar(&preset, "preset", "", "named config bundle (built in: compacted, durable; more in config.json)")
	f.BoolVar(&spec.IfNotExists, "if-not-exists", false, "skip topics that already exist")
	_ = cmd.RegisterFlagCompletionFunc("preset", func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
		p, err := prefs.Load()
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return p.PresetNames(), cobra.ShellCompDirectiveNoFileComp
	})
	return cmd
}

func (a *app) topicCreateForm(ctx context.Context, p *prefs.Prefs) ([]string, int32, int16, string, []string, error) {
	var name, parts, rf, preset, extra string
	presetOpts := []huh.Option[string]{huh.NewOption("none", "")}
	for _, n := range p.PresetNames() {
		presetOpts = append(presetOpts, huh.NewOption(n, n))
	}
	num := func(v string) error {
		if v == "" {
			return nil
		}
		_, err := fmt.Sscanf(v, "%d", new(int))
		return err
	}
	form := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Topic name").Value(&name).Validate(huh.ValidateNotEmpty()),
		huh.NewInput().Title("Partitions").Description("empty = broker default").Value(&parts).Validate(num),
		huh.NewInput().Title("Replication factor").Description("empty = broker default").Value(&rf).Validate(num),
		huh.NewSelect[string]().Title("Preset").Options(presetOpts...).Value(&preset),
		huh.NewInput().Title("Configs").Description("optional, comma-separated key=value").Value(&extra),
	))
	if err := a.runForm(ctx, form); err != nil {
		return nil, 0, 0, "", nil, err
	}
	var np, nrf int = -1, -1
	if parts != "" {
		fmt.Sscanf(parts, "%d", &np)
	}
	if rf != "" {
		fmt.Sscanf(rf, "%d", &nrf)
	}
	return []string{name}, int32(np), int16(nrf), preset, splitList(extra), nil
}

func (a *app) newTopicDeleteCmd() *cobra.Command {
	var regex bool
	cmd := &cobra.Command{
		Use:               "delete <topic>...",
		Aliases:           []string{"rm"},
		Short:             "Delete topics",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			names := args
			if regex {
				names = nil
				for _, pattern := range args {
					ts, err := topics.List(cmd.Context(), s.cl.Admin, topics.ListOptions{Pattern: pattern, Regex: true, Fast: true})
					if err != nil {
						return err
					}
					for _, t := range ts {
						names = append(names, t.Name)
					}
				}
				if len(names) == 0 {
					return usageErr("no topics match %s", strings.Join(args, ", "))
				}
			}
			pl, err := topics.PlanDelete(cmd.Context(), s.cl.Admin, names)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	cmd.Flags().BoolVar(&regex, "regex", false, "treat arguments as regular expressions")
	return cmd
}

func (a *app) newTopicAddPartitionsCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "add-partitions <topic> <total>",
		Short:             "Increase a topic's partition count to <total>",
		Args:              usageArgs(cobra.ExactArgs(2)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			var total int
			if _, err := fmt.Sscanf(args[1], "%d", &total); err != nil {
				return usageErr("total %q is not a number", args[1])
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := topics.PlanAddPartitions(cmd.Context(), s.cl.Admin, args[0], total)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
}

func (a *app) newTopicTruncateCmd() *cobra.Command {
	var (
		beforeOffset int64
		beforeTime   string
		all          bool
		partitions   string
	)
	cmd := &cobra.Command{
		Use:               "truncate <topic>",
		Short:             "Delete records from the start of partitions (DeleteRecords)",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			var o topics.TruncateOptions
			set := 0
			if cmd.Flags().Changed("before-offset") {
				o.BeforeOffset = &beforeOffset
				set++
			}
			if beforeTime != "" {
				t, err := parseTime(beforeTime)
				if err != nil {
					return err
				}
				o.BeforeTime = &t
				set++
			}
			if all {
				o.All = true
				set++
			}
			if set != 1 {
				return usageErr("use exactly one of --before-offset, --before-time, --all")
			}
			var err error
			if o.Partitions, err = parseIDs(partitions); err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := topics.PlanTruncate(cmd.Context(), s.cl.Admin, args[0], o)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	f := cmd.Flags()
	f.Int64Var(&beforeOffset, "before-offset", 0, "delete records before this offset")
	f.StringVar(&beforeTime, "before-time", "", "delete records before this time (RFC 3339, -2h, epoch ms)")
	f.BoolVar(&all, "all", false, "delete all records")
	f.StringVar(&partitions, "partitions", "", "only these partitions, e.g. 0,3-5")
	return cmd
}

type offsetRows []topics.PartitionOffsets

func (r offsetRows) Header(bool) []string {
	h := []string{"PART", "EARLIEST", "LATEST", "MESSAGES"}
	if len(r) > 0 && r[0].AtTime != nil {
		h = append(h, "OFFSET@TIME", "TIMESTAMP")
	}
	return h
}

func (r offsetRows) Rows(bool) [][]string {
	var rows [][]string
	for _, o := range r {
		row := []string{itoa(o.Partition), units.Count(o.Earliest), units.Count(o.Latest), units.Count(max(o.Latest-o.Earliest, 0))}
		if o.AtTime != nil {
			ts := "-"
			if o.Timestamp != nil {
				ts = time.UnixMilli(*o.Timestamp).Format(time.RFC3339)
			}
			row = append(row, units.Count(*o.AtTime), ts)
		}
		rows = append(rows, row)
	}
	return rows
}

func (a *app) newTopicOffsetsCmd() *cobra.Command {
	var at string
	cmd := &cobra.Command{
		Use:               "offsets <topic>",
		Short:             "Show earliest/latest offsets per partition, and the offset for a time",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			var ms *int64
			if at != "" {
				t, err := parseTime(at)
				if err != nil {
					return err
				}
				v := t.UnixMilli()
				ms = &v
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			offs, err := topics.Offsets(cmd.Context(), s.cl.Admin, args[0], ms)
			if err != nil {
				return err
			}
			return a.render(offsetRows(offs))
		},
	}
	cmd.Flags().StringVar(&at, "time", "", "also show the first offset at/after this time (RFC 3339, -2h, epoch ms)")
	return cmd
}

type assignmentRows struct {
	topics.Assignment
	current map[string][]int32
}

func (r assignmentRows) Header(bool) []string {
	return []string{"TOPIC", "PART", "CURRENT", "PROPOSED", "CHANGE"}
}

func (r assignmentRows) Rows(bool) [][]string {
	var rows [][]string
	for _, p := range r.Partitions {
		cur := r.current[fmt.Sprintf("%s/%d", p.Topic, p.Partition)]
		change := "="
		if !slices.Equal(cur, p.Replicas) {
			change = "move"
		}
		rows = append(rows, []string{p.Topic, itoa(p.Partition), joinInts(cur), joinInts(p.Replicas), change})
	}
	return rows
}

func (a *app) newTopicReassignCmd() *cobra.Command {
	cmd := groupCmd("reassign", "Move partition replicas between brokers")

	var planTopics, planBrokers string
	var rackAware bool
	planCmd := &cobra.Command{
		Use:   "plan",
		Short: "Generate a balanced assignment (print a diff; -o json writes a plan file)",
		Example: "  ntk topic reassign plan --topics orders,payments --brokers 1,2,3 -o json > plan.json\n" +
			"  ntk topic reassign apply plan.json --throttle 50MiB/s",
		Args: usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			names := splitList(planTopics)
			brokers, err := parseIDs(planBrokers)
			if err != nil {
				return err
			}
			if len(names) == 0 || len(brokers) == 0 {
				return usageErr("--topics and --brokers are required")
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			asg, err := topics.Generate(cmd.Context(), s.cl.Admin, names, brokers, rackAware)
			if err != nil {
				return err
			}
			if a.flags.output == "json" {
				return a.render(asg)
			}
			md, err := s.cl.Admin.Metadata(cmd.Context(), names...)
			if err != nil {
				return err
			}
			cur := map[string][]int32{}
			for _, t := range md.Topics {
				for _, p := range t.Partitions {
					cur[fmt.Sprintf("%s/%d", t.Topic, p.Partition)] = p.Replicas
				}
			}
			return a.render(assignmentRows{asg, cur})
		},
	}
	planCmd.Flags().StringVar(&planTopics, "topics", "", "comma-separated topics")
	planCmd.Flags().StringVar(&planBrokers, "brokers", "", "target broker ids, e.g. 1,2,3")
	planCmd.Flags().BoolVar(&rackAware, "rack-aware", false, "spread replicas across racks")

	var throttle string
	applyCmd := &cobra.Command{
		Use:   "apply <plan.json>",
		Short: "Start a reassignment from a plan file (ntk or kafka-reassign-partitions.sh format)",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			var asg topics.Assignment
			if err := json.Unmarshal(b, &asg); err != nil || len(asg.Partitions) == 0 {
				return usageErr("%s is not a reassignment file (expected {\"version\":1,\"partitions\":[...]})", args[0])
			}
			var rate int64
			if throttle != "" {
				if rate, err = units.ParseRate(throttle); err != nil {
					return usageErr("%v", err)
				}
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := topics.PlanReassign(cmd.Context(), s.cl.Admin, asg, rate)
			if err != nil {
				return err
			}
			if err := a.run(cmd.Context(), s, pl); err != nil || a.flags.dryRun || pl.Empty() {
				return err
			}
			fmt.Fprintln(a.stderr, "Reassignment started. Follow it with: ntk topic reassign status")
			return nil
		},
	}
	applyCmd.Flags().StringVar(&throttle, "throttle", "", "replication throttle, e.g. 50MiB/s (removed by `status` when done)")

	var statusTopics string
	statusCmd := &cobra.Command{
		Use:   "status",
		Short: "Show reassignments in progress (and remove throttles once all are done)",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			running, err := topics.InProgress(cmd.Context(), s.cl.Admin, splitList(statusTopics))
			if err != nil {
				return err
			}
			if len(running) == 0 {
				fmt.Fprintln(a.stderr, "No reassignments in progress.")
				if !s.prof.ReadOnly {
					cleared, err := topics.ClearThrottles(cmd.Context(), s.cl.Admin)
					if err != nil {
						return err
					}
					if len(cleared) > 0 {
						fmt.Fprintf(a.stderr, "Removed replication throttles from %s and all brokers.\n", strings.Join(cleared, ", "))
					}
				}
				if a.flags.output == "json" {
					return a.render([]topics.Reassignment{})
				}
				return nil
			}
			return a.render(reassignRows(running))
		},
	}
	statusCmd.Flags().StringVar(&statusTopics, "topics", "", "comma-separated topics (default: all)")

	var cancelTopics string
	cancelCmd := &cobra.Command{
		Use:   "cancel",
		Short: "Cancel reassignments in progress",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := topics.PlanCancel(cmd.Context(), s.cl.Admin, splitList(cancelTopics))
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	cancelCmd.Flags().StringVar(&cancelTopics, "topics", "", "comma-separated topics (default: all)")

	cmd.AddCommand(planCmd, applyCmd, statusCmd, cancelCmd)
	return cmd
}

type reassignRows []topics.Reassignment

func (r reassignRows) Header(bool) []string {
	return []string{"TOPIC", "PART", "REPLICAS", "ADDING", "REMOVING"}
}

func (r reassignRows) Rows(bool) [][]string {
	var rows [][]string
	for _, x := range r {
		rows = append(rows, []string{x.Topic, itoa(x.Partition), joinInts(x.Replicas), joinInts(x.Adding), joinInts(x.Removing)})
	}
	return rows
}

func (a *app) newTopicElectCmd() *cobra.Command {
	var names, partitions, typ string
	cmd := &cobra.Command{
		Use:   "elect-leaders",
		Short: "Run preferred (or unclean) leader election",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if typ != "preferred" && typ != "unclean" {
				return usageErr("--type must be preferred or unclean")
			}
			ids, err := parseIDs(partitions)
			if err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := topics.PlanElect(cmd.Context(), s.cl.Admin, splitList(names), ids, typ == "unclean")
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	cmd.Flags().StringVar(&names, "topics", "", "comma-separated topics (default: all)")
	cmd.Flags().StringVar(&partitions, "partitions", "", "only these partitions, e.g. 0,3-5")
	cmd.Flags().StringVar(&typ, "type", "preferred", "preferred or unclean")
	_ = cmd.RegisterFlagCompletionFunc("type", cobra.FixedCompletions([]string{"preferred", "unclean"}, cobra.ShellCompDirectiveNoFileComp))
	return cmd
}

func orDash(s string) string {
	if s == "" || s == "0" {
		return "-"
	}
	return s
}
