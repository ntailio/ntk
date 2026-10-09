// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/factualtech/ntk/groups"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/units"
)

func (a *app) newGroupCmd() *cobra.Command {
	cmd := groupCmd("group", "Manage consumer groups", "groups", "g")
	cmd.AddCommand(
		a.newGroupListCmd(),
		a.newGroupDescribeCmd(),
		a.newGroupLagCmd(),
		a.newGroupResetCmd(),
		a.newGroupDeleteCmd(),
		a.newGroupDeleteOffsetsCmd(),
		a.newGroupWatchCmd(),
		a.newGroupTopCmd(),
	)
	return cmd
}

func fetchGroups(ctx context.Context, s *session) ([]string, error) {
	return groups.Names(ctx, s.cl.Client)
}

func (a *app) completeGroupArg(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return a.complete("groups", fetchGroups, false)(cmd, args, toComplete)
}

func (a *app) completeGroupArgs(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return a.complete("groups", fetchGroups, true)(cmd, args, toComplete)
}

type groupRows struct {
	groups  []groups.Group
	showLag bool
}

func (r groupRows) Header(wide bool) []string {
	h := []string{"GROUP", "TYPE", "STATE", "MEMBERS", "TOPICS"}
	if r.showLag {
		h = append(h, "LAG")
	}
	return append(h, "COORDINATOR")
}

func (r groupRows) Rows(wide bool) [][]string {
	var rows [][]string
	for _, g := range r.groups {
		ts := strings.Join(g.Topics, ", ")
		if !wide && len(g.Topics) > 3 {
			ts = strings.Join(g.Topics[:3], ", ") + fmt.Sprintf(" (+%d)", len(g.Topics)-3)
		}
		row := []string{g.Name, g.Type, g.State, itoa(g.Members), orDash(ts)}
		if r.showLag {
			row = append(row, lagString(g.Lag))
		}
		rows = append(rows, append(row, itoa(g.Coordinator)))
	}
	return rows
}

func (r groupRows) Names() []string {
	var n []string
	for _, g := range r.groups {
		n = append(n, g.Name)
	}
	return n
}

func (r groupRows) MarshalJSON() ([]byte, error) { return json.Marshal(r.groups) }

func lagString(n int64) string {
	if n < 0 {
		return "-"
	}
	return units.Count(n)
}

func (a *app) newGroupListCmd() *cobra.Command {
	var opts groups.ListOptions
	var states, types string
	cmd := &cobra.Command{
		Use:     "list [pattern]",
		Aliases: []string{"ls"},
		Short:   "List consumer groups",
		Args:    usageArgs(cobra.MaximumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 1 {
				opts.Pattern = args[0]
			}
			opts.States, opts.Types = splitList(states), splitList(types)
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			gs, err := groups.List(cmd.Context(), s.cl.Client, s.cl.Admin, opts)
			if err != nil {
				return err
			}
			return a.render(groupRows{gs, opts.Lag})
		},
	}
	f := cmd.Flags()
	f.BoolVar(&opts.Regex, "regex", false, "treat the pattern as a regular expression")
	f.StringVar(&states, "state", "", "only these states, e.g. stable,empty")
	f.StringVar(&types, "type", "", "only these types: classic, consumer, share")
	f.StringVar(&opts.Topic, "topic", "", "only groups with committed offsets on this topic")
	f.BoolVar(&opts.Lag, "lag", false, "include total lag (one extra round trip)")
	_ = cmd.RegisterFlagCompletionFunc("topic", a.completeTopicArg)
	_ = cmd.RegisterFlagCompletionFunc("type", cobra.FixedCompletions([]string{"classic", "consumer", "share"}, cobra.ShellCompDirectiveNoFileComp))
	_ = cmd.RegisterFlagCompletionFunc("state", cobra.FixedCompletions([]string{"stable", "empty", "dead", "preparingrebalance", "completingrebalance", "assigning", "reconciling"}, cobra.ShellCompDirectiveNoFileComp))
	return cmd
}

type groupOffsetRows []groups.PartitionLag

func (r groupOffsetRows) Header(bool) []string {
	return []string{"TOPIC", "PART", "COMMITTED", "END", "LAG", "MEMBER", "HOST", "CLIENT-ID"}
}

func (r groupOffsetRows) Rows(wide bool) [][]string {
	var rows [][]string
	for _, o := range r {
		committed := "-"
		if o.Committed >= 0 {
			committed = units.Count(o.Committed)
		}
		member := o.MemberID
		if !wide && len(member) > 24 {
			member = member[:21] + "…"
		}
		rows = append(rows, []string{o.Topic, itoa(o.Partition), committed, units.Count(o.End), lagString(o.Lag), orDash(member), orDash(o.Host), orDash(o.ClientID)})
	}
	return rows
}

type memberRows []groups.Member

func (r memberRows) Header(bool) []string {
	return []string{"MEMBER", "CLIENT-ID", "HOST", "INSTANCE", "ASSIGNED"}
}

func (r memberRows) Rows(bool) [][]string {
	var rows [][]string
	for _, m := range r {
		var parts []string
		for _, tp := range m.Assigned {
			parts = append(parts, tp.Topic+":"+joinInts(tp.Partitions))
		}
		rows = append(rows, []string{m.ID, m.ClientID, m.Host, orDash(m.InstanceID), orDash(strings.Join(parts, " "))})
	}
	return rows
}

func (a *app) newGroupDescribeCmd() *cobra.Command {
	var members bool
	var topic string
	cmd := &cobra.Command{
		Use:               "describe <group>",
		Aliases:           []string{"get"},
		Short:             "Show a group's members, assignments, committed offsets, and lag",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeGroupArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			d, err := groups.Describe(cmd.Context(), s.cl.Client, s.cl.Admin, args[0])
			if err != nil {
				return err
			}
			if topic != "" {
				d.Offsets = slices.DeleteFunc(d.Offsets, func(o groups.PartitionLag) bool { return o.Topic != topic })
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				return a.render(d)
			}
			typ := d.Type
			if typ == "consumer" {
				typ = "consumer (KIP-848)"
			}
			fmt.Fprintf(a.stdout, "Group:        %s   Type: %s   State: %s", d.Name, typ, d.State)
			if d.Epoch > 0 {
				fmt.Fprintf(a.stdout, "   Epoch: %d", d.Epoch)
			}
			fmt.Fprintf(a.stdout, "\nCoordinator:  broker %d", d.Coordinator)
			if d.Assignor != "" {
				fmt.Fprintf(a.stdout, "   Assignor: %s", d.Assignor)
			}
			if d.Protocol != "" {
				fmt.Fprintf(a.stdout, "   Protocol: %s", d.Protocol)
			}
			fmt.Fprintf(a.stdout, "\nMembers:      %d   Total lag: %s\n\n", d.Members, units.Count(d.TotalLag()))
			if members {
				if err := a.render(memberRows(d.Members_)); err != nil {
					return err
				}
				fmt.Fprintln(a.stdout)
			}
			if len(d.Offsets) == 0 {
				fmt.Fprintln(a.stdout, "No committed offsets.")
				return nil
			}
			return a.render(groupOffsetRows(d.Offsets))
		},
	}
	cmd.Flags().BoolVar(&members, "members", false, "also list members with their assignments")
	cmd.Flags().StringVar(&topic, "topic", "", "only this topic")
	_ = cmd.RegisterFlagCompletionFunc("topic", a.completeGroupTopics)
	return cmd
}

func (a *app) newGroupResetCmd() *cobra.Command {
	var (
		topicArgs []string
		allTopics bool
		earliest  bool
		latest    bool
		toOffset  int64
		toTime    string
		shiftBy   int64
		fromFile  string
		execute   bool
	)
	cmd := &cobra.Command{
		Use:   "reset <group>",
		Short: "Reset committed offsets (dry run unless --execute)",
		Example: "  ntk group reset svc-orders --topic orders --to-time 2026-09-30T08:00:00Z\n" +
			"  ntk group reset svc-orders --topic orders:0,3-5 --shift-by -100 --execute",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeGroupArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			o := groups.ResetOptions{Topics: map[string][]int32{}, AllTopics: allTopics}
			modes := 0
			if earliest {
				o.Mode, modes = groups.ToEarliest, modes+1
			}
			if latest {
				o.Mode, modes = groups.ToLatest, modes+1
			}
			if cmd.Flags().Changed("to-offset") {
				o.Mode, o.Offset, modes = groups.ToOffset, toOffset, modes+1
			}
			if toTime != "" {
				t, err := parseTime(toTime)
				if err != nil {
					return err
				}
				o.Mode, o.Time, modes = groups.ToTime, t, modes+1
			}
			if cmd.Flags().Changed("shift-by") {
				o.Mode, o.Shift, modes = groups.ShiftBy, shiftBy, modes+1
			}
			if fromFile != "" {
				file, err := readResetFile(fromFile)
				if err != nil {
					return err
				}
				o.Mode, o.File, modes = groups.FromFile, file, modes+1
			}
			if modes != 1 {
				return usageErr("use exactly one of --to-earliest, --to-latest, --to-offset, --to-time, --shift-by, --from-file")
			}
			for _, t := range topicArgs {
				name, parts, err := parseTopicPartitions(t)
				if err != nil {
					return err
				}
				o.Topics[name] = parts
			}
			if len(o.Topics) == 0 && !allTopics && o.Mode != groups.FromFile {
				return usageErr("--topic or --all-topics is required")
			}
			if !execute {
				a.flags.dryRun = true
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := groups.PlanReset(cmd.Context(), s.cl.Client, s.cl.Admin, args[0], o)
			if err != nil {
				return err
			}
			if !execute && a.flags.output != "json" {
				defer fmt.Fprintln(a.stderr, "(dry run; add --execute to apply)")
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	f := cmd.Flags()
	f.StringArrayVar(&topicArgs, "topic", nil, "topic[:partitions], repeatable (e.g. orders:0,3-5)")
	f.BoolVar(&allTopics, "all-topics", false, "every topic the group has offsets for")
	f.BoolVar(&earliest, "to-earliest", false, "reset to the start of each partition")
	f.BoolVar(&latest, "to-latest", false, "reset to the end of each partition")
	f.Int64Var(&toOffset, "to-offset", 0, "reset to this offset")
	f.StringVar(&toTime, "to-time", "", "reset to the first offset at/after this time (RFC 3339, -2h, epoch ms)")
	f.Int64Var(&shiftBy, "shift-by", 0, "move the committed offset by ±N")
	f.StringVar(&fromFile, "from-file", "", "offsets from a JSON plan/map or a topic,partition,offset CSV")
	f.BoolVar(&execute, "execute", false, "apply the reset (default is a dry run)")
	_ = cmd.RegisterFlagCompletionFunc("topic", a.completeGroupTopics)
	return cmd
}

func readResetFile(path string) (map[string]map[int32]int64, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]map[int32]int64{}
	var pl plan.Plan
	if json.Unmarshal(b, &pl) == nil && pl.Kind == groups.KindReset {
		var spec groups.ResetSpec
		if err := pl.Decode(&spec); err != nil {
			return nil, err
		}
		return spec.Offsets, nil
	}
	var m map[string]map[string]int64
	if json.Unmarshal(b, &m) == nil {
		for t, parts := range m {
			out[t] = map[int32]int64{}
			for p, off := range parts {
				n, err := strconv.Atoi(p)
				if err != nil {
					return nil, usageErr("%s: partition %q is not a number", path, p)
				}
				out[t][int32(n)] = off
			}
		}
		return out, nil
	}
	recs, err := csv.NewReader(strings.NewReader(string(b))).ReadAll()
	if err != nil {
		return nil, usageErr("%s: not a JSON plan or topic,partition,offset CSV: %v", path, err)
	}
	for _, r := range recs {
		if len(r) == 4 {
			r = r[1:]
		}
		if len(r) != 3 {
			return nil, usageErr("%s: expected topic,partition,offset", path)
		}
		p, err1 := strconv.Atoi(strings.TrimSpace(r[1]))
		off, err2 := strconv.ParseInt(strings.TrimSpace(r[2]), 10, 64)
		if err1 != nil || err2 != nil {
			continue
		}
		t := strings.TrimSpace(r[0])
		if out[t] == nil {
			out[t] = map[int32]int64{}
		}
		out[t][int32(p)] = off
	}
	return out, nil
}

func (a *app) newGroupDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "delete <group>...",
		Aliases:           []string{"rm"},
		Short:             "Delete consumer groups (they must have no active members)",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeGroupArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := groups.PlanDelete(cmd.Context(), s.cl.Client, s.cl.Admin, args)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
}

func (a *app) newGroupDeleteOffsetsCmd() *cobra.Command {
	var topic string
	cmd := &cobra.Command{
		Use:               "delete-offsets <group> --topic t[:partitions]",
		Short:             "Delete a group's committed offsets for a topic",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeGroupArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			if topic == "" {
				return usageErr("--topic is required")
			}
			name, parts, err := parseTopicPartitions(topic)
			if err != nil {
				return err
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := groups.PlanDeleteOffsets(cmd.Context(), s.cl.Client, s.cl.Admin, args[0], name, parts)
			if err != nil {
				return err
			}
			return a.run(cmd.Context(), s, pl)
		},
	}
	cmd.Flags().StringVar(&topic, "topic", "", "topic[:partitions]")
	_ = cmd.RegisterFlagCompletionFunc("topic", a.completeGroupTopics)
	return cmd
}

func (a *app) topicConsumers(ctx context.Context, s *session, topic string) string {
	lags, err := groups.ForTopic(ctx, s.cl.Client, s.cl.Admin, topic)
	if err != nil || len(lags) == 0 {
		return ""
	}
	var parts []string
	for _, g := range slices.Sorted(maps.Keys(lags)) {
		parts = append(parts, fmt.Sprintf("%s (lag %s)", g, units.Count(lags[g])))
	}
	return strings.Join(parts, ", ")
}

// completeGroupTopics completes the topics the group (first argument) has offsets for.
func (a *app) completeGroupTopics(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return a.completeTopicArg(cmd, args, toComplete)
	}
	group := args[0]
	items := a.cached(cmd.Context(), "group-topics-"+group, func(ctx context.Context, s *session) ([]string, error) {
		offs, err := s.cl.Admin.FetchOffsets(ctx, group)
		if err != nil {
			return nil, err
		}
		var out []string
		for t := range offs {
			out = append(out, t)
		}
		slices.Sort(out)
		return out, nil
	})
	return filterPrefix(items, toComplete, nil), cobra.ShellCompDirectiveNoFileComp
}
