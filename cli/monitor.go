// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/monitor"
	"github.com/ntailio/ntk/output"
	"github.com/ntailio/ntk/units"
)

var sparkChars = []rune("▁▂▃▄▅▆▇█")

func spark[T int64 | float64](v []T, width int) string {
	if len(v) > width {
		v = v[len(v)-width:]
	}
	if len(v) == 0 {
		return ""
	}
	lo, hi := v[0], v[0]
	for _, x := range v {
		lo, hi = min(lo, x), max(hi, x)
	}
	var b strings.Builder
	for _, x := range v {
		i := 0
		if hi > lo {
			i = int(float64(x-lo) / float64(hi-lo) * float64(len(sparkChars)-1))
		}
		b.WriteRune(sparkChars[i])
	}
	return b.String()
}

func (a *app) stdoutTTY() bool {
	f, ok := a.stdout.(*os.File)
	return ok && term.IsTerminal(f.Fd())
}

// loop calls draw every interval until the context ends. On a terminal the
// screen is redrawn in place; otherwise each sample is emitted as it comes.
func (a *app) loop(ctx context.Context, interval time.Duration, draw func(ctx context.Context, first bool) error) error {
	if interval < time.Second {
		return usageErr("--interval must be at least 1s")
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	first := true
	for {
		if err := draw(ctx, first); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		first = false
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

func (a *app) redraw(render func(w *strings.Builder)) {
	var b strings.Builder
	render(&b)
	if a.stdoutTTY() && a.flags.output == "table" {
		fmt.Fprint(a.stdout, "\x1b[H\x1b[2J"+b.String())
		return
	}
	fmt.Fprint(a.stdout, b.String())
}

func (a *app) table(w *strings.Builder, t output.Tabular) {
	f, _ := output.Parse("table")
	if a.flags.output == "wide" {
		f, _ = output.Parse("wide")
	}
	_ = output.Write(w, f, t, output.Options{NoHeaders: a.flags.noHeaders})
}

func (a *app) jsonLine(v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(a.stdout, string(b))
	return err
}

func (a *app) streaming() bool { return a.flags.output == "json" || a.flags.output == "jsonl" }

type topicStatRows struct {
	stats       []monitor.TopicStats
	byPartition bool
}

func (r topicStatRows) Header(wide bool) []string {
	if r.byPartition {
		h := []string{"PART", "LEADER", "ISR", "MSG/S", "Δ WINDOW", "SIZE", "SKEW", "TREND", "STATUS"}
		if wide {
			h = append(h, "BYTES/S")
		}
		return h
	}
	h := []string{"TOPIC", "PARTS", "MSG/S", "Δ WINDOW", "SIZE", "URP", "TREND"}
	if wide {
		h = append(h, "BYTES/S", "AVG", "MAX")
	}
	return h
}

func (r topicStatRows) Rows(wide bool) [][]string {
	var rows [][]string
	for _, s := range r.stats {
		if r.byPartition {
			for _, p := range s.PartitionStats {
				row := []string{itoa(p.Partition), itoa(p.Leader), joinInts(p.ISR), units.Rate(p.MsgRate), units.Count(p.WindowDelta),
					units.Bytes(p.SizeBytes), fmt.Sprintf("%.1f×", p.Skew), spark(p.Trend, 12), p.Status}
				if wide {
					row = append(row, units.Bytes(int64(p.BytesRate))+"/s (approx)")
				}
				rows = append(rows, row)
			}
			rows = append(rows, []string{"TOTAL", "", "", units.Rate(s.MsgRate), units.Count(s.WindowDelta), units.Bytes(s.SizeBytes), "", spark(s.Trend, 12), ""})
			continue
		}
		row := []string{s.Topic, itoa(s.Partitions), units.Rate(s.MsgRate), units.Count(s.WindowDelta), units.Bytes(s.SizeBytes), itoa(s.URP), spark(s.Trend, 12)}
		if wide {
			row = append(row, units.Bytes(int64(s.BytesRate))+"/s", units.Rate(s.AvgRate), units.Rate(s.MaxRate))
		}
		rows = append(rows, row)
	}
	return rows
}

func (a *app) newTopicWatchCmd() *cobra.Command {
	var interval, window time.Duration
	var byPartition bool
	cmd := &cobra.Command{
		Use:               "watch <topic>...",
		Short:             "Live throughput, size, skew, and replication per topic (from offset deltas)",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			w := monitor.NewTopicWatcher(s.cl.Admin, args, window)
			return a.loop(cmd.Context(), interval, func(ctx context.Context, _ bool) error {
				stats, err := w.Sample(ctx)
				if err != nil {
					return err
				}
				if a.streaming() {
					for _, st := range stats {
						if err := a.jsonLine(st); err != nil {
							return err
						}
					}
					return nil
				}
				a.redraw(func(b *strings.Builder) {
					for i, st := range stats {
						if i > 0 {
							b.WriteString("\n")
						}
						fmt.Fprintf(b, "%s · %d partitions · RF %d · %s · %s/s now · %s avg · interval %s · window %s\n",
							st.Topic, st.Partitions, st.RF, units.Bytes(st.SizeBytes), units.Rate(st.MsgRate), units.Rate(st.AvgRate), interval, window)
						a.table(b, topicStatRows{[]monitor.TopicStats{st}, byPartition || len(stats) == 1})
					}
				})
				return nil
			})
		},
	}
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "sampling interval")
	cmd.Flags().DurationVar(&window, "window", time.Minute, "window for Δ columns")
	cmd.Flags().BoolVar(&byPartition, "by-partition", false, "one row per partition (default for a single topic)")
	return cmd
}

func (a *app) newTopicStatsCmd() *cobra.Command {
	var sample time.Duration
	cmd := &cobra.Command{
		Use:               "stats <topic>...",
		Short:             "One-shot snapshot: rates (sampled over --sample), sizes, skew",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			w := monitor.NewTopicWatcher(s.cl.Admin, args, time.Hour)
			w.SizeEvery = time.Hour
			if _, err := w.Sample(cmd.Context()); err != nil {
				return err
			}
			select {
			case <-cmd.Context().Done():
				return nil
			case <-time.After(sample):
			}
			stats, err := w.Sample(cmd.Context())
			if err != nil {
				return err
			}
			if a.streaming() {
				return a.render(stats)
			}
			var b strings.Builder
			a.table(&b, topicStatRows{stats, len(stats) == 1})
			fmt.Fprint(a.stdout, b.String())
			return nil
		},
	}
	cmd.Flags().DurationVar(&sample, "sample", 2*time.Second, "time between the two samples used for rates")
	return cmd
}

func (a *app) newTopicTopCmd() *cobra.Command {
	var sortBy string
	var n int
	var interval time.Duration
	var once bool
	cmd := &cobra.Command{
		Use:   "top [pattern]",
		Short: "Rank topics by throughput, size, or partitions",
		Args:  usageArgs(cobra.MaximumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			var names []string
			if len(args) == 1 {
				ts, err := topicNamesMatching(cmd.Context(), s, args[0])
				if err != nil {
					return err
				}
				if len(ts) == 0 {
					return exitcode.With(exitcode.NotFound, fmt.Errorf("no topics match %q", args[0]))
				}
				names = ts
			}
			w := monitor.NewTopicWatcher(s.cl.Admin, names, time.Minute)
			if md, err := s.cl.Admin.Metadata(cmd.Context()); err == nil {
				parts := 0
				for _, t := range md.Topics {
					parts += len(t.Partitions)
				}
				if parts > 5000 && !cmd.Flags().Changed("interval") {
					interval = 15 * time.Second
					fmt.Fprintf(a.stderr, "warning: %d partitions; using a 15s interval\n", parts)
				}
			}
			if _, err := w.Sample(cmd.Context()); err != nil {
				return err
			}
			if once {
				select {
				case <-cmd.Context().Done():
					return nil
				case <-time.After(2 * time.Second):
				}
			}
			return a.loop(cmd.Context(), interval, func(ctx context.Context, first bool) error {
				if first && !once {
					return nil
				}
				stats, err := w.Sample(ctx)
				if err != nil {
					return err
				}
				monitor.SortTopics(stats, sortBy)
				if len(stats) > n {
					stats = stats[:n]
				}
				if a.streaming() {
					if err := a.jsonLine(stats); err != nil {
						return err
					}
				} else {
					a.redraw(func(b *strings.Builder) { a.table(b, topicStatRows{stats: stats}) })
				}
				if once {
					return errStop
				}
				return nil
			})
		},
	}
	cmd.Flags().StringVar(&sortBy, "sort", "msgs", "msgs, bytes, size, or partitions")
	cmd.Flags().IntVarP(&n, "limit", "n", 20, "show the top N")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "sampling interval")
	cmd.Flags().BoolVar(&once, "once", false, "print one ranking and exit")
	return a.stoppable(cmd)
}

var errStop = errors.New("stop")

// stoppable lets a loop end early by returning errStop.
func (a *app) stoppable(cmd *cobra.Command) *cobra.Command {
	run := cmd.RunE
	cmd.RunE = func(c *cobra.Command, args []string) error {
		if err := run(c, args); !errors.Is(err, errStop) {
			return err
		}
		return nil
	}
	return cmd
}

func topicNamesMatching(ctx context.Context, s *session, pattern string) ([]string, error) {
	all, err := fetchTopics(ctx, s)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, t := range all {
		if ok, _ := path.Match(pattern, t); ok {
			out = append(out, t)
		}
	}
	return out, nil
}

type groupStatRows struct {
	stats       []monitor.GroupStats
	byPartition bool
	byMember    bool
}

func (r groupStatRows) Header(bool) []string {
	switch {
	case r.byPartition:
		return []string{"TOPIC/PART", "MEMBER", "LAG", "TREND", "CONSUME/S", "PRODUCE/S", "TIME LAG", "STATUS"}
	case r.byMember:
		return []string{"MEMBER", "PARTITIONS", "LAG", "CONSUME/S"}
	}
	return []string{"GROUP", "TOPIC", "LAG", "TREND", "CONSUME/S", "PRODUCE/S", "TIME LAG", "ETA", "STATUS"}
}

func (r groupStatRows) Rows(bool) [][]string {
	var rows [][]string
	for _, s := range r.stats {
		switch {
		case r.byPartition:
			for _, p := range s.Partitions {
				rows = append(rows, []string{fmt.Sprintf("%s/%d", p.Topic, p.Partition), orDash(p.ClientID), units.Count(p.Lag), spark(p.Trend, 8),
					units.Rate(p.ConsumeRate), units.Rate(p.ProduceRate), timeLag(p.TimeLag, p.TimeLagOK), string(p.Status)})
			}
		case r.byMember:
			type agg struct {
				parts int
				lag   int64
				rate  float64
			}
			members := map[string]*agg{}
			for _, p := range s.Partitions {
				m := orDash(p.ClientID)
				if members[m] == nil {
					members[m] = &agg{}
				}
				members[m].parts++
				members[m].lag += p.Lag
				members[m].rate += p.ConsumeRate
			}
			for _, m := range slices.Sorted(maps.Keys(members)) {
				x := members[m]
				rows = append(rows, []string{m, itoa(x.parts), units.Count(x.lag), units.Rate(x.rate)})
			}
		default:
			if len(s.Topics) == 0 {
				rows = append(rows, []string{s.Group, "-", "0", "", "0", "0", "-", "-", string(s.Status)})
			}
			for _, t := range s.Topics {
				trend := spark(t.Trend, 8)
				switch {
				case len(t.Trend) >= 2 && t.Trend[len(t.Trend)-1] < t.Trend[0]:
					trend += " ↓"
				case len(t.Trend) >= 2 && t.Trend[len(t.Trend)-1] > t.Trend[0]:
					trend += " ↑"
				case len(t.Trend) >= 2:
					trend += " ="
				}
				eta := "-"
				if t.ETAOK {
					eta = "~" + roundDur(t.ETA)
				}
				rows = append(rows, []string{s.Group, t.Topic, units.Count(t.Lag), trend, units.Rate(t.ConsumeRate), units.Rate(t.ProduceRate),
					timeLag(t.TimeLag, t.TimeLagOK), eta, string(t.Status)})
			}
		}
	}
	return rows
}

func timeLag(d time.Duration, ok bool) string {
	if !ok {
		return "-"
	}
	if d == 0 {
		return "0s"
	}
	return "~" + roundDur(d)
}

func roundDur(d time.Duration) string {
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds", int(d.Seconds()+0.5))
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()+0.5))
	}
	return fmt.Sprintf("%.1fh", d.Hours())
}

func (a *app) exactTimestamp(s *session) monitor.TimestampAt {
	return func(ctx context.Context, topic string, partition int32, offset int64) (time.Time, error) {
		return fetchTimestamp(ctx, s, topic, partition, offset)
	}
}

func fetchTimestamp(ctx context.Context, s *session, topic string, partition int32, offset int64) (time.Time, error) {
	cl, err := newConsumerClient(s, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(offset)}}))
	if err != nil {
		return time.Time{}, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	fs := cl.PollRecords(ctx, 1)
	if recs := fs.Records(); len(recs) > 0 {
		return recs[0].Timestamp, nil
	}
	return time.Time{}, errors.New("no record at offset")
}

func (a *app) newGroupWatchCmd() *cobra.Command {
	var interval, window, stuckAfter time.Duration
	var byPartition, exact bool
	var topic string
	cmd := &cobra.Command{
		Use:               "watch <group>...",
		Short:             "Live lag, consume vs produce rate, time lag, ETA, and stuck/rebalance detection",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeGroupArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			w := monitor.NewGroupWatcher(s.cl.Client, s.cl.Admin, args, "", window)
			w.StuckAfter = stuckAfter
			if exact {
				w.Exact = a.exactTimestamp(s)
			}
			var events []string
			return a.loop(cmd.Context(), interval, func(ctx context.Context, _ bool) error {
				stats, err := w.Sample(ctx)
				if err != nil {
					return err
				}
				if topic != "" {
					for i := range stats {
						stats[i].Topics = slices.DeleteFunc(stats[i].Topics, func(t monitor.TopicLag) bool { return t.Topic != topic })
						stats[i].Partitions = slices.DeleteFunc(stats[i].Partitions, func(p monitor.PartitionLag) bool { return p.Topic != topic })
					}
				}
				if a.streaming() {
					for _, st := range stats {
						if err := a.jsonLine(st); err != nil {
							return err
						}
					}
					return nil
				}
				for _, st := range stats {
					events = append(events, st.Events...)
				}
				if len(events) > 5 {
					events = events[len(events)-5:]
				}
				a.redraw(func(b *strings.Builder) {
					for _, st := range stats {
						fmt.Fprintf(b, "%s · %s · %s · %d members · lag %s (%+d in %s) · %s\n", st.Group, st.Type, st.State, st.Members,
							units.Count(st.Lag), st.LagDelta, window, st.Status)
					}
					a.table(b, groupStatRows{stats: stats, byPartition: byPartition})
					if len(events) > 0 {
						b.WriteString("\nEvents:\n")
						for _, e := range events {
							b.WriteString("  " + e + "\n")
						}
					}
				})
				return nil
			})
		},
	}
	f := cmd.Flags()
	f.DurationVar(&interval, "interval", 5*time.Second, "sampling interval")
	f.DurationVar(&window, "window", time.Minute, "window for trends")
	f.DurationVar(&stuckAfter, "stuck-after", 2*time.Minute, "lag > 0 with no commits for this long = stuck")
	f.BoolVar(&byPartition, "by-partition", false, "one row per partition")
	f.BoolVar(&exact, "exact-time-lag", false, "read the record at the committed offset for an exact time lag")
	f.StringVar(&topic, "topic", "", "only this topic")
	return cmd
}

func (a *app) newGroupLagCmd() *cobra.Command {
	var threshold int64
	var maxTimeLag, stuckAfter, sample time.Duration
	var failOnStuck, exact bool
	var topic string
	cmd := &cobra.Command{
		Use:   "lag <group>...",
		Short: "One-shot lag check; with thresholds, exits 5 when breached",
		Example: "  ntk group lag svc-orders --threshold 10000 --max-time-lag 5m\n" +
			"  ntk group lag 'svc-*' --fail-on-stuck --stuck-after 2m",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeGroupArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			names, pattern := args, ""
			if len(args) == 1 && strings.ContainsAny(args[0], "*?[") {
				names, pattern = nil, args[0]
			}
			w := monitor.NewGroupWatcher(s.cl.Client, s.cl.Admin, names, pattern, time.Hour)
			w.StuckAfter = stuckAfter
			if exact {
				w.Exact = a.exactTimestamp(s)
			}
			stats, err := w.Sample(cmd.Context())
			if err != nil {
				return err
			}
			wait := time.Duration(0)
			if failOnStuck {
				wait = stuckAfter
			} else if maxTimeLag > 0 && !exact {
				wait = sample
			}
			if wait > 0 {
				fmt.Fprintf(a.stderr, "sampling for %s ...\n", wait)
				deadline := time.Now().Add(wait)
				for time.Now().Before(deadline) {
					select {
					case <-cmd.Context().Done():
						return cmd.Context().Err()
					case <-time.After(min(sample, time.Until(deadline))):
					}
					if stats, err = w.Sample(cmd.Context()); err != nil {
						return err
					}
				}
			}
			if topic != "" {
				for i := range stats {
					stats[i].Topics = slices.DeleteFunc(stats[i].Topics, func(t monitor.TopicLag) bool { return t.Topic != topic })
				}
			}
			var breaches []string
			for _, st := range stats {
				for _, t := range st.Topics {
					if threshold > 0 && t.Lag > threshold {
						breaches = append(breaches, fmt.Sprintf("%s %s: lag %s > %s", st.Group, t.Topic, units.Count(t.Lag), units.Count(threshold)))
					}
					if maxTimeLag > 0 && t.TimeLagOK && t.TimeLag > maxTimeLag {
						breaches = append(breaches, fmt.Sprintf("%s %s: time lag %s > %s", st.Group, t.Topic, roundDur(t.TimeLag), maxTimeLag))
					}
					if failOnStuck && t.Status == monitor.Stuck {
						breaches = append(breaches, fmt.Sprintf("%s %s: stuck", st.Group, t.Topic))
					}
				}
			}
			if a.streaming() {
				if err := a.render(stats); err != nil {
					return err
				}
			} else {
				var b strings.Builder
				a.table(&b, groupStatRows{stats: stats})
				fmt.Fprint(a.stdout, b.String())
			}
			if len(breaches) > 0 {
				return exitcode.With(exitcode.CheckFailed, errors.New(strings.Join(breaches, "; ")))
			}
			return nil
		},
	}
	f := cmd.Flags()
	f.Int64Var(&threshold, "threshold", 0, "fail (exit 5) if any topic's lag is above N")
	f.DurationVar(&maxTimeLag, "max-time-lag", 0, "fail (exit 5) if any topic's time lag is above this")
	f.BoolVar(&failOnStuck, "fail-on-stuck", false, "fail (exit 5) if any partition is stuck (samples for --stuck-after)")
	f.DurationVar(&stuckAfter, "stuck-after", 2*time.Minute, "lag > 0 with no commits for this long = stuck")
	f.DurationVar(&sample, "sample", 5*time.Second, "sampling interval when rates are needed")
	f.BoolVar(&exact, "exact-time-lag", false, "read the record at the committed offset for an exact time lag")
	f.StringVar(&topic, "topic", "", "only this topic")
	return cmd
}

func (a *app) newGroupTopCmd() *cobra.Command {
	var sortBy string
	var n int
	var interval time.Duration
	var once bool
	cmd := &cobra.Command{
		Use:   "top [pattern]",
		Short: "Rank consumer groups by lag, time lag, or consume rate",
		Args:  usageArgs(cobra.MaximumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pattern := ""
			if len(args) == 1 {
				pattern = args[0]
			}
			w := monitor.NewGroupWatcher(s.cl.Client, s.cl.Admin, nil, pattern, time.Minute)
			if _, err := w.Sample(cmd.Context()); err != nil {
				return err
			}
			if once {
				select {
				case <-cmd.Context().Done():
					return nil
				case <-time.After(2 * time.Second):
				}
			}
			return a.loop(cmd.Context(), interval, func(ctx context.Context, first bool) error {
				if first && !once {
					return nil
				}
				stats, err := w.Sample(ctx)
				if err != nil {
					return err
				}
				monitor.SortGroups(stats, sortBy)
				if len(stats) > n {
					stats = stats[:n]
				}
				if a.streaming() {
					if err := a.jsonLine(stats); err != nil {
						return err
					}
				} else {
					a.redraw(func(b *strings.Builder) { a.table(b, groupStatRows{stats: stats}) })
				}
				if once {
					return errStop
				}
				return nil
			})
		},
	}
	cmd.Flags().StringVar(&sortBy, "sort", "lag", "lag, time-lag, or rate")
	cmd.Flags().IntVarP(&n, "limit", "n", 20, "show the top N")
	cmd.Flags().DurationVar(&interval, "interval", 5*time.Second, "sampling interval")
	cmd.Flags().BoolVar(&once, "once", false, "print one ranking and exit")
	return a.stoppable(cmd)
}
