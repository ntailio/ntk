// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/consume"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/sink"
	"github.com/ntailio/ntk/units"
)

type consumeFlags struct {
	partitions   string
	keyPartition string
	from, until  string
	limit        int
	follow       bool
	meta         bool
	delimiter    string
	noEscape     bool
	unixWait     bool
	execTimeout  time.Duration
	execParallel int
	execContinue bool
	receipt      bool
	group        string
	noCommit     bool
	isolation    string
	quiet        bool
}

func completeConsumeOutput(toComplete string) ([]string, cobra.ShellCompDirective) {
	if rest, ok := strings.CutPrefix(toComplete, "unix:"); ok {
		return unixPaths(rest), cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	}
	return []string{"raw", "jsonl", "unix:", "exec:"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}

func (a *app) newConsumeCmd() *cobra.Command {
	var f consumeFlags
	cmd := &cobra.Command{
		Use:     "consume <topic>[,topic...]",
		Aliases: []string{"c"},
		Short:   "Read messages (raw bytes; no consumer group unless -g)",
		Long: "Reads all partitions by default and writes raw message values.\n" +
			"-o selects the output target: raw (default), jsonl, unix:<path>, exec:<command>.",
		Example: "  ntk c orders --from -5 -m\n" +
			"  ntk c orders --from -1h -o 'exec:jq -c \"select(.status == \\\"FAILED\\\")\"'\n" +
			"  ntk c orders -f -m -o unix:/run/myapp/ingest.sock --unix-wait\n" +
			"  ntk c orders --from earliest --until -1d -o jsonl > old.jsonl",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeTopicList,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runConsume(cmd, splitList(args[0]), f)
		},
	}
	fl := cmd.Flags()
	fl.StringVarP(&f.partitions, "partition", "P", "", "only these partitions, e.g. 0,3-5 (single topic only)")
	fl.StringVar(&f.keyPartition, "key-partition", "", "only the partition this key hashes to (murmur2)")
	fl.StringVar(&f.from, "from", "", "start: latest (default), earliest, -N per partition, @offset, a time, -1h")
	fl.StringVar(&f.until, "until", "", "stop before: @offset, a time, -1h, latest")
	fl.IntVarP(&f.limit, "limit", "n", 0, "stop after N messages")
	fl.BoolVarP(&f.follow, "follow", "f", false, "keep waiting for new messages (default with --from latest)")
	fl.BoolVarP(&f.meta, "meta", "m", false, "prefix each message with a single-line JSON metadata line")
	fl.StringVar(&f.delimiter, "delimiter", `\n`, `-o raw: message separator (ASCII; \n \r \t \0 \\ \xHH)`)
	fl.BoolVar(&f.noEscape, "no-escape", false, "-o raw: write control bytes as-is even to a terminal")
	fl.BoolVar(&f.unixWait, "unix-wait", false, "-o unix: wait for the socket to appear")
	fl.DurationVar(&f.execTimeout, "exec-timeout", 30*time.Second, "-o exec: per-command timeout (0 = none)")
	fl.IntVar(&f.execParallel, "exec-parallel", 1, "-o exec: commands at once (>1 loses ordering)")
	fl.BoolVar(&f.execContinue, "exec-continue", false, "-o exec: log failures and keep going")
	fl.BoolVar(&f.receipt, "receipt", false, "-o unix:/exec: write one JSON receipt line per message to stdout")
	fl.StringVarP(&f.group, "group", "g", "", "consume as a member of this group (commits offsets)")
	fl.BoolVar(&f.noCommit, "no-commit", false, "with -g: never commit offsets")
	fl.StringVar(&f.isolation, "isolation", "read_uncommitted", "read_uncommitted or read_committed")
	fl.BoolVarP(&f.quiet, "quiet", "q", false, "no progress line on stderr")
	_ = cmd.RegisterFlagCompletionFunc("from", cobra.FixedCompletions([]string{"latest", "earliest", "-10", "-1h", "-15m", "@"}, cobra.ShellCompDirectiveNoFileComp|cobra.ShellCompDirectiveNoSpace))
	_ = cmd.RegisterFlagCompletionFunc("until", cobra.FixedCompletions([]string{"latest", "-1h", "-15m", "@"}, cobra.ShellCompDirectiveNoFileComp|cobra.ShellCompDirectiveNoSpace))
	_ = cmd.RegisterFlagCompletionFunc("isolation", cobra.FixedCompletions([]string{"read_uncommitted", "read_committed"}, cobra.ShellCompDirectiveNoFileComp))
	_ = cmd.RegisterFlagCompletionFunc("group", a.completeGroupArg)
	_ = cmd.RegisterFlagCompletionFunc("partition", a.completePartitions)
	return cmd
}

func (a *app) runConsume(cmd *cobra.Command, topicNames []string, f consumeFlags) error {
	out := "raw"
	if cmd.Flag("output").Changed {
		out = a.flags.output
	}
	target, err := sink.ParseTarget(out)
	if err != nil {
		return usageErr("%v", err)
	}
	fl := cmd.Flags()
	check := func(flag string, kinds ...string) error {
		if !fl.Changed(flag) {
			return nil
		}
		for _, k := range kinds {
			if target.Kind == k {
				return nil
			}
		}
		return usageErr("--%s only applies to -o %s", flag, strings.Join(kinds, "/"))
	}
	for _, c := range []struct {
		flag  string
		kinds []string
	}{
		{"delimiter", []string{"raw"}}, {"no-escape", []string{"raw"}}, {"unix-wait", []string{"unix"}},
		{"exec-timeout", []string{"exec"}}, {"exec-parallel", []string{"exec"}}, {"exec-continue", []string{"exec"}},
		{"receipt", []string{"unix", "exec"}},
	} {
		if err := check(c.flag, c.kinds...); err != nil {
			return err
		}
	}
	delim, err := sink.ParseDelimiter(f.delimiter)
	if err != nil {
		return usageErr("%v", err)
	}
	if f.isolation != "read_uncommitted" && f.isolation != "read_committed" {
		return usageErr("--isolation must be read_uncommitted or read_committed")
	}

	now := time.Now()
	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	fromStr := f.from
	if fromStr == "" && s.prof.Defaults != nil && s.prof.Defaults.Consume != nil {
		fromStr = s.prof.Defaults.Consume.Start
	}
	from, err := consume.ParsePosition(fromStr, now)
	if err != nil {
		return usageErr("--from: %v", err)
	}
	if a.flags.dryRun && f.group != "" {
		f.noCommit = true
	}
	o := consume.Options{Topics: topicNames, From: from, Limit: f.limit, Group: f.group, NoCommit: f.noCommit,
		ReadCommit: f.isolation == "read_committed", Follow: f.follow || from.Kind == consume.Latest}
	if f.until != "" {
		u, err := consume.ParsePosition(f.until, now)
		if err != nil {
			return usageErr("--until: %v", err)
		}
		o.Until = &u
		o.Follow = false
	}
	if f.partitions != "" {
		if len(topicNames) > 1 {
			return usageErr("-P/--partition needs a single topic")
		}
		if o.Partitions, err = parseIDs(f.partitions); err != nil {
			return err
		}
	}
	if f.keyPartition != "" {
		o.KeyPartition = []byte(f.keyPartition)
	}
	if f.group != "" && !f.noCommit && s.prof.ReadOnly {
		return exitcode.With(exitcode.Refused, fmt.Errorf("refused: profile %q is read-only and -g commits offsets (add --no-commit)", s.name))
	}

	pl, err := consume.Resolve(cmd.Context(), s.cl.Admin, o)
	if err != nil {
		return err
	}
	if from.Kind == consume.Earliest && f.limit == 0 && o.Until == nil && f.group == "" {
		if size := consume.EstimateBytes(cmd.Context(), s.cl.Admin, pl); size > 1<<30 {
			if a.canPrompt() && !a.flags.yes {
				if ans := strings.ToLower(a.prompt(fmt.Sprintf("This will read %s. Continue? [y/N] ", units.Bytes(size)))); ans != "y" && ans != "yes" {
					return exitcode.With(exitcode.Refused, errors.New("aborted"))
				}
			} else {
				fmt.Fprintf(a.stderr, "warning: reading %s from the start of %s\n", units.Bytes(size), strings.Join(topicNames, ","))
			}
		}
	}

	var receiptMu sync.Mutex
	var receiptFn func(sink.Receipt)
	if f.receipt {
		receiptFn = func(r sink.Receipt) {
			b, _ := json.Marshal(r)
			receiptMu.Lock()
			fmt.Fprintln(a.stdout, string(b))
			receiptMu.Unlock()
		}
	}
	var out_ sink.Sink
	switch target.Kind {
	case "raw":
		out_ = &sink.Raw{W: a.stdout, Meta: f.meta, Delimiter: delim, Escape: a.stdoutTTY() && !f.noEscape}
	case "jsonl":
		out_ = &sink.JSONL{W: a.stdout}
	case "unix":
		u, err := sink.DialUnix(cmd.Context(), target.Arg, f.meta, f.unixWait)
		if err != nil {
			return exitcode.With(exitcode.OutputTarget, err)
		}
		out_ = u
	case "exec":
		stdout := a.stdout
		if f.receipt {
			stdout = a.stderr
		}
		out_ = &sink.Exec{Command: target.Arg, Meta: f.meta, Timeout: f.execTimeout, Parallel: f.execParallel, Stdout: stdout, Stderr: a.stderr}
	}
	defer out_.Close()

	cl, err := newConsumerClient(s, consume.ClientOptions(pl, o)...)
	if err != nil {
		return err
	}
	defer cl.Close()

	if f.group != "" && !f.quiet {
		if f.noCommit {
			fmt.Fprintf(a.stderr, "notice: joining group %q without committing offsets\n", f.group)
		} else {
			fmt.Fprintf(a.stderr, "notice: consuming as group %q: offsets are committed after each message is delivered (--no-commit to avoid)\n", f.group)
		}
	}
	showProgress := !f.quiet && stderrTTY(a)
	if showProgress {
		iso := ""
		if f.isolation == "read_committed" {
			iso = " · read_committed"
		}
		fmt.Fprintf(a.stderr, "consuming %s · %s%s\n", strings.Join(topicNames, ","), units.Plural(int64(pl.Partitions()), "partition"), iso)
	}
	// The redrawn line would get mixed into message output on the same terminal.
	liveProgress := showProgress && (!stdoutTTY(a) || (target.Kind == "unix" && !f.receipt))
	var lastDraw time.Time
	progress := func(st consume.Stats) {
		if !liveProgress || time.Since(lastDraw) < 200*time.Millisecond {
			return
		}
		lastDraw = time.Now()
		at := ""
		if st.Last != nil {
			at = fmt.Sprintf(" · at %s/%d @%s", st.Last.Topic, st.Last.Partition, units.Count(st.Last.Offset))
		}
		fmt.Fprintf(a.stderr, "\r\x1b[2K%s%s", units.Plural(st.Consumed, "message"), at)
	}
	stats, err := consume.Run(cmd.Context(), cl.Client, pl, o, out_, receiptFn, f.execContinue, progress)
	if showProgress {
		fmt.Fprintf(a.stderr, "\r\x1b[2KConsumed %s from %s\n", units.Plural(stats.Consumed, "message"), units.Plural(int64(stats.Partitions), "partition"))
	}
	var target_ *sink.ErrTarget
	switch {
	case errors.As(err, &target_):
		return exitcode.With(exitcode.OutputTarget, err)
	case err != nil && cmd.Context().Err() != nil:
		return nil
	}
	return err
}

func stdoutTTY(a *app) bool {
	f, ok := a.stdout.(*os.File)
	return ok && term.IsTerminal(f.Fd())
}

func stderrTTY(a *app) bool {
	f, ok := a.stderr.(*os.File)
	return ok && term.IsTerminal(f.Fd())
}

func (a *app) completePartitions(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	if len(args) == 0 {
		return nil, cobra.ShellCompDirectiveNoFileComp
	}
	topic := splitList(args[0])[0]
	items := a.cached(cmd.Context(), "partitions-"+topic, func(ctx context.Context, s *session) ([]string, error) {
		td, err := s.cl.Admin.ListTopics(ctx, topic)
		if err != nil {
			return nil, err
		}
		var out []string
		for _, p := range td[topic].Partitions.Numbers() {
			out = append(out, itoa(p))
		}
		return out, nil
	})
	return filterPrefix(items, toComplete, nil), cobra.ShellCompDirectiveNoFileComp
}
