// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/consume"
	"github.com/factualtech/ntk/exitcode"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/produce"
	"github.com/factualtech/ntk/sink"
	"github.com/factualtech/ntk/tui"
	"github.com/factualtech/ntk/units"
)

type produceFlags struct {
	key, value    string
	headers       []string
	delimiter     string
	keySep        string
	file          string
	fromTopic     string
	fromProfile   string
	from, until   string
	limit         int
	interactive   bool
	partition     int32
	opts          produce.Options
	rate          string
	count         int
	repeat        bool
	template      bool
	tombstone     bool
	keepPartition bool
	keepTimestamp bool
	keepTopic     bool
	in            string
	meta          bool
	idle          time.Duration
}

func (a *app) newProduceCmd() *cobra.Command {
	var f produceFlags
	cmd := &cobra.Command{
		Use:     "produce <topic>",
		Aliases: []string{"p"},
		Short:   "Send messages (bytes as given; from flags, stdin, a record file, or another topic)",
		Example: "  ntk p orders -k order-1 -v '{\"id\":\"order-1\"}' -H tenant=acme\n" +
			"  cat values.txt | ntk p orders\n" +
			"  ntk p orders --in jsonl -f failed.jsonl\n" +
			"  ntk p orders --in unix:/tmp/orders.sock -m\n" +
			"  ntk c orders -m | ntk -p staging p orders -m\n" +
			"  ntk p orders.retry --from-topic orders.dlq --from earliest",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.runProduce(cmd, args[0], f)
		},
	}
	fl := cmd.Flags()
	fl.StringVarP(&f.key, "key", "k", "", "message key (@path reads a file)")
	fl.StringVarP(&f.value, "value", "v", "", "message value (@path reads a file, @- reads stdin as one message)")
	fl.StringArrayVarP(&f.headers, "header", "H", nil, "header key=value (repeatable)")
	fl.StringVarP(&f.in, "in", "I", "raw", "input: raw (delimited values), jsonl (ntk records), or unix:<path> (one datagram per message)")
	fl.BoolVarP(&f.meta, "meta", "m", false, "raw and unix: each message starts with the consume -m metadata line")
	fl.StringVarP(&f.file, "file", "f", "", "read the input from this file instead of stdin (format from --in)")
	fl.StringVar(&f.delimiter, "delimiter", `\n`, "raw: message separator (same syntax as consume)")
	fl.StringVar(&f.keySep, "key-sep", "", "raw without -m: split each message into key<sep>value")
	fl.DurationVar(&f.idle, "idle-timeout", 0, "unix: stop after this long without a datagram (default: until interrupted)")
	fl.StringVar(&f.fromTopic, "from-topic", "", "copy messages from this topic")
	fl.StringVar(&f.fromProfile, "from-profile", "", "with --from-topic: read from this profile")
	fl.StringVar(&f.from, "from", "earliest", "with --from-topic: start position (as consume --from)")
	fl.StringVar(&f.until, "until", "", "with --from-topic: stop position (as consume --until)")
	fl.IntVarP(&f.limit, "limit", "n", 0, "stop after N messages (stdin, file, unix socket, or --from-topic)")
	fl.BoolVarP(&f.interactive, "interactive", "i", false, "open the interactive composer")
	fl.Int32VarP(&f.partition, "partition", "P", -1, "force a partition")
	fl.StringVar(&f.opts.Partitioner, "partitioner", "murmur2", "murmur2 (Java-compatible), round-robin, or sticky")
	fl.StringVar(&f.opts.Acks, "acks", "all", "all, 1, or 0")
	fl.StringVar(&f.opts.Compression, "compression", "none", "none, gzip, snappy, lz4, or zstd")
	fl.BoolVar(&f.opts.Idempotent, "idempotent", true, "idempotent producer")
	fl.StringVar(&f.opts.Transactional, "transactional-id", "", "produce everything in one transaction")
	fl.StringVar(&f.rate, "rate", "", "throttle, e.g. 100/s")
	fl.IntVar(&f.count, "count", 0, "send the flag message N times")
	fl.BoolVar(&f.repeat, "repeat", false, "send the flag message until interrupted")
	fl.BoolVar(&f.template, "template", false, "expand {{.i}}, {{uuid}}, {{now}} in -k/-v")
	fl.BoolVar(&f.tombstone, "tombstone", false, "send a null value (needs -k)")
	fl.BoolVar(&f.keepPartition, "keep-partition", false, "metadata input (--in jsonl or -m): keep each record's partition")
	fl.BoolVar(&f.keepTimestamp, "keep-timestamp", false, "metadata input (--in jsonl or -m): keep each record's timestamp")
	fl.BoolVar(&f.keepTopic, "keep-topic", false, "metadata input (--in jsonl or -m): send each record to its original topic")
	_ = cmd.RegisterFlagCompletionFunc("from-topic", a.completeTopicArg)
	_ = cmd.RegisterFlagCompletionFunc("from-profile", a.completeProfiles)
	_ = cmd.RegisterFlagCompletionFunc("in", func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if rest, ok := strings.CutPrefix(toComplete, "unix:"); ok {
			return unixPaths(rest), cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
		}
		return []string{"raw", "jsonl", "unix:"}, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	})
	for flag, vals := range map[string][]string{
		"partitioner": {"murmur2", "round-robin", "sticky"}, "acks": {"all", "1", "0"},
		"compression": {"none", "gzip", "snappy", "lz4", "zstd"},
	} {
		_ = cmd.RegisterFlagCompletionFunc(flag, cobra.FixedCompletions(vals, cobra.ShellCompDirectiveNoFileComp))
	}
	return cmd
}

func readArg(v string, stdin io.Reader) ([]byte, error) {
	switch {
	case v == "@-":
		return io.ReadAll(stdin)
	case strings.HasPrefix(v, "@"):
		return os.ReadFile(v[1:])
	}
	return []byte(v), nil
}

func uuid() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6], b[8] = (b[6]&0x0f)|0x40, (b[8]&0x3f)|0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

type produceInput struct{ kind, path string }

func parseProduceInput(s string) (produceInput, error) {
	switch {
	case s == "" || s == "raw":
		return produceInput{kind: "raw"}, nil
	case s == "jsonl":
		return produceInput{kind: "jsonl"}, nil
	case strings.HasPrefix(s, "unix:"):
		if strings.TrimPrefix(s, "unix:") == "" {
			return produceInput{}, errors.New("--in unix: needs a socket path")
		}
		return produceInput{kind: "unix", path: strings.TrimPrefix(s, "unix:")}, nil
	}
	return produceInput{}, fmt.Errorf("unknown produce input %q (use raw, jsonl, or unix:<path>)", s)
}

func (a *app) runProduce(cmd *cobra.Command, topic string, f produceFlags) error {
	fl := cmd.Flags()
	in, err := parseProduceInput(f.in)
	if err != nil {
		return usageErr("%v", err)
	}
	single := fl.Changed("key") || fl.Changed("value") || f.tombstone
	stream := fl.Changed("in") || f.file != ""
	sources := 0
	for _, on := range []bool{single, stream, f.fromTopic != "", f.interactive} {
		if on {
			sources++
		}
	}
	if sources > 1 {
		return usageErr("use one input: -k/-v, --in/-f (or stdin), --from-topic, or -i")
	}
	stream = sources == 0 || stream
	keep := produce.Keep{Topic: f.keepTopic, Partition: f.keepPartition, Timestamp: f.keepTimestamp}
	switch {
	case f.meta && !stream:
		return usageErr("-m applies to --in raw and --in unix:")
	case keep != produce.Keep{} && !(stream && (in.kind == "jsonl" || f.meta)):
		return usageErr("--keep-topic, --keep-partition, and --keep-timestamp need metadata input (--in jsonl, or -m)")
	case in.kind == "unix" && f.file != "":
		return usageErr("-f reads a file; --in unix: reads the socket. Use one")
	case in.kind != "raw" && (fl.Changed("delimiter") || fl.Changed("key-sep")):
		return usageErr("--delimiter and --key-sep only apply to --in raw")
	case f.keySep != "" && f.meta:
		return usageErr("--key-sep can't be used with -m (the key comes from the metadata line)")
	case fl.Changed("idle-timeout") && in.kind != "unix":
		return usageErr("--idle-timeout only applies to --in unix:")
	case f.limit > 0 && single:
		return usageErr("-n applies to bulk input; use --count to repeat a message")
	}
	if f.tombstone && (f.key == "" || fl.Changed("value")) {
		return usageErr("--tombstone needs -k and no -v")
	}
	if f.rate != "" {
		n, err := fmt.Sscanf(strings.TrimSuffix(f.rate, "/s"), "%g", &f.opts.Rate)
		if n != 1 || err != nil || f.opts.Rate <= 0 {
			return usageErr("--rate %q: use e.g. 100/s", f.rate)
		}
	}
	f.opts.Partition = f.partition

	var base kgo.Record
	base.Topic, base.Partition = topic, -1
	for _, h := range f.headers {
		k, v, ok := strings.Cut(h, "=")
		if !ok {
			return usageErr("--header %q is not key=value", h)
		}
		base.Headers = append(base.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}

	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	if s.prof.ReadOnly {
		return exitcode.With(exitcode.Refused, fmt.Errorf("refused: profile %q is read-only", s.name))
	}
	if f.interactive {
		return a.produceInteractive(cmd.Context(), s, topic, f)
	}
	if f.fromTopic != "" {
		return a.copyTopic(cmd, s, topic, f)
	}

	manual := f.keepPartition
	var src produce.Source
	bulk, summary := false, ""
	if single {
		k, err := readArg(f.key, a.stdin)
		if err != nil {
			return err
		}
		v, err := readArg(f.value, a.stdin)
		if err != nil {
			return err
		}
		if fl.Changed("key") {
			base.Key = k
		}
		if !f.tombstone {
			base.Value = v
			if base.Value == nil {
				base.Value = []byte{}
			}
		}
		count := 1
		if f.count > 0 {
			count = f.count
		}
		if f.repeat {
			count = 0
		}
		if f.template {
			i := 0
			src = func(context.Context) (*kgo.Record, error) {
				if count > 0 && i >= count {
					return nil, io.EOF
				}
				i++
				r := base
				r.Headers = slices.Clone(base.Headers)
				if base.Key != nil {
					r.Key = []byte(produce.Template(string(base.Key), i, uuid))
				}
				if base.Value != nil {
					r.Value = []byte(produce.Template(string(base.Value), i, uuid))
				}
				return &r, nil
			}
		} else {
			src = produce.Single(&base, count)
		}
		bulk = count != 1
		summary = fmt.Sprintf("%d message(s)", count)
	} else {
		bulk = true
		var r io.Reader = a.stdin
		from := "stdin"
		switch {
		case in.kind == "unix":
			from = "unix socket " + in.path
		case f.file != "" && f.file != "-":
			fh, err := os.Open(f.file)
			if err != nil {
				return err
			}
			defer fh.Close()
			r, from = fh, f.file
		case a.canPrompt() && !accessible():
			return usageErr("no input: use -k/-v, --in/-f, --from-topic, -i, or pipe data to stdin")
		}
		f.opts.Graceful = from == "stdin" || in.kind == "unix"
		switch in.kind {
		case "jsonl":
			src = produce.WithHeaders(produce.Records(r, topic, keep), base.Headers)
		case "raw":
			delim, err := sink.ParseDelimiter(f.delimiter)
			if err != nil {
				return usageErr("%v", err)
			}
			if f.meta {
				src = produce.WithHeaders(produce.MetaStream(r, delim, topic, keep), base.Headers)
				break
			}
			var sep []byte
			if f.keySep != "" {
				if sep, err = sink.ParseDelimiter(f.keySep); err != nil {
					return usageErr("--key-sep: %v", err)
				}
			}
			src = produce.Lines(r, delim, sep, base)
		}
		if from == "stdin" {
			src = produce.UntilCancel(src)
		}
		summary = "messages from " + from + " to " + topic
	}

	if bulk && slices.Contains(s.prof.Labels, "prod") {
		pl := &plan.Plan{Class: plan.Change, Summary: "Produce " + summary + ":", Changes: []string{"→ " + topic}}
		if f.keepTopic {
			pl.Changes = []string{"→ each record's original topic"}
		}
		if err := a.confirmPlan(s, pl); err != nil {
			return err
		}
	}
	if f.tombstone {
		if d, err := describeCleanup(cmd.Context(), s, topic); err == nil && strings.Contains(d, "compact") && slices.Contains(s.prof.Labels, "prod") {
			pl := &plan.Plan{Class: plan.Change, Summary: "Delete key from compacted topic:", Changes: []string{fmt.Sprintf("- %s key=%q", topic, f.key)}}
			if err := a.confirmPlan(s, pl); err != nil {
				return err
			}
		}
	}

	opts, err := produce.ClientOptions(f.opts, manual)
	if err != nil {
		return usageErr("%v", err)
	}
	cl, err := newConsumerClient(s, opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	// Guards stderr/stdout: deliveries and the stop note are written from other goroutines.
	var mu sync.Mutex
	if in.kind == "unix" && !single {
		l, err := produce.Listen(in.path)
		if errors.Is(err, produce.ErrUnixUnsupported) {
			return exitcode.With(exitcode.Unsupported, err)
		}
		if err != nil {
			return err
		}
		defer l.Close()
		src = l.Source(f.meta, base, keep, f.idle)
		fmt.Fprintf(a.stderr, "Listening on %s (ctrl-c to stop)\n", in.path)
		stopNote := context.AfterFunc(cmd.Context(), func() {
			mu.Lock()
			defer mu.Unlock()
			fmt.Fprintln(a.stderr, "Stopping: producing what was received (interrupt again to quit immediately)")
		})
		defer stopNote()
	}
	src = produce.Limit(src, f.limit)

	jsonOut := a.flags.output == "json" || a.flags.output == "jsonl"
	deliver := func(d produce.Delivery) {
		mu.Lock()
		defer mu.Unlock()
		switch {
		case jsonOut:
			b, _ := json.Marshal(d)
			fmt.Fprintln(a.stdout, string(b))
		case !bulk && d.Error == "":
			fmt.Fprintf(a.stderr, "→ %s/%d @%s\n", d.Topic, d.Partition, units.Count(d.Offset))
		case d.Error != "":
			fmt.Fprintf(a.stderr, "✗ %s: %s\n", d.Topic, d.Error)
		}
	}
	stats, err := produce.Run(cmd.Context(), cl.Client, src, f.opts, deliver)
	if bulk && !jsonOut {
		mu.Lock()
		defer mu.Unlock()
		rate := float64(stats.Produced) / max(stats.Elapsed.Seconds(), 0.001)
		topics := strings.Join(slices.Sorted(maps.Keys(stats.Topics)), ", ")
		if topics == "" {
			topics = topic
		}
		fmt.Fprintf(a.stderr, "Produced %s to %s in %s (%s msg/s, %s) · %d failed\n", units.Plural(stats.Produced, "message"),
			topics, stats.Elapsed.Round(time.Millisecond), units.Rate(rate),
			units.Bytes(stats.Bytes), stats.Failed)
	}
	if err != nil && cmd.Context().Err() != nil && f.repeat {
		return nil
	}
	return err
}

func describeCleanup(ctx context.Context, s *session, topic string) (string, error) {
	rcs, err := s.cl.Admin.DescribeTopicConfigs(ctx, topic)
	if err != nil || len(rcs) == 0 {
		return "", err
	}
	for _, c := range rcs[0].Configs {
		if c.Key == "cleanup.policy" {
			return c.MaybeValue(), nil
		}
	}
	return "", nil
}

type copySink struct {
	cl    *kgo.Client
	topic string
	part  int32
	wg    sync.WaitGroup
	mu    sync.Mutex
	n     int64
	err   error
}

func (c *copySink) Deliver(ctx context.Context, r *kgo.Record, done func(sink.Receipt, error)) {
	out := &kgo.Record{Topic: c.topic, Key: r.Key, Value: r.Value, Headers: r.Headers, Partition: c.part}
	c.wg.Add(1)
	c.cl.Produce(context.WithoutCancel(ctx), out, func(_ *kgo.Record, err error) {
		defer c.wg.Done()
		c.mu.Lock()
		if err != nil && c.err == nil {
			c.err = err
		} else if err == nil {
			c.n++
		}
		c.mu.Unlock()
	})
	done(sink.Receipt{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Status: "ok"}, nil)
}

func (c *copySink) Close() error { c.wg.Wait(); return nil }

func (a *app) copyTopic(cmd *cobra.Command, dst *session, topic string, f produceFlags) error {
	src := dst
	if f.fromProfile != "" {
		var err error
		if src, err = a.sessionFor(f.fromProfile); err != nil {
			return err
		}
		defer src.Close()
	}
	now := time.Now()
	from, err := consume.ParsePosition(f.from, now)
	if err != nil {
		return usageErr("--from: %v", err)
	}
	o := consume.Options{Topics: []string{f.fromTopic}, From: from, Limit: f.limit}
	if f.until != "" {
		u, err := consume.ParsePosition(f.until, now)
		if err != nil {
			return usageErr("--until: %v", err)
		}
		o.Until = &u
	}
	pl, err := consume.Resolve(cmd.Context(), src.cl.Admin, o)
	if err != nil {
		return err
	}
	if slices.Contains(dst.prof.Labels, "prod") {
		p := &plan.Plan{Class: plan.Change, Summary: fmt.Sprintf("Copy messages from %s:%s to %s:", src.name, f.fromTopic, topic),
			Changes: []string{fmt.Sprintf("%d partitions from %s", pl.Partitions(), f.from)}}
		if err := a.confirmPlan(dst, p); err != nil {
			return err
		}
	}
	consumer, err := newConsumerClient(src, consume.ClientOptions(pl, o)...)
	if err != nil {
		return err
	}
	defer consumer.Close()
	opts, err := produce.ClientOptions(f.opts, f.partition >= 0)
	if err != nil {
		return usageErr("%v", err)
	}
	producer, err := newConsumerClient(dst, opts...)
	if err != nil {
		return err
	}
	defer producer.Close()
	cs := &copySink{cl: producer.Client, topic: topic, part: f.partition}
	start := time.Now()
	_, err = consume.Run(cmd.Context(), consumer.Client, pl, o, cs, nil, false, nil)
	_ = producer.Flush(context.WithoutCancel(cmd.Context()))
	cs.Close()
	if err == nil {
		err = cs.err
	}
	fmt.Fprintf(a.stderr, "Copied %s from %s to %s in %s\n", units.Plural(cs.n, "message"), f.fromTopic, topic, time.Since(start).Round(time.Millisecond))
	return err
}

func (a *app) produceInteractive(ctx context.Context, s *session, topic string, f produceFlags) error {
	if !a.interactive() {
		return usageErr("-i needs a terminal")
	}
	opts, err := produce.ClientOptions(f.opts, f.partition >= 0)
	if err != nil {
		return usageErr("%v", err)
	}
	cl, err := newConsumerClient(s, opts...)
	if err != nil {
		return err
	}
	defer cl.Close()
	return tui.RunComposer(ctx, cl, s.name, s.prof, topic)
}
