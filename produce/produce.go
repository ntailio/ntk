// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package produce

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/record"
)

var ErrUnixUnsupported = errors.New("--in unix: is not available on Windows (it only supports stream Unix sockets)")

type Delivery struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Timestamp string `json:"timestamp"`
	Error     string `json:"error,omitempty"`
}

type Options struct {
	Partition     int32 // -1 = partitioner decides
	Partitioner   string
	Acks          string
	Compression   string
	Idempotent    bool
	Transactional string
	Rate          float64 // records per second, 0 = unlimited

	// Graceful: cancellation ends the input instead of aborting. The source
	// returns io.EOF once it has handed over what it already received, those
	// records are still produced, and a transaction is committed.
	Graceful bool
}

func ClientOptions(o Options, manualPartitions bool) ([]kgo.Opt, error) {
	var opts []kgo.Opt
	switch o.Acks {
	case "", "all", "-1":
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	case "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()))
	case "0":
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()))
	default:
		return nil, fmt.Errorf("--acks must be all, 1, or 0")
	}
	if !o.Idempotent || o.Acks == "0" || o.Acks == "1" {
		if o.Transactional != "" {
			return nil, errors.New("--transactional-id needs --idempotent and --acks all")
		}
		opts = append(opts, kgo.DisableIdempotentWrite())
	}
	switch o.Compression {
	case "", "none":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.NoCompression()))
	case "gzip":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.GzipCompression()))
	case "snappy":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.SnappyCompression()))
	case "lz4":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.Lz4Compression()))
	case "zstd":
		opts = append(opts, kgo.ProducerBatchCompression(kgo.ZstdCompression()))
	default:
		return nil, fmt.Errorf("--compression must be none, gzip, snappy, lz4, or zstd")
	}
	switch {
	case manualPartitions || o.Partition >= 0:
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	case o.Partitioner == "round-robin":
		opts = append(opts, kgo.RecordPartitioner(kgo.RoundRobinPartitioner()))
	case o.Partitioner == "sticky":
		opts = append(opts, kgo.RecordPartitioner(kgo.StickyPartitioner()))
	case o.Partitioner == "" || o.Partitioner == "murmur2":
		opts = append(opts, kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)))
	default:
		return nil, fmt.Errorf("--partitioner must be murmur2, round-robin, or sticky")
	}
	if o.Transactional != "" {
		opts = append(opts, kgo.TransactionalID(o.Transactional))
	}
	return opts, nil
}

// Source yields records until it returns io.EOF.
type Source func(ctx context.Context) (*kgo.Record, error)

func Single(r *kgo.Record, count int) Source {
	i := 0
	return func(context.Context) (*kgo.Record, error) {
		if count > 0 && i >= count {
			return nil, io.EOF
		}
		i++
		c := *r
		c.Headers = append([]kgo.RecordHeader(nil), r.Headers...)
		return &c, nil
	}
}

// Lines splits r on delim; with keySep, each message is key<sep>value.
func Lines(r io.Reader, delim []byte, keySep []byte, base kgo.Record) Source {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	sc.Split(splitOn(delim))
	return func(context.Context) (*kgo.Record, error) {
		if !sc.Scan() {
			if err := sc.Err(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
		msg := append([]byte(nil), sc.Bytes()...)
		rec := base
		rec.Headers = append([]kgo.RecordHeader(nil), base.Headers...)
		if len(keySep) > 0 {
			if k, v, ok := bytes.Cut(msg, keySep); ok {
				rec.Key, msg = k, v
			}
		}
		rec.Value = msg
		return &rec, nil
	}
}

func splitOn(delim []byte) bufio.SplitFunc {
	return func(data []byte, atEOF bool) (int, []byte, error) {
		if i := bytes.Index(data, delim); i >= 0 {
			return i + len(delim), data[:i], nil
		}
		if atEOF && len(data) > 0 {
			return len(data), bytes.TrimSuffix(data, []byte("\r")), nil
		}
		return 0, nil, nil
	}
}

type Keep struct {
	Topic, Partition, Timestamp bool
}

// Records reads the ntk record format (one JSON object per line).
func Records(r io.Reader, topic string, keep Keep) Source {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 64<<20)
	line := 0
	return func(context.Context) (*kgo.Record, error) {
		for sc.Scan() {
			line++
			b := bytes.TrimSpace(sc.Bytes())
			if len(b) == 0 {
				continue
			}
			f, err := record.Parse(b)
			if err != nil {
				return nil, fmt.Errorf("line %d: not an ntk record: %w", line, err)
			}
			rec, err := f.ToKgo()
			if err != nil {
				return nil, fmt.Errorf("line %d: %w", line, err)
			}
			return keep.apply(rec, topic), nil
		}
		if err := sc.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
}

func (k Keep) apply(rec *kgo.Record, topic string) *kgo.Record {
	if !k.Topic || rec.Topic == "" {
		rec.Topic = topic
	}
	if !k.Partition {
		rec.Partition = -1
	}
	if !k.Timestamp {
		rec.Timestamp = time.Time{}
	}
	return rec
}

// MetaStream reads what consume -m writes to stdout: a metadata line, exactly
// value_size value bytes, then the delimiter. Values may contain the delimiter.
func MetaStream(r io.Reader, delim []byte, topic string, keep Keep) Source {
	br := bufio.NewReaderSize(r, 64<<10)
	n := 0
	return func(context.Context) (*kgo.Record, error) {
		line, err := br.ReadBytes('\n')
		if err == io.EOF && len(line) == 0 {
			return nil, io.EOF
		}
		n++
		if err != nil {
			return nil, fmt.Errorf("message %d: input ends inside the metadata line", n)
		}
		m, err := record.ParseMeta(line[:len(line)-1])
		if err != nil {
			return nil, fmt.Errorf("message %d: not a metadata line: %w", n, err)
		}
		if m.ValueSize < 0 {
			return nil, fmt.Errorf("message %d: negative value_size", n)
		}
		value := make([]byte, m.ValueSize)
		if _, err := io.ReadFull(br, value); err != nil {
			return nil, fmt.Errorf("message %d: input ends inside the value (value_size %d)", n, m.ValueSize)
		}
		got := make([]byte, len(delim))
		if k, err := io.ReadFull(br, got); err != nil {
			if !(err == io.EOF && k == 0) {
				return nil, fmt.Errorf("message %d: input ends before the delimiter", n)
			}
		} else if !bytes.Equal(got, delim) {
			return nil, fmt.Errorf("message %d: expected the delimiter %q after %d value bytes, got %q (same --delimiter on both sides?)", n, delim, m.ValueSize, got)
		}
		rec, err := m.ToKgo(value)
		if err != nil {
			return nil, fmt.Errorf("message %d: %w", n, err)
		}
		return keep.apply(rec, topic), nil
	}
}

// UntilCancel ends src (io.EOF) once ctx is canceled. A read already blocked
// in src still finishes first.
func UntilCancel(src Source) Source {
	return func(ctx context.Context) (*kgo.Record, error) {
		if ctx.Err() != nil {
			return nil, io.EOF
		}
		return src(ctx)
	}
}

// WithHeaders appends hs to every record from src.
func WithHeaders(src Source, hs []kgo.RecordHeader) Source {
	if len(hs) == 0 {
		return src
	}
	return func(ctx context.Context) (*kgo.Record, error) {
		r, err := src(ctx)
		if err == nil {
			r.Headers = append(r.Headers, hs...)
		}
		return r, err
	}
}

// Limit stops src after n records (n <= 0: no limit).
func Limit(src Source, n int) Source {
	if n <= 0 {
		return src
	}
	i := 0
	return func(ctx context.Context) (*kgo.Record, error) {
		if i >= n {
			return nil, io.EOF
		}
		r, err := src(ctx)
		if err == nil {
			i++
		}
		return r, err
	}
}

type Stats struct {
	Produced int64
	Failed   int64
	Bytes    int64
	Elapsed  time.Duration
	Topics   map[string]bool
}

// Run produces everything from src. deliver is called for each result.
func Run(ctx context.Context, cl *kgo.Client, src Source, o Options, deliver func(Delivery)) (Stats, error) {
	start := time.Now()
	stats := Stats{Topics: map[string]bool{}}
	var mu sync.Mutex
	var firstErr error
	var wg sync.WaitGroup

	if o.Transactional != "" {
		if err := cl.BeginTransaction(); err != nil {
			return stats, err
		}
	}
	pctx := ctx
	if o.Graceful {
		pctx = context.WithoutCancel(ctx)
	}
	var tick <-chan time.Time
	if o.Rate > 0 {
		t := time.NewTicker(time.Duration(float64(time.Second) / o.Rate))
		defer t.Stop()
		tick = t.C
	}
	for {
		rec, err := src(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			mu.Lock()
			firstErr = err
			mu.Unlock()
			break
		}
		if o.Partition >= 0 && rec.Partition < 0 {
			rec.Partition = o.Partition
		}
		if tick != nil {
			select {
			case <-ctx.Done():
			case <-tick:
			}
		}
		if ctx.Err() != nil && !o.Graceful {
			break
		}
		wg.Add(1)
		size := int64(len(rec.Key) + len(rec.Value))
		cl.Produce(pctx, rec, func(r *kgo.Record, err error) {
			defer wg.Done()
			d := Delivery{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, Timestamp: r.Timestamp.UTC().Format(time.RFC3339Nano)}
			mu.Lock()
			if err != nil {
				d.Error = err.Error()
				stats.Failed++
				if firstErr == nil {
					firstErr = err
				}
			} else {
				stats.Produced++
				stats.Bytes += size
				stats.Topics[r.Topic] = true
			}
			mu.Unlock()
			if deliver != nil {
				deliver(d)
			}
		})
	}
	if err := cl.Flush(context.WithoutCancel(ctx)); err != nil && firstErr == nil {
		firstErr = err
	}
	wg.Wait()
	if o.Transactional != "" {
		commit := kgo.TryCommit
		if firstErr != nil || (ctx.Err() != nil && !o.Graceful) {
			commit = kgo.TryAbort
		}
		if err := cl.EndTransaction(context.WithoutCancel(ctx), commit); err != nil && firstErr == nil {
			firstErr = err
		}
		if commit == kgo.TryAbort && firstErr != nil {
			firstErr = fmt.Errorf("transaction aborted: %w", firstErr)
		}
	}
	stats.Elapsed = time.Since(start)
	return stats, firstErr
}

// Template expands {{.i}}, {{uuid}}, and {{now}} in -k/-v with --template.
func Template(s string, i int, uuid func() string) string {
	r := strings.NewReplacer(
		"{{.i}}", fmt.Sprint(i),
		"{{ .i }}", fmt.Sprint(i),
		"{{uuid}}", uuid(),
		"{{ uuid }}", uuid(),
		"{{now}}", time.Now().UTC().Format(time.RFC3339Nano),
		"{{ now }}", time.Now().UTC().Format(time.RFC3339Nano),
	)
	return r.Replace(s)
}
