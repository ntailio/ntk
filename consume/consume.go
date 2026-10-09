// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package consume

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/sink"
	"github.com/ntailio/ntk/units"
)

type PosKind int

const (
	Latest PosKind = iota
	Earliest
	LastN
	AtOffset
	AtTime
)

type Position struct {
	Kind   PosKind
	N      int64
	Offset int64
	Time   time.Time
}

// ParsePosition parses --from/--until: latest, earliest, -N (per partition),
// @offset, or a time (RFC 3339, -1h, epoch ms).
func ParsePosition(s string, now time.Time) (Position, error) {
	switch {
	case s == "" || s == "latest":
		return Position{Kind: Latest}, nil
	case s == "earliest":
		return Position{Kind: Earliest}, nil
	case strings.HasPrefix(s, "@"):
		n, err := strconv.ParseInt(s[1:], 10, 64)
		if err != nil || n < 0 {
			return Position{}, fmt.Errorf("invalid offset %q", s)
		}
		return Position{Kind: AtOffset, Offset: n}, nil
	case strings.HasPrefix(s, "-") && isDigits(s[1:]):
		n, _ := strconv.ParseInt(s[1:], 10, 64)
		return Position{Kind: LastN, N: n}, nil
	}
	t, err := units.ParseTime(s, now)
	if err != nil {
		return Position{}, fmt.Errorf("invalid position %q (use latest, earliest, -N, @offset, a time, or -1h)", s)
	}
	return Position{Kind: AtTime, Time: t}, nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

type Options struct {
	Topics       []string
	Partitions   []int32
	KeyPartition []byte
	From         Position
	Until        *Position
	Limit        int
	Follow       bool
	Group        string
	NoCommit     bool
	ReadCommit   bool
}

type bounds struct {
	start, stop int64 // stop is exclusive; -1 = follow forever
}

// Plan resolves the partitions and offsets to read.
type Plan struct {
	Offsets map[string]map[int32]bounds
	Bytes   int64
}

func (p *Plan) Partitions() int {
	n := 0
	for _, parts := range p.Offsets {
		n += len(parts)
	}
	return n
}

func Resolve(ctx context.Context, adm *kadm.Client, o Options) (*Plan, error) {
	md, err := adm.ListTopicsWithInternal(ctx, o.Topics...)
	if err != nil {
		return nil, err
	}
	for _, t := range o.Topics {
		if td, ok := md[t]; !ok || td.Err != nil {
			return nil, fmt.Errorf("topic %q: %w", t, kerr.UnknownTopicOrPartition)
		}
	}
	starts, err := adm.ListStartOffsets(ctx, o.Topics...)
	if err != nil {
		return nil, err
	}
	// read_committed consumers can't read past the last stable offset (an open transaction).
	ends, err := adm.ListEndOffsets(ctx, o.Topics...)
	if o.ReadCommit {
		ends, err = adm.ListCommittedOffsets(ctx, o.Topics...)
	}
	if err != nil {
		return nil, err
	}
	at := func(pos Position) (kadm.ListedOffsets, error) {
		if pos.Kind != AtTime {
			return nil, nil
		}
		return adm.ListOffsetsAfterMilli(ctx, pos.Time.UnixMilli(), o.Topics...)
	}
	fromTime, err := at(o.From)
	if err != nil {
		return nil, err
	}
	var untilTime kadm.ListedOffsets
	if o.Until != nil {
		if untilTime, err = at(*o.Until); err != nil {
			return nil, err
		}
	}

	p := &Plan{Offsets: map[string]map[int32]bounds{}}
	for _, t := range o.Topics {
		parts := md[t].Partitions.Numbers()
		if o.KeyPartition != nil {
			parts = []int32{KeyPartition(o.KeyPartition, len(parts))}
		} else if len(o.Partitions) > 0 {
			for _, want := range o.Partitions {
				if !slices.Contains(parts, want) {
					return nil, fmt.Errorf("topic %q has no partition %d", t, want)
				}
			}
			parts = o.Partitions
		}
		p.Offsets[t] = map[int32]bounds{}
		for _, part := range parts {
			lo, hi := int64(0), int64(0)
			if s, ok := starts.Lookup(t, part); ok && s.Err == nil {
				lo = s.Offset
			}
			if e, ok := ends.Lookup(t, part); ok && e.Err == nil {
				hi = e.Offset
			}
			b := bounds{stop: -1}
			switch o.From.Kind {
			case Latest:
				b.start = hi
			case Earliest:
				b.start = lo
			case LastN:
				b.start = max(hi-o.From.N, lo)
			case AtOffset:
				b.start = min(max(o.From.Offset, lo), hi)
			case AtTime:
				b.start = hi
				if l, ok := fromTime.Lookup(t, part); ok && l.Err == nil && l.Offset >= 0 {
					b.start = l.Offset
				}
			}
			if !o.Follow {
				b.stop = hi
			}
			if o.Until != nil {
				switch o.Until.Kind {
				case AtOffset:
					b.stop = o.Until.Offset
				case AtTime:
					b.stop = hi
					if l, ok := untilTime.Lookup(t, part); ok && l.Err == nil && l.Offset >= 0 {
						b.stop = l.Offset
					}
				case Latest:
					b.stop = hi
				case LastN:
					b.stop = max(hi-o.Until.N, lo)
				case Earliest:
					b.stop = lo
				}
				b.stop = min(b.stop, hi)
			}
			p.Offsets[t][part] = b
		}
	}
	return p, nil
}

// EstimateBytes returns the on-disk size of the selected partitions.
func EstimateBytes(ctx context.Context, adm *kadm.Client, p *Plan) int64 {
	var set kadm.TopicsSet
	for t, parts := range p.Offsets {
		for part := range parts {
			set.Add(t, part)
		}
	}
	dirs, err := adm.DescribeAllLogDirs(ctx, set)
	if err != nil {
		return 0
	}
	sizes := map[string]int64{}
	dirs.Each(func(d kadm.DescribedLogDir) {
		for t, parts := range d.Topics {
			for part, dp := range parts {
				if _, ok := p.Offsets[t][part]; ok {
					k := fmt.Sprintf("%s/%d", t, part)
					sizes[k] = max(sizes[k], dp.Size)
				}
			}
		}
	})
	var total int64
	for _, s := range sizes {
		total += s
	}
	return total
}

type Stats struct {
	Consumed   int64
	Partitions int
	Last       *kgo.Record
}

// Run reads records and delivers them to out until the plan is exhausted,
// the limit is reached, or ctx ends. receipt is called for every delivery attempt.
func Run(ctx context.Context, cl *kgo.Client, p *Plan, o Options, out sink.Sink, receipt func(sink.Receipt), continueOnError bool, progress func(Stats)) (Stats, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	remaining := map[string]map[int32]bool{}
	if o.Group == "" {
		for t, parts := range p.Offsets {
			for part, b := range parts {
				if b.stop >= 0 && b.start >= b.stop {
					continue
				}
				if remaining[t] == nil {
					remaining[t] = map[int32]bool{}
				}
				remaining[t][part] = true
			}
		}
		if len(remaining) == 0 {
			return Stats{Partitions: p.Partitions()}, nil
		}
	}
	finite := o.Group == "" && !anyForever(p)

	var (
		mu       sync.Mutex
		stats    = Stats{Partitions: p.Partitions()}
		firstErr error
	)
	done := func(rec *kgo.Record) func(sink.Receipt, error) {
		return func(rc sink.Receipt, err error) {
			if receipt != nil {
				receipt(rc)
			}
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if !continueOnError || !isTarget(err) {
					if firstErr == nil {
						firstErr = err
					}
					cancel()
				}
				return
			}
			if o.Group != "" && !o.NoCommit && rc.Delivered() {
				cl.MarkCommitRecords(rec)
			}
		}
	}

	for {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			break
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if errors.Is(fe.Err, context.Canceled) || errors.Is(fe.Err, context.DeadlineExceeded) {
					continue
				}
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("fetching %s/%d: %w", fe.Topic, fe.Partition, fe.Err)
				}
				mu.Unlock()
				cancel()
			}
		}
		stop := false
		fetches.EachRecord(func(r *kgo.Record) {
			if stop || ctx.Err() != nil {
				return
			}
			if o.Group == "" {
				b := p.Offsets[r.Topic][r.Partition]
				if b.stop >= 0 && r.Offset >= b.stop {
					delete(remaining[r.Topic], r.Partition)
					return
				}
				if b.stop >= 0 && r.Offset >= b.stop-1 {
					delete(remaining[r.Topic], r.Partition)
				}
			}
			// Transaction markers take up offsets; they only count toward the stop offset.
			if r.Attrs.IsControl() {
				return
			}
			out.Deliver(ctx, r, done(r))
			mu.Lock()
			stats.Consumed++
			stats.Last = r
			s := stats
			mu.Unlock()
			if progress != nil {
				progress(s)
			}
			if o.Limit > 0 && s.Consumed >= int64(o.Limit) {
				stop = true
			}
		})
		if stop {
			break
		}
		if finite && allDone(remaining) {
			break
		}
	}
	if w, ok := out.(interface{ Wait() }); ok {
		w.Wait()
	}
	if o.Group != "" && !o.NoCommit {
		cctx, ccancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := cl.CommitMarkedOffsets(cctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("committing offsets: %w", err)
		}
		ccancel()
	}
	mu.Lock()
	defer mu.Unlock()
	return stats, firstErr
}

func isTarget(err error) bool {
	var t *sink.ErrTarget
	return errors.As(err, &t)
}

func anyForever(p *Plan) bool {
	for _, parts := range p.Offsets {
		for _, b := range parts {
			if b.stop < 0 {
				return true
			}
		}
	}
	return false
}

func allDone(remaining map[string]map[int32]bool) bool {
	for _, parts := range remaining {
		if len(parts) > 0 {
			return false
		}
	}
	return true
}

// ClientOptions returns the kgo options that start reading at the planned offsets.
func ClientOptions(p *Plan, o Options) []kgo.Opt {
	opts := []kgo.Opt{kgo.KeepControlRecords()}
	if o.ReadCommit {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}
	if o.Group != "" {
		reset := kgo.NewOffset().AtEnd()
		switch o.From.Kind {
		case Earliest:
			reset = kgo.NewOffset().AtStart()
		case AtTime:
			reset = kgo.NewOffset().AfterMilli(o.From.Time.UnixMilli())
		}
		commit := kgo.AutoCommitMarks()
		if o.NoCommit {
			commit = kgo.DisableAutoCommit()
		}
		return append(opts, kgo.ConsumerGroup(o.Group), kgo.ConsumeTopics(o.Topics...), kgo.ConsumeResetOffset(reset), commit)
	}
	parts := map[string]map[int32]kgo.Offset{}
	for t, ps := range p.Offsets {
		parts[t] = map[int32]kgo.Offset{}
		for part, b := range ps {
			parts[t][part] = kgo.NewOffset().At(b.start)
		}
	}
	return append(opts, kgo.ConsumePartitions(parts))
}

// KeyPartition is the partition the Java client's default partitioner picks for key.
func KeyPartition(key []byte, partitions int) int32 {
	return int32((murmur2(key) & 0x7fffffff) % uint32(partitions))
}

func murmur2(data []byte) uint32 {
	const (
		seed uint32 = 0x9747b28c
		m    uint32 = 0x5bd1e995
		r           = 24
	)
	h := seed ^ uint32(len(data))
	for len(data) >= 4 {
		k := uint32(data[0]) | uint32(data[1])<<8 | uint32(data[2])<<16 | uint32(data[3])<<24
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
		data = data[4:]
	}
	switch len(data) {
	case 3:
		h ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		h ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		h ^= uint32(data[0])
		h *= m
	}
	h ^= h >> 13
	h *= m
	h ^= h >> 15
	return h
}
