// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package groups

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/units"
)

const (
	KindReset         = "group.reset"
	KindDelete        = "group.delete"
	KindDeleteOffsets = "group.delete-offsets"
)

type ResetMode string

const (
	ToEarliest ResetMode = "earliest"
	ToLatest   ResetMode = "latest"
	ToOffset   ResetMode = "offset"
	ToTime     ResetMode = "time"
	ShiftBy    ResetMode = "shift-by"
	FromFile   ResetMode = "file"
)

type ResetOptions struct {
	Topics    map[string][]int32 // nil partitions = all
	AllTopics bool
	Mode      ResetMode
	Offset    int64
	Time      time.Time
	Shift     int64
	File      map[string]map[int32]int64
}

type ResetSpec struct {
	Group   string                     `json:"group"`
	Offsets map[string]map[int32]int64 `json:"offsets"`
}

// ensureEmpty refuses changes to groups that have active members.
func ensureEmpty(ctx context.Context, cl *kgo.Client, adm *kadm.Client, group string) error {
	d, err := Describe(ctx, cl, adm, group)
	if errors.Is(err, kerr.GroupIDNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if d.Members == 0 {
		return nil
	}
	var who []string
	for _, m := range d.Members_ {
		who = append(who, fmt.Sprintf("%s (%s)", m.ClientID, m.Host))
	}
	return fmt.Errorf("group %q has %d active members (%s); stop them first: committing to an active group is overwritten by its members",
		group, d.Members, strings.Join(who, ", "))
}

func PlanReset(ctx context.Context, cl *kgo.Client, adm *kadm.Client, group string, o ResetOptions) (*plan.Plan, error) {
	if err := ensureEmpty(ctx, cl, adm, group); err != nil {
		return nil, err
	}
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, err
	}
	targets := map[string][]int32{}
	if o.AllTopics || (o.Mode == FromFile && len(o.Topics) == 0) {
		committed.Each(func(r kadm.OffsetResponse) { targets[r.Topic] = nil })
		if o.Mode == FromFile {
			for t := range o.File {
				targets[t] = nil
			}
		}
	}
	for t, ps := range o.Topics {
		targets[t] = ps
	}
	if len(targets) == 0 {
		return nil, errors.New("no topics selected: use --topic or --all-topics")
	}
	var topicNames []string
	for t := range targets {
		topicNames = append(topicNames, t)
	}
	starts, err := adm.ListStartOffsets(ctx, topicNames...)
	if err != nil {
		return nil, err
	}
	ends, err := adm.ListEndOffsets(ctx, topicNames...)
	if err != nil {
		return nil, err
	}
	var atTime kadm.ListedOffsets
	if o.Mode == ToTime {
		if atTime, err = adm.ListOffsetsAfterMilli(ctx, o.Time.UnixMilli(), topicNames...); err != nil {
			return nil, err
		}
	}

	spec := ResetSpec{Group: group, Offsets: map[string]map[int32]int64{}}
	type row struct {
		topic      string
		part       int32
		cur, next  int64
		hasCurrent bool
	}
	var rows []row
	for t, parts := range targets {
		if _, ok := ends[t]; !ok {
			return nil, fmt.Errorf("topic %q: %w", t, kerr.UnknownTopicOrPartition)
		}
		for p, end := range ends[t] {
			if end.Err != nil {
				return nil, fmt.Errorf("%s/%d: %w", t, p, end.Err)
			}
			if len(parts) > 0 && !slices.Contains(parts, p) {
				continue
			}
			start, _ := starts.Lookup(t, p)
			cur, hasCur := int64(-1), false
			if c, ok := committed.Lookup(t, p); ok && c.Err == nil && c.At >= 0 {
				cur, hasCur = c.At, true
			}
			var next int64
			switch o.Mode {
			case ToEarliest:
				next = start.Offset
			case ToLatest:
				next = end.Offset
			case ToOffset:
				next = o.Offset
			case ToTime:
				next = end.Offset
				if l, ok := atTime.Lookup(t, p); ok && l.Err == nil && l.Offset >= 0 {
					next = l.Offset
				}
			case ShiftBy:
				base := cur
				if !hasCur {
					base = end.Offset
				}
				next = base + o.Shift
			case FromFile:
				v, ok := o.File[t][p]
				if !ok {
					continue
				}
				next = v
			}
			next = min(max(next, start.Offset), end.Offset)
			spec.Offsets[t] = mapSet(spec.Offsets[t], p, next)
			rows = append(rows, row{t, p, cur, next, hasCur})
		}
	}
	slices.SortFunc(rows, func(a, b row) int { return cmp.Or(strings.Compare(a.topic, b.topic), cmp.Compare(a.part, b.part)) })

	pl, err := plan.New(KindReset, plan.Change, fmt.Sprintf("Reset offsets for group %q:", group), spec)
	if err != nil {
		return nil, err
	}
	var total int64
	pl.Add("%-30s %5s  %14s  %14s  %12s", "TOPIC", "PART", "CURRENT", "NEW", "DIFF")
	for _, r := range rows {
		cur, diff := "-", ""
		if r.hasCurrent {
			cur = units.Count(r.cur)
			d := r.next - r.cur
			total += d
			diff = units.Count(d)
			if d > 0 {
				diff = "+" + diff
			}
		}
		pl.Add("%-30s %5d  %14s  %14s  %12s", r.topic, r.part, cur, units.Count(r.next), diff)
	}
	if len(rows) == 0 {
		pl.Changes = nil
	}
	switch {
	case total < 0:
		pl.Warn("%s messages will be re-consumed", units.Count(-total))
	case total > 0:
		pl.Warn("%s messages will be skipped", units.Count(total))
	}
	return pl, nil
}

func mapSet(m map[int32]int64, p int32, v int64) map[int32]int64 {
	if m == nil {
		m = map[int32]int64{}
	}
	m[p] = v
	return m
}

func ApplyReset(ctx context.Context, cl *kgo.Client, adm *kadm.Client, pl *plan.Plan) error {
	var spec ResetSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	if err := ensureEmpty(ctx, cl, adm, spec.Group); err != nil {
		return err
	}
	var os kadm.Offsets
	for t, parts := range spec.Offsets {
		for p, off := range parts {
			os.AddOffset(t, p, off, -1)
		}
	}
	resp, err := adm.CommitOffsets(ctx, spec.Group, os)
	if err != nil {
		return err
	}
	return resp.Error()
}

type DeleteSpec struct {
	Groups []string `json:"groups"`
}

func PlanDelete(ctx context.Context, cl *kgo.Client, adm *kadm.Client, names []string) (*plan.Plan, error) {
	pl, err := plan.New(KindDelete, plan.Destructive, fmt.Sprintf("Delete %d group(s):", len(names)), DeleteSpec{names})
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		d, err := Describe(ctx, cl, adm, n)
		if err != nil {
			return nil, err
		}
		if d.Members > 0 {
			return nil, ensureEmpty(ctx, cl, adm, n)
		}
		pl.Add("- %s (%s, %d topics, committed offsets for %d partitions)", n, d.Type, len(d.Topics), len(d.Offsets))
	}
	if len(names) == 1 {
		pl.Confirm = names[0]
	} else {
		pl.Confirm = fmt.Sprintf("delete %d groups", len(names))
	}
	return pl, nil
}

func ApplyDelete(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec DeleteSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	resp, err := adm.DeleteGroups(ctx, spec.Groups...)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range resp {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Group, r.Err))
		}
	}
	return errors.Join(errs...)
}

type DeleteOffsetsSpec struct {
	Group      string  `json:"group"`
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

func PlanDeleteOffsets(ctx context.Context, cl *kgo.Client, adm *kadm.Client, group, topic string, partitions []int32) (*plan.Plan, error) {
	if err := ensureEmpty(ctx, cl, adm, group); err != nil {
		return nil, err
	}
	committed, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		return nil, err
	}
	spec := DeleteOffsetsSpec{Group: group, Topic: topic}
	pl, err := plan.New(KindDeleteOffsets, plan.Destructive, fmt.Sprintf("Delete committed offsets of group %q on %q:", group, topic), nil)
	if err != nil {
		return nil, err
	}
	for _, o := range committed.Sorted() {
		if o.Topic != topic || (len(partitions) > 0 && !slices.Contains(partitions, o.Partition)) {
			continue
		}
		spec.Partitions = append(spec.Partitions, o.Partition)
		pl.Add("- %s/%d (committed %s)", topic, o.Partition, units.Count(o.At))
	}
	pl.Confirm = group
	np, err := plan.New(pl.Kind, pl.Class, pl.Summary, spec)
	if err != nil {
		return nil, err
	}
	np.Changes, np.Confirm = pl.Changes, pl.Confirm
	return np, nil
}

func ApplyDeleteOffsets(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec DeleteOffsetsSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	var set kadm.TopicsSet
	set.Add(spec.Topic, spec.Partitions...)
	resp, err := adm.DeleteOffsets(ctx, spec.Group, set)
	if err != nil {
		return err
	}
	return resp.Error()
}
