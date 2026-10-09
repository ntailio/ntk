// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/configs"
	"github.com/factualtech/ntk/plan"
	"github.com/factualtech/ntk/units"
)

const (
	KindCreate        = "topic.create"
	KindDelete        = "topic.delete"
	KindAddPartitions = "topic.add-partitions"
	KindTruncate      = "topic.truncate"
)

type CreateSpec struct {
	Topics            []string          `json:"topics"`
	Partitions        int32             `json:"partitions"`
	ReplicationFactor int16             `json:"replication_factor"`
	Configs           map[string]string `json:"configs,omitempty"`
	IfNotExists       bool              `json:"if_not_exists,omitempty"`
}

func PlanCreate(ctx context.Context, adm *kadm.Client, spec CreateSpec) (*plan.Plan, error) {
	normalized := map[string]string{}
	for k, v := range spec.Configs {
		n, err := configs.Normalize(k, v, "")
		if err != nil {
			return nil, err
		}
		normalized[k] = n
	}
	spec.Configs = normalized

	var create []string
	var skipped []string
	for _, t := range spec.Topics {
		exists, err := Exists(ctx, adm, t)
		if err != nil {
			return nil, err
		}
		switch {
		case exists && spec.IfNotExists:
			skipped = append(skipped, t)
		case exists:
			return nil, fmt.Errorf("topic %q: %w", t, kerr.TopicAlreadyExists)
		default:
			create = append(create, t)
		}
	}
	spec.Topics = create

	if len(create) > 0 {
		resp, err := adm.ValidateCreateTopics(ctx, spec.Partitions, spec.ReplicationFactor, ptrs(spec.Configs), create...)
		if err != nil {
			return nil, err
		}
		if spec.IfNotExists {
			for t, r := range resp {
				if errors.Is(r.Err, kerr.TopicAlreadyExists) {
					delete(resp, t)
					create = slices.DeleteFunc(create, func(c string) bool { return c == t })
					skipped = append(skipped, t)
				}
			}
			spec.Topics = create
		}
		if err := firstCreateErr(resp); err != nil {
			return nil, err
		}
	}

	pl, err := plan.New(KindCreate, plan.SafeWrite, fmt.Sprintf("Create %s:", plural(len(create), "topic")), spec)
	if err != nil {
		return nil, err
	}
	for _, t := range create {
		parts, rf := "broker default", "broker default"
		if spec.Partitions > 0 {
			parts = fmt.Sprint(spec.Partitions)
		}
		if spec.ReplicationFactor > 0 {
			rf = fmt.Sprint(spec.ReplicationFactor)
		}
		line := fmt.Sprintf("+ %s (partitions: %s, RF: %s)", t, parts, rf)
		for _, k := range slices.Sorted(maps.Keys(spec.Configs)) {
			line += fmt.Sprintf(" %s=%s", k, configs.Humanize(k, spec.Configs[k]))
		}
		pl.Add("%s", line)
	}
	for _, t := range skipped {
		pl.Warn("%s already exists; skipped (--if-not-exists)", t)
	}
	return pl, nil
}

func ApplyCreate(ctx context.Context, cl *kgo.Client, adm *kadm.Client, pl *plan.Plan) error {
	var spec CreateSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	resp, err := adm.CreateTopics(ctx, spec.Partitions, spec.ReplicationFactor, ptrs(spec.Configs), spec.Topics...)
	if err != nil {
		return err
	}
	if spec.IfNotExists {
		for t, r := range resp {
			if errors.Is(r.Err, kerr.TopicAlreadyExists) {
				delete(resp, t)
			}
		}
	}
	if err := firstCreateErr(resp); err != nil {
		return err
	}
	WaitPropagated(ctx, cl, spec.Topics, true, 5*time.Second)
	return nil
}

func firstCreateErr(resp kadm.CreateTopicResponses) error {
	var errs []error
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Topic, wrapMsg(r.Err, r.ErrMessage)))
		}
	}
	return errors.Join(errs...)
}

type DeleteSpec struct {
	Topics []string `json:"topics"`
}

func PlanDelete(ctx context.Context, adm *kadm.Client, names []string) (*plan.Plan, error) {
	if len(names) == 0 {
		return nil, errors.New("no topics to delete")
	}
	ts, err := List(ctx, adm, ListOptions{Names: names, Internal: true, Messages: true})
	if err != nil {
		return nil, err
	}
	byName := map[string]Topic{}
	for _, t := range ts {
		byName[t.Name] = t
	}
	pl, err := plan.New(KindDelete, plan.Destructive, fmt.Sprintf("Delete %s:", plural(len(names), "topic")), DeleteSpec{Topics: names})
	if err != nil {
		return nil, err
	}
	for _, n := range names {
		t, ok := byName[n]
		if !ok {
			return nil, fmt.Errorf("topic %q: %w", n, kerr.UnknownTopicOrPartition)
		}
		pl.Add("- %s (%d partitions, %s, %s messages)", n, t.Partitions, units.Bytes(t.SizeBytes), units.Count(t.Messages))
		if t.Internal {
			pl.Warn("%s is an internal topic", n)
		}
	}
	if len(names) == 1 {
		pl.Confirm = names[0]
	} else {
		pl.Confirm = fmt.Sprintf("delete %d topics", len(names))
	}
	return pl, nil
}

func ApplyDelete(ctx context.Context, cl *kgo.Client, adm *kadm.Client, pl *plan.Plan) error {
	var spec DeleteSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	resp, err := adm.DeleteTopics(ctx, spec.Topics...)
	if err != nil {
		return err
	}
	var errs []error
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", r.Topic, wrapMsg(r.Err, r.ErrMessage)))
		}
	}
	if len(errs) == 0 {
		WaitPropagated(ctx, cl, spec.Topics, false, 5*time.Second)
	}
	return errors.Join(errs...)
}

type AddPartitionsSpec struct {
	Topic string `json:"topic"`
	Total int    `json:"total"`
}

func PlanAddPartitions(ctx context.Context, adm *kadm.Client, topic string, total int) (*plan.Plan, error) {
	d, err := Describe(ctx, adm, topic)
	if err != nil {
		return nil, err
	}
	if total <= d.Partitions {
		return nil, fmt.Errorf("topic %q already has %d partitions; the new total must be larger (partitions can't be removed)", topic, d.Partitions)
	}
	pl, err := plan.New(KindAddPartitions, plan.Change, fmt.Sprintf("Add partitions to %q:", topic), AddPartitionsSpec{topic, total})
	if err != nil {
		return nil, err
	}
	pl.Add("partitions %d → %d", d.Partitions, total)
	pl.Warn("for keyed data, the key → partition mapping changes for new records")
	return pl, nil
}

func ApplyAddPartitions(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec AddPartitionsSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	resp, err := adm.UpdatePartitions(ctx, spec.Total, spec.Topic)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("%s: %w", r.Topic, wrapMsg(r.Err, r.ErrMessage))
		}
	}
	return nil
}

type TruncateSpec struct {
	Topic   string          `json:"topic"`
	Offsets map[int32]int64 `json:"offsets"`
}

type TruncateOptions struct {
	BeforeOffset *int64
	BeforeTime   *time.Time
	All          bool
	Partitions   []int32
}

func PlanTruncate(ctx context.Context, adm *kadm.Client, topic string, o TruncateOptions) (*plan.Plan, error) {
	offs, err := Offsets(ctx, adm, topic, nil)
	if err != nil {
		return nil, err
	}
	var at kadm.ListedOffsets
	if o.BeforeTime != nil {
		if at, err = adm.ListOffsetsAfterMilli(ctx, o.BeforeTime.UnixMilli(), topic); err != nil {
			return nil, err
		}
	}
	spec := TruncateSpec{Topic: topic, Offsets: map[int32]int64{}}
	pl, err := plan.New(KindTruncate, plan.Destructive, fmt.Sprintf("Delete records from %q:", topic), &spec)
	if err != nil {
		return nil, err
	}
	var total int64
	for _, po := range offs {
		if len(o.Partitions) > 0 && !slices.Contains(o.Partitions, po.Partition) {
			continue
		}
		target := po.Latest
		switch {
		case o.All:
		case o.BeforeOffset != nil:
			target = min(max(*o.BeforeOffset, po.Earliest), po.Latest)
		case o.BeforeTime != nil:
			if l, ok := at.Lookup(topic, po.Partition); ok && l.Err == nil && l.Offset >= 0 {
				target = min(l.Offset, po.Latest)
			}
		}
		if target <= po.Earliest {
			continue
		}
		spec.Offsets[po.Partition] = target
		total += target - po.Earliest
		pl.Add("partition %d: log start %s → %s (−%s messages)", po.Partition, units.Count(po.Earliest), units.Count(target), units.Count(target-po.Earliest))
	}
	if len(spec.Offsets) > 0 {
		pl.Summary = fmt.Sprintf("Delete %s messages from %q:", units.Count(total), topic)
	}
	pl.Confirm = topic
	return rebuild(pl, spec)
}

func ApplyTruncate(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec TruncateSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	var os kadm.Offsets
	for p, off := range spec.Offsets {
		os.AddOffset(spec.Topic, p, off, -1)
	}
	resp, err := adm.DeleteRecords(ctx, os)
	if err != nil {
		return err
	}
	var errs []error
	for _, parts := range resp {
		for p, r := range parts {
			if r.Err != nil {
				errs = append(errs, fmt.Errorf("%s/%d: %w", spec.Topic, p, r.Err))
			}
		}
	}
	return errors.Join(errs...)
}

func rebuild(pl *plan.Plan, payload any) (*plan.Plan, error) {
	np, err := plan.New(pl.Kind, pl.Class, pl.Summary, payload)
	if err != nil {
		return nil, err
	}
	np.Changes, np.Warnings, np.Confirm = pl.Changes, pl.Warnings, pl.Confirm
	return np, nil
}

func ptrs(m map[string]string) map[string]*string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*string, len(m))
	for k, v := range m {
		out[k] = kadm.StringPtr(v)
	}
	return out
}

func wrapMsg(err error, msg string) error {
	if msg == "" || strings.Contains(err.Error(), msg) {
		return err
	}
	return fmt.Errorf("%w: %s", err, msg)
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}
