// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Topic struct {
	Name               string        `json:"name"`
	ID                 string        `json:"id"`
	Partitions         int           `json:"partitions"`
	ReplicationFactor  int           `json:"replication_factor"`
	UnderReplicated    int           `json:"under_replicated_partitions"`
	UnderMinISR        int           `json:"under_min_isr_partitions"`
	Offline            int           `json:"offline_partitions"`
	Internal           bool          `json:"internal"`
	SizeBytes          int64         `json:"size_bytes"`
	Messages           int64         `json:"messages"`
	RetentionMs        *int64        `json:"retention_ms,omitempty"`
	CleanupPolicy      string        `json:"cleanup_policy,omitempty"`
	MinInsyncReplicas  int           `json:"min_insync_replicas,omitempty"`
	LeadersByBroker    map[int32]int `json:"leaders_by_broker,omitempty"`
	partitionsByLeader map[int32][]int32
}

type ListOptions struct {
	Names           []string // exact names; overrides Pattern
	Pattern         string
	Regex           bool
	Internal        bool
	Fast            bool // skip sizes and configs
	Messages        bool // also count messages (one ListOffsets round trip)
	UnderReplicated bool
	NoLeader        bool
	UnderMinISR     bool
}

func List(ctx context.Context, adm *kadm.Client, opts ListOptions) ([]Topic, error) {
	match, err := matcher(opts)
	if err != nil {
		return nil, err
	}
	var details kadm.TopicDetails
	if opts.Internal {
		details, err = adm.ListTopicsWithInternal(ctx)
	} else {
		details, err = adm.ListTopics(ctx)
	}
	if err != nil {
		return nil, err
	}
	var out []Topic
	var names []string
	for _, td := range details {
		if td.Err != nil || !match(td.Topic) || (len(opts.Names) > 0 && !slices.Contains(opts.Names, td.Topic)) {
			continue
		}
		out = append(out, summarize(td))
		names = append(names, td.Topic)
	}
	if len(out) > 0 && (!opts.Fast || opts.UnderMinISR) {
		if err := addConfigs(ctx, adm, out, details); err != nil {
			return nil, err
		}
	}
	if len(out) > 0 && !opts.Fast {
		if err := addSizes(ctx, adm, out, names); err != nil {
			return nil, err
		}
	}
	if len(out) > 0 && opts.Messages {
		if err := addMessages(ctx, adm, out, names); err != nil {
			return nil, err
		}
	}
	out = slices.DeleteFunc(out, func(t Topic) bool {
		return (opts.UnderReplicated && t.UnderReplicated == 0) ||
			(opts.NoLeader && t.Offline == 0) ||
			(opts.UnderMinISR && t.UnderMinISR == 0)
	})
	slices.SortFunc(out, func(a, b Topic) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

func Names(ctx context.Context, adm *kadm.Client, prefix string) ([]string, error) {
	ts, err := List(ctx, adm, ListOptions{Internal: strings.HasPrefix(prefix, "_"), Fast: true})
	if err != nil {
		return nil, err
	}
	var names []string
	for _, t := range ts {
		if strings.HasPrefix(t.Name, prefix) {
			names = append(names, t.Name)
		}
	}
	return names, nil
}

func summarize(td kadm.TopicDetail) Topic {
	t := Topic{
		Name:            td.Topic,
		ID:              base64.RawURLEncoding.EncodeToString(td.ID[:]), // the format Kafka's own tools print
		Partitions:      len(td.Partitions),
		Internal:        td.IsInternal,
		LeadersByBroker: map[int32]int{},
		SizeBytes:       -1,
		Messages:        -1,
	}
	for _, p := range td.Partitions {
		t.ReplicationFactor = max(t.ReplicationFactor, len(p.Replicas))
		if p.Leader < 0 {
			t.Offline++
		} else {
			t.LeadersByBroker[p.Leader]++
		}
		if len(p.ISR) < len(p.Replicas) {
			t.UnderReplicated++
		}
	}
	return t
}

func addConfigs(ctx context.Context, adm *kadm.Client, ts []Topic, details kadm.TopicDetails) error {
	names := make([]string, len(ts))
	for i, t := range ts {
		names[i] = t.Name
	}
	rcs, err := adm.DescribeTopicConfigs(ctx, names...)
	if err != nil && !partial(err) {
		return err
	}
	byName := map[string]kadm.ResourceConfig{}
	for _, rc := range rcs {
		byName[rc.Name] = rc
	}
	for i := range ts {
		rc := byName[ts[i].Name]
		for _, c := range rc.Configs {
			v := c.MaybeValue()
			switch c.Key {
			case "retention.ms":
				if n, err := strconv.ParseInt(v, 10, 64); err == nil {
					ts[i].RetentionMs = &n
				}
			case "cleanup.policy":
				ts[i].CleanupPolicy = v
			case "min.insync.replicas":
				ts[i].MinInsyncReplicas, _ = strconv.Atoi(v)
			}
		}
		if ts[i].MinInsyncReplicas > 0 {
			for _, p := range details[ts[i].Name].Partitions {
				if len(p.ISR) < ts[i].MinInsyncReplicas {
					ts[i].UnderMinISR++
				}
			}
		}
	}
	return nil
}

// Sizes are the size of each partition's largest replica, so a topic's size is its logical size.
func addSizes(ctx context.Context, adm *kadm.Client, ts []Topic, names []string) error {
	sizes, err := PartitionSizes(ctx, adm, names...)
	if err != nil {
		return err
	}
	for i := range ts {
		var total int64
		for _, s := range sizes[ts[i].Name] {
			total += s
		}
		ts[i].SizeBytes = total
	}
	return nil
}

func PartitionSizes(ctx context.Context, adm *kadm.Client, topics ...string) (map[string]map[int32]int64, error) {
	md, err := adm.ListTopicsWithInternal(ctx, topics...)
	if err != nil {
		return nil, err
	}
	var set kadm.TopicsSet
	for _, t := range md {
		set.Add(t.Topic, t.Partitions.Numbers()...)
	}
	dirs, err := adm.DescribeAllLogDirs(ctx, set)
	if err != nil && !partial(err) {
		return nil, err
	}
	out := map[string]map[int32]int64{}
	dirs.Each(func(d kadm.DescribedLogDir) {
		for t, parts := range d.Topics {
			if out[t] == nil {
				out[t] = map[int32]int64{}
			}
			for p, dp := range parts {
				if !dp.IsFuture {
					out[t][p] = max(out[t][p], dp.Size)
				}
			}
		}
	})
	return out, nil
}

func addMessages(ctx context.Context, adm *kadm.Client, ts []Topic, names []string) error {
	starts, err := adm.ListStartOffsets(ctx, names...)
	if err != nil && !partial(err) {
		return err
	}
	ends, err := adm.ListEndOffsets(ctx, names...)
	if err != nil && !partial(err) {
		return err
	}
	for i := range ts {
		var n int64
		for p, e := range ends[ts[i].Name] {
			if s, ok := starts.Lookup(ts[i].Name, p); ok && s.Err == nil && e.Err == nil {
				n += max(e.Offset-s.Offset, 0)
			}
		}
		ts[i].Messages = n
	}
	return nil
}

// partial reports whether err only covers some brokers or topics (e.g. a topic
// deleted while listing), so the results that did arrive are still usable.
func partial(err error) bool {
	var se *kadm.ShardErrors
	return errors.As(err, &se) || errors.Is(err, kerr.UnknownTopicOrPartition)
}

func matcher(opts ListOptions) (func(string) bool, error) {
	switch {
	case opts.Pattern == "":
		return func(string) bool { return true }, nil
	case opts.Regex:
		re, err := regexp.Compile(opts.Pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex: %w", err)
		}
		return re.MatchString, nil
	default:
		if _, err := path.Match(opts.Pattern, ""); err != nil {
			return nil, fmt.Errorf("invalid pattern %q: %w", opts.Pattern, err)
		}
		return func(name string) bool {
			ok, _ := path.Match(opts.Pattern, name)
			return ok
		}, nil
	}
}

type Partition struct {
	Partition     int32   `json:"partition"`
	Leader        int32   `json:"leader"`
	Replicas      []int32 `json:"replicas"`
	ISR           []int32 `json:"isr"`
	Offline       []int32 `json:"offline_replicas"`
	LogStart      int64   `json:"log_start_offset"`
	HighWatermark int64   `json:"high_watermark"`
	SizeBytes     int64   `json:"size_bytes"`
}

func (p Partition) Messages() int64 { return max(p.HighWatermark-p.LogStart, 0) }

func (p Partition) Status() string {
	switch {
	case p.Leader < 0:
		return "offline"
	case len(p.ISR) < len(p.Replicas):
		var missing []string
		for _, r := range p.Replicas {
			if !slices.Contains(p.ISR, r) {
				missing = append(missing, strconv.Itoa(int(r)))
			}
		}
		return "under-replicated (missing " + strings.Join(missing, ",") + ")"
	}
	return "ok"
}

type Detail struct {
	Topic
	Overrides        map[string]string `json:"overrides"`
	PartitionDetails []Partition       `json:"partition_details"`
}

func Describe(ctx context.Context, adm *kadm.Client, name string) (Detail, error) {
	details, err := adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return Detail{}, err
	}
	td, ok := details[name]
	if !ok {
		return Detail{}, fmt.Errorf("topic %q: %w", name, kerr.UnknownTopicOrPartition)
	}
	if td.Err != nil {
		return Detail{}, fmt.Errorf("topic %q: %w", name, td.Err)
	}
	starts, err := adm.ListStartOffsets(ctx, name)
	if err != nil {
		return Detail{}, err
	}
	ends, err := adm.ListEndOffsets(ctx, name)
	if err != nil {
		return Detail{}, err
	}
	sizes, err := PartitionSizes(ctx, adm, name)
	if err != nil {
		return Detail{}, err
	}

	d := Detail{Topic: summarize(td), Overrides: map[string]string{}}
	ts := []Topic{d.Topic}
	if err := addConfigs(ctx, adm, ts, details); err != nil {
		return Detail{}, err
	}
	d.Topic = ts[0]
	if rcs, err := adm.DescribeTopicConfigs(ctx, name); err == nil && len(rcs) == 1 {
		for _, c := range rcs[0].Configs {
			if c.Source == kmsg.ConfigSourceDynamicTopicConfig {
				d.Overrides[c.Key] = c.MaybeValue()
			}
		}
	}
	d.SizeBytes, d.Messages = 0, 0
	for _, p := range td.Partitions {
		part := Partition{
			Partition: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR, Offline: p.OfflineReplicas,
			LogStart: -1, HighWatermark: -1, SizeBytes: sizes[name][p.Partition],
		}
		if o, ok := starts.Lookup(name, p.Partition); ok && o.Err == nil {
			part.LogStart = o.Offset
		}
		if o, ok := ends.Lookup(name, p.Partition); ok && o.Err == nil {
			part.HighWatermark = o.Offset
		}
		d.SizeBytes += part.SizeBytes
		d.Messages += part.Messages()
		d.PartitionDetails = append(d.PartitionDetails, part)
	}
	slices.SortFunc(d.PartitionDetails, func(a, b Partition) int { return int(a.Partition - b.Partition) })
	return d, nil
}

type PartitionOffsets struct {
	Partition int32  `json:"partition"`
	Earliest  int64  `json:"earliest"`
	Latest    int64  `json:"latest"`
	AtTime    *int64 `json:"offset_at_time,omitempty"`
	Timestamp *int64 `json:"timestamp_at_offset,omitempty"`
}

func Offsets(ctx context.Context, adm *kadm.Client, topic string, atMilli *int64) ([]PartitionOffsets, error) {
	starts, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("topic %q: %w", topic, err)
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	var at kadm.ListedOffsets
	if atMilli != nil {
		if at, err = adm.ListOffsetsAfterMilli(ctx, *atMilli, topic); err != nil {
			return nil, err
		}
	}
	var out []PartitionOffsets
	for p, s := range starts[topic] {
		po := PartitionOffsets{Partition: p, Earliest: s.Offset, Latest: -1}
		if e, ok := ends.Lookup(topic, p); ok {
			po.Latest = e.Offset
		}
		if o, ok := at.Lookup(topic, p); ok && o.Err == nil {
			off, ts := o.Offset, o.Timestamp
			po.AtTime = &off
			if ts >= 0 {
				po.Timestamp = &ts
			}
		}
		out = append(out, po)
	}
	slices.SortFunc(out, func(a, b PartitionOffsets) int { return int(a.Partition - b.Partition) })
	return out, nil
}

func Exists(ctx context.Context, adm *kadm.Client, name string) (bool, error) {
	details, err := adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return false, err
	}
	td, ok := details[name]
	return ok && td.Err == nil, nil
}
