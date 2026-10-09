// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package brokers

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ntailio/ntk/configs"
)

type Broker struct {
	ID         int32  `json:"id"`
	Host       string `json:"host"`
	Port       int32  `json:"port"`
	Rack       string `json:"rack,omitempty"`
	Roles      string `json:"roles"`
	Controller bool   `json:"active_controller"`
	Leaders    int    `json:"leaders"`
	Replicas   int    `json:"replicas"`
	DiskUsed   int64  `json:"disk_used_bytes"`
	DiskTotal  int64  `json:"disk_total_bytes"`
	DiskUsable int64  `json:"disk_usable_bytes"`
	Version    string `json:"version"`
}

func (b Broker) DiskPercent() float64 {
	if b.DiskTotal <= 0 {
		return -1
	}
	return float64(b.DiskTotal-b.DiskUsable) / float64(b.DiskTotal) * 100
}

type Replica struct {
	ID           int32 `json:"id"`
	LogEndOffset int64 `json:"log_end_offset"`
	Lag          int64 `json:"lag"`
	LastFetch    int64 `json:"last_fetch_ms"`
	LastCaughtUp int64 `json:"last_caught_up_ms"`
}

type Quorum struct {
	LeaderID      int32     `json:"leader_id"`
	LeaderEpoch   int32     `json:"leader_epoch"`
	HighWatermark int64     `json:"high_watermark"`
	Voters        []Replica `json:"voters"`
	Observers     []Replica `json:"observers"`
}

func DescribeQuorum(ctx context.Context, cl *kgo.Client) (Quorum, error) {
	req := kmsg.NewPtrDescribeQuorumRequest()
	t := kmsg.NewDescribeQuorumRequestTopic()
	t.Topic = "__cluster_metadata"
	p := kmsg.NewDescribeQuorumRequestTopicPartition()
	t.Partitions = append(t.Partitions, p)
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return Quorum{}, err
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return Quorum{}, err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		return Quorum{}, fmt.Errorf("describe quorum: empty response")
	}
	part := resp.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(part.ErrorCode); err != nil {
		return Quorum{}, err
	}
	q := Quorum{LeaderID: part.LeaderID, LeaderEpoch: part.LeaderEpoch, HighWatermark: part.HighWatermark}
	conv := func(rs []kmsg.DescribeQuorumResponseTopicPartitionReplicaState) []Replica {
		var out []Replica
		for _, r := range rs {
			out = append(out, Replica{r.ReplicaID, r.LogEndOffset, max(part.HighWatermark-r.LogEndOffset, 0), r.LastFetchTimestamp, r.LastCaughtUpTimestamp})
		}
		slices.SortFunc(out, func(a, b Replica) int { return cmp.Compare(a.ID, b.ID) })
		return out
	}
	q.Voters, q.Observers = conv(part.CurrentVoters), conv(part.Observers)
	return q, nil
}

func List(ctx context.Context, cl *kgo.Client, adm *kadm.Client) ([]Broker, error) {
	md, err := adm.Metadata(ctx)
	if err != nil {
		return nil, err
	}
	controller := md.Controller
	if q, err := DescribeQuorum(ctx, cl); err == nil {
		controller = q.LeaderID
	}
	byID := map[int32]*Broker{}
	var out []*Broker
	for _, b := range md.Brokers {
		br := &Broker{ID: b.NodeID, Host: b.Host, Port: b.Port, Controller: b.NodeID == controller, DiskTotal: -1, DiskUsable: -1}
		if b.Rack != nil {
			br.Rack = *b.Rack
		}
		byID[b.NodeID] = br
		out = append(out, br)
	}
	for _, t := range md.Topics {
		for _, p := range t.Partitions {
			if b := byID[p.Leader]; b != nil {
				b.Leaders++
			}
			for _, r := range p.Replicas {
				if b := byID[r]; b != nil {
					b.Replicas++
				}
			}
		}
	}
	if dirs, err := adm.DescribeAllLogDirs(ctx, nil); err == nil {
		dirs.Each(func(d kadm.DescribedLogDir) {
			if b := byID[d.Broker]; b != nil {
				b.DiskUsed += d.Size()
				if d.TotalBytes >= 0 {
					b.DiskTotal = max(b.DiskTotal, 0) + d.TotalBytes
					b.DiskUsable = max(b.DiskUsable, 0) + d.UsableBytes
				}
			}
		})
	}
	if versions, err := adm.ApiVersions(ctx); err == nil {
		for _, v := range versions {
			if b := byID[v.NodeID]; b != nil && v.Err == nil {
				b.Version = strings.TrimPrefix(v.VersionGuess(), "v")
			}
		}
	}
	for _, b := range out {
		if es, err := configs.Describe(ctx, cl, configs.Broker, strconv.Itoa(int(b.ID))); err == nil {
			if e, ok := configs.Find(es, "process.roles"); ok {
				b.Roles = e.String()
			}
		}
		if b.Roles == "" {
			b.Roles = "broker"
		}
	}
	res := make([]Broker, len(out))
	for i, b := range out {
		res[i] = *b
	}
	slices.SortFunc(res, func(a, b Broker) int { return cmp.Compare(a.ID, b.ID) })
	return res, nil
}

type LogDir struct {
	Broker     int32  `json:"broker"`
	Dir        string `json:"dir"`
	SizeBytes  int64  `json:"size_bytes"`
	TotalBytes int64  `json:"total_bytes"`
	Usable     int64  `json:"usable_bytes"`
	Error      string `json:"error,omitempty"`
}

type LogDirPartition struct {
	Broker    int32  `json:"broker"`
	Dir       string `json:"dir"`
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	SizeBytes int64  `json:"size_bytes"`
	OffsetLag int64  `json:"offset_lag"`
	Future    bool   `json:"future"`
}

func LogDirs(ctx context.Context, adm *kadm.Client, broker int32, topic string) ([]LogDir, []LogDirPartition, error) {
	var set kadm.TopicsSet
	if topic != "" {
		md, err := adm.ListTopicsWithInternal(ctx, topic)
		if err != nil {
			return nil, nil, err
		}
		set.Add(topic, md[topic].Partitions.Numbers()...)
	}
	dirs, err := adm.DescribeAllLogDirs(ctx, set)
	var se *kadm.ShardErrors
	if err != nil && !errors.As(err, &se) {
		return nil, nil, err
	}
	var ds []LogDir
	var ps []LogDirPartition
	dirs.Each(func(d kadm.DescribedLogDir) {
		if broker >= 0 && d.Broker != broker {
			return
		}
		ld := LogDir{Broker: d.Broker, Dir: d.Dir, SizeBytes: d.Size(), TotalBytes: d.TotalBytes, Usable: d.UsableBytes}
		if d.Err != nil {
			ld.Error = d.Err.Error()
		}
		ds = append(ds, ld)
		for t, parts := range d.Topics {
			if topic != "" && t != topic {
				continue
			}
			for p, dp := range parts {
				ps = append(ps, LogDirPartition{d.Broker, d.Dir, t, p, dp.Size, dp.OffsetLag, dp.IsFuture})
			}
		}
	})
	slices.SortFunc(ds, func(a, b LogDir) int { return cmp.Or(cmp.Compare(a.Broker, b.Broker), strings.Compare(a.Dir, b.Dir)) })
	slices.SortFunc(ps, func(a, b LogDirPartition) int {
		return cmp.Or(cmp.Compare(a.Broker, b.Broker), strings.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	return ds, ps, nil
}

type Detail struct {
	Broker
	OutOfISR  []string          `json:"out_of_isr_partitions"`
	LogDirs   []LogDir          `json:"log_dirs"`
	Overrides map[string]string `json:"dynamic_overrides"`
}

func Describe(ctx context.Context, cl *kgo.Client, adm *kadm.Client, id int32) (Detail, error) {
	all, err := List(ctx, cl, adm)
	if err != nil {
		return Detail{}, err
	}
	i := slices.IndexFunc(all, func(b Broker) bool { return b.ID == id })
	if i < 0 {
		return Detail{}, fmt.Errorf("broker %d is not in the cluster", id)
	}
	d := Detail{Broker: all[i], Overrides: map[string]string{}}
	md, err := adm.Metadata(ctx)
	if err != nil {
		return Detail{}, err
	}
	for _, t := range md.Topics.Sorted() {
		for _, p := range t.Partitions.Sorted() {
			if slices.Contains(p.Replicas, id) && !slices.Contains(p.ISR, id) {
				d.OutOfISR = append(d.OutOfISR, fmt.Sprintf("%s/%d", t.Topic, p.Partition))
			}
		}
	}
	if d.LogDirs, _, err = LogDirs(ctx, adm, id, ""); err != nil {
		return Detail{}, err
	}
	es, err := configs.Describe(ctx, cl, configs.Broker, strconv.Itoa(int(id)))
	if err != nil {
		return Detail{}, err
	}
	for _, e := range es {
		if e.Source == "dynamic-broker" || e.Source == "dynamic-default-broker" {
			d.Overrides[e.Key] = e.String()
		}
	}
	return d, nil
}

type APIVersion struct {
	Broker int32  `json:"broker"`
	Key    int16  `json:"key"`
	Name   string `json:"name"`
	Min    int16  `json:"min"`
	Max    int16  `json:"max"`
}

func APIVersions(ctx context.Context, adm *kadm.Client, broker int32) ([]APIVersion, error) {
	vs, err := adm.ApiVersions(ctx)
	if err != nil {
		return nil, err
	}
	var out []APIVersion
	for _, v := range vs.Sorted() {
		if broker >= 0 && v.NodeID != broker {
			continue
		}
		if v.Err != nil {
			return nil, fmt.Errorf("broker %d: %w", v.NodeID, v.Err)
		}
		v.EachKeySorted(func(key, lo, hi int16) {
			out = append(out, APIVersion{v.NodeID, key, kmsg.NameForKey(key), lo, hi})
		})
	}
	return out, nil
}

type Feature struct {
	Name         string `json:"name"`
	FinalizedMin int16  `json:"finalized_min"`
	FinalizedMax int16  `json:"finalized_max"`
	SupportedMin int16  `json:"supported_min"`
	SupportedMax int16  `json:"supported_max"`
}

func Features(ctx context.Context, cl *kgo.Client) ([]Feature, int64, error) {
	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName, req.ClientSoftwareVersion = "ntk", "1"
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, 0, err
	}
	byName := map[string]*Feature{}
	for _, f := range resp.SupportedFeatures {
		byName[f.Name] = &Feature{Name: f.Name, SupportedMin: f.MinVersion, SupportedMax: f.MaxVersion, FinalizedMin: -1, FinalizedMax: -1}
	}
	for _, f := range resp.FinalizedFeatures {
		x := byName[f.Name]
		if x == nil {
			x = &Feature{Name: f.Name, SupportedMin: -1, SupportedMax: -1}
			byName[f.Name] = x
		}
		x.FinalizedMin, x.FinalizedMax = f.MinVersionLevel, f.MaxVersionLevel
	}
	var out []Feature
	for _, f := range byName {
		out = append(out, *f)
	}
	slices.SortFunc(out, func(a, b Feature) int { return strings.Compare(a.Name, b.Name) })
	return out, resp.FinalizedFeaturesEpoch, nil
}

type Cluster struct {
	ID         string    `json:"cluster_id"`
	Controller int32     `json:"active_controller"`
	Brokers    []Broker  `json:"brokers"`
	Quorum     *Quorum   `json:"quorum,omitempty"`
	Features   []Feature `json:"features"`
	Version    string    `json:"version"`
	Topics     int       `json:"topics"`
	Partitions int       `json:"partitions"`
}

func DescribeCluster(ctx context.Context, cl *kgo.Client, adm *kadm.Client) (Cluster, error) {
	md, err := adm.Metadata(ctx)
	if err != nil {
		return Cluster{}, err
	}
	c := Cluster{ID: md.Cluster, Controller: md.Controller, Topics: len(md.Topics)}
	for _, t := range md.Topics {
		c.Partitions += len(t.Partitions)
	}
	if q, err := DescribeQuorum(ctx, cl); err == nil {
		c.Quorum, c.Controller = &q, q.LeaderID
	}
	if c.Brokers, err = List(ctx, cl, adm); err != nil {
		return Cluster{}, err
	}
	for _, b := range c.Brokers {
		if b.Version != "" && (c.Version == "" || b.Version < c.Version) {
			c.Version = b.Version
		}
	}
	c.Features, _, _ = Features(ctx, cl)
	return c, nil
}

func Ago(ms int64) string {
	if ms <= 0 {
		return "-"
	}
	d := time.Since(time.UnixMilli(ms))
	if d < time.Second {
		return fmt.Sprintf("%.1fs ago", d.Seconds())
	}
	return d.Round(100*time.Millisecond).String() + " ago"
}
