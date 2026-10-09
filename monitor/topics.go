// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package monitor

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/ntailio/ntk/topics"
)

type point struct {
	at    time.Time
	ends  map[int32]int64
	sizes map[int32]int64
}

type PartitionStats struct {
	Partition   int32     `json:"partition"`
	Leader      int32     `json:"leader"`
	ISR         []int32   `json:"isr"`
	Replicas    []int32   `json:"replicas"`
	MsgRate     float64   `json:"msgs_per_sec"`
	BytesRate   float64   `json:"bytes_per_sec"`
	WindowDelta int64     `json:"messages_in_window"`
	SizeBytes   int64     `json:"size_bytes"`
	Skew        float64   `json:"skew"`
	Status      string    `json:"status"`
	Trend       []float64 `json:"-"`
}

type TopicStats struct {
	At              time.Time        `json:"at"`
	Topic           string           `json:"topic"`
	Partitions      int              `json:"partitions"`
	RF              int              `json:"replication_factor"`
	SizeBytes       int64            `json:"size_bytes"`
	MsgRate         float64          `json:"msgs_per_sec"`
	BytesRate       float64          `json:"bytes_per_sec"`
	WindowDelta     int64            `json:"messages_in_window"`
	WindowSizeDelta int64            `json:"bytes_in_window"`
	MaxRate         float64          `json:"max_msgs_per_sec"`
	AvgRate         float64          `json:"avg_msgs_per_sec"`
	URP             int              `json:"under_replicated_partitions"`
	Offline         int              `json:"offline_partitions"`
	LeadersByBroker map[int32]int    `json:"leaders_by_broker"`
	PartitionStats  []PartitionStats `json:"partition_stats"`
	Trend           []float64        `json:"-"`
}

type TopicWatcher struct {
	adm        *kadm.Client
	topics     []string
	all        bool
	Window     time.Duration
	SizeEvery  time.Duration
	history    map[string][]point
	rates      map[string][]float64
	partRates  map[string]map[int32][]float64
	lastSizes  map[string]map[int32]int64
	lastSizeAt time.Time
}

// NewTopicWatcher watches the given topics, or every topic when none are given.
func NewTopicWatcher(adm *kadm.Client, names []string, window time.Duration) *TopicWatcher {
	return &TopicWatcher{
		adm: adm, topics: names, all: len(names) == 0, Window: window, SizeEvery: 30 * time.Second,
		history: map[string][]point{}, rates: map[string][]float64{}, partRates: map[string]map[int32][]float64{},
		lastSizes: map[string]map[int32]int64{},
	}
}

func (w *TopicWatcher) Sample(ctx context.Context) ([]TopicStats, error) {
	now := time.Now()
	md, err := w.adm.Metadata(ctx, w.topics...)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, t := range md.Topics {
		if t.Err == nil && (!w.all || !t.IsInternal) {
			names = append(names, t.Topic)
		}
	}
	if len(names) == 0 {
		return nil, nil
	}
	ends, err := w.adm.ListEndOffsets(ctx, names...)
	var se *kadm.ShardErrors
	if err != nil && !errors.As(err, &se) {
		return nil, err
	}
	if now.Sub(w.lastSizeAt) >= w.SizeEvery {
		if sizes, err := topics.PartitionSizes(ctx, w.adm, names...); err == nil {
			w.lastSizes, w.lastSizeAt = sizes, now
		}
	}

	var out []TopicStats
	for _, name := range names {
		td := md.Topics[name]
		p := point{at: now, ends: map[int32]int64{}, sizes: w.lastSizes[name]}
		for part, o := range ends[name] {
			if o.Err == nil {
				p.ends[part] = o.Offset
			}
		}
		hist := append(w.history[name], p)
		for len(hist) > 2 && now.Sub(hist[1].at) >= w.Window {
			hist = hist[1:]
		}
		w.history[name] = hist
		out = append(out, w.stats(name, td, hist))
	}
	slices.SortFunc(out, func(a, b TopicStats) int { return strings.Compare(a.Topic, b.Topic) })
	return out, nil
}

func (w *TopicWatcher) stats(name string, td kadm.TopicDetail, hist []point) TopicStats {
	cur := hist[len(hist)-1]
	s := TopicStats{At: cur.at, Topic: name, Partitions: len(td.Partitions), LeadersByBroker: map[int32]int{}}
	var prev *point
	if len(hist) > 1 {
		prev = &hist[len(hist)-2]
	}
	first := hist[0]
	if w.partRates[name] == nil {
		w.partRates[name] = map[int32][]float64{}
	}
	for _, pd := range td.Partitions.Sorted() {
		ps := PartitionStats{Partition: pd.Partition, Leader: pd.Leader, ISR: pd.ISR, Replicas: pd.Replicas, Status: "ok"}
		s.RF = max(s.RF, len(pd.Replicas))
		ps.SizeBytes = cur.sizes[pd.Partition]
		s.SizeBytes += ps.SizeBytes
		if prev != nil {
			dt := cur.at.Sub(prev.at).Seconds()
			if dt > 0 {
				ps.MsgRate = float64(max(cur.ends[pd.Partition]-prev.ends[pd.Partition], 0)) / dt
				if prev.sizes != nil && cur.sizes != nil {
					ps.BytesRate = float64(max(cur.sizes[pd.Partition]-prev.sizes[pd.Partition], 0)) / dt
				}
			}
		}
		ps.WindowDelta = max(cur.ends[pd.Partition]-first.ends[pd.Partition], 0)
		pr := append(w.partRates[name][pd.Partition], ps.MsgRate)
		if len(pr) > 30 {
			pr = pr[len(pr)-30:]
		}
		w.partRates[name][pd.Partition] = pr
		ps.Trend = pr
		switch {
		case pd.Leader < 0:
			ps.Status = "offline"
			s.Offline++
		case len(pd.ISR) < len(pd.Replicas):
			ps.Status = "under-replicated"
			s.URP++
		}
		if pd.Leader >= 0 {
			s.LeadersByBroker[pd.Leader]++
		}
		s.MsgRate += ps.MsgRate
		s.BytesRate += ps.BytesRate
		s.WindowDelta += ps.WindowDelta
		s.PartitionStats = append(s.PartitionStats, ps)
	}
	if first.sizes != nil && cur.sizes != nil {
		var a, b int64
		for _, v := range first.sizes {
			a += v
		}
		for _, v := range cur.sizes {
			b += v
		}
		s.WindowSizeDelta = b - a
	}
	mean := s.MsgRate / float64(max(len(s.PartitionStats), 1))
	for i := range s.PartitionStats {
		ps := &s.PartitionStats[i]
		if mean > 0 {
			ps.Skew = ps.MsgRate / mean
			if ps.Status == "ok" && prev != nil {
				switch {
				case ps.Skew < 0.25:
					ps.Status = "low throughput"
				case ps.Skew > 4:
					ps.Status = "hot partition"
				}
			}
		}
	}
	rates := append(w.rates[name], s.MsgRate)
	if len(rates) > 60 {
		rates = rates[len(rates)-60:]
	}
	w.rates[name] = rates
	s.Trend = rates
	var sum float64
	for _, r := range rates {
		sum += r
		s.MaxRate = max(s.MaxRate, r)
	}
	s.AvgRate = sum / float64(len(rates))
	return s
}

func SortTopics(stats []TopicStats, by string) {
	slices.SortFunc(stats, func(a, b TopicStats) int {
		switch by {
		case "bytes":
			return cmp.Compare(b.BytesRate, a.BytesRate)
		case "size":
			return cmp.Compare(b.SizeBytes, a.SizeBytes)
		case "partitions":
			return cmp.Compare(b.Partitions, a.Partitions)
		}
		return cmp.Or(cmp.Compare(b.MsgRate, a.MsgRate), strings.Compare(a.Topic, b.Topic))
	})
}
