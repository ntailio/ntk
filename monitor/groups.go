// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package monitor

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/groups"
)

type Status string

const (
	OK            Status = "ok"
	CatchingUp    Status = "catching up"
	FallingBehind Status = "falling behind"
	Stuck         Status = "stuck"
	Rebalancing   Status = "rebalancing"
	NoMembers     Status = "no members"
)

type groupPoint struct {
	at        time.Time
	committed map[string]map[int32]int64
	ends      map[string]map[int32]int64
	state     string
	epoch     int32
	members   int
}

type PartitionLag struct {
	Topic       string        `json:"topic"`
	Partition   int32         `json:"partition"`
	Member      string        `json:"member,omitempty"`
	ClientID    string        `json:"client_id,omitempty"`
	Lag         int64         `json:"lag"`
	ConsumeRate float64       `json:"consume_per_sec"`
	ProduceRate float64       `json:"produce_per_sec"`
	TimeLag     time.Duration `json:"time_lag_ns"`
	TimeLagOK   bool          `json:"time_lag_known"`
	Status      Status        `json:"status"`
	Trend       []int64       `json:"-"`
	stuckSince  time.Time
}

type TopicLag struct {
	Topic       string        `json:"topic"`
	Lag         int64         `json:"lag"`
	ConsumeRate float64       `json:"consume_per_sec"`
	ProduceRate float64       `json:"produce_per_sec"`
	TimeLag     time.Duration `json:"time_lag_ns"`
	TimeLagOK   bool          `json:"time_lag_known"`
	ETA         time.Duration `json:"eta_ns"`
	ETAOK       bool          `json:"eta_known"`
	Status      Status        `json:"status"`
	Trend       []int64       `json:"-"`
}

type GroupStats struct {
	At         time.Time      `json:"at"`
	Group      string         `json:"group"`
	Type       string         `json:"type"`
	State      string         `json:"state"`
	Members    int            `json:"members"`
	Epoch      int32          `json:"epoch"`
	Lag        int64          `json:"lag"`
	LagDelta   int64          `json:"lag_change_in_window"`
	Status     Status         `json:"status"`
	Topics     []TopicLag     `json:"topics"`
	Partitions []PartitionLag `json:"partitions"`
	Events     []string       `json:"events,omitempty"`
	Trend      []int64        `json:"-"`
}

// TimestampAt returns the timestamp of the record at offset, for --exact-time-lag.
type TimestampAt func(ctx context.Context, topic string, partition int32, offset int64) (time.Time, error)

type GroupWatcher struct {
	cl         *kgo.Client
	adm        *kadm.Client
	names      []string
	pattern    string
	Window     time.Duration
	StuckAfter time.Duration
	Exact      TimestampAt

	history    map[string][]groupPoint
	lagTrend   map[string][]int64
	topicTrend map[string]map[string][]int64
	partTrend  map[string]map[string][]int64
	lastMove   map[string]map[string]time.Time
	lastSeen   map[string]groupPoint
	members    map[string]map[string]string
}

// NewGroupWatcher watches the named groups, or all groups matching pattern when names is empty.
func NewGroupWatcher(cl *kgo.Client, adm *kadm.Client, names []string, pattern string, window time.Duration) *GroupWatcher {
	return &GroupWatcher{
		cl: cl, adm: adm, names: names, pattern: pattern, Window: window, StuckAfter: 2 * time.Minute,
		history: map[string][]groupPoint{}, lagTrend: map[string][]int64{},
		topicTrend: map[string]map[string][]int64{}, partTrend: map[string]map[string][]int64{},
		lastMove: map[string]map[string]time.Time{}, lastSeen: map[string]groupPoint{}, members: map[string]map[string]string{},
	}
}

func key(t string, p int32) string { return fmt.Sprintf("%s/%d", t, p) }

func (w *GroupWatcher) Sample(ctx context.Context) ([]GroupStats, error) {
	names := w.names
	listedTypes := map[string]string{}
	if len(names) == 0 {
		gs, err := groups.List(ctx, w.cl, w.adm, groups.ListOptions{Pattern: w.pattern})
		if err != nil {
			return nil, err
		}
		for _, g := range gs {
			if g.Type != "share" {
				names = append(names, g.Name)
				listedTypes[g.Name] = g.Type
			}
		}
	}
	var out []GroupStats
	for _, name := range names {
		d, err := groups.Describe(ctx, w.cl, w.adm, name)
		if err != nil {
			return nil, fmt.Errorf("group %q: %w", name, err)
		}
		out = append(out, w.update(ctx, d))
	}
	slices.SortFunc(out, func(a, b GroupStats) int { return strings.Compare(a.Group, b.Group) })
	return out, nil
}

func (w *GroupWatcher) update(ctx context.Context, d groups.Detail) GroupStats {
	now := time.Now()
	p := groupPoint{at: now, committed: map[string]map[int32]int64{}, ends: map[string]map[int32]int64{}, state: d.State, epoch: d.Epoch, members: d.Members}
	for _, o := range d.Offsets {
		if p.committed[o.Topic] == nil {
			p.committed[o.Topic], p.ends[o.Topic] = map[int32]int64{}, map[int32]int64{}
		}
		p.committed[o.Topic][o.Partition] = o.Committed
		p.ends[o.Topic][o.Partition] = o.End
	}
	hist := append(w.history[d.Name], p)
	for len(hist) > 2 && now.Sub(hist[1].at) >= w.Window {
		hist = hist[1:]
	}
	w.history[d.Name] = hist
	var prev *groupPoint
	if len(hist) > 1 {
		prev = &hist[len(hist)-2]
	}
	first := hist[0]

	s := GroupStats{At: now, Group: d.Name, Type: d.Type, State: d.State, Members: d.Members, Epoch: d.Epoch}
	if last, ok := w.lastSeen[d.Name]; ok {
		if last.state != d.State {
			s.Events = append(s.Events, fmt.Sprintf("%s state %s → %s", now.Format("15:04:05"), last.state, d.State))
		}
		if d.Epoch != last.epoch && d.Epoch > 0 {
			s.Events = append(s.Events, fmt.Sprintf("%s rebalance: epoch %d → %d", now.Format("15:04:05"), last.epoch, d.Epoch))
		}
		if d.Members != last.members {
			s.Events = append(s.Events, fmt.Sprintf("%s members %d → %d", now.Format("15:04:05"), last.members, d.Members))
		}
	}
	w.lastSeen[d.Name] = p

	if w.lastMove[d.Name] == nil {
		w.lastMove[d.Name] = map[string]time.Time{}
	}
	if w.partTrend[d.Name] == nil {
		w.partTrend[d.Name] = map[string][]int64{}
	}
	if w.topicTrend[d.Name] == nil {
		w.topicTrend[d.Name] = map[string][]int64{}
	}
	rebalancing := strings.Contains(strings.ToLower(d.State), "rebalanc") || d.State == "Assigning" || d.State == "Reconciling" ||
		(prev != nil && prev.epoch != d.Epoch && d.Epoch > 0)

	topicAgg := map[string]*TopicLag{}
	for _, o := range d.Offsets {
		pl := PartitionLag{Topic: o.Topic, Partition: o.Partition, Member: o.MemberID, ClientID: o.ClientID, Lag: max(o.Lag, 0), Status: OK}
		k := key(o.Topic, o.Partition)
		if prev != nil {
			dt := now.Sub(prev.at).Seconds()
			pc, pe := prev.committed[o.Topic][o.Partition], prev.ends[o.Topic][o.Partition]
			if dt > 0 {
				if o.Committed >= 0 && pc >= 0 {
					pl.ConsumeRate = float64(max(o.Committed-pc, 0)) / dt
				}
				pl.ProduceRate = float64(max(o.End-pe, 0)) / dt
			}
			if o.Committed != pc {
				w.lastMove[d.Name][k] = now
			}
		}
		if _, ok := w.lastMove[d.Name][k]; !ok {
			w.lastMove[d.Name][k] = now
		}
		switch {
		case pl.Lag == 0:
			pl.TimeLag, pl.TimeLagOK = 0, true
		case w.Exact != nil && o.Committed >= 0:
			if ts, err := w.Exact(ctx, o.Topic, o.Partition, o.Committed); err == nil {
				pl.TimeLag, pl.TimeLagOK = max(now.Sub(ts), 0), true
			}
		case pl.ProduceRate > 0:
			pl.TimeLag, pl.TimeLagOK = time.Duration(float64(pl.Lag)/pl.ProduceRate*float64(time.Second)), true
		}
		trend := append(w.partTrend[d.Name][k], pl.Lag)
		if len(trend) > 30 {
			trend = trend[len(trend)-30:]
		}
		w.partTrend[d.Name][k] = trend
		pl.Trend = trend
		pl.stuckSince = w.lastMove[d.Name][k]
		pl.Status = status(pl.Lag, trend, pl.ConsumeRate, pl.ProduceRate, rebalancing, d.Members, now.Sub(pl.stuckSince) >= w.StuckAfter && prev != nil)
		s.Partitions = append(s.Partitions, pl)

		t := topicAgg[o.Topic]
		if t == nil {
			t = &TopicLag{Topic: o.Topic, TimeLagOK: true}
			topicAgg[o.Topic] = t
		}
		t.Lag += pl.Lag
		t.ConsumeRate += pl.ConsumeRate
		t.ProduceRate += pl.ProduceRate
		if pl.TimeLagOK {
			t.TimeLag = max(t.TimeLag, pl.TimeLag)
		} else if pl.Lag > 0 {
			t.TimeLagOK = false
		}
		s.Lag += pl.Lag
	}
	var firstLag int64
	for t, parts := range first.committed {
		for part, c := range parts {
			if e := first.ends[t][part]; c >= 0 && e >= 0 {
				firstLag += max(e-c, 0)
			}
		}
	}
	s.LagDelta = s.Lag - firstLag

	for _, t := range topicAgg {
		trend := append(w.topicTrend[d.Name][t.Topic], t.Lag)
		if len(trend) > 30 {
			trend = trend[len(trend)-30:]
		}
		w.topicTrend[d.Name][t.Topic] = trend
		t.Trend = trend
		if net := t.ConsumeRate - t.ProduceRate; t.Lag > 0 && net > 0 {
			t.ETA, t.ETAOK = time.Duration(float64(t.Lag)/net*float64(time.Second)), true
		}
		t.Status = OK
		for _, pl := range s.Partitions {
			if pl.Topic == t.Topic && severity(pl.Status) > severity(t.Status) {
				t.Status = pl.Status
			}
		}
		s.Topics = append(s.Topics, *t)
	}
	slices.SortFunc(s.Topics, func(a, b TopicLag) int { return strings.Compare(a.Topic, b.Topic) })
	s.Status = OK
	for _, t := range s.Topics {
		if severity(t.Status) > severity(s.Status) {
			s.Status = t.Status
		}
	}
	lt := append(w.lagTrend[d.Name], s.Lag)
	if len(lt) > 60 {
		lt = lt[len(lt)-60:]
	}
	w.lagTrend[d.Name] = lt
	s.Trend = lt
	return s
}

func status(lag int64, trend []int64, consume, produce float64, rebalancing bool, members int, stuck bool) Status {
	switch {
	case rebalancing:
		return Rebalancing
	case lag == 0:
		return OK
	case members == 0:
		return NoMembers
	case stuck:
		return Stuck
	case len(trend) >= 2 && trend[len(trend)-1] > trend[0] && consume <= produce:
		return FallingBehind
	case len(trend) >= 2 && trend[len(trend)-1] < trend[len(trend)-2]:
		return CatchingUp
	case consume > produce:
		return CatchingUp
	}
	return OK
}

func severity(s Status) int {
	switch s {
	case Stuck:
		return 5
	case NoMembers:
		return 4
	case FallingBehind:
		return 3
	case Rebalancing:
		return 2
	case CatchingUp:
		return 1
	}
	return 0
}

func SortGroups(stats []GroupStats, by string) {
	slices.SortFunc(stats, func(a, b GroupStats) int {
		switch by {
		case "time-lag":
			return cmp.Compare(maxTimeLag(b), maxTimeLag(a))
		case "rate":
			return cmp.Compare(rate(b), rate(a))
		}
		return cmp.Or(cmp.Compare(b.Lag, a.Lag), strings.Compare(a.Group, b.Group))
	})
}

func maxTimeLag(s GroupStats) time.Duration {
	var m time.Duration
	for _, t := range s.Topics {
		m = max(m, t.TimeLag)
	}
	return m
}

func rate(s GroupStats) float64 {
	var r float64
	for _, t := range s.Topics {
		r += t.ConsumeRate
	}
	return r
}

func MaxTimeLag(s GroupStats) (time.Duration, bool) {
	ok := true
	var m time.Duration
	for _, t := range s.Topics {
		if !t.TimeLagOK {
			ok = false
		}
		m = max(m, t.TimeLag)
	}
	return m, ok
}
