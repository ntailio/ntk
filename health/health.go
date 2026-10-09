// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package health

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/brokers"
	"github.com/ntailio/ntk/monitor"
	"github.com/ntailio/ntk/topics"
	"github.com/ntailio/ntk/tx"
)

type Level int

const (
	OK Level = iota
	Warn
	Critical
)

func (l Level) String() string {
	switch l {
	case Warn:
		return "warn"
	case Critical:
		return "critical"
	}
	return "ok"
}

func (l Level) MarshalText() ([]byte, error) { return []byte(l.String()), nil }

func (l *Level) UnmarshalText(b []byte) error {
	switch string(b) {
	case "ok":
		*l = OK
	case "warn":
		*l = Warn
	case "critical":
		*l = Critical
	default:
		return fmt.Errorf("unknown health level %q", b)
	}
	return nil
}

func ParseLevel(s string) (Level, error) {
	switch s {
	case "warn":
		return Warn, nil
	case "critical":
		return Critical, nil
	}
	return OK, fmt.Errorf("--fail-on must be warn or critical")
}

type Check struct {
	Name    string   `json:"name"`
	Level   Level    `json:"status"`
	Message string   `json:"message"`
	Items   []string `json:"items,omitempty"`
}

type Report struct {
	At        time.Time `json:"at"`
	ClusterID string    `json:"cluster_id"`
	Version   string    `json:"version"`
	Brokers   int       `json:"brokers"`
	Voters    int       `json:"voters"`
	Status    Level     `json:"status"`
	Checks    []Check   `json:"checks"`
}

type Options struct {
	Checks          []string
	ExpectBrokers   int
	Groups          string
	DiskWarn        float64
	DiskCritical    float64
	ImbalanceWarn   float64
	QuorumLagWarn   int64
	TxnTimeout      time.Duration
	GroupSampleWait time.Duration
}

var AllChecks = []string{"brokers", "controller", "quorum", "offline", "under-replicated", "under-min-isr", "at-min-isr",
	"leader-imbalance", "reassignments", "log-dirs", "disk", "transactions", "groups"}

func (o Options) want(name string) bool {
	if name == "groups" && o.Groups == "" {
		return false
	}
	return len(o.Checks) == 0 || slices.Contains(o.Checks, "all") || slices.Contains(o.Checks, name)
}

func Run(ctx context.Context, cl *kgo.Client, adm *kadm.Client, o Options) (Report, error) {
	if o.DiskWarn == 0 {
		o.DiskWarn, o.DiskCritical = 80, 90
	}
	if o.ImbalanceWarn == 0 {
		o.ImbalanceWarn = 10
	}
	if o.QuorumLagWarn == 0 {
		o.QuorumLagWarn = 1000
	}
	r := Report{At: time.Now()}
	md, err := adm.Metadata(ctx)
	if err != nil {
		return r, err
	}
	r.ClusterID, r.Brokers = md.Cluster, len(md.Brokers)
	add := func(c Check) {
		r.Checks = append(r.Checks, c)
		r.Status = max(r.Status, c.Level)
	}

	if o.want("brokers") {
		c := Check{Name: "brokers"}
		vs, _ := adm.ApiVersions(ctx)
		reachable := 0
		for _, v := range vs {
			if v.Err == nil {
				reachable++
				if r.Version == "" {
					r.Version = strings.TrimPrefix(v.VersionGuess(), "v")
				}
			} else {
				c.Items = append(c.Items, fmt.Sprintf("broker %d: %v", v.NodeID, v.Err))
			}
		}
		c.Message = fmt.Sprintf("%d/%d reachable", reachable, len(md.Brokers))
		if reachable < len(md.Brokers) {
			c.Level = Warn
		}
		if o.ExpectBrokers > 0 && len(md.Brokers) < o.ExpectBrokers {
			c.Level, c.Message = Critical, fmt.Sprintf("%d brokers, expected %d", len(md.Brokers), o.ExpectBrokers)
		}
		add(c)
	}

	q, qerr := brokers.DescribeQuorum(ctx, cl)
	if qerr == nil {
		r.Voters = len(q.Voters)
	}
	if o.want("controller") {
		switch {
		case qerr == nil && q.LeaderID >= 0:
			add(Check{Name: "controller", Message: strconv.Itoa(int(q.LeaderID))})
		case qerr == nil:
			add(Check{Name: "controller", Level: Critical, Message: "no active controller"})
		case md.Controller >= 0:
			add(Check{Name: "controller", Message: strconv.Itoa(int(md.Controller)) + " (from metadata)"})
		default:
			add(Check{Name: "controller", Level: Critical, Message: "no active controller"})
		}
	}
	if o.want("quorum") && qerr == nil {
		c := Check{Name: "quorum"}
		var maxLag int64
		healthy := 0
		for _, v := range q.Voters {
			maxLag = max(maxLag, v.Lag)
			stale := v.ID != q.LeaderID && v.LastFetch > 0 && time.Since(time.UnixMilli(v.LastFetch)) > 5*time.Second
			switch {
			case stale:
				c.Items = append(c.Items, fmt.Sprintf("voter %d last fetched %s", v.ID, brokers.Ago(v.LastFetch)))
				c.Level = max(c.Level, Warn)
			case v.Lag > o.QuorumLagWarn:
				c.Items = append(c.Items, fmt.Sprintf("voter %d lags %d records", v.ID, v.Lag))
				c.Level = max(c.Level, Warn)
				healthy++
			default:
				healthy++
			}
		}
		if healthy < len(q.Voters) && healthy <= len(q.Voters)/2+1 {
			c.Level = Critical
		}
		c.Message = fmt.Sprintf("leader %d, max voter lag %d", q.LeaderID, maxLag)
		add(c)
	}

	needTopics := o.want("offline") || o.want("under-replicated") || o.want("under-min-isr") || o.want("at-min-isr") || o.want("leader-imbalance")
	if needTopics {
		ts, err := topics.List(ctx, adm, topics.ListOptions{Internal: true, UnderMinISR: false})
		if err != nil {
			return r, err
		}
		minISR := map[string]int{}
		for _, t := range ts {
			minISR[t.Name] = t.MinInsyncReplicas
		}
		var offline, urp, underMin, atMin []string
		total, notPreferred := 0, 0
		for _, t := range md.Topics.Sorted() {
			for _, p := range t.Partitions.Sorted() {
				total++
				id := fmt.Sprintf("%s/%d", t.Topic, p.Partition)
				if p.Leader < 0 {
					offline = append(offline, id)
				}
				if len(p.ISR) < len(p.Replicas) {
					var missing []string
					for _, rep := range p.Replicas {
						if !slices.Contains(p.ISR, rep) {
							missing = append(missing, strconv.Itoa(int(rep)))
						}
					}
					urp = append(urp, id+" missing broker "+strings.Join(missing, ","))
				}
				if m := minISR[t.Topic]; m > 0 {
					switch {
					case len(p.ISR) < m:
						underMin = append(underMin, fmt.Sprintf("%s (ISR %s; min.insync.replicas %d)", id, ids(p.ISR), m))
					case len(p.ISR) == m && len(p.Replicas) > m:
						atMin = append(atMin, fmt.Sprintf("%s (ISR %s; min.insync.replicas %d)", id, ids(p.ISR), m))
					}
				}
				if len(p.Replicas) > 0 && p.Leader != p.Replicas[0] {
					notPreferred++
				}
			}
		}
		list := func(name string, items []string, lvl Level) {
			if !o.want(name) {
				return
			}
			c := Check{Name: name, Message: fmt.Sprintf("%d", len(items)), Items: items}
			if len(items) > 0 {
				c.Level = lvl
				c.Message = fmt.Sprintf("%d partition(s)", len(items))
			}
			add(c)
		}
		list("offline", offline, Critical)
		list("under-replicated", urp, Warn)
		list("under-min-isr", underMin, Critical)
		list("at-min-isr", atMin, Warn)
		if o.want("leader-imbalance") {
			pct := 0.0
			if total > 0 {
				pct = float64(notPreferred) / float64(total) * 100
			}
			c := Check{Name: "leader-imbalance", Message: fmt.Sprintf("%.1f%% of partitions not on their preferred leader", pct)}
			if pct > o.ImbalanceWarn {
				c.Level = Warn
				c.Items = []string{"fix: ntk topic elect-leaders --type preferred"}
			}
			add(c)
		}
	}

	if o.want("reassignments") {
		running, err := topics.InProgress(ctx, adm, nil)
		c := Check{Name: "reassignments", Message: "none"}
		switch {
		case err != nil:
			c.Level, c.Message = Warn, err.Error()
		case len(running) > 0:
			c.Message = fmt.Sprintf("%d partition(s) moving (see ntk topic reassign status)", len(running))
		}
		add(c)
	}

	if o.want("log-dirs") || o.want("disk") {
		dirs, _, err := brokers.LogDirs(ctx, adm, -1, "")
		if err != nil {
			return r, err
		}
		if o.want("log-dirs") {
			c := Check{Name: "log-dirs", Message: fmt.Sprintf("%d dirs, no errors", len(dirs))}
			for _, d := range dirs {
				if d.Error != "" {
					c.Level = Critical
					c.Items = append(c.Items, fmt.Sprintf("broker %d %s: %s", d.Broker, d.Dir, d.Error))
				}
			}
			if c.Level == Critical {
				c.Message = fmt.Sprintf("%d offline log dir(s)", len(c.Items))
			}
			add(c)
		}
		if o.want("disk") {
			c := Check{Name: "disk"}
			worst, worstB := -1.0, int32(-1)
			for _, d := range dirs {
				if d.TotalBytes <= 0 {
					continue
				}
				pct := float64(d.TotalBytes-d.Usable) / float64(d.TotalBytes) * 100
				if pct > worst {
					worst, worstB = pct, d.Broker
				}
				switch {
				case pct > o.DiskCritical:
					c.Level = Critical
					c.Items = append(c.Items, fmt.Sprintf("broker %d %s %.0f%%", d.Broker, d.Dir, pct))
				case pct > o.DiskWarn:
					c.Level = max(c.Level, Warn)
					c.Items = append(c.Items, fmt.Sprintf("broker %d %s %.0f%%", d.Broker, d.Dir, pct))
				}
			}
			if worst < 0 {
				c.Message = "not reported by brokers"
			} else {
				c.Message = fmt.Sprintf("max %.0f%% (broker %d)", worst, worstB)
			}
			add(c)
		}
	}

	if o.want("transactions") {
		timeout := o.TxnTimeout
		if timeout == 0 {
			timeout = 15 * time.Minute
		}
		hanging, err := tx.FindHanging(ctx, adm, nil, -1, timeout)
		c := Check{Name: "transactions", Message: "no hanging transactions"}
		switch {
		case err != nil:
			c.Message = "not checked: " + err.Error()
		case len(hanging) > 0:
			c.Level = Warn
			c.Message = fmt.Sprintf("%d hanging transaction(s)", len(hanging))
			for _, h := range hanging {
				c.Items = append(c.Items, fmt.Sprintf("%s/%d producer %d (%s)", h.Topic, h.Partition, h.ProducerID, h.Reason))
				if h.OpenSince > 2*timeout {
					c.Level = Critical
				}
			}
		}
		add(c)
	}

	if o.want("groups") {
		w := monitor.NewGroupWatcher(cl, adm, nil, o.Groups, time.Minute)
		c := Check{Name: "groups"}
		stats, err := w.Sample(ctx)
		if err == nil && o.GroupSampleWait > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(o.GroupSampleWait):
			}
			stats, err = w.Sample(ctx)
		}
		if err != nil {
			c.Level, c.Message = Warn, err.Error()
		} else {
			bad := 0
			for _, s := range stats {
				switch s.Status {
				case monitor.Stuck:
					c.Level = Critical
					bad++
					c.Items = append(c.Items, fmt.Sprintf("%s: stuck (lag %d)", s.Group, s.Lag))
				case monitor.FallingBehind, monitor.NoMembers:
					c.Level = max(c.Level, Warn)
					bad++
					c.Items = append(c.Items, fmt.Sprintf("%s: %s (lag %d)", s.Group, s.Status, s.Lag))
				}
			}
			c.Message = fmt.Sprintf("%d group(s), %d with problems", len(stats), bad)
		}
		add(c)
	}
	return r, nil
}

func ids(v []int32) string {
	s := make([]string, len(v))
	for i, n := range v {
		s[i] = strconv.Itoa(int(n))
	}
	return strings.Join(s, ",")
}
