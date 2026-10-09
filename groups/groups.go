// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package groups

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type Group struct {
	Name         string   `json:"name"`
	Type         string   `json:"type"`
	ProtocolType string   `json:"protocol_type"`
	State        string   `json:"state"`
	Members      int      `json:"members"`
	Coordinator  int32    `json:"coordinator"`
	Topics       []string `json:"topics"`
	Lag          int64    `json:"lag"`
}

type ListOptions struct {
	Pattern string
	Regex   bool
	States  []string
	Types   []string
	Topic   string
	Lag     bool
}

type listed struct {
	name, typ, protocolType, state string
	coordinator                    int32
}

func listRaw(ctx context.Context, cl *kgo.Client, states, types []string) ([]listed, error) {
	req := kmsg.NewPtrListGroupsRequest()
	req.StatesFilter = states
	req.TypesFilter = types
	var out []listed
	var errs []error
	for _, shard := range cl.RequestSharded(ctx, req) {
		if shard.Err != nil {
			errs = append(errs, fmt.Errorf("broker %d: %w", shard.Meta.NodeID, shard.Err))
			continue
		}
		resp := shard.Resp.(*kmsg.ListGroupsResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			errs = append(errs, err)
			continue
		}
		for _, g := range resp.Groups {
			typ := strings.ToLower(g.GroupType)
			if typ == "" {
				typ = "classic"
			}
			out = append(out, listed{g.Group, typ, g.ProtocolType, g.GroupState, shard.Meta.NodeID})
		}
	}
	if len(out) == 0 && len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return out, nil
}

func matcher(pattern string, regex bool) (func(string) bool, error) {
	switch {
	case pattern == "":
		return func(string) bool { return true }, nil
	case regex:
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("invalid regex: %w", err)
		}
		return re.MatchString, nil
	}
	if _, err := path.Match(pattern, ""); err != nil {
		return nil, fmt.Errorf("invalid pattern %q: %w", pattern, err)
	}
	return func(s string) bool { ok, _ := path.Match(pattern, s); return ok }, nil
}

func List(ctx context.Context, cl *kgo.Client, adm *kadm.Client, opts ListOptions) ([]Group, error) {
	match, err := matcher(opts.Pattern, opts.Regex)
	if err != nil {
		return nil, err
	}
	raw, err := listRaw(ctx, cl, canonicalStates(opts.States), opts.Types)
	if err != nil {
		return nil, err
	}
	byType := map[string][]string{}
	groups := map[string]*Group{}
	for _, l := range raw {
		if !match(l.name) {
			continue
		}
		groups[l.name] = &Group{Name: l.name, Type: l.typ, ProtocolType: l.protocolType, State: l.state, Coordinator: l.coordinator, Lag: -1}
		byType[l.typ] = append(byType[l.typ], l.name)
	}
	if len(groups) == 0 {
		return []Group{}, nil
	}
	if err := fillMembers(ctx, adm, groups, byType); err != nil {
		return nil, err
	}

	var names []string
	for n, g := range groups {
		if g.Type != "share" {
			names = append(names, n)
		}
	}
	fetched := adm.FetchManyOffsets(ctx, names...)
	committed := map[string]kadm.OffsetResponses{}
	var allTopics []string
	for n, r := range fetched {
		if r.Err != nil {
			continue
		}
		committed[n] = r.Fetched
		for t := range r.Fetched {
			if !slices.Contains(groups[n].Topics, t) {
				groups[n].Topics = append(groups[n].Topics, t)
			}
			if !slices.Contains(allTopics, t) {
				allTopics = append(allTopics, t)
			}
		}
		slices.Sort(groups[n].Topics)
	}
	if opts.Lag && len(allTopics) > 0 {
		ends, err := adm.ListEndOffsets(ctx, allTopics...)
		if err != nil {
			return nil, err
		}
		for n, offs := range committed {
			var total int64
			offs.Each(func(o kadm.OffsetResponse) {
				if e, ok := ends.Lookup(o.Topic, o.Partition); ok && e.Err == nil && o.Err == nil && o.At >= 0 {
					total += max(e.Offset-o.At, 0)
				}
			})
			groups[n].Lag = total
		}
	}

	var out []Group
	for _, g := range groups {
		if opts.Topic != "" && !slices.Contains(g.Topics, opts.Topic) {
			continue
		}
		out = append(out, *g)
	}
	slices.SortFunc(out, func(a, b Group) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

var stateNames = map[string]string{
	"stable": "Stable", "empty": "Empty", "dead": "Dead", "preparingrebalance": "PreparingRebalance",
	"completingrebalance": "CompletingRebalance", "assigning": "Assigning", "reconciling": "Reconciling",
}

func canonicalStates(in []string) []string {
	var out []string
	for _, s := range in {
		if c, ok := stateNames[strings.ToLower(s)]; ok {
			out = append(out, c)
		} else {
			out = append(out, s)
		}
	}
	return out
}

func fillMembers(ctx context.Context, adm *kadm.Client, groups map[string]*Group, byType map[string][]string) error {
	if names := byType["classic"]; len(names) > 0 {
		described, err := adm.DescribeGroups(ctx, names...)
		if err != nil && len(described) == 0 {
			return err
		}
		for _, d := range described {
			if g := groups[d.Group]; g != nil && d.Err == nil {
				g.Members = len(d.Members)
				g.Topics = append(g.Topics, d.AssignedPartitions().Sorted().Topics()...)
			}
		}
	}
	if names := byType["consumer"]; len(names) > 0 {
		described, err := adm.DescribeConsumerGroups(ctx, names...)
		if err != nil && len(described) == 0 {
			return err
		}
		for _, d := range described {
			if g := groups[d.Group]; g != nil && d.Err == nil {
				g.Members = len(d.Members)
				g.Topics = append(g.Topics, d.SubscribedTopics()...)
			}
		}
	}
	if names := byType["share"]; len(names) > 0 {
		described, err := adm.DescribeShareGroups(ctx, names...)
		if err == nil {
			for _, d := range described {
				if g := groups[d.GroupID]; g != nil && d.Err == nil {
					g.Members = len(d.Members)
					g.Topics = d.SubscribedTopics()
				}
			}
		}
	}
	for _, g := range groups {
		slices.Sort(g.Topics)
		g.Topics = slices.Compact(g.Topics)
	}
	return nil
}

type Member struct {
	ID         string   `json:"member_id"`
	InstanceID string   `json:"instance_id,omitempty"`
	ClientID   string   `json:"client_id"`
	Host       string   `json:"client_host"`
	Rack       string   `json:"rack,omitempty"`
	Epoch      int32    `json:"member_epoch,omitempty"`
	Assigned   []TopicP `json:"assigned"`
}

type TopicP struct {
	Topic      string  `json:"topic"`
	Partitions []int32 `json:"partitions"`
}

type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed_offset"`
	Start     int64  `json:"log_start_offset"`
	End       int64  `json:"end_offset"`
	Lag       int64  `json:"lag"`
	MemberID  string `json:"member_id,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	Host      string `json:"client_host,omitempty"`
}

type Detail struct {
	Group
	Epoch    int32          `json:"epoch,omitempty"`
	Assignor string         `json:"assignor,omitempty"`
	Protocol string         `json:"protocol,omitempty"`
	Members_ []Member       `json:"member_details"`
	Offsets  []PartitionLag `json:"offsets"`
}

func (d Detail) TotalLag() int64 {
	var n int64
	for _, o := range d.Offsets {
		n += max(o.Lag, 0)
	}
	return n
}

func lookupType(ctx context.Context, cl *kgo.Client, name string) (listed, error) {
	raw, err := listRaw(ctx, cl, nil, nil)
	if err != nil {
		return listed{}, err
	}
	for _, l := range raw {
		if l.name == name {
			return l, nil
		}
	}
	return listed{}, fmt.Errorf("group %q: %w", name, kerr.GroupIDNotFound)
}

func Describe(ctx context.Context, cl *kgo.Client, adm *kadm.Client, name string) (Detail, error) {
	l, err := lookupType(ctx, cl, name)
	if err != nil {
		return Detail{}, err
	}
	d := Detail{Group: Group{Name: name, Type: l.typ, ProtocolType: l.protocolType, State: l.state, Coordinator: l.coordinator, Lag: -1}}
	assigned := map[string]map[int32]*Member{}
	addAssign := func(m *Member, set kadm.TopicsSet) {
		for _, tp := range set.Sorted() {
			m.Assigned = append(m.Assigned, TopicP{tp.Topic, tp.Partitions})
			for _, p := range tp.Partitions {
				if assigned[tp.Topic] == nil {
					assigned[tp.Topic] = map[int32]*Member{}
				}
				assigned[tp.Topic][p] = m
			}
		}
	}
	switch l.typ {
	case "consumer":
		described, err := adm.DescribeConsumerGroups(ctx, name)
		if err != nil {
			return Detail{}, err
		}
		g := described[name]
		if g.Err != nil {
			return Detail{}, g.Err
		}
		d.State, d.Epoch, d.Assignor = g.State, g.Epoch, g.AssignorName
		d.Members_ = make([]Member, len(g.Members))
		for i, m := range g.Members {
			d.Members_[i] = Member{ID: m.MemberID, InstanceID: deref(m.InstanceID), ClientID: m.ClientID, Host: m.ClientHost, Rack: deref(m.RackID), Epoch: m.MemberEpoch}
			addAssign(&d.Members_[i], m.Assignment)
		}
	case "share":
		described, err := adm.DescribeShareGroups(ctx, name)
		if err != nil {
			return Detail{}, err
		}
		g := described[name]
		if g.Err != nil {
			return Detail{}, g.Err
		}
		d.State, d.Epoch, d.Assignor = g.GroupState, g.GroupEpoch, g.Assignor
		d.Members_ = make([]Member, len(g.Members))
		for i, m := range g.Members {
			d.Members_[i] = Member{ID: m.MemberID, ClientID: m.ClientID, Host: m.ClientHost, Rack: deref(m.RackID), Epoch: m.MemberEpoch}
			addAssign(&d.Members_[i], m.Assignment)
		}
	default:
		described, err := adm.DescribeGroups(ctx, name)
		if err != nil {
			return Detail{}, err
		}
		g := described[name]
		if g.Err != nil {
			return Detail{}, g.Err
		}
		d.State, d.Protocol = g.State, g.Protocol
		d.Members_ = make([]Member, len(g.Members))
		for i, m := range g.Members {
			d.Members_[i] = Member{ID: m.MemberID, InstanceID: deref(m.InstanceID), ClientID: m.ClientID, Host: m.ClientHost}
			var set kadm.TopicsSet
			if a, ok := m.Assigned.AsConsumer(); ok {
				for _, t := range a.Topics {
					set.Add(t.Topic, t.Partitions...)
				}
			}
			addAssign(&d.Members_[i], set)
		}
	}
	d.Members = len(d.Members_)

	if l.typ != "share" {
		committed, err := adm.FetchOffsets(ctx, name)
		if err != nil {
			return Detail{}, err
		}
		topicSet := map[string]bool{}
		committed.Each(func(o kadm.OffsetResponse) { topicSet[o.Topic] = true })
		for t := range assigned {
			topicSet[t] = true
		}
		var topicNames []string
		for t := range topicSet {
			topicNames = append(topicNames, t)
		}
		if len(topicNames) > 0 {
			starts, err := adm.ListStartOffsets(ctx, topicNames...)
			if err != nil {
				return Detail{}, err
			}
			ends, err := adm.ListEndOffsets(ctx, topicNames...)
			if err != nil {
				return Detail{}, err
			}
			seen := map[string]map[int32]bool{}
			add := func(t string, p int32) {
				if seen[t] == nil {
					seen[t] = map[int32]bool{}
				}
				if seen[t][p] {
					return
				}
				seen[t][p] = true
				pl := PartitionLag{Topic: t, Partition: p, Committed: -1, Start: -1, End: -1, Lag: -1}
				if o, ok := committed.Lookup(t, p); ok && o.Err == nil {
					pl.Committed = o.At
				}
				if s, ok := starts.Lookup(t, p); ok && s.Err == nil {
					pl.Start = s.Offset
				}
				if e, ok := ends.Lookup(t, p); ok && e.Err == nil {
					pl.End = e.Offset
				}
				switch {
				case pl.Committed >= 0 && pl.End >= 0:
					pl.Lag = max(pl.End-pl.Committed, 0)
				case pl.End >= 0 && pl.Start >= 0:
					pl.Lag = pl.End - pl.Start
				}
				if m := assigned[t][p]; m != nil {
					pl.MemberID, pl.ClientID, pl.Host = m.ID, m.ClientID, m.Host
				}
				d.Offsets = append(d.Offsets, pl)
			}
			committed.Each(func(o kadm.OffsetResponse) { add(o.Topic, o.Partition) })
			for t, parts := range assigned {
				for p := range parts {
					add(t, p)
				}
			}
			slices.SortFunc(d.Offsets, func(a, b PartitionLag) int {
				return cmp.Or(strings.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
			})
		}
		for _, o := range d.Offsets {
			if !slices.Contains(d.Topics, o.Topic) {
				d.Topics = append(d.Topics, o.Topic)
			}
		}
		d.Lag = d.TotalLag()
	}
	return d, nil
}

func Names(ctx context.Context, cl *kgo.Client) ([]string, error) {
	raw, err := listRaw(ctx, cl, nil, nil)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, l := range raw {
		out = append(out, l.name)
	}
	slices.Sort(out)
	return out, nil
}

// ForTopic returns the groups with committed offsets on topic, with their lag on it.
func ForTopic(ctx context.Context, cl *kgo.Client, adm *kadm.Client, topic string) (map[string]int64, error) {
	gs, err := List(ctx, cl, adm, ListOptions{Topic: topic})
	if err != nil {
		return nil, err
	}
	out := map[string]int64{}
	if len(gs) == 0 {
		return out, nil
	}
	ends, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, g := range gs {
		names = append(names, g.Name)
	}
	for n, r := range adm.FetchManyOffsets(ctx, names...) {
		var lag int64
		for p, o := range r.Fetched[topic] {
			if e, ok := ends.Lookup(topic, p); ok && e.Err == nil && o.Err == nil && o.At >= 0 {
				lag += max(e.Offset-o.At, 0)
			}
		}
		out[n] = lag
	}
	return out, nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
