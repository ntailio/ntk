// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/units"
)

const (
	KindReassign       = "topic.reassign"
	KindCancelReassign = "topic.reassign-cancel"
	KindElect          = "topic.elect-leaders"
)

// Assignment uses the kafka-reassign-partitions.sh JSON format, so files work with both tools.
type Assignment struct {
	Version    int                   `json:"version"`
	Partitions []AssignmentPartition `json:"partitions"`
}

type AssignmentPartition struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Replicas  []int32 `json:"replicas"`
}

func Generate(ctx context.Context, adm *kadm.Client, topicNames []string, brokers []int32, rackAware bool) (Assignment, error) {
	md, err := adm.Metadata(ctx, topicNames...)
	if err != nil {
		return Assignment{}, err
	}
	racks := map[int32]string{}
	for _, b := range md.Brokers {
		if b.Rack != nil {
			racks[b.NodeID] = *b.Rack
		}
	}
	for _, b := range brokers {
		if !slices.ContainsFunc(md.Brokers, func(d kadm.BrokerDetail) bool { return d.NodeID == b }) {
			return Assignment{}, fmt.Errorf("broker %d is not in the cluster", b)
		}
	}
	order := slices.Clone(brokers)
	slices.Sort(order)
	if rackAware {
		order = rackAlternated(order, racks)
	}

	a := Assignment{Version: 1}
	for ti, t := range topicNames {
		td, ok := md.Topics[t]
		if !ok || td.Err != nil {
			return Assignment{}, fmt.Errorf("topic %q: %w", t, kerr.UnknownTopicOrPartition)
		}
		parts := td.Partitions.Sorted()
		for _, p := range parts {
			rf := len(p.Replicas)
			if rf > len(order) {
				return Assignment{}, fmt.Errorf("topic %q has replication factor %d but only %d brokers were given", t, rf, len(order))
			}
			start := (int(p.Partition) + ti) % len(order)
			replicas := make([]int32, rf)
			for i := range rf {
				replicas[i] = order[(start+i)%len(order)]
			}
			a.Partitions = append(a.Partitions, AssignmentPartition{Topic: t, Partition: p.Partition, Replicas: replicas})
		}
	}
	return a, nil
}

func rackAlternated(brokers []int32, racks map[int32]string) []int32 {
	byRack := map[string][]int32{}
	var rackNames []string
	for _, b := range brokers {
		r := racks[b]
		if _, ok := byRack[r]; !ok {
			rackNames = append(rackNames, r)
		}
		byRack[r] = append(byRack[r], b)
	}
	slices.Sort(rackNames)
	var out []int32
	for i := 0; len(out) < len(brokers); i++ {
		for _, r := range rackNames {
			if i < len(byRack[r]) {
				out = append(out, byRack[r][i])
			}
		}
	}
	return out
}

type ReassignSpec struct {
	Assignment Assignment `json:"assignment"`
	Throttle   int64      `json:"throttle_bytes_per_sec,omitempty"`
}

func PlanReassign(ctx context.Context, adm *kadm.Client, a Assignment, throttle int64) (*plan.Plan, error) {
	var topicNames []string
	for _, p := range a.Partitions {
		if !slices.Contains(topicNames, p.Topic) {
			topicNames = append(topicNames, p.Topic)
		}
	}
	md, err := adm.Metadata(ctx, topicNames...)
	if err != nil {
		return nil, err
	}
	sizes, err := PartitionSizes(ctx, adm, topicNames...)
	if err != nil {
		return nil, err
	}
	spec := ReassignSpec{Assignment: Assignment{Version: 1}, Throttle: throttle}
	pl, err := plan.New(KindReassign, plan.Change, "", nil)
	if err != nil {
		return nil, err
	}
	var moved int64
	for _, p := range a.Partitions {
		td, ok := md.Topics[p.Topic]
		if !ok || td.Err != nil {
			return nil, fmt.Errorf("topic %q: %w", p.Topic, kerr.UnknownTopicOrPartition)
		}
		cur, ok := td.Partitions[p.Partition]
		if !ok {
			return nil, fmt.Errorf("%s/%d does not exist", p.Topic, p.Partition)
		}
		if slices.Equal(cur.Replicas, p.Replicas) {
			continue
		}
		spec.Assignment.Partitions = append(spec.Assignment.Partitions, p)
		added := 0
		for _, r := range p.Replicas {
			if !slices.Contains(cur.Replicas, r) {
				added++
			}
		}
		moved += int64(added) * sizes[p.Topic][p.Partition]
		pl.Add("%s/%d: %s → %s", p.Topic, p.Partition, joinIDs(cur.Replicas), joinIDs(p.Replicas))
	}
	pl.Summary = fmt.Sprintf("Reassign %s (estimated data movement %s):", plural(len(spec.Assignment.Partitions), "partition"), units.Bytes(moved))
	if throttle > 0 {
		pl.Warn("replication throttled to %s/s until `ntk topic reassign status` reports completion", units.Bytes(throttle))
	} else if moved > 10<<30 {
		pl.Warn("moving %s without --throttle can saturate broker networks", units.Bytes(moved))
	}
	return rebuild(pl, spec)
}

func ApplyReassign(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec ReassignSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	if spec.Throttle > 0 {
		if err := setThrottle(ctx, adm, spec); err != nil {
			return fmt.Errorf("setting replication throttle: %w", err)
		}
	}
	var req kadm.AlterPartitionAssignmentsReq
	for _, p := range spec.Assignment.Partitions {
		req.Assign(p.Topic, p.Partition, p.Replicas)
	}
	resp, err := adm.AlterPartitionAssignments(ctx, req)
	if err != nil {
		return err
	}
	return assignmentErrs(resp)
}

func assignmentErrs(resp kadm.AlterPartitionAssignmentsResponses) error {
	var errs []error
	for t, parts := range resp {
		for p, r := range parts {
			if r.Err != nil {
				errs = append(errs, fmt.Errorf("%s/%d: %w", t, p, wrapMsg(r.Err, r.ErrMessage)))
			}
		}
	}
	return errors.Join(errs...)
}

var throttleBrokerKeys = []string{"leader.replication.throttled.rate", "follower.replication.throttled.rate"}
var throttleTopicKeys = []string{"leader.replication.throttled.replicas", "follower.replication.throttled.replicas"}

func setThrottle(ctx context.Context, adm *kadm.Client, spec ReassignSpec) error {
	rate := strconv.FormatInt(spec.Throttle, 10)
	var ops []kadm.AlterConfig
	for _, k := range throttleBrokerKeys {
		ops = append(ops, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr(rate)})
	}
	md, err := adm.BrokerMetadata(ctx)
	if err != nil {
		return err
	}
	for _, b := range md.Brokers {
		resp, err := adm.AlterBrokerConfigs(ctx, ops, b.NodeID)
		if err != nil {
			return err
		}
		for _, r := range resp {
			if r.Err != nil {
				return fmt.Errorf("broker %s: %w", r.Name, r.Err)
			}
		}
	}
	topicOps := []kadm.AlterConfig{}
	for _, k := range throttleTopicKeys {
		topicOps = append(topicOps, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: kadm.StringPtr("*")})
	}
	var names []string
	for _, p := range spec.Assignment.Partitions {
		if !slices.Contains(names, p.Topic) {
			names = append(names, p.Topic)
		}
	}
	resp, err := adm.AlterTopicConfigs(ctx, topicOps, names...)
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			return fmt.Errorf("topic %s: %w", r.Name, r.Err)
		}
	}
	return nil
}

type Reassignment struct {
	Topic     string  `json:"topic"`
	Partition int32   `json:"partition"`
	Replicas  []int32 `json:"replicas"`
	Adding    []int32 `json:"adding_replicas"`
	Removing  []int32 `json:"removing_replicas"`
}

func InProgress(ctx context.Context, adm *kadm.Client, topicNames []string) ([]Reassignment, error) {
	var set kadm.TopicsSet
	if len(topicNames) > 0 {
		md, err := adm.Metadata(ctx, topicNames...)
		if err != nil {
			return nil, err
		}
		for _, t := range topicNames {
			for p := range md.Topics[t].Partitions {
				set.Add(t, p)
			}
		}
	}
	resp, err := adm.ListPartitionReassignments(ctx, set)
	if err != nil {
		return nil, err
	}
	var out []Reassignment
	for t, parts := range resp {
		for p, r := range parts {
			out = append(out, Reassignment{Topic: t, Partition: p, Replicas: r.Replicas, Adding: r.AddingReplicas, Removing: r.RemovingReplicas})
		}
	}
	slices.SortFunc(out, func(a, b Reassignment) int {
		return cmp.Or(strings.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition))
	})
	return out, nil
}

// ClearThrottles removes replication throttles once no reassignment is running.
// It returns the topics it cleared.
func ClearThrottles(ctx context.Context, adm *kadm.Client) ([]string, error) {
	running, err := InProgress(ctx, adm, nil)
	if err != nil || len(running) > 0 {
		return nil, err
	}
	rcs, err := adm.DescribeTopicConfigs(ctx)
	if err != nil {
		return nil, err
	}
	var throttled []string
	for _, rc := range rcs {
		for _, c := range rc.Configs {
			if slices.Contains(throttleTopicKeys, c.Key) && c.Source == kmsg.ConfigSourceDynamicTopicConfig {
				throttled = append(throttled, rc.Name)
				break
			}
		}
	}
	if len(throttled) == 0 {
		return nil, nil
	}
	var del []kadm.AlterConfig
	for _, k := range throttleTopicKeys {
		del = append(del, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
	}
	if _, err := adm.AlterTopicConfigs(ctx, del, throttled...); err != nil {
		return nil, err
	}
	var bdel []kadm.AlterConfig
	for _, k := range throttleBrokerKeys {
		bdel = append(bdel, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
	}
	md, err := adm.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}
	for _, b := range md.Brokers {
		if _, err := adm.AlterBrokerConfigs(ctx, bdel, b.NodeID); err != nil {
			return nil, err
		}
	}
	return throttled, nil
}

type CancelSpec struct {
	Partitions []AssignmentPartition `json:"partitions"`
}

func PlanCancel(ctx context.Context, adm *kadm.Client, topicNames []string) (*plan.Plan, error) {
	running, err := InProgress(ctx, adm, topicNames)
	if err != nil {
		return nil, err
	}
	var spec CancelSpec
	pl, err := plan.New(KindCancelReassign, plan.Change, fmt.Sprintf("Cancel %s:", plural(len(running), "reassignment")), nil)
	if err != nil {
		return nil, err
	}
	for _, r := range running {
		spec.Partitions = append(spec.Partitions, AssignmentPartition{Topic: r.Topic, Partition: r.Partition})
		pl.Add("%s/%d (adding %s, removing %s)", r.Topic, r.Partition, joinIDs(r.Adding), joinIDs(r.Removing))
	}
	return rebuild(pl, spec)
}

func ApplyCancel(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec CancelSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	var req kadm.AlterPartitionAssignmentsReq
	for _, p := range spec.Partitions {
		req.CancelAssign(p.Topic, p.Partition)
	}
	resp, err := adm.AlterPartitionAssignments(ctx, req)
	if err != nil {
		return err
	}
	return assignmentErrs(resp)
}

type ElectSpec struct {
	Unclean    bool                  `json:"unclean"`
	Partitions []AssignmentPartition `json:"partitions"`
}

func PlanElect(ctx context.Context, adm *kadm.Client, topicNames []string, partitions []int32, unclean bool) (*plan.Plan, error) {
	md, err := adm.Metadata(ctx, topicNames...)
	if err != nil {
		return nil, err
	}
	class, what := plan.Change, "preferred"
	if unclean {
		class, what = plan.Destructive, "unclean"
	}
	spec := ElectSpec{Unclean: unclean}
	pl, err := plan.New(KindElect, class, "", nil)
	if err != nil {
		return nil, err
	}
	for _, t := range md.Topics.Sorted() {
		if t.Err != nil {
			if len(topicNames) > 0 {
				return nil, fmt.Errorf("topic %q: %w", t.Topic, t.Err)
			}
			continue
		}
		for _, p := range t.Partitions.Sorted() {
			if len(partitions) > 0 && !slices.Contains(partitions, p.Partition) {
				continue
			}
			need := false
			if unclean {
				need = p.Leader < 0
			} else {
				need = len(p.Replicas) > 0 && p.Leader != p.Replicas[0]
			}
			if !need {
				continue
			}
			spec.Partitions = append(spec.Partitions, AssignmentPartition{Topic: t.Topic, Partition: p.Partition})
			if unclean {
				pl.Add("%s/%d: no leader → first live replica of %s", t.Topic, p.Partition, joinIDs(p.Replicas))
			} else {
				pl.Add("%s/%d: leader %d → preferred %d", t.Topic, p.Partition, p.Leader, p.Replicas[0])
			}
		}
	}
	pl.Summary = fmt.Sprintf("Run %s leader election for %s:", what, plural(len(spec.Partitions), "partition"))
	if unclean {
		pl.Warn("unclean election can lose acknowledged data: the new leader may be missing records")
		if len(topicNames) == 1 {
			pl.Confirm = topicNames[0]
		} else {
			pl.Confirm = "unclean election"
		}
	}
	return rebuild(pl, spec)
}

func ApplyElect(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec ElectSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	var set kadm.TopicsSet
	for _, p := range spec.Partitions {
		set.Add(p.Topic, p.Partition)
	}
	how := kadm.ElectPreferredReplica
	if spec.Unclean {
		how = kadm.ElectLiveReplica
	}
	res, err := adm.ElectLeaders(ctx, how, set)
	if err != nil {
		return err
	}
	var errs []error
	for t, parts := range res {
		for p, r := range parts {
			if r.Err != nil && !errors.Is(r.Err, kerr.ElectionNotNeeded) {
				errs = append(errs, fmt.Errorf("%s/%d: %w", t, p, wrapMsg(r.Err, r.ErrMessage)))
			}
		}
	}
	return errors.Join(errs...)
}

func joinIDs(v []int32) string {
	if len(v) == 0 {
		return "-"
	}
	s := make([]string, len(v))
	for i, n := range v {
		s[i] = strconv.Itoa(int(n))
	}
	return strings.Join(s, ",")
}
