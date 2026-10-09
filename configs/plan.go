// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/plan"
)

const KindAlter = "config.alter"

type AlterSpec struct {
	Kind Kind   `json:"resource_type"`
	Name string `json:"resource_name"`
	Ops  []Op   `json:"ops"`
}

func Label(kind Kind, name string) string {
	switch {
	case kind == Topic:
		return fmt.Sprintf("topic %q", name)
	case name == "":
		return "cluster-wide default broker config"
	}
	return "broker " + name
}

func PlanAlter(ctx context.Context, cl *kgo.Client, adm *kadm.Client, kind Kind, name string, ops []Op) (*plan.Plan, error) {
	entries, err := Describe(ctx, cl, kind, name)
	if err != nil {
		return nil, err
	}
	if kind == Broker && name == "" {
		if entries, err = withBrokerKeys(ctx, cl, entries); err != nil {
			return nil, err
		}
	}
	resolved, err := Resolve(ops, entries)
	if err != nil {
		return nil, err
	}

	spec := AlterSpec{Kind: kind, Name: name}
	pl, err := plan.New(KindAlter, plan.Change, "Change config of "+Label(kind, name)+":", nil)
	if err != nil {
		return nil, err
	}
	var effective []kadm.AlterConfig
	for i, o := range resolved {
		cur, _ := Find(entries, o.Name)
		switch o.Op {
		case kadm.DeleteConfig:
			if !cur.Overridden(kind) {
				continue
			}
			def := cur.Default()
			if def == "" {
				def = "default"
			}
			pl.Add("%-32s %s → (%s)", o.Name, cur.Human(), def)
		case kadm.SetConfig:
			if cur.Overridden(kind) && cur.String() == *o.Value {
				continue
			}
			pl.Add("%-32s %s → %s", o.Name, orDash(cur.Human()), Humanize(o.Name, *o.Value))
		default:
			sign := "+"
			if o.Op == kadm.SubtractConfig {
				sign = "-"
			}
			pl.Add("%-32s %s %s= %s", o.Name, orDash(cur.Human()), sign, *o.Value)
		}
		effective = append(effective, o)
		op := ops[i]
		if o.Value != nil {
			op.Value = *o.Value
		}
		spec.Ops = append(spec.Ops, op)
	}
	if kind == Topic {
		if err := checkMinISR(ctx, adm, name, effective); err != nil {
			return nil, err
		}
	}
	if kind == Broker && name == "" && len(effective) > 0 {
		pl.Warn("affects all brokers")
	}
	for _, w := range Warnings(effective, entries) {
		pl.Warn("%s", w)
	}
	if len(effective) > 0 {
		if err := Alter(ctx, adm, kind, name, effective, true); err != nil {
			return nil, fmt.Errorf("broker rejected the change: %w", err)
		}
	}
	np, err := plan.New(pl.Kind, pl.Class, pl.Summary, spec)
	if err != nil {
		return nil, err
	}
	np.Changes, np.Warnings = pl.Changes, pl.Warnings
	return np, nil
}

// withBrokerKeys adds every broker key (from one broker) to the cluster-default
// entries, which only list keys that already have a cluster-wide value.
func withBrokerKeys(ctx context.Context, cl *kgo.Client, defaults []Entry) ([]Entry, error) {
	md, err := kadm.NewClient(cl).BrokerMetadata(ctx)
	if err != nil || len(md.Brokers) == 0 {
		return defaults, err
	}
	all, err := Describe(ctx, cl, Broker, strconv.Itoa(int(md.Brokers[0].NodeID)))
	if err != nil {
		return nil, err
	}
	out := slices.Clone(defaults)
	for _, e := range all {
		if _, ok := Find(defaults, e.Key); !ok {
			e.Value, e.Source = nil, "default"
			out = append(out, e)
		}
	}
	return out, nil
}

func checkMinISR(ctx context.Context, adm *kadm.Client, topic string, ops []kadm.AlterConfig) error {
	for _, o := range ops {
		if o.Name != "min.insync.replicas" || o.Op != kadm.SetConfig {
			continue
		}
		n, _ := strconv.Atoi(*o.Value)
		td, err := adm.ListTopics(ctx, topic)
		if err != nil {
			return err
		}
		rf := 0
		for _, p := range td[topic].Partitions {
			rf = max(rf, len(p.Replicas))
		}
		if rf > 0 && n > rf {
			return fmt.Errorf("min.insync.replicas=%d is larger than the replication factor (%d): producers with acks=all would always fail", n, rf)
		}
	}
	return nil
}

func ApplyAlter(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec AlterSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	var ops []kadm.AlterConfig
	for _, o := range spec.Ops {
		switch o.Op {
		case "delete":
			ops = append(ops, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: o.Key})
		case "append":
			ops = append(ops, kadm.AlterConfig{Op: kadm.AppendConfig, Name: o.Key, Value: kadm.StringPtr(o.Value)})
		case "subtract":
			ops = append(ops, kadm.AlterConfig{Op: kadm.SubtractConfig, Name: o.Key, Value: kadm.StringPtr(o.Value)})
		default:
			ops = append(ops, kadm.AlterConfig{Op: kadm.SetConfig, Name: o.Key, Value: kadm.StringPtr(o.Value)})
		}
	}
	return Alter(ctx, adm, spec.Kind, spec.Name, ops, false)
}

// FileOps turns a desired {key: value} file into ops against the current overrides.
func FileOps(desired map[string]string, entries []Entry, kind Kind, prune bool) []Op {
	var ops []Op
	for _, k := range slices.Sorted(maps.Keys(desired)) {
		ops = append(ops, Op{Key: k, Op: "set", Value: desired[k]})
	}
	if prune {
		for _, e := range entries {
			if _, keep := desired[e.Key]; !keep && e.Overridden(kind) {
				ops = append(ops, Op{Key: e.Key, Op: "delete"})
			}
		}
	}
	return ops
}

func Overrides(entries []Entry, kind Kind) map[string]string {
	out := map[string]string{}
	for _, e := range entries {
		if e.Overridden(kind) && !e.Sensitive {
			out[e.Key] = e.String()
		}
	}
	return out
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
