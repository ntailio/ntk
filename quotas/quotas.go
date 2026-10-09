// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package quotas

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/units"
)

var aliases = map[string]string{
	"produce": "producer_byte_rate", "consume": "consumer_byte_rate", "request": "request_percentage",
	"mutations": "controller_mutation_rate", "connections": "connection_creation_rate",
}

var Keys = []string{"producer_byte_rate", "consumer_byte_rate", "request_percentage", "controller_mutation_rate", "connection_creation_rate"}

func Key(k string) (string, error) {
	if full, ok := aliases[k]; ok {
		return full, nil
	}
	if slices.Contains(Keys, k) {
		return k, nil
	}
	return "", fmt.Errorf("unknown quota %q (use produce, consume, request, mutations, connections, or the full key)", k)
}

func isRate(k string) bool { return strings.HasSuffix(k, "_byte_rate") }

func ParseValue(key, v string) (float64, error) {
	if isRate(key) {
		n, err := units.ParseRate(v)
		return float64(n), err
	}
	f, err := strconv.ParseFloat(strings.TrimSuffix(v, "/s"), 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a number", key, v)
	}
	return f, nil
}

func Format(key string, v float64) string {
	if isRate(key) {
		return units.Bytes(int64(v)) + "/s"
	}
	if key == "connection_creation_rate" {
		return strconv.FormatFloat(v, 'f', -1, 64) + "/s"
	}
	return strconv.FormatFloat(v, 'f', -1, 64)
}

type Component struct {
	Type string  `json:"type"`
	Name *string `json:"name"`
}

func (c Component) String() string {
	if c.Name == nil {
		return c.Type + "=<default>"
	}
	return c.Type + "=" + *c.Name
}

type Entity []Component

func (e Entity) String() string {
	var parts []string
	for _, c := range e {
		parts = append(parts, c.String())
	}
	return strings.Join(parts, ", ")
}

func (e Entity) IsDefault() bool {
	for _, c := range e {
		if c.Name == nil {
			return true
		}
	}
	return false
}

type Quota struct {
	Entity Entity             `json:"entity"`
	Values map[string]float64 `json:"values"`
}

func (q Quota) ValueString() string {
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(q.Values)) {
		parts = append(parts, k+"="+Format(k, q.Values[k]))
	}
	return strings.Join(parts, ",")
}

// Filter selects entities; a nil name with the *Default flag matches the default entity.
type Filter struct {
	User, ClientID, IP                *string
	UserDefault, ClientDefault, IPDef bool
}

func (f Filter) components() []kadm.DescribeClientQuotaComponent {
	var out []kadm.DescribeClientQuotaComponent
	add := func(typ string, name *string, def bool) {
		switch {
		case def:
			out = append(out, kadm.DescribeClientQuotaComponent{Type: typ, MatchType: 1})
		case name != nil:
			out = append(out, kadm.DescribeClientQuotaComponent{Type: typ, MatchName: name, MatchType: 0})
		}
	}
	add("user", f.User, f.UserDefault)
	add("client-id", f.ClientID, f.ClientDefault)
	add("ip", f.IP, f.IPDef)
	return out
}

func List(ctx context.Context, adm *kadm.Client, f Filter) ([]Quota, error) {
	comps := f.components()
	strict := len(comps) > 0
	var described kadm.DescribedClientQuotas
	var err error
	if !strict {
		for _, typ := range []string{"user", "client-id", "ip"} {
			d, err := adm.DescribeClientQuotas(ctx, false, []kadm.DescribeClientQuotaComponent{{Type: typ, MatchType: 2}})
			if err != nil {
				return nil, err
			}
			described = append(described, d...)
		}
	} else if described, err = adm.DescribeClientQuotas(ctx, false, comps); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var out []Quota
	for _, d := range described {
		q := Quota{Values: map[string]float64{}}
		for _, c := range d.Entity {
			q.Entity = append(q.Entity, Component{Type: c.Type, Name: c.Name})
		}
		slices.SortFunc(q.Entity, func(a, b Component) int { return cmp.Compare(order(a.Type), order(b.Type)) })
		if seen[q.Entity.String()] {
			continue
		}
		seen[q.Entity.String()] = true
		for _, v := range d.Values {
			q.Values[v.Key] = v.Value
		}
		out = append(out, q)
	}
	slices.SortFunc(out, func(a, b Quota) int { return strings.Compare(a.Entity.String(), b.Entity.String()) })
	return out, nil
}

func order(t string) int {
	switch t {
	case "user":
		return 0
	case "client-id":
		return 1
	}
	return 2
}

const KindAlter = "quota.alter"

type Op struct {
	Key    string  `json:"key"`
	Value  float64 `json:"value"`
	Remove bool    `json:"remove,omitempty"`
}

type AlterSpec struct {
	Entity Entity `json:"entity"`
	Ops    []Op   `json:"ops"`
}

func PlanAlter(ctx context.Context, adm *kadm.Client, e Entity, ops []Op) (*plan.Plan, error) {
	if len(e) == 0 {
		return nil, errors.New("no entity: use --user, --client-id, --ip, or their --*-default forms")
	}
	f := Filter{}
	for _, c := range e {
		switch c.Type {
		case "user":
			f.User, f.UserDefault = c.Name, c.Name == nil
		case "client-id":
			f.ClientID, f.ClientDefault = c.Name, c.Name == nil
		case "ip":
			f.IP, f.IPDef = c.Name, c.Name == nil
		}
	}
	cur := map[string]float64{}
	if qs, err := List(ctx, adm, f); err == nil {
		for _, q := range qs {
			if q.Entity.String() == e.String() {
				cur = q.Values
			}
		}
	}
	pl, err := plan.New(KindAlter, plan.Change, fmt.Sprintf("Change quotas of %s:", e), AlterSpec{e, ops})
	if err != nil {
		return nil, err
	}
	var effective []Op
	for _, o := range ops {
		old, had := cur[o.Key]
		oldS := "-"
		if had {
			oldS = Format(o.Key, old)
		}
		switch {
		case o.Remove && !had:
			continue
		case o.Remove:
			pl.Add("%-26s %s → (none)", o.Key, oldS)
		case had && old == o.Value:
			continue
		default:
			pl.Add("%-26s %s → %s", o.Key, oldS, Format(o.Key, o.Value))
			if isRate(o.Key) && o.Value < 1024 {
				pl.Warn("%s=%s is very low; check the unit", o.Key, Format(o.Key, o.Value))
				pl.Class, pl.Confirm = plan.Destructive, e.String()
			}
		}
		effective = append(effective, o)
	}
	if e.IsDefault() && len(effective) > 0 {
		pl.Warn("a <default> entity applies to every user/client without a more specific quota")
	}
	np, err := plan.New(pl.Kind, pl.Class, pl.Summary, AlterSpec{e, effective})
	if err != nil {
		return nil, err
	}
	np.Changes, np.Warnings, np.Confirm = pl.Changes, pl.Warnings, pl.Confirm
	if err := alter(ctx, adm, AlterSpec{e, effective}, true); err != nil && len(effective) > 0 {
		return nil, fmt.Errorf("broker rejected the change: %w", err)
	}
	return np, nil
}

func alter(ctx context.Context, adm *kadm.Client, spec AlterSpec, validate bool) error {
	var entity kadm.ClientQuotaEntity
	for _, c := range spec.Entity {
		entity = append(entity, kadm.ClientQuotaEntityComponent{Type: c.Type, Name: c.Name})
	}
	var ops []kadm.AlterClientQuotaOp
	for _, o := range spec.Ops {
		ops = append(ops, kadm.AlterClientQuotaOp{Key: o.Key, Value: o.Value, Remove: o.Remove})
	}
	if len(ops) == 0 {
		return nil
	}
	entries := []kadm.AlterClientQuotaEntry{{Entity: entity, Ops: ops}}
	var resp kadm.AlteredClientQuotas
	var err error
	if validate {
		resp, err = adm.ValidateAlterClientQuotas(ctx, entries)
	} else {
		resp, err = adm.AlterClientQuotas(ctx, entries)
	}
	if err != nil {
		return err
	}
	for _, r := range resp {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("%w: %s", r.Err, r.ErrMessage)
			}
			return r.Err
		}
	}
	return nil
}

func ApplyAlter(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec AlterSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	return alter(ctx, adm, spec, false)
}

type Effective struct {
	Key   string  `json:"key"`
	Value float64 `json:"value"`
	From  string  `json:"from"`
}

// Resolve applies Kafka's quota precedence for a user and client id.
func Resolve(ctx context.Context, adm *kadm.Client, user, clientID string) ([]Effective, error) {
	all, err := List(ctx, adm, Filter{})
	if err != nil {
		return nil, err
	}
	byEntity := map[string]map[string]float64{}
	for _, q := range all {
		byEntity[q.Entity.String()] = q.Values
	}
	str := func(s string) *string { return &s }
	var chain []Entity
	u := func(n *string) Component { return Component{"user", n} }
	c := func(n *string) Component { return Component{"client-id", n} }
	if user != "" && clientID != "" {
		chain = append(chain, Entity{u(str(user)), c(str(clientID))})
	}
	if user != "" {
		chain = append(chain, Entity{u(str(user)), c(nil)}, Entity{u(str(user))})
	}
	if clientID != "" {
		chain = append(chain, Entity{u(nil), c(str(clientID))})
	}
	chain = append(chain, Entity{u(nil), c(nil)}, Entity{u(nil)})
	if clientID != "" {
		chain = append(chain, Entity{c(str(clientID))})
	}
	chain = append(chain, Entity{c(nil)})

	var out []Effective
	for _, key := range Keys[:4] {
		for _, e := range chain {
			if v, ok := byEntity[e.String()][key]; ok {
				out = append(out, Effective{key, v, e.String()})
				break
			}
		}
	}
	return out, nil
}
