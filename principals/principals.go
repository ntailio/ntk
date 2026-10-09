// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package principals

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/acls"
	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/quotas"
)

type Cred struct {
	Mechanism  string `json:"mechanism"`
	Iterations int32  `json:"iterations"`
}

type User struct {
	Name        string `json:"name"`
	Credentials []Cred `json:"credentials"`
}

func mechName(m kadm.ScramMechanism) string {
	if m == kadm.ScramSha512 {
		return "SCRAM-SHA-512"
	}
	return "SCRAM-SHA-256"
}

func ParseMechanism(s string) (kadm.ScramMechanism, error) {
	switch strings.ToLower(s) {
	case "scram-sha-512", "sha-512", "sha512":
		return kadm.ScramSha512, nil
	case "scram-sha-256", "sha-256", "sha256":
		return kadm.ScramSha256, nil
	}
	return 0, fmt.Errorf("unknown SCRAM mechanism %q (use scram-sha-256 or scram-sha-512)", s)
}

func ListUsers(ctx context.Context, adm *kadm.Client, names ...string) ([]User, error) {
	described, err := adm.DescribeUserSCRAMs(ctx, names...)
	if err != nil {
		return nil, err
	}
	var out []User
	for _, d := range described.Sorted() {
		if d.Err != nil {
			if len(names) > 0 {
				return nil, fmt.Errorf("user %q: %w", d.User, d.Err)
			}
			continue
		}
		u := User{Name: d.User}
		for _, c := range d.CredInfos {
			u.Credentials = append(u.Credentials, Cred{mechName(c.Mechanism), c.Iterations})
		}
		out = append(out, u)
	}
	return out, nil
}

const (
	KindUpsert = "user.upsert"
	KindDelete = "user.delete"
)

type UpsertSpec struct {
	User       string `json:"user"`
	Mechanism  int8   `json:"mechanism"`
	Iterations int32  `json:"iterations"`
	Password   string `json:"password"`
}

// SecretKinds are plan kinds whose payload holds a credential; they can't be saved.
var SecretKinds = []string{KindUpsert}

func PlanUpsert(ctx context.Context, adm *kadm.Client, user string, mech kadm.ScramMechanism, iterations int32, password string, rotate bool) (*plan.Plan, error) {
	if iterations < 4096 || iterations > 16384 {
		return nil, errors.New("--iterations must be between 4096 and 16384")
	}
	existing, err := ListUsers(ctx, adm)
	if err != nil {
		return nil, err
	}
	var cur *User
	for i := range existing {
		if existing[i].Name == user {
			cur = &existing[i]
		}
	}
	has := cur != nil && slices.ContainsFunc(cur.Credentials, func(c Cred) bool { return c.Mechanism == mechName(mech) })
	switch {
	case rotate && !has:
		return nil, fmt.Errorf("user %q has no %s credential to rotate (use `ntk user create`)", user, mechName(mech))
	case !rotate && has:
		return nil, fmt.Errorf("user %q already has a %s credential (use `ntk user rotate`)", user, mechName(mech))
	}
	class, verb := plan.SafeWrite, "Create"
	if rotate {
		class, verb = plan.Change, "Rotate"
	}
	pl, err := plan.New(KindUpsert, class, verb+" SCRAM credential:", UpsertSpec{user, int8(mech), iterations, password})
	if err != nil {
		return nil, err
	}
	sign := "+"
	if rotate {
		sign = "~"
	}
	pl.Add("%s User:%s  %s  iterations=%d", sign, user, mechName(mech), iterations)
	if rotate {
		pl.Warn("clients using the old password fail when they next reconnect")
	}
	return pl, nil
}

func ApplyUpsert(ctx context.Context, adm *kadm.Client, pl *plan.Plan) error {
	var spec UpsertSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	resp, err := adm.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User: spec.User, Mechanism: kadm.ScramMechanism(spec.Mechanism), Iterations: spec.Iterations, Password: spec.Password,
	}})
	if err != nil {
		return err
	}
	return resp.Error()
}

type DeleteSpec struct {
	User       string     `json:"user"`
	Mechanisms []int8     `json:"mechanisms"`
	ACLs       []acls.ACL `json:"acls,omitempty"`
}

func PlanDelete(ctx context.Context, cl *kgo.Client, adm *kadm.Client, user string, mechs []kadm.ScramMechanism, withACLs bool) (*plan.Plan, error) {
	us, err := ListUsers(ctx, adm, user)
	if err != nil {
		return nil, err
	}
	if len(us) == 0 {
		return nil, fmt.Errorf("user %q has no SCRAM credentials", user)
	}
	spec := DeleteSpec{User: user}
	pl, err := plan.New(KindDelete, plan.Destructive, fmt.Sprintf("Delete SCRAM credentials of User:%s:", user), nil)
	if err != nil {
		return nil, err
	}
	for _, c := range us[0].Credentials {
		m, _ := ParseMechanism(c.Mechanism)
		if len(mechs) > 0 && !slices.Contains(mechs, m) {
			continue
		}
		spec.Mechanisms = append(spec.Mechanisms, int8(m))
		pl.Add("- %s (%d iterations)", c.Mechanism, c.Iterations)
	}
	own, err := acls.List(ctx, cl, acls.Filter{Principal: "User:" + user})
	if err != nil {
		return nil, err
	}
	if withACLs {
		spec.ACLs = own
		for _, a := range own {
			pl.Add("- ACL %s", a)
		}
	} else if len(own) > 0 {
		pl.Warn("User:%s still has %d ACL(s); add --with-acls to delete them too", user, len(own))
	}
	pl.Confirm = user
	np, err := plan.New(pl.Kind, pl.Class, pl.Summary, spec)
	if err != nil {
		return nil, err
	}
	np.Changes, np.Warnings, np.Confirm = pl.Changes, pl.Warnings, pl.Confirm
	return np, nil
}

func ApplyDelete(ctx context.Context, cl *kgo.Client, adm *kadm.Client, pl *plan.Plan) error {
	var spec DeleteSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	var del []kadm.DeleteSCRAM
	for _, m := range spec.Mechanisms {
		del = append(del, kadm.DeleteSCRAM{User: spec.User, Mechanism: kadm.ScramMechanism(m)})
	}
	for _, d := range del {
		resp, err := adm.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{d}, nil)
		if err != nil {
			return err
		}
		if err := resp.Error(); err != nil && !errors.Is(err, kerr.ResourceNotFound) {
			return err
		}
	}
	if len(spec.ACLs) > 0 {
		return acls.Delete(ctx, cl, spec.ACLs)
	}
	return nil
}

type Principal struct {
	Name   string   `json:"principal"`
	SCRAM  []string `json:"scram,omitempty"`
	ACLs   int      `json:"acls"`
	Quotas []string `json:"quotas,omitempty"`
}

func List(ctx context.Context, cl *kgo.Client, adm *kadm.Client, sources []string) ([]Principal, error) {
	want := func(s string) bool { return len(sources) == 0 || slices.Contains(sources, s) }
	byName := map[string]*Principal{}
	get := func(n string) *Principal {
		if byName[n] == nil {
			byName[n] = &Principal{Name: n}
		}
		return byName[n]
	}
	if want("acl") {
		all, err := acls.List(ctx, cl, acls.Filter{})
		if err != nil {
			return nil, err
		}
		for _, a := range all {
			get(a.Principal).ACLs++
		}
	}
	if want("scram") {
		users, err := ListUsers(ctx, adm)
		if err != nil {
			return nil, err
		}
		for _, u := range users {
			p := get("User:" + u.Name)
			for _, c := range u.Credentials {
				p.SCRAM = append(p.SCRAM, strings.ToLower(strings.TrimPrefix(c.Mechanism, "SCRAM-")))
			}
		}
	}
	if want("quota") {
		qs, err := quotas.List(ctx, adm, quotas.Filter{})
		if err != nil {
			return nil, err
		}
		for _, q := range qs {
			for _, c := range q.Entity {
				if c.Type == "user" && c.Name != nil {
					p := get("User:" + *c.Name)
					p.Quotas = append(p.Quotas, q.ValueString())
				}
			}
		}
	}
	var out []Principal
	for _, p := range byName {
		out = append(out, *p)
	}
	slices.SortFunc(out, func(a, b Principal) int { return strings.Compare(a.Name, b.Name) })
	return out, nil
}

type Detail struct {
	Principal string             `json:"principal"`
	SCRAM     []Cred             `json:"scram"`
	Quotas    []quotas.Quota     `json:"quotas"`
	Effective []quotas.Effective `json:"effective_quotas"`
	ACLs      []acls.ACL         `json:"acls"`
	Access    []string           `json:"effective_access"`
	Groups    []string           `json:"groups_with_matching_client_ids,omitempty"`
}

func Describe(ctx context.Context, cl *kgo.Client, adm *kadm.Client, principal string, groupsFor func(ctx context.Context, clientIDPrefix string) []string) (Detail, error) {
	principal = acls.NormalizePrincipal(principal)
	d := Detail{Principal: principal}
	user, isUser := strings.CutPrefix(principal, "User:")
	if isUser {
		if us, err := ListUsers(ctx, adm, user); err == nil && len(us) == 1 {
			d.SCRAM = us[0].Credentials
		}
		qs, err := quotas.List(ctx, adm, quotas.Filter{User: &user})
		if err != nil {
			return Detail{}, err
		}
		d.Quotas = qs
		if eff, err := quotas.Resolve(ctx, adm, user, ""); err == nil {
			d.Effective = eff
		}
	}
	list, err := acls.List(ctx, cl, acls.Filter{Principal: principal})
	if err != nil {
		return Detail{}, err
	}
	d.ACLs = list
	d.Access = acls.Summarize(list)
	if isUser && groupsFor != nil {
		d.Groups = groupsFor(ctx, user)
	}
	return d, nil
}
