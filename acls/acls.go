// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package acls

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ntailio/ntk/plan"
)

type ACL struct {
	Principal    string `json:"principal"`
	Host         string `json:"host"`
	Permission   string `json:"permission"`
	Operation    string `json:"operation"`
	ResourceType string `json:"resource_type"`
	Pattern      string `json:"pattern"`
	Name         string `json:"name"`
}

func (a ACL) Resource() string { return fmt.Sprintf("%s:%s:%s", a.ResourceType, a.Pattern, a.Name) }

func (a ACL) String() string {
	return fmt.Sprintf("%s %s %-16s %s host=%s", a.Permission, a.Principal, a.Operation, a.Resource(), a.Host)
}

func Less(a, b ACL) int {
	return cmp.Or(strings.Compare(a.Principal, b.Principal), strings.Compare(a.ResourceType, b.ResourceType),
		strings.Compare(a.Name, b.Name), strings.Compare(a.Operation, b.Operation), strings.Compare(a.Permission, b.Permission))
}

var resourceTypes = map[string]kmsg.ACLResourceType{
	"ANY": kmsg.ACLResourceTypeAny, "TOPIC": kmsg.ACLResourceTypeTopic, "GROUP": kmsg.ACLResourceTypeGroup,
	"CLUSTER": kmsg.ACLResourceTypeCluster, "TRANSACTIONAL_ID": kmsg.ACLResourceTypeTransactionalId,
	"DELEGATION_TOKEN": kmsg.ACLResourceTypeDelegationToken, "USER": kmsg.ACLResourceTypeUser,
}

var patterns = map[string]kmsg.ACLResourcePatternType{
	"ANY": kmsg.ACLResourcePatternTypeAny, "MATCH": kmsg.ACLResourcePatternTypeMatch,
	"LITERAL": kmsg.ACLResourcePatternTypeLiteral, "PREFIXED": kmsg.ACLResourcePatternTypePrefixed,
}

var operations = map[string]kmsg.ACLOperation{
	"ANY": kmsg.ACLOperationAny, "ALL": kmsg.ACLOperationAll, "READ": kmsg.ACLOperationRead, "WRITE": kmsg.ACLOperationWrite,
	"CREATE": kmsg.ACLOperationCreate, "DELETE": kmsg.ACLOperationDelete, "ALTER": kmsg.ACLOperationAlter,
	"DESCRIBE": kmsg.ACLOperationDescribe, "CLUSTER_ACTION": kmsg.ACLOperationClusterAction,
	"DESCRIBE_CONFIGS": kmsg.ACLOperationDescribeConfigs, "ALTER_CONFIGS": kmsg.ACLOperationAlterConfigs,
	"IDEMPOTENT_WRITE": kmsg.ACLOperationIdempotentWrite, "CREATE_TOKENS": kmsg.ACLOperationCreateTokens,
	"DESCRIBE_TOKENS": kmsg.ACLOperationDescribeTokens,
}

var permissions = map[string]kmsg.ACLPermissionType{
	"ANY": kmsg.ACLPermissionTypeAny, "DENY": kmsg.ACLPermissionTypeDeny, "ALLOW": kmsg.ACLPermissionTypeAllow,
}

func name[T comparable](m map[string]T, v T) string {
	for k, x := range m {
		if x == v {
			return k
		}
	}
	return "UNKNOWN"
}

func norm(s string) string {
	return strings.ToUpper(strings.ReplaceAll(strings.TrimSpace(s), "-", "_"))
}

func Operations() []string {
	var out []string
	for k := range operations {
		if k != "ANY" {
			out = append(out, k)
		}
	}
	slices.Sort(out)
	return out
}

func ResourceTypes() []string {
	var out []string
	for k := range resourceTypes {
		if k != "ANY" {
			out = append(out, strings.ToLower(k))
		}
	}
	slices.Sort(out)
	return out
}

func ParseOperation(s string) (string, error) {
	n := norm(s)
	if _, ok := operations[n]; !ok {
		return "", fmt.Errorf("unknown operation %q (use %s)", s, strings.Join(Operations(), ", "))
	}
	return n, nil
}

func ParseResourceType(s string) (string, error) {
	n := norm(s)
	if _, ok := resourceTypes[n]; !ok {
		return "", fmt.Errorf("unknown resource type %q (use %s)", s, strings.Join(ResourceTypes(), ", "))
	}
	return n, nil
}

func ParsePattern(s string) (string, error) {
	n := norm(s)
	if _, ok := patterns[n]; !ok {
		return "", fmt.Errorf("unknown pattern %q (use literal, prefixed, or match)", s)
	}
	return n, nil
}

// NormalizePrincipal adds "User:" when no type is given.
func NormalizePrincipal(p string) string {
	if p == "" || strings.Contains(p, ":") {
		return p
	}
	return "User:" + p
}

type Filter struct {
	Principal    string
	Host         string
	Permission   string
	Operation    string
	ResourceType string
	Pattern      string
	Name         string
}

func (f Filter) Empty() bool { return f == Filter{} || f == Filter{Pattern: "ANY"} }

func (f Filter) request() kmsg.DescribeACLsRequest {
	r := kmsg.NewDescribeACLsRequest()
	r.ResourceType, r.ResourcePatternType, r.Operation, r.PermissionType =
		kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny, kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	if f.ResourceType != "" {
		r.ResourceType = resourceTypes[f.ResourceType]
	}
	if f.Pattern != "" {
		r.ResourcePatternType = patterns[f.Pattern]
	}
	if f.Operation != "" {
		r.Operation = operations[f.Operation]
	}
	if f.Permission != "" {
		r.PermissionType = permissions[f.Permission]
	}
	if f.Name != "" {
		r.ResourceName = kmsg.StringPtr(f.Name)
	}
	if f.ResourceType == "CLUSTER" && f.Name == "" {
		r.ResourceName = kmsg.StringPtr("kafka-cluster")
	}
	if f.Principal != "" {
		r.Principal = kmsg.StringPtr(f.Principal)
	}
	if f.Host != "" {
		r.Host = kmsg.StringPtr(f.Host)
	}
	return r
}

func List(ctx context.Context, cl *kgo.Client, f Filter) ([]ACL, error) {
	req := f.request()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, err
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		if resp.ErrorMessage != nil {
			return nil, fmt.Errorf("%w: %s", err, *resp.ErrorMessage)
		}
		return nil, err
	}
	var out []ACL
	for _, r := range resp.Resources {
		for _, a := range r.ACLs {
			out = append(out, ACL{
				Principal: a.Principal, Host: a.Host, Permission: name(permissions, a.PermissionType),
				Operation: name(operations, a.Operation), ResourceType: name(resourceTypes, r.ResourceType),
				Pattern: name(patterns, r.ResourcePatternType), Name: r.ResourceName,
			})
		}
	}
	slices.SortFunc(out, Less)
	return out, nil
}

func Create(ctx context.Context, cl *kgo.Client, list []ACL) error {
	req := kmsg.NewPtrCreateACLsRequest()
	for _, a := range list {
		c := kmsg.NewCreateACLsRequestCreation()
		c.ResourceType, c.ResourceName, c.ResourcePatternType = resourceTypes[a.ResourceType], a.Name, patterns[a.Pattern]
		c.Principal, c.Host, c.Operation, c.PermissionType = a.Principal, a.Host, operations[a.Operation], permissions[a.Permission]
		req.Creations = append(req.Creations, c)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	var errs []error
	for i, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			msg := ""
			if r.ErrorMessage != nil {
				msg = ": " + *r.ErrorMessage
			}
			errs = append(errs, fmt.Errorf("%s: %w%s", list[i], err, msg))
		}
	}
	return errors.Join(errs...)
}

func Delete(ctx context.Context, cl *kgo.Client, list []ACL) error {
	req := kmsg.NewPtrDeleteACLsRequest()
	for _, a := range list {
		f := kmsg.NewDeleteACLsRequestFilter()
		f.ResourceType, f.ResourceName, f.ResourcePatternType = resourceTypes[a.ResourceType], kmsg.StringPtr(a.Name), patterns[a.Pattern]
		f.Principal, f.Host, f.Operation, f.PermissionType = kmsg.StringPtr(a.Principal), kmsg.StringPtr(a.Host), operations[a.Operation], permissions[a.Permission]
		req.Filters = append(req.Filters, f)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	var errs []error
	for i, r := range resp.Results {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", list[i], err))
		}
	}
	return errors.Join(errs...)
}

const (
	KindCreate = "acl.create"
	KindDelete = "acl.delete"
	KindImport = "acl.import"
)

type Spec struct {
	Create []ACL `json:"create,omitempty"`
	Delete []ACL `json:"delete,omitempty"`
}

func PlanCreate(ctx context.Context, cl *kgo.Client, list []ACL, summary string) (*plan.Plan, error) {
	existing, err := List(ctx, cl, Filter{})
	if err != nil {
		return nil, err
	}
	var spec Spec
	for _, a := range list {
		if !slices.Contains(existing, a) && !slices.Contains(spec.Create, a) {
			spec.Create = append(spec.Create, a)
		}
	}
	pl, err := plan.New(KindCreate, plan.SafeWrite, fmt.Sprintf("%s: create %d ACL(s):", summary, len(spec.Create)), spec)
	if err != nil {
		return nil, err
	}
	for _, a := range spec.Create {
		pl.Add("+ %s", a)
	}
	if skipped := len(list) - len(spec.Create); skipped > 0 {
		pl.Warn("%d ACL(s) already exist", skipped)
	}
	return pl, nil
}

func PlanDelete(ctx context.Context, cl *kgo.Client, list []ACL, summary, self string) (*plan.Plan, error) {
	pl, err := plan.New(KindDelete, plan.Destructive, fmt.Sprintf("%s: delete %d ACL(s):", summary, len(list)), Spec{Delete: list})
	if err != nil {
		return nil, err
	}
	for _, a := range list {
		pl.Add("- %s", a)
		if self != "" && a.Principal == self && a.Permission == "ALLOW" && a.ResourceType == "CLUSTER" &&
			(a.Operation == "DESCRIBE" || a.Operation == "ALTER" || a.Operation == "ALL") {
			pl.Warn("this removes %s %s on the cluster from %s, the principal this profile uses", a.Permission, a.Operation, self)
			pl.Confirm = self
		}
	}
	if pl.Confirm == "" && len(list) > 0 {
		pl.Confirm = fmt.Sprintf("delete %d acls", len(list))
	}
	return pl, nil
}

func Apply(ctx context.Context, cl *kgo.Client, pl *plan.Plan) error {
	var spec Spec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	if len(spec.Delete) > 0 {
		if err := Delete(ctx, cl, spec.Delete); err != nil {
			return err
		}
	}
	if len(spec.Create) > 0 {
		return Create(ctx, cl, spec.Create)
	}
	return nil
}

// Recipe builds the ACLs for a common role (spec/features/acls.md#recipes).
type RecipeArgs struct {
	Principal string
	Host      string
	Topics    []string
	Groups    []string
	TxnIDs    []string
	AppID     string
	TopicsIn  []string
	TopicsOut []string
	Prefixed  bool
}

var Recipes = []string{"producer", "consumer", "transactional-producer", "streams-app", "topic-admin", "read-only-cluster"}

func Recipe(name string, r RecipeArgs) ([]ACL, error) {
	host := r.Host
	if host == "" {
		host = "*"
	}
	pattern := "LITERAL"
	if r.Prefixed {
		pattern = "PREFIXED"
	}
	var out []ACL
	add := func(rt, pat, res string, ops ...string) {
		for _, op := range ops {
			out = append(out, ACL{Principal: r.Principal, Host: host, Permission: "ALLOW", Operation: op, ResourceType: rt, Pattern: pat, Name: res})
		}
	}
	need := func(what string, v []string) error {
		if len(v) == 0 {
			return fmt.Errorf("recipe %s needs --%s", name, what)
		}
		return nil
	}
	switch name {
	case "producer":
		if err := need("topic", r.Topics); err != nil {
			return nil, err
		}
		for _, t := range r.Topics {
			add("TOPIC", pattern, t, "WRITE", "DESCRIBE")
		}
	case "consumer":
		if err := need("topic", r.Topics); err != nil {
			return nil, err
		}
		if err := need("group", r.Groups); err != nil {
			return nil, err
		}
		for _, t := range r.Topics {
			add("TOPIC", pattern, t, "READ", "DESCRIBE")
		}
		for _, g := range r.Groups {
			add("GROUP", pattern, g, "READ")
		}
	case "transactional-producer":
		if err := need("topic", r.Topics); err != nil {
			return nil, err
		}
		if err := need("txn-id", r.TxnIDs); err != nil {
			return nil, err
		}
		for _, t := range r.Topics {
			add("TOPIC", pattern, t, "WRITE", "DESCRIBE")
		}
		for _, x := range r.TxnIDs {
			add("TRANSACTIONAL_ID", pattern, x, "WRITE", "DESCRIBE")
		}
		add("CLUSTER", "LITERAL", "kafka-cluster", "IDEMPOTENT_WRITE")
	case "streams-app":
		if r.AppID == "" {
			return nil, errors.New("recipe streams-app needs --app-id")
		}
		for _, t := range r.TopicsIn {
			add("TOPIC", "LITERAL", t, "READ", "DESCRIBE")
		}
		for _, t := range r.TopicsOut {
			add("TOPIC", "LITERAL", t, "WRITE", "DESCRIBE")
		}
		add("TOPIC", "PREFIXED", r.AppID, "ALL")
		add("GROUP", "PREFIXED", r.AppID, "ALL")
		add("TRANSACTIONAL_ID", "PREFIXED", r.AppID, "ALL")
	case "topic-admin":
		if err := need("topic", r.Topics); err != nil {
			return nil, err
		}
		for _, t := range r.Topics {
			add("TOPIC", pattern, t, "ALTER", "ALTER_CONFIGS", "CREATE", "DELETE", "DESCRIBE", "DESCRIBE_CONFIGS")
		}
	case "read-only-cluster":
		add("CLUSTER", "LITERAL", "kafka-cluster", "DESCRIBE", "DESCRIBE_CONFIGS")
		add("TOPIC", "LITERAL", "*", "DESCRIBE", "DESCRIBE_CONFIGS")
		add("GROUP", "LITERAL", "*", "DESCRIBE")
	default:
		return nil, fmt.Errorf("unknown recipe %q (use %s)", name, strings.Join(Recipes, ", "))
	}
	return out, nil
}

// implied lists the operations that grant op (Kafka's authorizer rules).
func implied(op string) []string {
	out := []string{op, "ALL"}
	switch op {
	case "DESCRIBE":
		out = append(out, "READ", "WRITE", "DELETE", "ALTER")
	case "DESCRIBE_CONFIGS":
		out = append(out, "ALTER_CONFIGS")
	}
	return out
}

func matchesResource(a ACL, rt, name string) bool {
	if a.ResourceType != rt {
		return false
	}
	switch a.Pattern {
	case "LITERAL":
		return a.Name == name || a.Name == "*"
	case "PREFIXED":
		return strings.HasPrefix(name, a.Name)
	}
	return false
}

type Decision struct {
	Allowed bool   `json:"allowed"`
	By      []ACL  `json:"matched_acls"`
	Reason  string `json:"reason"`
}

// Check evaluates ACLs the way Kafka's StandardAuthorizer does: DENY wins, then
// any matching ALLOW. It cannot see super.users or custom authorizers.
func Check(all []ACL, principal, host, op, rt, name string) Decision {
	var allow, deny []ACL
	for _, a := range all {
		if !matchesResource(a, rt, name) {
			continue
		}
		if a.Principal != principal && a.Principal != "User:*" {
			continue
		}
		if a.Host != "*" && a.Host != host {
			continue
		}
		if a.Permission == "DENY" && (a.Operation == op || a.Operation == "ALL") {
			deny = append(deny, a)
		}
		if a.Permission == "ALLOW" && slices.Contains(implied(op), a.Operation) {
			allow = append(allow, a)
		}
	}
	switch {
	case len(deny) > 0:
		return Decision{false, deny, "DENIED by"}
	case len(allow) > 0:
		return Decision{true, allow, "ALLOWED by"}
	}
	return Decision{false, nil, "DENIED: no matching ALLOW ACL (super.users and custom authorizers are not visible to ntk)"}
}

// ExportEntry is the JSON export format, grouped by principal.
type ExportEntry struct {
	Principal string        `json:"principal"`
	Allow     []ExportGrant `json:"allow,omitempty"`
	Deny      []ExportGrant `json:"deny,omitempty"`
}

type ExportGrant struct {
	Operations      []string `json:"operations"`
	Topic           string   `json:"topic,omitempty"`
	Group           string   `json:"group,omitempty"`
	Cluster         bool     `json:"cluster,omitempty"`
	TransactionalID string   `json:"transactional_id,omitempty"`
	DelegationToken string   `json:"delegation_token,omitempty"`
	User            string   `json:"user,omitempty"`
	Pattern         string   `json:"pattern,omitempty"`
	Host            string   `json:"host,omitempty"`
}

func Export(list []ACL) []ExportEntry {
	type k struct{ principal, perm, rt, pattern, name, host string }
	grouped := map[k][]string{}
	var order []k
	for _, a := range list {
		key := k{a.Principal, a.Permission, a.ResourceType, a.Pattern, a.Name, a.Host}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], a.Operation)
	}
	byPrincipal := map[string]*ExportEntry{}
	var principals []string
	for _, key := range order {
		e := byPrincipal[key.principal]
		if e == nil {
			e = &ExportEntry{Principal: key.principal}
			byPrincipal[key.principal] = e
			principals = append(principals, key.principal)
		}
		g := ExportGrant{Operations: grouped[key]}
		if key.pattern == "PREFIXED" {
			g.Pattern = "prefixed"
		}
		if key.host != "*" {
			g.Host = key.host
		}
		switch key.rt {
		case "TOPIC":
			g.Topic = key.name
		case "GROUP":
			g.Group = key.name
		case "CLUSTER":
			g.Cluster = true
		case "TRANSACTIONAL_ID":
			g.TransactionalID = key.name
		case "DELEGATION_TOKEN":
			g.DelegationToken = key.name
		case "USER":
			g.User = key.name
		}
		if key.perm == "DENY" {
			e.Deny = append(e.Deny, g)
		} else {
			e.Allow = append(e.Allow, g)
		}
	}
	slices.Sort(principals)
	out := make([]ExportEntry, 0, len(principals))
	for _, p := range principals {
		out = append(out, *byPrincipal[p])
	}
	return out
}

func Import(entries []ExportEntry) ([]ACL, error) {
	var out []ACL
	for _, e := range entries {
		if e.Principal == "" {
			return nil, errors.New("entry without a principal")
		}
		for perm, grants := range map[string][]ExportGrant{"ALLOW": e.Allow, "DENY": e.Deny} {
			for _, g := range grants {
				rt, res := "", ""
				switch {
				case g.Topic != "":
					rt, res = "TOPIC", g.Topic
				case g.Group != "":
					rt, res = "GROUP", g.Group
				case g.Cluster:
					rt, res = "CLUSTER", "kafka-cluster"
				case g.TransactionalID != "":
					rt, res = "TRANSACTIONAL_ID", g.TransactionalID
				case g.DelegationToken != "":
					rt, res = "DELEGATION_TOKEN", g.DelegationToken
				case g.User != "":
					rt, res = "USER", g.User
				default:
					return nil, fmt.Errorf("%s: grant without a resource", e.Principal)
				}
				pattern := "LITERAL"
				if strings.EqualFold(g.Pattern, "prefixed") {
					pattern = "PREFIXED"
				}
				host := g.Host
				if host == "" {
					host = "*"
				}
				for _, op := range g.Operations {
					o, err := ParseOperation(op)
					if err != nil {
						return nil, err
					}
					out = append(out, ACL{Principal: NormalizePrincipal(e.Principal), Host: host, Permission: perm, Operation: o, ResourceType: rt, Pattern: pattern, Name: res})
				}
			}
		}
	}
	slices.SortFunc(out, Less)
	return out, nil
}

// PlanImport adds missing ACLs and, with prune, removes ACLs of principals
// matching scope that are not in the file.
func PlanImport(ctx context.Context, cl *kgo.Client, desired []ACL, prune bool, scope string) (*plan.Plan, error) {
	existing, err := List(ctx, cl, Filter{})
	if err != nil {
		return nil, err
	}
	var spec Spec
	for _, a := range desired {
		if !slices.Contains(existing, a) {
			spec.Create = append(spec.Create, a)
		}
	}
	if prune {
		for _, a := range existing {
			if ok, _ := path.Match(scope, a.Principal); ok && !slices.Contains(desired, a) {
				spec.Delete = append(spec.Delete, a)
			}
		}
	}
	class := plan.SafeWrite
	if len(spec.Delete) > 0 {
		class = plan.Destructive
	}
	pl, err := plan.New(KindImport, class, fmt.Sprintf("Import ACLs: create %d, delete %d:", len(spec.Create), len(spec.Delete)), spec)
	if err != nil {
		return nil, err
	}
	for _, a := range spec.Create {
		pl.Add("+ %s", a)
	}
	for _, a := range spec.Delete {
		pl.Add("- %s", a)
	}
	if len(spec.Delete) > 0 {
		pl.Confirm = fmt.Sprintf("delete %d acls", len(spec.Delete))
	}
	return pl, nil
}

// Summarize translates ACLs back into recipe terms ("produce → orders").
func Summarize(list []ACL) []string {
	has := func(rt, name, op string) bool {
		for _, a := range list {
			if a.Permission == "ALLOW" && a.ResourceType == rt && a.Name == name && (a.Operation == op || a.Operation == "ALL") {
				return true
			}
		}
		return false
	}
	var out []string
	seen := map[string]bool{}
	var groupsRead []string
	for _, a := range list {
		if a.Permission == "ALLOW" && a.ResourceType == "GROUP" && (a.Operation == "READ" || a.Operation == "ALL") {
			groupsRead = append(groupsRead, a.Name)
		}
	}
	for _, a := range list {
		if a.Permission != "ALLOW" || a.ResourceType != "TOPIC" {
			continue
		}
		label := a.Name
		if a.Pattern == "PREFIXED" {
			label += "*"
		}
		if has("TOPIC", a.Name, "WRITE") && !seen["p"+label] {
			seen["p"+label] = true
			out = append(out, "produce → "+label)
		}
		if has("TOPIC", a.Name, "READ") && !seen["c"+label] {
			seen["c"+label] = true
			via := ""
			if len(groupsRead) > 0 {
				via = " via group " + strings.Join(groupsRead, ", ")
			}
			out = append(out, "consume → "+label+via)
		}
	}
	for _, a := range list {
		if a.Permission == "DENY" {
			out = append(out, "denied: "+a.Operation+" on "+a.Resource())
		}
	}
	return out
}
