// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package acls

import (
	"slices"
	"testing"
)

func acl(principal, perm, op, rt, pattern, name string) ACL {
	return ACL{Principal: principal, Host: "*", Permission: perm, Operation: op, ResourceType: rt, Pattern: pattern, Name: name}
}

func TestCheck(t *testing.T) {
	all := []ACL{
		acl("User:a", "ALLOW", "READ", "TOPIC", "LITERAL", "orders"),
		acl("User:a", "ALLOW", "WRITE", "TOPIC", "PREFIXED", "pay."),
		acl("User:a", "DENY", "WRITE", "TOPIC", "LITERAL", "pay.secret"),
		acl("User:*", "ALLOW", "DESCRIBE", "TOPIC", "LITERAL", "*"),
		acl("User:b", "ALLOW", "ALL", "GROUP", "LITERAL", "g"),
	}
	tests := []struct {
		principal, op, rt, name string
		want                    bool
	}{
		{"User:a", "READ", "TOPIC", "orders", true},
		{"User:a", "DESCRIBE", "TOPIC", "orders", true},
		{"User:a", "WRITE", "TOPIC", "orders", false},
		{"User:a", "WRITE", "TOPIC", "pay.in", true},
		{"User:a", "WRITE", "TOPIC", "pay.secret", false},
		{"User:c", "DESCRIBE", "TOPIC", "anything", true},
		{"User:c", "READ", "TOPIC", "anything", false},
		{"User:b", "READ", "GROUP", "g", true},
	}
	for _, tt := range tests {
		if got := Check(all, tt.principal, "*", tt.op, tt.rt, tt.name); got.Allowed != tt.want {
			t.Errorf("%s %s %s:%s = %v (%s), want %v", tt.principal, tt.op, tt.rt, tt.name, got.Allowed, got.Reason, tt.want)
		}
	}
}

func TestRecipes(t *testing.T) {
	c, err := Recipe("consumer", RecipeArgs{Principal: "User:x", Topics: []string{"t"}, Groups: []string{"g"}})
	if err != nil || len(c) != 3 {
		t.Fatalf("consumer: %v %v", c, err)
	}
	tp, _ := Recipe("transactional-producer", RecipeArgs{Principal: "User:x", Topics: []string{"t"}, TxnIDs: []string{"tx"}})
	if !slices.ContainsFunc(tp, func(a ACL) bool { return a.ResourceType == "TRANSACTIONAL_ID" && a.Operation == "WRITE" }) {
		t.Errorf("transactional-producer: %v", tp)
	}
	s, _ := Recipe("streams-app", RecipeArgs{Principal: "User:x", AppID: "app", TopicsIn: []string{"in"}, TopicsOut: []string{"out"}})
	if len(s) != 7 {
		t.Errorf("streams-app: %d ACLs", len(s))
	}
	for _, bad := range []struct {
		name string
		args RecipeArgs
	}{{"consumer", RecipeArgs{Topics: []string{"t"}}}, {"producer", RecipeArgs{}}, {"streams-app", RecipeArgs{}}, {"bogus", RecipeArgs{}}} {
		if _, err := Recipe(bad.name, bad.args); err == nil {
			t.Errorf("%s %+v: expected an error", bad.name, bad.args)
		}
	}
}

func TestExportImportRoundTrip(t *testing.T) {
	in := []ACL{
		acl("User:a", "ALLOW", "READ", "TOPIC", "LITERAL", "orders"),
		acl("User:a", "ALLOW", "DESCRIBE", "TOPIC", "LITERAL", "orders"),
		acl("User:a", "ALLOW", "READ", "GROUP", "PREFIXED", "svc-"),
		acl("User:b", "DENY", "WRITE", "CLUSTER", "LITERAL", "kafka-cluster"),
	}
	out, err := Import(Export(in))
	if err != nil {
		t.Fatal(err)
	}
	slices.SortFunc(in, Less)
	if !slices.Equal(in, out) {
		t.Errorf("round trip:\n in  %v\n out %v", in, out)
	}
}

func TestSummarize(t *testing.T) {
	s := Summarize([]ACL{
		acl("User:a", "ALLOW", "WRITE", "TOPIC", "LITERAL", "orders"),
		acl("User:a", "ALLOW", "READ", "TOPIC", "PREFIXED", "pay."),
		acl("User:a", "ALLOW", "READ", "GROUP", "LITERAL", "g"),
	})
	if !slices.Contains(s, "produce → orders") || !slices.Contains(s, "consume → pay.* via group g") {
		t.Errorf("summary = %v", s)
	}
}

func TestParse(t *testing.T) {
	if op, err := ParseOperation("describe-configs"); err != nil || op != "DESCRIBE_CONFIGS" {
		t.Errorf("ParseOperation: %s %v", op, err)
	}
	if NormalizePrincipal("bob") != "User:bob" || NormalizePrincipal("Group:x") != "Group:x" {
		t.Error("NormalizePrincipal")
	}
}
