// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package configs

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

func TestNormalize(t *testing.T) {
	tests := []struct{ key, in, typ, want string }{
		{"retention.ms", "7d", "long", "604800000"},
		{"retention.ms", "-1", "long", "-1"},
		{"retention.ms", "3600000", "long", "3600000"},
		{"log.retention.hours", "2d", "int", "48"},
		{"max.message.bytes", "4MiB", "int", "4194304"},
		{"leader.replication.throttled.rate", "50MiB/s", "long", "52428800"},
		{"cleanup.policy", "compact", "list", "compact"},
		{"unclean.leader.election.enable", "true", "boolean", "true"},
	}
	for _, tt := range tests {
		got, err := Normalize(tt.key, tt.in, tt.typ)
		if err != nil || got != tt.want {
			t.Errorf("Normalize(%s=%s) = %s, %v; want %s", tt.key, tt.in, got, err, tt.want)
		}
	}
	for _, bad := range [][3]string{{"retention.ms", "soon", "long"}, {"min.insync.replicas", "two", "int"}, {"preallocate", "yes", "boolean"}} {
		if _, err := Normalize(bad[0], bad[1], bad[2]); err == nil {
			t.Errorf("Normalize(%v): expected an error", bad)
		}
	}
}

func TestHumanize(t *testing.T) {
	for _, tt := range [][3]string{
		{"retention.ms", "604800000", "1w"}, {"retention.ms", "-1", "∞"}, {"segment.bytes", "1073741824", "1.0 GiB"},
		{"log.retention.hours", "168", "1w"}, {"cleanup.policy", "delete", "delete"},
	} {
		if got := Humanize(tt[0], tt[1]); got != tt[2] {
			t.Errorf("Humanize(%s=%s) = %s, want %s", tt[0], tt[1], got, tt[2])
		}
	}
}

func TestParseAssignments(t *testing.T) {
	ops, err := ParseAssignments([]string{"a=1", "list+=x", "list-=y"})
	if err != nil || len(ops) != 3 || ops[0].Op != "set" || ops[1].Op != "append" || ops[1].Key != "list" || ops[2].Op != "subtract" {
		t.Errorf("ParseAssignments: %+v %v", ops, err)
	}
	if _, err := ParseAssignments([]string{"novalue"}); err == nil {
		t.Error("expected an error")
	}
}

func TestResolveSuggests(t *testing.T) {
	entries := []Entry{{Key: "retention.ms", Type: "long"}, {Key: "segment.ms", Type: "long"}, {Key: "broker.id", ReadOnly: true, Type: "int"}}
	if _, err := Resolve([]Op{{Key: "retention.mss", Op: "set", Value: "1"}}, entries); err == nil || !contains(err.Error(), `did you mean "retention.ms"`) {
		t.Errorf("suggestion: %v", err)
	}
	if _, err := Resolve([]Op{{Key: "broker.id", Op: "set", Value: "1"}}, entries); err == nil {
		t.Error("read-only key accepted")
	}
	ops, err := Resolve([]Op{{Key: "retention.ms", Op: "set", Value: "1d"}, {Key: "segment.ms", Op: "delete"}}, entries)
	if err != nil || *ops[0].Value != "86400000" || ops[1].Op != kadm.DeleteConfig {
		t.Errorf("resolve: %+v %v", ops, err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
