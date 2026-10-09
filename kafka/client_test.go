// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/ntailio/ntk/profile"
	"github.com/ntailio/ntk/testkit"
)

func TestSandboxProfiles(t *testing.T) {
	adm := testkit.Admin(t) // skips when the sandbox is down
	testkit.WaitSCRAMUsers(t, adm, "admin", "bob")

	s, err := profile.Load(filepath.Join(testkit.RepoRoot(t), "sandbox", "profiles.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range s.Names() {
		t.Run(name, func(t *testing.T) {
			cl, err := NewClient(s.File.Profiles[name], s.ResolveFile)
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			md, err := cl.Admin.BrokerMetadata(ctx)
			if err != nil {
				t.Fatalf("metadata: %v", err)
			}
			if len(md.Brokers) != 3 {
				t.Errorf("brokers = %d, want 3", len(md.Brokers))
			}
		})
	}
}

func TestOptionsRejectsUnknownMechanism(t *testing.T) {
	p := &profile.Profile{BootstrapServers: []string{"localhost:1"}, Auth: profile.Auth{Mechanism: "kerberos-ish"}}
	if _, err := Options(p, func(s string) string { return s }); err == nil {
		t.Fatal("expected an error")
	}
}

func TestOptionsRequiresCredentials(t *testing.T) {
	p := &profile.Profile{BootstrapServers: []string{"localhost:1"}, Auth: profile.Auth{Mechanism: "scram-sha-512", Username: "u"}}
	if _, err := Options(p, func(s string) string { return s }); err == nil {
		t.Fatal("expected an error for a missing password")
	}
}
