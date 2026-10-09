// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSecurityProtocol(t *testing.T) {
	tests := []struct {
		name string
		p    Profile
		want string
	}{
		{"plaintext", Profile{Auth: Auth{Mechanism: "none"}}, "PLAINTEXT"},
		{"tls enabled", Profile{TLS: &TLS{Enabled: true}, Auth: Auth{Mechanism: "none"}}, "SSL"},
		{"tls implied by ca", Profile{TLS: &TLS{CAFile: "ca.crt"}, Auth: Auth{Mechanism: "none"}}, "SSL"},
		{"sasl", Profile{Auth: Auth{Mechanism: "scram-sha-512"}}, "SASL_PLAINTEXT"},
		{"sasl tls", Profile{TLS: &TLS{Enabled: true}, Auth: Auth{Mechanism: "plain"}}, "SASL_SSL"},
		{"empty tls block", Profile{TLS: &TLS{}, Auth: Auth{Mechanism: "none"}}, "PLAINTEXT"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.p.SecurityProtocol(); got != tt.want {
				t.Errorf("got %s, want %s", got, tt.want)
			}
		})
	}
}

func TestValidate(t *testing.T) {
	valid := Profile{BootstrapServers: []string{"kafka:9092"}, Auth: Auth{Mechanism: "none"}}
	if err := valid.Validate(); err != nil {
		t.Errorf("valid profile: %v", err)
	}
	bad := []Profile{
		{Auth: Auth{Mechanism: "none"}},
		{BootstrapServers: []string{"kafka"}, Auth: Auth{Mechanism: "none"}},
		{BootstrapServers: []string{"kafka:0"}, Auth: Auth{Mechanism: "none"}},
		{BootstrapServers: []string{"kafka:9092"}},
		{BootstrapServers: []string{"kafka:9092"}, Auth: Auth{Mechanism: "none"},
			TLS: &TLS{CertFile: "c.crt"}},
		{BootstrapServers: []string{"kafka:9092"}, Auth: Auth{Mechanism: "none"},
			TLS: &TLS{CAFile: "ca.crt", CAPEM: "-----BEGIN"}},
	}
	for i, p := range bad {
		if err := p.Validate(); err == nil {
			t.Errorf("bad[%d]: expected an error", i)
		}
	}
}

func TestRedactedDoesNotMutate(t *testing.T) {
	p := &Profile{Auth: Auth{Mechanism: "plain", Password: "s3cret"}, TLS: &TLS{KeyPassword: "k", KeyPEM: "pem"}}
	r := p.Redacted()
	if r.Auth.Password == "s3cret" || r.TLS.KeyPassword == "k" || r.TLS.KeyPEM == "pem" {
		t.Errorf("secrets not masked: %+v %+v", r.Auth, r.TLS)
	}
	if p.Auth.Password != "s3cret" || p.TLS.KeyPassword != "k" || p.TLS.KeyPEM != "pem" {
		t.Error("Redacted modified the original profile")
	}
}

func TestValidateName(t *testing.T) {
	for _, ok := range []string{"dev", "prod-eu.1", "a_b"} {
		if err := ValidateName(ok); err != nil {
			t.Errorf("%q: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "has space", "slash/name"} {
		if err := ValidateName(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestDurationJSON(t *testing.T) {
	var to Timeouts
	if err := json.Unmarshal([]byte(`{"dial":"10s"}`), &to); err != nil {
		t.Fatal(err)
	}
	if to.Dial.Duration != 10*time.Second {
		t.Errorf("dial = %v", to.Dial)
	}
	b, err := json.Marshal(to)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"dial":"10s"}` {
		t.Errorf("marshal = %s", b)
	}
}
