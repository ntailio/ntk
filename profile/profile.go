// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"time"
)

const CurrentVersion = 1

type File struct {
	Version  int                 `json:"version"`
	Active   string              `json:"active,omitempty"`
	Profiles map[string]*Profile `json:"profiles"`
}

type Profile struct {
	Description      string    `json:"description,omitempty"`
	BootstrapServers []string  `json:"bootstrap_servers"`
	ClientID         string    `json:"client_id,omitempty"`
	Principal        string    `json:"principal,omitempty"`
	TLS              *TLS      `json:"tls,omitempty"`
	Auth             Auth      `json:"auth"`
	ReadOnly         bool      `json:"read_only,omitempty"`
	Color            string    `json:"color,omitempty"`
	Labels           []string  `json:"labels,omitempty"`
	Timeouts         *Timeouts `json:"timeouts,omitempty"`
	Defaults         *Defaults `json:"defaults,omitempty"`
}

type TLS struct {
	Enabled            bool   `json:"enabled,omitempty"`
	CAFile             string `json:"ca_file,omitempty"`
	CAPEM              string `json:"ca_pem,omitempty"`
	CertFile           string `json:"cert_file,omitempty"`
	CertPEM            string `json:"cert_pem,omitempty"`
	KeyFile            string `json:"key_file,omitempty"`
	KeyPEM             string `json:"key_pem,omitempty"`
	KeyPassword        string `json:"key_password,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

func (t *TLS) On() bool {
	if t == nil {
		return false
	}
	return t.Enabled || *t != TLS{}
}

func (t *TLS) HasClientCert() bool {
	return t != nil && (t.CertFile != "" || t.CertPEM != "")
}

type Auth struct {
	Mechanism string `json:"mechanism"`
	Username  string `json:"username,omitempty"`
	Password  string `json:"password,omitempty"`
}

type Timeouts struct {
	Dial    Duration `json:"dial,omitzero"`
	Request Duration `json:"request,omitzero"`
}

type Defaults struct {
	Output  string           `json:"output,omitempty"`
	Consume *ConsumeDefaults `json:"consume,omitempty"`
}

type ConsumeDefaults struct {
	Start string `json:"start,omitempty"`
}

func (p *Profile) SecurityProtocol() string {
	sasl := p.Auth.Mechanism != "" && p.Auth.Mechanism != "none"
	switch {
	case p.TLS.On() && sasl:
		return "SASL_SSL"
	case sasl:
		return "SASL_PLAINTEXT"
	case p.TLS.On():
		return "SSL"
	default:
		return "PLAINTEXT"
	}
}

func (p *Profile) ExpectedPrincipal() (principal string, derived bool) {
	if p.Principal != "" {
		return p.Principal, false
	}
	if p.Auth.Username != "" {
		return "User:" + p.Auth.Username, true
	}
	// mTLS principals depend on the broker's ssl.principal.mapping.rules, so they can't be derived.
	return "", true
}

func (p *Profile) Redacted() *Profile {
	const mask = "********"
	c := *p
	if c.Auth.Password != "" {
		c.Auth.Password = mask
	}
	if c.TLS != nil {
		t := *c.TLS
		if t.KeyPassword != "" {
			t.KeyPassword = mask
		}
		if t.KeyPEM != "" {
			t.KeyPEM = mask
		}
		c.TLS = &t
	}
	return &c
}

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

func ValidateName(name string) error {
	if !nameRE.MatchString(name) {
		return fmt.Errorf("invalid profile name %q: use letters, digits, '.', '_' and '-'", name)
	}
	return nil
}

func (p *Profile) Validate() error {
	var errs []error
	if len(p.BootstrapServers) == 0 {
		errs = append(errs, errors.New("bootstrap_servers: at least one host:port is required"))
	}
	for _, s := range p.BootstrapServers {
		if _, port, err := net.SplitHostPort(s); err != nil {
			errs = append(errs, fmt.Errorf("bootstrap_servers: %q is not host:port", s))
		} else if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
			errs = append(errs, fmt.Errorf("bootstrap_servers: %q has an invalid port", s))
		}
	}
	if p.Auth.Mechanism == "" {
		errs = append(errs, errors.New(`auth.mechanism: required (use "none" for no SASL)`))
	}
	if t := p.TLS; t != nil {
		if t.CAFile != "" && t.CAPEM != "" {
			errs = append(errs, errors.New("tls: set ca_file or ca_pem, not both"))
		}
		if t.CertFile != "" && t.CertPEM != "" {
			errs = append(errs, errors.New("tls: set cert_file or cert_pem, not both"))
		}
		if t.KeyFile != "" && t.KeyPEM != "" {
			errs = append(errs, errors.New("tls: set key_file or key_pem, not both"))
		}
		hasCert, hasKey := t.CertFile != "" || t.CertPEM != "", t.KeyFile != "" || t.KeyPEM != ""
		if hasCert != hasKey {
			errs = append(errs, errors.New("tls: a client certificate needs both a cert and a key"))
		}
	}
	return errors.Join(errs...)
}

type Duration struct{ time.Duration }

func (d Duration) IsZero() bool { return d.Duration == 0 }

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("duration must be a string like \"10s\": %w", err)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}
