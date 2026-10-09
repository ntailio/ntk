// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/profile"
)

const DefaultClientID = "ntk"

func Options(p *profile.Profile, resolve func(string) string) ([]kgo.Opt, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	clientID := p.ClientID
	if clientID == "" {
		clientID = DefaultClientID
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(p.BootstrapServers...),
		kgo.ClientID(clientID),
	}
	if p.Timeouts != nil && p.Timeouts.Dial.Duration > 0 {
		opts = append(opts, kgo.DialTimeout(p.Timeouts.Dial.Duration))
	}
	if p.Timeouts != nil && p.Timeouts.Request.Duration > 0 {
		opts = append(opts, kgo.RetryTimeout(p.Timeouts.Request.Duration))
	}
	if p.TLS.On() {
		cfg, err := tlsConfig(p.TLS, resolve)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	saslOpts, err := authOptions(p.Auth)
	if err != nil {
		return nil, err
	}
	return append(opts, saslOpts...), nil
}

type Client struct {
	*kgo.Client
	Admin *kadm.Client
}

func NewClient(p *profile.Profile, resolve func(string) string, extra ...kgo.Opt) (*Client, error) {
	opts, err := Options(p, resolve)
	if err != nil {
		return nil, err
	}
	cl, err := kgo.NewClient(append(opts, extra...)...)
	if err != nil {
		return nil, fmt.Errorf("creating kafka client: %w", err)
	}
	return &Client{Client: cl, Admin: kadm.NewClient(cl)}, nil
}
