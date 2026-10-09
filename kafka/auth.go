// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/ntailio/ntk/profile"
)

var saslMechanisms = map[string]func(profile.Auth) (sasl.Mechanism, error){
	"plain": func(a profile.Auth) (sasl.Mechanism, error) {
		if err := needUserPass(a); err != nil {
			return nil, err
		}
		return plain.Auth{User: a.Username, Pass: a.Password}.AsMechanism(), nil
	},
	"scram-sha-256": func(a profile.Auth) (sasl.Mechanism, error) {
		if err := needUserPass(a); err != nil {
			return nil, err
		}
		return scram.Auth{User: a.Username, Pass: a.Password}.AsSha256Mechanism(), nil
	},
	"scram-sha-512": func(a profile.Auth) (sasl.Mechanism, error) {
		if err := needUserPass(a); err != nil {
			return nil, err
		}
		return scram.Auth{User: a.Username, Pass: a.Password}.AsSha512Mechanism(), nil
	},
}

func Mechanisms() []string {
	names := []string{"none"}
	for n := range saslMechanisms {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

func authOptions(a profile.Auth) ([]kgo.Opt, error) {
	if a.Mechanism == "none" {
		return nil, nil
	}
	build, ok := saslMechanisms[a.Mechanism]
	if !ok {
		return nil, fmt.Errorf("auth.mechanism %q is not supported (use one of: %s)", a.Mechanism, strings.Join(Mechanisms(), ", "))
	}
	m, err := build(a)
	if err != nil {
		return nil, fmt.Errorf("auth (%s): %w", a.Mechanism, err)
	}
	return []kgo.Opt{kgo.SASL(m)}, nil
}

func needUserPass(a profile.Auth) error {
	if a.Username == "" || a.Password == "" {
		return errors.New("username and password are required")
	}
	return nil
}
