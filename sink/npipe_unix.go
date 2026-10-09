// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package sink

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Pipe struct{}

func DialPipe(context.Context, string, bool, bool) (*Pipe, error) {
	return nil, errors.New("-o npipe: is only available on Windows; use -o unix:<path>")
}

func (s *Pipe) Deliver(context.Context, *kgo.Record, func(Receipt, error)) {}

func (s *Pipe) Close() error { return nil }
