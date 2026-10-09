// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Unix struct{}

func DialUnix(context.Context, string, bool, bool) (*Unix, error) {
	return nil, errors.New("-o unix: is not available on Windows (it only supports stream Unix sockets); use -o npipe:<name>")
}

func (s *Unix) Deliver(context.Context, *kgo.Record, func(Receipt, error)) {}

func (s *Unix) Close() error { return nil }
