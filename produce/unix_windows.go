// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package produce

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Listener struct{ Path string }

func Listen(string) (*Listener, error) { return nil, ErrUnixUnsupported }

func (l *Listener) Close() error { return nil }

func (l *Listener) Source(bool, kgo.Record, Keep, time.Duration) Source { return nil }
