// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package produce

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type PipeListener struct{ Path string }

func ListenPipe(string) (*PipeListener, error) { return nil, ErrNpipeUnsupported }

func (l *PipeListener) Close() error { return nil }

func (l *PipeListener) Source(bool, kgo.Record, Keep, time.Duration) Source { return nil }
