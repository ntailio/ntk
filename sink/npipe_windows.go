// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/windows"
)

type Pipe struct {
	Path string
	Meta bool
	h    windows.Handle
}

// DialPipe connects to a message-mode named pipe created by the receiving
// program. With wait, it retries until the pipe appears.
func DialPipe(ctx context.Context, path string, meta, wait bool) (*Pipe, error) {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	for {
		// FILE_READ_ATTRIBUTES lets GetNamedPipeInfo check the pipe type.
		h, err := windows.CreateFile(name, windows.GENERIC_WRITE|windows.FILE_READ_ATTRIBUTES, 0, nil, windows.OPEN_EXISTING, 0, 0)
		if err == nil {
			return checkPipe(h, path, meta)
		}
		// ERROR_PIPE_BUSY: every instance of the pipe has a client. Servers
		// usually create the next one right away, so it's worth a short retry.
		missing, busy := errors.Is(err, windows.ERROR_FILE_NOT_FOUND), errors.Is(err, windows.ERROR_PIPE_BUSY)
		retry := 200 * time.Millisecond
		switch {
		case busy && (wait || time.Since(start) < 5*time.Second):
			retry = 10 * time.Millisecond
		case wait && missing:
		case missing:
			return nil, fmt.Errorf("nothing is listening on %s: the receiving program must create the pipe (use --npipe-wait to wait for it)", path)
		case busy:
			return nil, fmt.Errorf("%s is busy with other clients (use --npipe-wait to wait for a free instance)", path)
		default:
			return nil, fmt.Errorf("opening %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retry):
		}
	}
}

func checkPipe(h windows.Handle, path string, meta bool) (*Pipe, error) {
	var flags uint32
	err := windows.GetNamedPipeInfo(h, &flags, nil, nil, nil)
	switch {
	case err != nil:
		err = fmt.Errorf("%s is not a named pipe: %w", path, err)
	case flags&windows.PIPE_TYPE_MESSAGE == 0:
		err = fmt.Errorf("%s is a byte-mode pipe, where messages run together: the receiving program must create it with PIPE_TYPE_MESSAGE", path)
	default:
		return &Pipe{Path: path, Meta: meta, h: h}, nil
	}
	_ = windows.CloseHandle(h)
	return nil, err
}

func (s *Pipe) Deliver(_ context.Context, r *kgo.Record, done func(Receipt, error)) {
	start := time.Now()
	var n uint32
	err := windows.WriteFile(s.h, payload(r, s.Meta), &n, nil)
	switch {
	case err == nil:
		done(receipt(r, start, "ok"), nil)
	case errors.Is(err, windows.ERROR_NO_DATA), errors.Is(err, windows.ERROR_BROKEN_PIPE), errors.Is(err, windows.ERROR_PIPE_NOT_CONNECTED):
		done(receipt(r, start, "failed"), &ErrTarget{fmt.Errorf("the receiver on %s went away (last delivered: %s/%d before offset %d)", s.Path, r.Topic, r.Partition, r.Offset)})
	default:
		done(receipt(r, start, "failed"), &ErrTarget{fmt.Errorf("sending to %s: %w", s.Path, err)})
	}
}

func (s *Pipe) Close() error { return windows.CloseHandle(s.h) }
