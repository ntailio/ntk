// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package produce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"slices"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/record"
)

const maxDatagram = 16 << 20

// drainWait is how long a read may wait for more queued datagrams during
// shutdown before the queue counts as empty.
const drainWait = 20 * time.Millisecond

type Listener struct {
	Path string
	conn *net.UnixConn
	buf  []byte
}

// Listen binds a datagram socket at path. A stale socket file (nobody bound to
// it) is replaced; a live socket or any other file is left alone.
func Listen(path string) (*Listener, error) {
	addr := &net.UnixAddr{Name: path, Net: "unixgram"}
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&fs.ModeSocket == 0 {
			return nil, fmt.Errorf("%s exists and is not a socket", path)
		}
		c, err := net.DialUnix("unixgram", nil, addr)
		if err == nil {
			c.Close()
			return nil, fmt.Errorf("another process is listening on %s", path)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("checking %s: %w", path, err)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("removing stale socket %s: %w", path, err)
		}
	}
	conn, err := net.ListenUnixgram("unixgram", addr)
	if err != nil {
		return nil, err
	}
	_ = conn.SetReadBuffer(64 << 20)
	return &Listener{Path: path, conn: conn, buf: make([]byte, maxDatagram)}, nil
}

func (l *Listener) Close() error {
	err := l.conn.Close()
	if rerr := os.Remove(l.Path); rerr != nil && !errors.Is(rerr, fs.ErrNotExist) && err == nil {
		err = rerr
	}
	return err
}

// Source yields one record per datagram. Without meta the datagram is the
// value; with meta it is the metadata line, \n, then the value. After ctx is
// canceled it hands over the datagrams already queued, then returns io.EOF.
// idle > 0 ends the input after that long without a datagram.
func (l *Listener) Source(meta bool, base kgo.Record, keep Keep, idle time.Duration) Source {
	n := 0
	draining := false
	registered := false
	var woken atomic.Bool
	return func(ctx context.Context) (*kgo.Record, error) {
		if !registered {
			registered = true
			context.AfterFunc(ctx, func() {
				woken.Store(true) // before the deadline moves, so a read it cuts short sees it
				_ = l.conn.SetReadDeadline(time.Now())
			})
		}
		for {
			if ctx.Err() != nil {
				draining = true
			}
			var deadline time.Time
			switch {
			case draining:
				deadline = time.Now().Add(drainWait)
			case idle > 0:
				deadline = time.Now().Add(idle)
			}
			_ = l.conn.SetReadDeadline(deadline)
			if ctx.Err() != nil && !draining {
				continue
			}
			size, _, flags, _, err := l.conn.ReadMsgUnix(l.buf, nil)
			if errors.Is(err, os.ErrDeadlineExceeded) {
				// The wake-up on cancel can land after a fresh deadline was set and cut that
				// read short, which isn't an empty queue: read again.
				if woken.Swap(false) {
					continue
				}
				if draining || (ctx.Err() == nil && idle > 0) {
					return nil, io.EOF
				}
				continue
			}
			if err != nil {
				return nil, fmt.Errorf("reading %s: %w", l.Path, err)
			}
			n++
			if flags&syscall.MSG_TRUNC != 0 {
				return nil, fmt.Errorf("datagram %d is larger than %d bytes", n, maxDatagram)
			}
			data := l.buf[:size]
			if !meta {
				rec := base
				rec.Headers = slices.Clone(base.Headers)
				rec.Value = append([]byte{}, data...)
				return &rec, nil
			}
			i := bytes.IndexByte(data, '\n')
			if i < 0 {
				return nil, fmt.Errorf("datagram %d has no metadata line (is the sender using -m?)", n)
			}
			m, err := record.ParseMeta(data[:i])
			if err != nil {
				return nil, fmt.Errorf("datagram %d: not a metadata line: %w", n, err)
			}
			value := data[i+1:]
			if m.ValueSize != len(value) {
				return nil, fmt.Errorf("datagram %d: value_size is %d but %d value bytes arrived (truncated by the sender?)", n, m.ValueSize, len(value))
			}
			rec, err := m.ToKgo(append([]byte{}, value...))
			if err != nil {
				return nil, fmt.Errorf("datagram %d: %w", n, err)
			}
			rec.Headers = append(rec.Headers, base.Headers...)
			return keep.apply(rec, base.Topic), nil
		}
	}
}
