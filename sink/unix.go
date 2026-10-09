// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package sink

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Unix struct {
	Path string
	Meta bool
	conn *net.UnixConn
	max  int
}

// DialUnix connects to a datagram socket bound by the receiving program.
// With wait, it retries until the socket appears.
func DialUnix(ctx context.Context, path string, meta, wait bool) (*Unix, error) {
	addr := &net.UnixAddr{Name: path, Net: "unixgram"}
	for {
		conn, err := net.DialUnix("unixgram", nil, addr)
		if err == nil {
			u := &Unix{Path: path, Meta: meta, conn: conn}
			u.max = raiseSendBuffer(conn)
			return u, nil
		}
		missing := errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENOENT)
		if !wait || !missing {
			if missing {
				return nil, fmt.Errorf("nothing is listening on %s: the receiving program must bind the socket (use --unix-wait to wait for it)", path)
			}
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// raiseSendBuffer asks for a large SO_SNDBUF (the kernel caps it) and returns
// the resulting size, which bounds the datagram size.
func raiseSendBuffer(c *net.UnixConn) int {
	_ = c.SetWriteBuffer(64 << 20)
	raw, err := c.SyscallConn()
	if err != nil {
		return 0
	}
	size := 0
	_ = raw.Control(func(fd uintptr) {
		size, _ = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF)
	})
	return size
}

func (s *Unix) Deliver(_ context.Context, r *kgo.Record, done func(Receipt, error)) {
	start := time.Now()
	b := payload(r, s.Meta)
	_, err := s.conn.Write(b)
	switch {
	case err == nil:
		done(receipt(r, start, "ok"), nil)
	case errors.Is(err, syscall.EMSGSIZE):
		limit := ""
		if s.max > 0 {
			limit = fmt.Sprintf(" (%d bytes)", s.max)
		}
		done(receipt(r, start, "failed"), &ErrTarget{fmt.Errorf("%s/%d @%d is %d bytes, larger than the unix datagram limit%s. "+
			"Raise net.core.wmem_max (Linux) / net.local.dgram.maxdgram (macOS), or use -o exec:<command>", r.Topic, r.Partition, r.Offset, len(b), limit)})
	case errors.Is(err, syscall.ECONNREFUSED):
		done(receipt(r, start, "failed"), &ErrTarget{fmt.Errorf("the receiver on %s went away (last delivered: %s/%d before offset %d)", s.Path, r.Topic, r.Partition, r.Offset)})
	default:
		done(receipt(r, start, "failed"), &ErrTarget{fmt.Errorf("sending to %s: %w", s.Path, err)})
	}
}

func (s *Unix) Close() error { return s.conn.Close() }
