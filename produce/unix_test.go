// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

//go:build !windows

package produce

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/record"
)

func sockPath(t *testing.T) string {
	dir, err := os.MkdirTemp("", "ntk")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "p.sock")
}

func send(t *testing.T, path string, msgs ...[]byte) {
	t.Helper()
	c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	for _, m := range msgs {
		if _, err := c.Write(m); err != nil {
			t.Fatal(err)
		}
	}
}

func listen(t *testing.T, path string) *Listener {
	t.Helper()
	l, err := Listen(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestListenerValues(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	send(t, path, []byte("one"), []byte{}, []byte("two\nlines"))
	src := Limit(l.Source(false, kgo.Record{Topic: "dst", Partition: -1, Headers: []kgo.RecordHeader{{Key: "via", Value: []byte("uds")}}}, Keep{}, time.Second), 3)
	out, err := drain(t, src)
	if err != nil || len(out) != 3 {
		t.Fatalf("%d records, %v", len(out), err)
	}
	if string(out[0].Value) != "one" || out[1].Value == nil || len(out[1].Value) != 0 || string(out[2].Value) != "two\nlines" {
		t.Errorf("values: %q %q %q", out[0].Value, out[1].Value, out[2].Value)
	}
	if out[0].Topic != "dst" || out[0].Key != nil || len(out[0].Headers) != 1 {
		t.Errorf("record: %+v", out[0])
	}
}

func TestListenerMeta(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	in := &kgo.Record{Topic: "src", Key: []byte{0xff}, Value: []byte("v\nw"), Headers: []kgo.RecordHeader{{Key: "h", Value: nil}}}
	dgram := append(append(record.MetaLine(in), '\n'), in.Value...)
	tomb := append(record.MetaLine(&kgo.Record{Key: []byte("t")}), '\n')
	send(t, path, dgram, tomb)
	out, err := drain(t, Limit(l.Source(true, kgo.Record{Topic: "dst"}, Keep{}, time.Second), 2))
	if err != nil || len(out) != 2 {
		t.Fatalf("%d records, %v", len(out), err)
	}
	if out[0].Topic != "dst" || out[0].Partition != -1 || string(out[0].Key) != "\xff" || string(out[0].Value) != "v\nw" || out[0].Headers[0].Value != nil {
		t.Errorf("record: %+v", out[0])
	}
	if out[1].Value != nil || string(out[1].Key) != "t" {
		t.Errorf("tombstone: %+v", out[1])
	}

	send(t, path, append(record.MetaLine(in), '\n', 'v'))
	if _, err := l.Source(true, kgo.Record{}, Keep{}, time.Second)(context.Background()); err == nil {
		t.Error("value_size mismatch: expected an error")
	}
	send(t, path, []byte("no meta"))
	if _, err := l.Source(true, kgo.Record{}, Keep{}, time.Second)(context.Background()); err == nil {
		t.Error("datagram without a metadata line: expected an error")
	}
}

func TestListenerSocketFile(t *testing.T) {
	path := sockPath(t)
	stale, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Error("live socket: expected an error")
	}
	stale.Close() // a closed unixgram socket keeps its file
	l, err := Listen(path)
	if err != nil {
		t.Fatalf("stale socket: %v", err)
	}
	l.Close()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket file still exists after Close: %v", err)
	}

	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Listen(path); err == nil {
		t.Error("regular file: expected an error")
	}
	if _, err := os.Stat(path); err != nil {
		t.Error("a regular file at the path was removed")
	}
}

func TestListenerIdleAndCancel(t *testing.T) {
	path := sockPath(t)
	l := listen(t, path)
	start := time.Now()
	if _, err := l.Source(false, kgo.Record{}, Keep{}, 200*time.Millisecond)(context.Background()); !errors.Is(err, io.EOF) {
		t.Fatalf("idle timeout: %v", err)
	}
	if d := time.Since(start); d < 150*time.Millisecond || d > 2*time.Second {
		t.Errorf("idle timeout took %v", d)
	}

	send(t, path, []byte("a"), []byte("b"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out, err := func() ([]*kgo.Record, error) {
		src := l.Source(false, kgo.Record{}, Keep{}, 0)
		var out []*kgo.Record
		for {
			r, err := src(ctx)
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return out, err
			}
			out = append(out, r)
		}
	}()
	if err != nil || len(out) != 2 {
		t.Errorf("drain after cancel: %d records, %v", len(out), err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := l.Source(false, kgo.Record{}, Keep{}, 0)(ctx)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Errorf("blocked read after cancel: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("a blocked read did not return after cancel")
	}
}
