// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package produce

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/windows"

	"github.com/ntailio/ntk/record"
)

func listenPipe(t *testing.T) (*PipeListener, string) {
	t.Helper()
	path := fmt.Sprintf(`\\.\pipe\ntk-test-%d`, time.Now().UnixNano())
	l, err := ListenPipe(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l, path
}

// pipeSend retries while all instances are taken, like any client must.
func pipeSend(path string, msgs ...[]byte) error {
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return err
	}
	var h windows.Handle
	for {
		h, err = windows.CreateFile(name, windows.GENERIC_WRITE, 0, nil, windows.OPEN_EXISTING, 0, 0)
		if !errors.Is(err, windows.ERROR_PIPE_BUSY) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err != nil {
		return err
	}
	defer windows.CloseHandle(h)
	for _, m := range msgs {
		var n uint32
		if err := windows.WriteFile(h, m, &n, nil); err != nil {
			return err
		}
	}
	return nil
}

func send(t *testing.T, path string, msgs ...[]byte) {
	t.Helper()
	if err := pipeSend(path, msgs...); err != nil {
		t.Fatal(err)
	}
}

func TestPipeListenerValues(t *testing.T) {
	l, path := listenPipe(t)
	send(t, path, []byte("one"), []byte{}, []byte("two\nlines"))
	src := Limit(l.Source(false, kgo.Record{Topic: "dst", Partition: -1, Headers: []kgo.RecordHeader{{Key: "via", Value: []byte("npipe")}}}, Keep{}, time.Second), 3)
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

func TestPipeListenerMeta(t *testing.T) {
	l, path := listenPipe(t)
	in := &kgo.Record{Topic: "src", Key: []byte{0xff}, Value: []byte("v\nw"), Headers: []kgo.RecordHeader{{Key: "h", Value: nil}}}
	msg := append(append(record.MetaLine(in), '\n'), in.Value...)
	tomb := append(record.MetaLine(&kgo.Record{Key: []byte("t")}), '\n')
	send(t, path, msg, tomb)
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
		t.Error("message without a metadata line: expected an error")
	}
}

func TestPipeListenerLarge(t *testing.T) {
	l, path := listenPipe(t)
	big := bytes.Repeat([]byte("0123456789abcdef"), 1<<16) // 1 MiB, many reads
	sent := make(chan error, 1)
	go func() { sent <- pipeSend(path, big, []byte("after")) }()
	out, err := drain(t, Limit(l.Source(false, kgo.Record{}, Keep{}, 5*time.Second), 2))
	if err != nil || len(out) != 2 {
		t.Fatalf("%d records, %v", len(out), err)
	}
	if !bytes.Equal(out[0].Value, big) || string(out[1].Value) != "after" {
		t.Errorf("got %d and %q bytes", len(out[0].Value), out[1].Value)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}

	go func() { _ = pipeSend(path, make([]byte, maxPipeMessage+1)) }()
	if _, err := l.Source(false, kgo.Record{}, Keep{}, 5*time.Second)(context.Background()); err == nil || !strings.Contains(err.Error(), "larger than") {
		t.Errorf("oversized message: %v", err)
	}
}

func TestPipeListenerClients(t *testing.T) {
	l, path := listenPipe(t)
	const clients, each = 4, 50
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for c := range clients {
		wg.Go(func() {
			var msgs [][]byte
			for i := range each {
				msgs = append(msgs, fmt.Appendf(nil, "%d:%d", c, i))
			}
			errs <- pipeSend(path, msgs...)
		})
	}
	out, err := drain(t, Limit(l.Source(false, kgo.Record{}, Keep{}, 5*time.Second), clients*each))
	if err != nil || len(out) != clients*each {
		t.Fatalf("%d records, %v", len(out), err)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
	next := make([]int, clients)
	for _, r := range out {
		var c, i int
		if _, err := fmt.Sscanf(string(r.Value), "%d:%d", &c, &i); err != nil || i != next[c] {
			t.Fatalf("%q out of order (want message %d from client %d)", r.Value, next[c], c)
		}
		next[c]++
	}
}

func TestPipeListenerExclusive(t *testing.T) {
	l, path := listenPipe(t)
	if _, err := ListenPipe(path); err == nil || !strings.Contains(err.Error(), "another process") {
		t.Errorf("second listener: %v", err)
	}
	l.Close()
	if err := pipeSend(path, []byte("x")); !errors.Is(err, windows.ERROR_FILE_NOT_FOUND) {
		t.Errorf("send after Close: %v", err)
	}
	l2, err := ListenPipe(path)
	if err != nil {
		t.Fatalf("listen again after Close: %v", err)
	}
	l2.Close()
}

func TestPipeListenerIdleAndCancel(t *testing.T) {
	l, path := listenPipe(t)
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
