// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sys/windows"

	"github.com/ntailio/ntk/produce"
)

func testPipePath() string { return fmt.Sprintf(`\\.\pipe\ntk-test-%d`, time.Now().UnixNano()) }

func pipeServer(t *testing.T, path string, mode uint32) windows.Handle {
	t.Helper()
	name, err := windows.UTF16PtrFromString(path)
	if err != nil {
		t.Fatal(err)
	}
	h, err := windows.CreateNamedPipe(name, windows.PIPE_ACCESS_INBOUND, mode|windows.PIPE_WAIT, 1, 0, 64<<10, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

const messageMode = windows.PIPE_TYPE_MESSAGE | windows.PIPE_READMODE_MESSAGE

func TestPipe(t *testing.T) {
	path := testPipePath()
	if _, err := DialPipe(context.Background(), path, false, false); err == nil || !strings.Contains(err.Error(), "--npipe-wait") {
		t.Fatalf("nothing listening: %v", err)
	}
	srv := pipeServer(t, path, messageMode)
	p, err := DialPipe(context.Background(), path, false, false)
	if err != nil {
		windows.CloseHandle(srv)
		t.Fatal(err)
	}
	defer p.Close()
	values := []string{"one", "", "two\nlines"}
	for _, v := range values {
		if err := deliver(t, p, &kgo.Record{Value: []byte(v)}); err != nil {
			t.Fatal(err)
		}
	}
	buf := make([]byte, 1024)
	for _, want := range values {
		var n uint32
		if err := windows.ReadFile(srv, buf, &n, nil); err != nil || string(buf[:n]) != want {
			t.Errorf("read %q, %v; want %q", buf[:n], err, want)
		}
	}
	windows.CloseHandle(srv)
	err = deliver(t, p, &kgo.Record{Topic: "t", Value: []byte("late")})
	var target *ErrTarget
	if !errors.As(err, &target) || !strings.Contains(err.Error(), "went away") {
		t.Errorf("receiver gone: %v", err)
	}
}

func TestPipeByteMode(t *testing.T) {
	path := testPipePath()
	srv := pipeServer(t, path, windows.PIPE_TYPE_BYTE|windows.PIPE_READMODE_BYTE)
	defer windows.CloseHandle(srv)
	if _, err := DialPipe(context.Background(), path, false, false); err == nil || !strings.Contains(err.Error(), "byte-mode") {
		t.Errorf("byte-mode pipe: %v", err)
	}
}

func TestPipeWait(t *testing.T) {
	path := testPipePath()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dialed := make(chan error, 1)
	go func() {
		p, err := DialPipe(ctx, path, false, true)
		if err == nil {
			p.Close()
		}
		dialed <- err
	}()
	time.Sleep(300 * time.Millisecond)
	srv := pipeServer(t, path, messageMode)
	defer windows.CloseHandle(srv)
	if err := <-dialed; err != nil {
		t.Fatal(err)
	}
}

func TestPipeToListener(t *testing.T) {
	path := testPipePath()
	l, err := produce.ListenPipe(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	p, err := DialPipe(context.Background(), path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	in := []*kgo.Record{
		{Topic: "src", Partition: 2, Offset: 7, Key: []byte("k"), Value: []byte("v\nw"), Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("x")}}},
		{Topic: "src", Key: []byte("tomb")},
		{Topic: "src", Value: []byte{}},
	}
	for _, r := range in {
		if err := deliver(t, p, r); err != nil {
			t.Fatal(err)
		}
	}
	p.Close() // what was sent before closing still arrives

	src := produce.Limit(l.Source(true, kgo.Record{Topic: "dst"}, produce.Keep{}, 5*time.Second), len(in))
	for i, want := range in {
		got, err := src(context.Background())
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if got.Topic != "dst" || !bytes.Equal(got.Key, want.Key) || !bytes.Equal(got.Value, want.Value) || (got.Value == nil) != (want.Value == nil) ||
			len(got.Headers) != len(want.Headers) {
			t.Errorf("record %d: got %+v, want %+v", i, got, want)
		}
	}
}
