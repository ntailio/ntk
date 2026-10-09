// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseDelimiter(t *testing.T) {
	tests := map[string]string{`\n`: "\n", `\0`: "\x00", `\x1e`: "\x1e", `\n---\n`: "\n---\n", `a\\b`: `a\b`, `\r\n`: "\r\n", ";": ";"}
	for in, want := range tests {
		got, err := ParseDelimiter(in)
		if err != nil || string(got) != want {
			t.Errorf("%q: got %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"", `\`, `\q`, `\x8f`, `\x1`, "é"} {
		if _, err := ParseDelimiter(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestParseTarget(t *testing.T) {
	for in, want := range map[string]Target{
		"": {Kind: "raw"}, "raw": {Kind: "raw"}, "jsonl": {Kind: "jsonl"},
		"unix:/run/a:b.sock": {Kind: "unix", Arg: "/run/a:b.sock"}, "exec:jq -c .": {Kind: "exec", Arg: "jq -c ."},
		"npipe:orders": {Kind: "npipe", Arg: `\\.\pipe\orders`}, `npipe:\\.\pipe\a b`: {Kind: "npipe", Arg: `\\.\pipe\a b`},
		`npipe:\\host\PIPE\x\y`: {Kind: "npipe", Arg: `\\host\PIPE\x\y`},
	} {
		got, err := ParseTarget(in)
		if err != nil || got != want {
			t.Errorf("%q: got %+v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"table", "json", "unix:", "exec:  ", "npipe:", `npipe:C:\x`, "npipe://./pipe/x", "npipe:a/b",
		`npipe:\\.\pipe\`, `npipe:\\.\other\x`, `npipe:\\\pipe\x`} {
		if _, err := ParseTarget(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func deliver(t *testing.T, s Sink, r *kgo.Record) error {
	t.Helper()
	var got error
	s.Deliver(context.Background(), r, func(_ Receipt, err error) { got = err })
	return got
}

func TestRaw(t *testing.T) {
	var b bytes.Buffer
	r := &kgo.Record{Topic: "t", Value: []byte("a\x01b\nc")}
	deliver(t, &Raw{W: &b, Delimiter: []byte("\x00")}, r)
	if b.String() != "a\x01b\nc\x00" {
		t.Errorf("raw = %q", b.String())
	}
	b.Reset()
	deliver(t, &Raw{W: &b, Delimiter: []byte("\n"), Escape: true}, r)
	if b.String() != `a\x01b`+"\nc\n" {
		t.Errorf("escaped = %q", b.String())
	}
	b.Reset()
	deliver(t, &Raw{W: &b, Delimiter: []byte("\n"), Meta: true}, &kgo.Record{Topic: "t", Key: []byte("k"), Value: nil})
	meta, rest, _ := strings.Cut(b.String(), "\n")
	if !strings.Contains(meta, `"value_null":true`) || !strings.Contains(meta, `"key":"k"`) || rest != "\n" {
		t.Errorf("meta output = %q", b.String())
	}
}

func TestUnixOversize(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no unixgram on Windows")
	}
	dir, err := os.MkdirTemp("", "ntk") // t.TempDir() can exceed macOS's 104-byte socket path limit
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "s.sock")
	if _, err := DialUnix(context.Background(), path, false, false); err == nil {
		t.Fatal("expected an error when nothing listens")
	}
	l, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	u, err := DialUnix(context.Background(), path, false, false)
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err := deliver(t, u, &kgo.Record{Value: []byte("small")}); err != nil {
		t.Fatalf("small datagram: %v", err)
	}
	err = deliver(t, u, &kgo.Record{Topic: "t", Value: bytes.Repeat([]byte("x"), 128<<20)})
	var target *ErrTarget
	if !errors.As(err, &target) || !strings.Contains(err.Error(), "larger than the unix datagram limit") {
		t.Fatalf("oversized datagram: %v", err)
	}
}

func TestExec(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses /bin/sh")
	}
	var out bytes.Buffer
	e := &Exec{Command: `printf '%s:' "$NTK_KEY"; cat`, Meta: false, Stdout: &out, Stderr: &out}
	if err := deliver(t, e, &kgo.Record{Topic: "t", Key: []byte("k1"), Value: []byte("v1")}); err != nil {
		t.Fatal(err)
	}
	if out.String() != "k1:v1" {
		t.Errorf("exec output = %q", out.String())
	}
	fail := &Exec{Command: "exit 2", Stdout: &out, Stderr: &out}
	var rc Receipt
	fail.Deliver(context.Background(), &kgo.Record{}, func(r Receipt, err error) { rc = r })
	if rc.Status != "failed" || rc.ExitCode == nil || *rc.ExitCode != 2 {
		t.Errorf("failed receipt = %+v", rc)
	}
}
