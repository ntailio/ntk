// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

// Package sink implements consume output targets.
package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/record"
)

type Receipt struct {
	Topic      string `json:"topic"`
	Partition  int32  `json:"partition"`
	Offset     int64  `json:"offset"`
	ValueSize  int    `json:"value_size"`
	Status     string `json:"status"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	DurationMs int64  `json:"duration_ms"`
	Error      string `json:"error,omitempty"`
}

// Delivered reports whether the message counts as delivered (for group commits).
func (r Receipt) Delivered() bool { return r.Status == "ok" }

// Sink delivers records. Deliver may return before delivery finishes (exec
// with parallelism); results are reported through the done callback.
type Sink interface {
	Deliver(ctx context.Context, r *kgo.Record, done func(Receipt, error))
	Close() error
}

type Target struct {
	Kind string // raw, jsonl, unix, npipe, exec
	Arg  string // path or command
}

func ParseTarget(s string) (Target, error) {
	switch {
	case s == "" || s == "raw":
		return Target{Kind: "raw"}, nil
	case s == "jsonl":
		return Target{Kind: "jsonl"}, nil
	case strings.HasPrefix(s, "unix:"):
		if strings.TrimPrefix(s, "unix:") == "" {
			return Target{}, errors.New("-o unix: needs a socket path")
		}
		return Target{Kind: "unix", Arg: strings.TrimPrefix(s, "unix:")}, nil
	case strings.HasPrefix(s, "npipe:"):
		if strings.TrimPrefix(s, "npipe:") == "" {
			return Target{}, errors.New("-o npipe: needs a pipe name")
		}
		path, err := PipePath(strings.TrimPrefix(s, "npipe:"))
		if err != nil {
			return Target{}, err
		}
		return Target{Kind: "npipe", Arg: path}, nil
	case strings.HasPrefix(s, "exec:"):
		if strings.TrimSpace(strings.TrimPrefix(s, "exec:")) == "" {
			return Target{}, errors.New("-o exec: needs a command")
		}
		return Target{Kind: "exec", Arg: strings.TrimPrefix(s, "exec:")}, nil
	}
	return Target{}, fmt.Errorf("unknown consume output %q (use raw, jsonl, unix:<path>, npipe:<path>, or exec:<command>)", s)
}

// PipePath expands a bare Windows pipe name to \\.\pipe\<name>. Anything else
// must already be a pipe path, \\<host>\pipe\<name>.
func PipePath(s string) (string, error) {
	if !strings.ContainsAny(s, `\/`) {
		return `\\.\pipe\` + s, nil
	}
	rest, ok := strings.CutPrefix(s, `\\`)
	host, rest, _ := strings.Cut(rest, `\`)
	dir, name, _ := strings.Cut(rest, `\`)
	if !ok || host == "" || !strings.EqualFold(dir, "pipe") || name == "" {
		return "", fmt.Errorf(`%s is not a named pipe: use <name> or \\.\pipe\<name>`, s)
	}
	return s, nil
}

// ParseDelimiter accepts ASCII text with \n \r \t \0 \\ and \xHH escapes.
func ParseDelimiter(s string) ([]byte, error) {
	var out []byte
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 0x80 {
			return nil, fmt.Errorf("delimiter %q must be ASCII", s)
		}
		if c != '\\' {
			out = append(out, c)
			continue
		}
		if i+1 >= len(s) {
			return nil, fmt.Errorf("delimiter %q ends with a lone backslash", s)
		}
		i++
		switch s[i] {
		case 'n':
			out = append(out, '\n')
		case 'r':
			out = append(out, '\r')
		case 't':
			out = append(out, '\t')
		case '0':
			out = append(out, 0)
		case '\\':
			out = append(out, '\\')
		case 'x':
			if i+3 > len(s) {
				return nil, fmt.Errorf("delimiter %q: incomplete \\x escape", s)
			}
			v, err := strconv.ParseUint(s[i+1:i+3], 16, 8)
			if err != nil || v >= 0x80 {
				return nil, fmt.Errorf("delimiter %q: \\x%s is not an ASCII byte", s, s[i+1:i+3])
			}
			out = append(out, byte(v))
			i += 2
		default:
			return nil, fmt.Errorf("delimiter %q: unknown escape \\%c", s, s[i])
		}
	}
	if len(out) == 0 {
		return nil, errors.New("delimiter must not be empty")
	}
	return out, nil
}

// payload is what unix, npipe and exec targets receive: [meta line \n] value.
func payload(r *kgo.Record, meta bool) []byte {
	if !meta {
		return r.Value
	}
	var b bytes.Buffer
	b.Write(record.MetaLine(r))
	b.WriteByte('\n')
	b.Write(r.Value)
	return b.Bytes()
}

func receipt(r *kgo.Record, start time.Time, status string) Receipt {
	return Receipt{Topic: r.Topic, Partition: r.Partition, Offset: r.Offset, ValueSize: len(r.Value), Status: status,
		DurationMs: time.Since(start).Milliseconds()}
}

type Raw struct {
	W         io.Writer
	Meta      bool
	Delimiter []byte
	Escape    bool
}

func (s *Raw) Deliver(_ context.Context, r *kgo.Record, done func(Receipt, error)) {
	start := time.Now()
	var b bytes.Buffer
	if s.Meta {
		b.Write(record.MetaLine(r))
		b.WriteByte('\n')
	}
	if s.Escape {
		b.WriteString(escape(r.Value))
	} else {
		b.Write(r.Value)
	}
	b.Write(s.Delimiter)
	_, err := s.W.Write(b.Bytes())
	done(receipt(r, start, "ok"), err)
}

func (s *Raw) Close() error { return nil }

// escape shows control bytes other than \n and \t as \xHH, so binary values
// can't garble a terminal.
func escape(b []byte) string {
	var sb strings.Builder
	for _, c := range b {
		if (c < 0x20 && c != '\n' && c != '\t') || c == 0x7f {
			fmt.Fprintf(&sb, "\\x%02x", c)
			continue
		}
		sb.WriteByte(c)
	}
	return sb.String()
}

type JSONL struct{ W io.Writer }

func (s *JSONL) Deliver(_ context.Context, r *kgo.Record, done func(Receipt, error)) {
	start := time.Now()
	b, err := json.Marshal(record.FullOf(r))
	if err == nil {
		b = append(b, '\n')
		_, err = s.W.Write(b)
	}
	done(receipt(r, start, "ok"), err)
}

func (s *JSONL) Close() error { return nil }

// ErrTarget marks failures of the output target itself (exit code 9).
type ErrTarget struct{ Err error }

func (e *ErrTarget) Error() string { return e.Err.Error() }
func (e *ErrTarget) Unwrap() error { return e.Err }
