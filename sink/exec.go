// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package sink

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/record"
)

type Exec struct {
	Command  string
	Meta     bool
	Timeout  time.Duration
	Parallel int
	Stdout   io.Writer
	Stderr   io.Writer

	once sync.Once
	sem  chan struct{}
	wg   sync.WaitGroup
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func (s *Exec) Deliver(ctx context.Context, r *kgo.Record, done func(Receipt, error)) {
	s.once.Do(func() {
		s.sem = make(chan struct{}, max(s.Parallel, 1))
		if s.Parallel > 1 {
			var mu sync.Mutex
			s.Stdout, s.Stderr = lockedWriter{&mu, s.Stdout}, lockedWriter{&mu, s.Stderr}
		}
	})
	s.sem <- struct{}{}
	s.wg.Add(1)
	run := func() {
		defer func() { <-s.sem; s.wg.Done() }()
		done(s.run(ctx, r))
	}
	if s.Parallel <= 1 {
		run()
		return
	}
	go run()
}

// Wait blocks until all running commands have finished.
func (s *Exec) Wait() { s.wg.Wait() }

func (s *Exec) Close() error { s.wg.Wait(); return nil }

func env(r *kgo.Record) []string {
	e := append(os.Environ(),
		"NTK_META="+string(record.MetaLine(r)),
		"NTK_TOPIC="+r.Topic,
		"NTK_PARTITION="+strconv.Itoa(int(r.Partition)),
		"NTK_OFFSET="+strconv.FormatInt(r.Offset, 10),
		"NTK_TIMESTAMP="+strconv.FormatInt(r.Timestamp.UnixMilli(), 10),
	)
	if r.Key != nil {
		e = append(e, "NTK_KEY_B64="+base64.StdEncoding.EncodeToString(r.Key))
		if utf8.Valid(r.Key) && !bytes.ContainsRune(r.Key, 0) {
			e = append(e, "NTK_KEY="+string(r.Key))
		}
	}
	return e
}

func (s *Exec) run(ctx context.Context, r *kgo.Record) (Receipt, error) {
	start := time.Now()
	cmd := shell(s.Command)
	cmd.Stdin = bytes.NewReader(payload(r, s.Meta))
	cmd.Stdout, cmd.Stderr = s.Stdout, s.Stderr
	cmd.Env = env(r)
	setProcessGroup(cmd)
	if err := cmd.Start(); err != nil {
		return receipt(r, start, "failed"), &ErrTarget{fmt.Errorf("starting command: %w", err)}
	}
	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()

	var timeout <-chan time.Time
	if s.Timeout > 0 {
		t := time.NewTimer(s.Timeout)
		defer t.Stop()
		timeout = t.C
	}
	var err error
	timedOut := false
	select {
	case err = <-waitErr:
	case <-timeout:
		timedOut = true
		err = terminate(cmd, waitErr)
	case <-ctx.Done():
		err = terminate(cmd, waitErr)
	}

	rc := receipt(r, start, "ok")
	code := 0
	if cmd.ProcessState != nil {
		code = cmd.ProcessState.ExitCode()
	}
	rc.ExitCode = &code
	where := fmt.Sprintf("%s/%d @%d", r.Topic, r.Partition, r.Offset)
	switch {
	case timedOut:
		rc.Status = "timeout"
		rc.Error = fmt.Sprintf("command timed out after %s", s.Timeout)
		return rc, &ErrTarget{fmt.Errorf("%s: command timed out after %s", where, s.Timeout)}
	case ctx.Err() != nil:
		rc.Status = "failed"
		return rc, ctx.Err()
	case err != nil:
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			rc.Status = "failed"
			rc.Error = fmt.Sprintf("exit status %d", code)
			return rc, &ErrTarget{fmt.Errorf("%s: command exited with status %d", where, code)}
		}
		rc.Status = "failed"
		return rc, &ErrTarget{fmt.Errorf("%s: %w", where, err)}
	}
	return rc, nil
}
