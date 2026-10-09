// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/testkit"
)

func TestMain(m *testing.M) {
	for _, k := range []string{"NTK_PROFILE", "NTK_PROFILE_PATH", "NTK_OUTPUT", "NTK_DEBUG", "ACCESSIBLE"} {
		os.Unsetenv(k)
	}
	tmp, _ := os.MkdirTemp("", "ntk-cli-test-")
	os.Setenv("XDG_CACHE_HOME", tmp)
	os.Setenv("XDG_CONFIG_HOME", tmp)
	os.Setenv("XDG_STATE_HOME", tmp)
	os.MkdirAll(tmp+"/ntk", 0o700)
	os.WriteFile(tmp+"/ntk/config.json", []byte(`{"completion":{"timeout":"20s"}}`), 0o600)
	defer os.RemoveAll(tmp)
	if cl, err := kgo.NewClient(kgo.SeedBrokers(testkit.Bootstrap()...)); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		if _, err := kadm.NewClient(cl).BrokerMetadata(ctx); err == nil {
			_, _ = testkit.Janitor(ctx, cl, time.Hour)
		}
		cancel()
		cl.Close()
	}
	code := m.Run()
	os.RemoveAll(tmp)
	os.Exit(code)
}

type env struct {
	t    *testing.T
	path string
	adm  *kadm.Client
}

// newEnv skips unless the sandbox is running, and writes a profile file for it.
func newEnv(t *testing.T, extra map[string]any) *env {
	t.Parallel()
	adm := testkit.Admin(t)
	return &env{t: t, path: testkit.Profiles(t, extra), adm: adm}
}

func (e *env) runIn(stdin string, args ...string) (string, string, int) {
	e.t.Helper()
	var stdout, stderr strings.Builder
	// Not t.Context(): that is canceled before cleanups run, and cleanups call ntk too.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	code := Execute(ctx, append([]string{"--profile-path", e.path}, args...), strings.NewReader(stdin), &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}

func (e *env) run(args ...string) (string, string, int) { return e.runIn("", args...) }

// ok runs ntk and fails the test unless it exits 0.
func (e *env) ok(args ...string) string {
	e.t.Helper()
	out, errOut, code := e.run(args...)
	if code != 0 {
		e.t.Fatalf("ntk %s: exit %d\nstdout: %s\nstderr: %s", strings.Join(args, " "), code, out, errOut)
	}
	return out
}

func (e *env) okIn(stdin string, args ...string) string {
	e.t.Helper()
	out, errOut, code := e.runIn(stdin, args...)
	if code != 0 {
		e.t.Fatalf("ntk %s: exit %d\nstdout: %s\nstderr: %s", strings.Join(args, " "), code, out, errOut)
	}
	return out
}

// fails runs ntk and fails the test unless it exits with want.
func (e *env) fails(want int, args ...string) (string, string) {
	e.t.Helper()
	out, errOut, code := e.run(args...)
	if code != want {
		e.t.Fatalf("ntk %s: exit %d, want %d\nstdout: %s\nstderr: %s", strings.Join(args, " "), code, want, out, errOut)
	}
	return out, errOut
}

func contains(t *testing.T, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Errorf("output does not contain %q:\n%s", sub, s)
		}
	}
}

func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// eventually retries check until it succeeds or 10s pass: Kafka metadata
// (new topics, config changes, deletions) reaches all brokers asynchronously.
func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	eventuallyWithin(t, 10*time.Second, what, check)
}

func eventuallyWithin(t *testing.T, d time.Duration, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// out runs ntk and returns stdout, or "" if it failed (for use inside eventually).
func (e *env) out(args ...string) string {
	o, _, code := e.run(args...)
	if code != 0 {
		return ""
	}
	return o
}

func (e *env) gone(topic string) {
	e.t.Helper()
	eventually(e.t, "topic "+topic+" to be deleted", func() bool {
		_, _, code := e.run("topic", "describe", topic)
		return code == 4
	})
}
