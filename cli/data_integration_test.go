// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/consume"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/groups"
	"github.com/ntailio/ntk/record"
	"github.com/ntailio/ntk/sink"
	"github.com/ntailio/ntk/testkit"
)

func TestProduceConsume(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "pc", 3)

	contains(t, e.okIn("", "produce", name, "-k", "order-1", "-v", `{"status":"FAILED"}`, "-H", "tenant=acme"))
	e.okIn("a\nb\nc\n", "p", name)
	e.okIn("k1:v1\nk2:v2\n", "p", name, "--key-sep", ":")
	e.okIn("x\x00y\x00", "p", name, "--delimiter", `\0`)
	e.ok("p", name, "-k", "t{{.i}}", "-v", "n{{.i}}", "--template", "--count", "3")
	e.ok("p", name, "-k", "gone", "--tombstone")

	vals := lines(e.ok("c", name, "--from", "earliest"))
	for _, want := range []string{`{"status":"FAILED"}`, "a", "b", "c", "v1", "v2", "x", "y", "n1", "n2", "n3"} {
		if !slices.Contains(vals, want) {
			t.Errorf("consumed values missing %q: %v", want, vals)
		}
	}
	if len(vals) != 11 {
		t.Errorf("consumed %d non-empty values, want 11 (+1 tombstone): %v", len(vals), vals)
	}

	metaOut := e.ok("c", name, "--from", "earliest", "-m", "--key-partition", "order-1", "-n", "1")
	var meta record.Meta
	if err := json.Unmarshal([]byte(lines(metaOut)[0]), &meta); err != nil || *meta.Key != "order-1" || meta.Headers[0].Key != "tenant" {
		t.Errorf("meta line: %s %v", metaOut, err)
	}
	if want := consume.KeyPartition([]byte("order-1"), 3); meta.Partition != want {
		t.Errorf("key-partition: record in %d, murmur2 says %d", meta.Partition, want)
	}

	export := filepath.Join(t.TempDir(), "export.jsonl")
	os.WriteFile(export, []byte(e.ok("c", name, "--from", "earliest", "-o", "jsonl")), 0o600)
	replay := testkit.Topic(t, e.adm, "replay", 1)
	e.ok("p", replay, "--in", "jsonl", "-f", export)
	replayed := e.ok("c", replay, "--from", "earliest", "-o", "jsonl")
	if n := len(lines(replayed)); n != 12 {
		t.Errorf("replayed %d records, want 12", n)
	}
	contains(t, replayed, `"value":null`, `"key":"order-1"`)

	cp := testkit.Topic(t, e.adm, "copy", 2)
	e.ok("p", cp, "--from-topic", name, "--from", "earliest")
	if n := len(lines(e.ok("c", cp, "--from", "earliest", "-o", "jsonl"))); n != 12 {
		t.Errorf("copied %d records, want 12", n)
	}

	e.fails(exitcode.Usage, "c", name, "--delimiter", "x", "-o", "jsonl")
	e.fails(exitcode.Usage, "c", name, "--receipt")
	e.fails(exitcode.Usage, "c", name+","+replay, "-P", "0")
	e.fails(exitcode.Usage, "c", name, "-o", "table")
	e.fails(exitcode.Usage, "p", name, "-v", "x", "-f", export)
	e.fails(exitcode.Usage, "p", name, "--keep-partition")

	rawFile := filepath.Join(t.TempDir(), "values.txt")
	os.WriteFile(rawFile, []byte("r1\nr2\n"), 0o600)
	fromFile := testkit.Topic(t, e.adm, "fromfile", 1)
	e.ok("p", fromFile, "-f", rawFile)
	if got := lines(e.ok("c", fromFile, "--from", "earliest")); !slices.Equal(got, []string{"r1", "r2"}) {
		t.Errorf("-f raw: %q", got)
	}
	e.fails(exitcode.NotFound, "c", testkit.Name(t, "missing"), "--from", "earliest")

	until := e.ok("c", name, "--from", "earliest", "--until", "@1", "-P", "0")
	if n := len(lines(until)); n > 1 {
		t.Errorf("--until @1 on one partition returned %d messages", n)
	}
}

func TestConsumeExec(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "exec", 1)
	testkit.Produce(t, name, "ok-1", "FAILED-2", "ok-3")

	found := e.ok("c", name, "--from", "earliest", "-o", `exec:if grep -q FAILED; then echo "$NTK_KEY@$NTK_OFFSET"; fi`)
	if strings.TrimSpace(found) != "k1@1" {
		t.Errorf("exec output = %q", found)
	}
	out, _ := e.fails(exitcode.OutputTarget, "c", name, "--from", "earliest", "-o", "exec:exit 3")
	_ = out
	receipts := e.ok("c", name, "--from", "earliest", "-o", "exec:exit 3", "--exec-continue", "--receipt")
	var r sink.Receipt
	if err := json.Unmarshal([]byte(lines(receipts)[0]), &r); err != nil || r.Status != "failed" || *r.ExitCode != 3 {
		t.Errorf("receipt: %s %v", receipts, err)
	}
	if n := len(lines(receipts)); n != 3 {
		t.Errorf("%d receipts, want 3", n)
	}
	e.fails(exitcode.OutputTarget, "c", name, "--from", "earliest", "-o", "exec:sleep 5", "--exec-timeout", "300ms")
	par := e.ok("c", name, "--from", "earliest", "-m", "-o", "exec:head -1", "--exec-parallel", "3")
	if n := len(lines(par)); n != 3 {
		t.Errorf("parallel exec printed %d meta lines, want 3", n)
	}
}

func TestConsumeUnix(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "unix", 1)
	testkit.Produce(t, name, "one", "two\nlines")

	path := filepath.Join(t.TempDir(), "ntk.sock")
	e.fails(exitcode.OutputTarget, "c", name, "--from", "earliest", "-o", "unix:"+path)

	conn, err := net.ListenUnixgram("unixgram", &net.UnixAddr{Name: path, Net: "unixgram"})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	var got [][]byte
	var mu sync.Mutex
	go func() {
		buf := make([]byte, 1<<20)
		for {
			n, err := conn.Read(buf)
			if err != nil {
				return
			}
			mu.Lock()
			got = append(got, bytes.Clone(buf[:n]))
			mu.Unlock()
		}
	}()
	receipts := e.ok("c", name, "--from", "earliest", "-m", "-o", "unix:"+path, "--receipt")
	if n := len(lines(receipts)); n != 2 {
		t.Errorf("%d receipts, want 2", n)
	}
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("received %d datagrams, want 2", len(got))
	}
	meta, value, _ := bytes.Cut(got[1], []byte("\n"))
	if !bytes.HasPrefix(meta, []byte(`{"topic":"`+name)) || string(value) != "two\nlines" {
		t.Errorf("datagram = %q", got[1])
	}
}

// records normalizes a jsonl dump to what a copy must keep: key, headers, value.
func records(t *testing.T, jsonl string) []string {
	t.Helper()
	var out []string
	for _, l := range lines(jsonl) {
		f, err := record.Parse([]byte(l))
		if err != nil {
			t.Fatal(err)
		}
		f.Topic, f.Partition, f.Offset, f.Timestamp, f.TimestampType = "", 0, 0, "", ""
		b, _ := json.Marshal(f)
		out = append(out, string(b))
	}
	slices.Sort(out)
	return out
}

func TestProduceUnix(t *testing.T) {
	e := newEnv(t, nil)
	src := testkit.Topic(t, e.adm, "uds-src", 2)
	cl, err := kgo.NewClient(kgo.SeedBrokers(testkit.Bootstrap()...))
	if err != nil {
		t.Fatal(err)
	}
	err = cl.ProduceSync(t.Context(),
		&kgo.Record{Topic: src, Key: []byte("k"), Value: []byte("a\nb"),
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("v")}, {Key: "null", Value: nil}, {Key: "empty", Value: []byte{}}}},
		&kgo.Record{Topic: src, Value: []byte{0x00, 0xff, '\n'}},
		&kgo.Record{Topic: src, Key: []byte("tomb"), Value: nil},
		&kgo.Record{Topic: src, Key: []byte("e"), Value: []byte{}},
	).FirstErr()
	cl.Close()
	if err != nil {
		t.Fatal(err)
	}
	want := records(t, e.ok("c", src, "--from", "earliest", "-o", "jsonl"))
	if len(want) != 4 {
		t.Fatalf("source has %d records", len(want))
	}

	// ntk → socket → ntk, stopped by a signal (context cancellation) once everything arrived.
	dst := testkit.Topic(t, e.adm, "uds-dst", 1)
	sock := filepath.Join(t.TempDir(), "p.sock")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		code   int
		stderr string
	}
	done := make(chan result, 1)
	go func() {
		var stdout, stderr strings.Builder
		code := Execute(ctx, []string{"--profile-path", e.path, "p", dst, "--in", "unix:" + sock, "-m"}, strings.NewReader(""), &stdout, &stderr)
		done <- result{code, stderr.String()}
	}()
	e.ok("c", src, "--from", "earliest", "-m", "-o", "unix:"+sock, "--unix-wait")
	eventually(t, "the copy to arrive", func() bool { return len(lines(e.out("c", dst, "--from", "earliest", "-o", "jsonl"))) == 4 })
	cancel()
	r := <-done
	if r.code != 0 || !strings.Contains(r.stderr, "Produced 4 messages") {
		t.Errorf("listener: exit %d, stderr %s", r.code, r.stderr)
	}
	if _, err := os.Stat(sock); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("socket file left behind: %v", err)
	}
	if got := records(t, e.ok("c", dst, "--from", "earliest", "-o", "jsonl")); !slices.Equal(got, want) {
		t.Errorf("socket copy differs:\n got %v\nwant %v", got, want)
	}

	// ntk → stdin → ntk.
	piped := testkit.Topic(t, e.adm, "uds-pipe", 1)
	e.okIn(e.ok("c", src, "--from", "earliest", "-m"), "p", piped, "-m")
	if got := records(t, e.ok("c", piped, "--from", "earliest", "-o", "jsonl")); !slices.Equal(got, want) {
		t.Errorf("stdin -m copy differs:\n got %v\nwant %v", got, want)
	}

	// External sender, value only, stopped by --idle-timeout.
	plain := testkit.Topic(t, e.adm, "uds-plain", 1)
	sock2 := filepath.Join(t.TempDir(), "p.sock")
	go func() {
		for range 100 {
			if c, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: sock2, Net: "unixgram"}); err == nil {
				c.Write([]byte("hello"))
				c.Close()
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	e.ok("p", plain, "--in", "unix:"+sock2, "--idle-timeout", "1s", "-H", "via=uds")
	if got := e.ok("c", plain, "--from", "earliest", "-m"); !strings.Contains(got, `"key":null,"headers":[{"key":"via","value":"uds"}]`) || !strings.HasSuffix(strings.TrimSpace(got), "hello") {
		t.Errorf("plain datagram: %s", got)
	}

	e.fails(exitcode.Usage, "p", plain, "--in", "unix:"+sock2, "-f", "x")
	e.fails(exitcode.Usage, "p", plain, "--idle-timeout", "1s")
}

func TestTransactionalProduce(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "txp", 1)
	e.okIn("a\nb\n", "p", name, "--transactional-id", testkit.Name(t, "txid"))
	eventually(t, "read_committed to see the committed transaction", func() bool {
		return len(lines(e.out("c", name, "--from", "earliest", "--isolation", "read_committed"))) == 2
	})
	e.fails(exitcode.Usage, "p", name, "-v", "x", "--acks", "1", "--transactional-id", "t")
}

func TestGroups(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "grp", 2)
	group := testkit.Group(t, e.adm, "g")
	testkit.Produce(t, name, "1", "2", "3", "4", "5", "6")

	e.ok("c", name, "-g", group, "--from", "earliest", "-n", "4")
	var gs []groups.Group
	json.Unmarshal([]byte(e.ok("group", "list", group, "--lag", "-o", "json")), &gs)
	if len(gs) != 1 || gs[0].Lag != 2 || gs[0].Type != "classic" {
		t.Fatalf("group list: %+v", gs)
	}
	contains(t, e.ok("g", "list", "--topic", name), group)
	desc := e.ok("group", "describe", group)
	contains(t, desc, "Total lag: 2", name)

	e.ok("group", "lag", group, "--threshold", "5")
	e.fails(exitcode.CheckFailed, "group", "lag", group, "--threshold", "1")

	dry := e.ok("group", "reset", group, "--topic", name, "--to-earliest")
	contains(t, dry, "(dry run)", "Reset offsets")
	e.ok("group", "reset", group, "--topic", name, "--to-earliest", "--execute", "-y")
	gs = nil
	json.Unmarshal([]byte(e.ok("group", "list", group, "--lag", "-o", "json")), &gs)
	if len(gs) != 1 || gs[0].Lag != 6 {
		t.Errorf("after reset to earliest: %+v", gs)
	}
	e.ok("group", "reset", group, "--topic", name+":0", "--shift-by", "2", "--execute", "-y")
	e.ok("group", "reset", group, "--all-topics", "--to-latest", "--execute", "-y")

	csv := filepath.Join(t.TempDir(), "offsets.csv")
	os.WriteFile(csv, []byte(name+",0,1\n"+name+",1,1\n"), 0o600)
	e.ok("group", "reset", group, "--from-file", csv, "--execute", "-y")
	gs = nil
	json.Unmarshal([]byte(e.ok("group", "list", group, "--lag", "-o", "json")), &gs)
	if len(gs) != 1 || gs[0].Lag != 4 {
		t.Errorf("after reset from file: %+v", gs)
	}
	e.fails(exitcode.Usage, "group", "reset", group, "--topic", name)

	e.ok("group", "delete-offsets", group, "--topic", name+":0", "-y")
	e.ok("group", "delete", group, "-y")
	eventually(t, "group deleted", func() bool { _, _, code := e.run("group", "describe", group); return code == 4 })
}

func TestMonitoring(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "mon", 2)
	group := testkit.Group(t, e.adm, "mon")
	testkit.Produce(t, name, "1", "2", "3")
	e.ok("c", name, "-g", group, "--from", "earliest", "-n", "1")

	contains(t, e.ok("topic", "stats", name, "--sample", "1s"), "MSG/S")
	var stats []map[string]any
	if err := json.Unmarshal([]byte(e.ok("topic", "stats", name, "--sample", "1s", "-o", "json")), &stats); err != nil || len(stats) != 1 {
		t.Errorf("stats json: %v %v", stats, err)
	}
	contains(t, e.ok("topic", "top", "--once", "-n", "500"), name)
	contains(t, e.ok("group", "top", "--once"), group)
	contains(t, e.ok("group", "lag", group), "no members")
	contains(t, e.ok("group", "lag", group, "--fail-on-stuck", "--stuck-after", "1s", "--sample", "1s"), "no members")
	out := e.ok("topic", "watch", name, "--interval", "1s", "-o", "jsonl", "--timeout", "2500ms")
	if n := len(lines(out)); n < 2 {
		t.Errorf("topic watch emitted %d samples", n)
	}
	out = e.ok("group", "watch", group, "--interval", "1s", "-o", "jsonl", "--timeout", "2500ms")
	if n := len(lines(out)); n < 2 {
		t.Errorf("group watch emitted %d samples", n)
	}
}
