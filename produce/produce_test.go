// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package produce

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/record"
)

func metaStream(delim string, recs ...*kgo.Record) []byte {
	var b bytes.Buffer
	for _, r := range recs {
		b.Write(record.MetaLine(r))
		b.WriteByte('\n')
		b.Write(r.Value)
		b.WriteString(delim)
	}
	return b.Bytes()
}

func drain(t *testing.T, src Source) ([]*kgo.Record, error) {
	t.Helper()
	var out []*kgo.Record
	for {
		r, err := src(context.Background())
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, r)
	}
}

func TestMetaStream(t *testing.T) {
	in := []*kgo.Record{
		{Topic: "a", Partition: 4, Key: []byte("k"), Value: []byte("has\na newline\n")},
		{Topic: "a", Key: []byte("tomb"), Value: nil},
		{Topic: "a", Value: []byte{0x00, 0xff}},
	}
	for _, delim := range []string{"\n", "\x00", "\n---\n"} {
		out, err := drain(t, MetaStream(bytes.NewReader(metaStream(delim, in...)), []byte(delim), "dst", Keep{}))
		if err != nil || len(out) != len(in) {
			t.Fatalf("delim %q: %d records, %v", delim, len(out), err)
		}
		for i, r := range out {
			if r.Topic != "dst" || r.Partition != -1 || !bytes.Equal(r.Value, in[i].Value) || (r.Value == nil) != (in[i].Value == nil) || !bytes.Equal(r.Key, in[i].Key) {
				t.Errorf("delim %q record %d = %+v", delim, i, r)
			}
		}
	}

	out, _ := drain(t, MetaStream(bytes.NewReader(metaStream("\n", in[0])), []byte("\n"), "dst", Keep{Topic: true, Partition: true}))
	if out[0].Topic != "a" || out[0].Partition != 4 {
		t.Errorf("keep: %+v", out[0])
	}

	full := metaStream("\n", in[0])
	for name, data := range map[string][]byte{
		"wrong delimiter": metaStream("\x00", in[0]),
		"truncated value": full[:len(full)-4],
		"not meta":        []byte("plain value\n"),
	} {
		if _, err := drain(t, MetaStream(bytes.NewReader(data), []byte("\n"), "dst", Keep{})); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
	if out, err := drain(t, MetaStream(bytes.NewReader(bytes.TrimSuffix(full, []byte("\n"))), []byte("\n"), "dst", Keep{})); err != nil || len(out) != 1 {
		t.Errorf("missing final delimiter: %d, %v", len(out), err)
	}
}

func TestLimitAndUntilCancel(t *testing.T) {
	src := Lines(strings.NewReader("a\nb\nc\n"), []byte("\n"), nil, kgo.Record{})
	if out, _ := drain(t, Limit(src, 2)); len(out) != 2 {
		t.Errorf("Limit: %d records", len(out))
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := UntilCancel(Lines(strings.NewReader("a\n"), []byte("\n"), nil, kgo.Record{}))(ctx); !errors.Is(err, io.EOF) {
		t.Errorf("UntilCancel after cancel: %v", err)
	}
}
