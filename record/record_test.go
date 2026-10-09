// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package record

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRoundTrip(t *testing.T) {
	in := []*kgo.Record{
		{Topic: "t", Partition: 2, Offset: 7, Key: []byte("k"), Value: []byte(`{"a":1}`), Timestamp: time.UnixMilli(1790000000123),
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("v")}, {Key: "bin", Value: []byte{0xff, 0x00}}}},
		{Topic: "t", Key: []byte{0x00, 0xfe}, Value: []byte{0xde, 0xad, 0xbe, 0xef}},
		{Topic: "t", Key: []byte("tomb"), Value: nil},
		{Topic: "t", Key: nil, Value: []byte{}},
	}
	for i, r := range in {
		b, err := json.Marshal(FullOf(r))
		if err != nil {
			t.Fatal(err)
		}
		f, err := Parse(b)
		if err != nil {
			t.Fatal(err)
		}
		out, err := f.ToKgo()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Key, r.Key) || (out.Key == nil) != (r.Key == nil) {
			t.Errorf("%d: key %q → %q", i, r.Key, out.Key)
		}
		if !bytes.Equal(out.Value, r.Value) || (out.Value == nil) != (r.Value == nil) {
			t.Errorf("%d: value %q → %q (nil %v→%v)", i, r.Value, out.Value, r.Value == nil, out.Value == nil)
		}
		if len(out.Headers) != len(r.Headers) {
			t.Errorf("%d: headers %v → %v", i, r.Headers, out.Headers)
		}
		for j := range r.Headers {
			if !bytes.Equal(out.Headers[j].Value, r.Headers[j].Value) {
				t.Errorf("%d: header %d %q → %q", i, j, r.Headers[j].Value, out.Headers[j].Value)
			}
		}
		if !r.Timestamp.IsZero() && !out.Timestamp.Equal(r.Timestamp) {
			t.Errorf("%d: timestamp %v → %v", i, r.Timestamp, out.Timestamp)
		}
	}
}

func TestMetaLine(t *testing.T) {
	line := MetaLine(&kgo.Record{Topic: "orders", Partition: 3, Offset: 9, Key: []byte{0xff}, Value: []byte("xyz")})
	if bytes.Contains(line, []byte("\n")) {
		t.Fatal("meta line contains a newline")
	}
	var m Meta
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatal(err)
	}
	if m.Key != nil || m.KeyB64 == nil || *m.KeyB64 != "/w==" || m.ValueSize != 3 || m.ValueNull {
		t.Errorf("meta = %+v", m)
	}
}

func TestMetaRoundTrip(t *testing.T) {
	in := []*kgo.Record{
		{Topic: "t", Key: []byte("k"), Value: []byte("a\nb"), Timestamp: time.UnixMilli(1790000000123),
			Headers: []kgo.RecordHeader{{Key: "h", Value: []byte("v")}, {Key: "null", Value: nil}, {Key: "empty", Value: []byte{}}, {Key: "bin", Value: []byte{0xff}}}},
		{Topic: "t", Key: []byte{0x00, 0xfe}, Value: []byte{0xde, 0xad}},
		{Topic: "t", Key: []byte("tomb"), Value: nil},
		{Topic: "t", Key: nil, Value: []byte{}},
	}
	for i, r := range in {
		m, err := ParseMeta(MetaLine(r))
		if err != nil {
			t.Fatal(err)
		}
		out, err := m.ToKgo(r.Value)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out.Key, r.Key) || (out.Key == nil) != (r.Key == nil) {
			t.Errorf("%d: key %q → %q", i, r.Key, out.Key)
		}
		if !bytes.Equal(out.Value, r.Value) || (out.Value == nil) != (r.Value == nil) {
			t.Errorf("%d: value %q → %q (nil %v→%v)", i, r.Value, out.Value, r.Value == nil, out.Value == nil)
		}
		for j, h := range r.Headers {
			got := out.Headers[j].Value
			if !bytes.Equal(got, h.Value) || (got == nil) != (h.Value == nil) {
				t.Errorf("%d: header %q %q → %q", i, h.Key, h.Value, got)
			}
		}
		if !r.Timestamp.IsZero() && !out.Timestamp.Equal(r.Timestamp) {
			t.Errorf("%d: timestamp %v → %v", i, r.Timestamp, out.Timestamp)
		}
	}
	if _, err := (Meta{ValueNull: true}).ToKgo([]byte("x")); err == nil {
		t.Error("value_null with value bytes: expected an error")
	}
}
