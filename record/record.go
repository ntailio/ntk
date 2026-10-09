// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

// Package record encodes Kafka records as the consume metadata line and the
// lossless jsonl "ntk record format" (spec/features/consuming.md#output).
package record

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Header struct {
	Key      string  `json:"key"`
	Value    *string `json:"value,omitempty"`
	ValueB64 *string `json:"value_b64,omitempty"`
}

type Meta struct {
	Topic         string   `json:"topic"`
	Partition     int32    `json:"partition"`
	Offset        int64    `json:"offset"`
	Timestamp     string   `json:"timestamp"`
	TimestampType string   `json:"timestamp_type"`
	Key           *string  `json:"key"`
	KeyB64        *string  `json:"key_b64,omitempty"`
	Headers       []Header `json:"headers"`
	ValueSize     int      `json:"value_size"`
	ValueNull     bool     `json:"value_null"`
}

type Full struct {
	Topic         string   `json:"topic"`
	Partition     int32    `json:"partition"`
	Offset        int64    `json:"offset"`
	Timestamp     string   `json:"timestamp"`
	TimestampType string   `json:"timestamp_type"`
	Key           *string  `json:"key"`
	KeyB64        *string  `json:"key_b64,omitempty"`
	Headers       []Header `json:"headers"`
	Value         *string  `json:"value"`
	ValueB64      *string  `json:"value_b64,omitempty"`
}

func text(b []byte) (s *string, b64 *string) {
	if b == nil {
		return nil, nil
	}
	if utf8.Valid(b) {
		v := string(b)
		return &v, nil
	}
	v := base64.StdEncoding.EncodeToString(b)
	return nil, &v
}

func tsType(r *kgo.Record) string {
	if r.Attrs.TimestampType() == 1 {
		return "log_append"
	}
	return "create"
}

func headers(r *kgo.Record) []Header {
	hs := make([]Header, 0, len(r.Headers))
	for _, h := range r.Headers {
		v, b64 := text(h.Value)
		hs = append(hs, Header{Key: h.Key, Value: v, ValueB64: b64})
	}
	return hs
}

func MetaOf(r *kgo.Record) Meta {
	m := Meta{
		Topic: r.Topic, Partition: r.Partition, Offset: r.Offset,
		Timestamp: r.Timestamp.UTC().Format("2006-01-02T15:04:05.000Z07:00"), TimestampType: tsType(r),
		Headers: headers(r), ValueSize: len(r.Value), ValueNull: r.Value == nil,
	}
	m.Key, m.KeyB64 = text(r.Key)
	return m
}

func FullOf(r *kgo.Record) Full {
	m := MetaOf(r)
	f := Full{Topic: m.Topic, Partition: m.Partition, Offset: m.Offset, Timestamp: m.Timestamp, TimestampType: m.TimestampType,
		Key: m.Key, KeyB64: m.KeyB64, Headers: m.Headers}
	f.Value, f.ValueB64 = text(r.Value)
	return f
}

// MetaLine is the single-line JSON metadata, without a trailing newline.
func MetaLine(r *kgo.Record) []byte {
	b, _ := json.Marshal(MetaOf(r))
	return b
}

func decode(s, b64 *string) ([]byte, error) {
	switch {
	case b64 != nil:
		return base64.StdEncoding.DecodeString(*b64)
	case s != nil:
		return []byte(*s), nil
	}
	return nil, nil
}

// Parse reads one jsonl line in the ntk record format.
func Parse(line []byte) (Full, error) {
	var f Full
	if err := json.Unmarshal(line, &f); err != nil {
		return Full{}, err
	}
	return f, nil
}

// ParseMeta reads a metadata line as written by consume -m.
func ParseMeta(line []byte) (Meta, error) {
	var m Meta
	if err := json.Unmarshal(line, &m); err != nil {
		return Meta{}, err
	}
	return m, nil
}

// ToKgo converts a parsed record back into a producible record. Topic, partition,
// and timestamp are copied; callers decide which to keep.
func (f Full) ToKgo() (*kgo.Record, error) {
	value, err := decode(f.Value, f.ValueB64)
	if err != nil {
		return nil, fmt.Errorf("value_b64: %w", err)
	}
	return build(f.Topic, f.Partition, f.Timestamp, f.Key, f.KeyB64, f.Headers, value)
}

// ToKgo builds a record from the metadata and the value bytes that followed it.
func (m Meta) ToKgo(value []byte) (*kgo.Record, error) {
	if m.ValueNull {
		if len(value) > 0 {
			return nil, fmt.Errorf("value_null is true but %d value bytes follow", len(value))
		}
		value = nil
	} else if value == nil {
		value = []byte{}
	}
	return build(m.Topic, m.Partition, m.Timestamp, m.Key, m.KeyB64, m.Headers, value)
}

func build(topic string, partition int32, ts string, key, keyB64 *string, headers []Header, value []byte) (*kgo.Record, error) {
	k, err := decode(key, keyB64)
	if err != nil {
		return nil, fmt.Errorf("key_b64: %w", err)
	}
	r := &kgo.Record{Topic: topic, Partition: partition, Key: k, Value: value}
	if ts != "" {
		if t, err := time.Parse(time.RFC3339Nano, ts); err == nil {
			r.Timestamp = t
		}
	}
	for _, h := range headers {
		v, err := decode(h.Value, h.ValueB64)
		if err != nil {
			return nil, fmt.Errorf("header %q: %w", h.Key, err)
		}
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: h.Key, Value: v})
	}
	return r, nil
}

var ErrEmpty = errors.New("empty line")
