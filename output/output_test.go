// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package output

import (
	"strings"
	"testing"
)

type item struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type items_ []item

func (r items_) Header(wide bool) []string {
	if wide {
		return []string{"NAME", "COUNT"}
	}
	return []string{"NAME"}
}

func (r items_) Rows(wide bool) [][]string {
	var rows [][]string
	for _, i := range r {
		if wide {
			rows = append(rows, []string{i.Name, "n"})
		} else {
			rows = append(rows, []string{i.Name})
		}
	}
	return rows
}

func (r items_) Names() []string {
	var n []string
	for _, i := range r {
		n = append(n, i.Name)
	}
	return n
}

func TestWrite(t *testing.T) {
	data := items_{{"alpha", 1}, {"beta", 2}}
	tests := []struct {
		format string
		opts   Options
		want   string
	}{
		{"table", Options{}, "NAME\nalpha\nbeta\n"},
		{"table", Options{NoHeaders: true}, "alpha\nbeta\n"},
		{"wide", Options{}, "NAME   COUNT\nalpha  n\nbeta   n\n"},
		{"name", Options{}, "alpha\nbeta\n"},
		{"jsonl", Options{}, "{\"name\":\"alpha\",\"count\":1}\n{\"name\":\"beta\",\"count\":2}\n"},
		{"json", Options{}, "[\n  {\n    \"name\": \"alpha\",\n    \"count\": 1\n  },\n  {\n    \"name\": \"beta\",\n    \"count\": 2\n  }\n]\n"},
		{"template={{.name}}={{.count}}", Options{}, "alpha=1\nbeta=2\n"},
		{"template={{upper .name}} {{json .}}", Options{}, "ALPHA {\"count\":1,\"name\":\"alpha\"}\nBETA {\"count\":2,\"name\":\"beta\"}\n"},
	}
	for _, tt := range tests {
		t.Run(tt.format, func(t *testing.T) {
			f, err := Parse(tt.format)
			if err != nil {
				t.Fatal(err)
			}
			var b strings.Builder
			if err := Write(&b, f, data, tt.opts); err != nil {
				t.Fatal(err)
			}
			if b.String() != tt.want {
				t.Errorf("got:\n%q\nwant:\n%q", b.String(), tt.want)
			}
		})
	}
}

func TestParseRejectsUnknown(t *testing.T) {
	if _, err := Parse("xml"); err == nil {
		t.Fatal("expected an error")
	}
}

func TestUnsupportedFormatForType(t *testing.T) {
	f, _ := Parse("table")
	if err := Write(&strings.Builder{}, f, map[string]int{"a": 1}, Options{}); err == nil {
		t.Fatal("expected an error for a type without a table form")
	}
}
