// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package output

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"text/tabwriter"
	"text/template"
)

type Kind string

const (
	Table    Kind = "table"
	Wide     Kind = "wide"
	JSON     Kind = "json"
	JSONL    Kind = "jsonl"
	Name     Kind = "name"
	Template Kind = "template"
)

type Format struct {
	Kind     Kind
	Template string
}

func Parse(s string) (Format, error) {
	if t, ok := strings.CutPrefix(s, "template="); ok {
		return Format{Kind: Template, Template: t}, nil
	}
	switch k := Kind(s); k {
	case Table, Wide, JSON, JSONL, Name:
		return Format{Kind: k}, nil
	}
	return Format{}, fmt.Errorf("unknown output format %q (use table, wide, json, jsonl, name, or template=...)", s)
}

type Tabular interface {
	Header(wide bool) []string
	Rows(wide bool) [][]string
}

type Named interface {
	Names() []string
}

type Options struct {
	NoHeaders bool
}

func Write(w io.Writer, f Format, v any, o Options) error {
	switch f.Kind {
	case Table, Wide:
		t, ok := v.(Tabular)
		if !ok {
			return fmt.Errorf("this command does not support -o %s", f.Kind)
		}
		return writeTable(w, t, f.Kind == Wide, o)
	case JSON:
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(v)
	case JSONL:
		enc := json.NewEncoder(w)
		for _, item := range items(v) {
			if err := enc.Encode(item); err != nil {
				return err
			}
		}
		return nil
	case Name:
		n, ok := v.(Named)
		if !ok {
			return errors.New("this command does not support -o name")
		}
		for _, name := range n.Names() {
			if _, err := fmt.Fprintln(w, name); err != nil {
				return err
			}
		}
		return nil
	case Template:
		return writeTemplate(w, f.Template, v)
	}
	return fmt.Errorf("unknown output format %q", f.Kind)
}

func writeTable(w io.Writer, t Tabular, wide bool, o Options) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	if !o.NoHeaders {
		fmt.Fprintln(tw, strings.Join(t.Header(wide), "\t"))
	}
	for _, row := range t.Rows(wide) {
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	return tw.Flush()
}

var funcs = template.FuncMap{
	"upper": strings.ToUpper,
	"lower": strings.ToLower,
	"join": func(sep string, v any) string {
		var parts []string
		for _, x := range items(v) {
			parts = append(parts, fmt.Sprint(x))
		}
		return strings.Join(parts, sep)
	},
	"json": func(v any) (string, error) {
		b, err := json.Marshal(v)
		return string(b), err
	},
	"default": func(def, v any) any {
		if v == nil || v == "" {
			return def
		}
		return v
	},
}

// Templates run over each item's JSON form, so they use the same field names as -o json.
func writeTemplate(w io.Writer, tmpl string, v any) error {
	t, err := template.New("output").Option("missingkey=zero").Funcs(funcs).Parse(tmpl)
	if err != nil {
		return fmt.Errorf("parsing output template: %w", err)
	}
	for _, item := range items(v) {
		b, err := json.Marshal(item)
		if err != nil {
			return err
		}
		var data any
		if err := json.Unmarshal(b, &data); err != nil {
			return err
		}
		if err := t.Execute(w, data); err != nil {
			return err
		}
		if _, err := io.WriteString(w, "\n"); err != nil {
			return err
		}
	}
	return nil
}

func items(v any) []any {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return []any{v}
	}
	out := make([]any, rv.Len())
	for i := range out {
		out[i] = rv.Index(i).Interface()
	}
	return out
}
