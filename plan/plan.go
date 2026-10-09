// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package plan

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Class int

const (
	SafeWrite Class = iota
	Change
	Destructive
)

func (c Class) String() string {
	switch c {
	case Change:
		return "change"
	case Destructive:
		return "destructive"
	}
	return "safe-write"
}

func (c Class) MarshalJSON() ([]byte, error) { return json.Marshal(c.String()) }

func (c *Class) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	switch s {
	case "safe-write":
		*c = SafeWrite
	case "change":
		*c = Change
	case "destructive":
		*c = Destructive
	default:
		return fmt.Errorf("unknown plan class %q", s)
	}
	return nil
}

type Plan struct {
	Kind      string          `json:"kind"`
	Profile   string          `json:"profile"`
	ClusterID string          `json:"cluster_id"`
	Class     Class           `json:"class"`
	Summary   string          `json:"summary"`
	Changes   []string        `json:"changes"`
	Warnings  []string        `json:"warnings,omitempty"`
	Confirm   string          `json:"confirm_name,omitempty"`
	Typed     bool            `json:"always_typed,omitempty"`
	Payload   json.RawMessage `json:"payload"`
}

func New(kind string, class Class, summary string, payload any) (*Plan, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return &Plan{Kind: kind, Class: class, Summary: summary, Payload: b}, nil
}

func (p *Plan) Add(format string, args ...any) {
	p.Changes = append(p.Changes, fmt.Sprintf(format, args...))
}

func (p *Plan) Warn(format string, args ...any) {
	p.Warnings = append(p.Warnings, fmt.Sprintf(format, args...))
}

func (p *Plan) Empty() bool { return len(p.Changes) == 0 }

func (p *Plan) Decode(v any) error { return json.Unmarshal(p.Payload, v) }

func (p *Plan) String() string {
	var b strings.Builder
	b.WriteString(p.Summary)
	b.WriteByte('\n')
	for _, c := range p.Changes {
		b.WriteString("  " + c + "\n")
	}
	for _, w := range p.Warnings {
		b.WriteString("  ! " + w + "\n")
	}
	return b.String()
}
