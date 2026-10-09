// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package prefs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/factualtech/ntk/profile"
)

type Prefs struct {
	Presets       map[string]map[string]string `json:"presets,omitempty"`
	ImportantKeys []string                     `json:"important_keys,omitempty"`
	TUI           TUI                          `json:"tui"`
	Completion    Completion                   `json:"completion"`
	LagThresholds map[string]int64             `json:"lag_thresholds,omitempty"`
}

type TUI struct {
	Refresh    profile.Duration  `json:"refresh,omitzero"`
	StartView  string            `json:"start_view,omitempty"`
	BufferSize int               `json:"buffer_size,omitempty"`
	Mouse      bool              `json:"mouse,omitempty"`
	Keys       map[string]string `json:"keys,omitempty"`
	Colors     map[string]string `json:"colors,omitempty"`
}

type Completion struct {
	Enabled  *bool            `json:"enabled,omitempty"`
	CacheTTL profile.Duration `json:"cache_ttl,omitzero"`
	Timeout  profile.Duration `json:"timeout,omitzero"`
}

var builtinPresets = map[string]map[string]string{
	"compacted": {"cleanup.policy": "compact", "min.compaction.lag.ms": "1h", "segment.ms": "1d"},
	"durable":   {"min.insync.replicas": "2", "unclean.leader.election.enable": "false"},
}

func Path() string {
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "ntk", "config.json")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "ntk", "config.json")
}

func Load() (*Prefs, error) {
	p := &Prefs{}
	b, err := os.ReadFile(Path())
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		if err := json.Unmarshal(b, p); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", Path(), err)
		}
	}
	return p, nil
}

func (p *Prefs) Preset(name string) (map[string]string, bool) {
	if v, ok := p.Presets[name]; ok {
		return v, true
	}
	v, ok := builtinPresets[name]
	return v, ok
}

func (p *Prefs) PresetNames() []string {
	var names []string
	for n := range builtinPresets {
		names = append(names, n)
	}
	for n := range p.Presets {
		if _, dup := builtinPresets[n]; !dup {
			names = append(names, n)
		}
	}
	return names
}

func (p *Prefs) RefreshInterval() time.Duration {
	if p.TUI.Refresh.Duration > 0 {
		return p.TUI.Refresh.Duration
	}
	return 5 * time.Second
}

func (p *Prefs) CompletionEnabled() bool { return p.Completion.Enabled == nil || *p.Completion.Enabled }

func (p *Prefs) CompletionTTL() time.Duration {
	if p.Completion.CacheTTL.Duration > 0 {
		return p.Completion.CacheTTL.Duration
	}
	return 10 * time.Second
}

func (p *Prefs) CompletionTimeout() time.Duration {
	if p.Completion.Timeout.Duration > 0 {
		return p.Completion.Timeout.Duration
	}
	return time.Second
}
