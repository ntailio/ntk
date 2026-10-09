// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"path/filepath"
	"testing"
)

func TestNextColor(t *testing.T) {
	f := &File{Profiles: map[string]*Profile{}}
	if got := f.NextColor(); got != Palette[0] {
		t.Errorf("empty file: %s, want %s", got, Palette[0])
	}
	f.Profiles["a"] = &Profile{Color: Palette[0]}
	f.Profiles["b"] = &Profile{Color: "red"}
	if got := f.NextColor(); got != Palette[1] {
		t.Errorf("first one used: %s, want %s", got, Palette[1])
	}
	for i, c := range Palette {
		f.Profiles[string(rune('c'+i))] = &Profile{Color: c}
	}
	f.Profiles["upper"] = &Profile{Color: "#FB923C"} // Palette[1] again, in capitals
	if got := f.NextColor(); got != Palette[2] {
		t.Errorf("all used once, the second twice: %s, want %s", got, Palette[2])
	}
}

func TestValidColor(t *testing.T) {
	for _, c := range []string{"#ff00ee", "#FF00EE", "red", "cyan", Palette[29]} {
		if !ValidColor(c) {
			t.Errorf("%q should be valid", c)
		}
	}
	for _, c := range []string{"ff00ee", "#ff00e", "#ff00eeff", "purple", "Red", "#gg0000"} {
		if ValidColor(c) {
			t.Errorf("%q should be invalid", c)
		}
	}
}

func TestSaveGivesProfilesAColor(t *testing.T) {
	s := &Store{Path: filepath.Join(t.TempDir(), "profiles.json"), File: &File{Profiles: map[string]*Profile{
		"a": {Color: "green"},
		"b": {},
		"c": {},
	}}}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, err := Load(s.Path)
	if err != nil {
		t.Fatal(err)
	}
	p := loaded.File.Profiles
	if p["a"].Color != "green" {
		t.Errorf("an existing colour changed: %s", p["a"].Color)
	}
	if p["b"].Color == "" || p["c"].Color == "" || p["b"].Color == p["c"].Color {
		t.Errorf("b and c should get different colours: %q %q", p["b"].Color, p["c"].Color)
	}
}
