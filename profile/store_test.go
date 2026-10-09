// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestSaveCreatesPrivateFileAndDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not apply on Windows")
	}
	path := filepath.Join(t.TempDir(), "ntk", "profiles.json")
	s, err := LoadOrEmpty(path)
	if err != nil {
		t.Fatal(err)
	}
	s.File.Profiles["dev"] = &Profile{BootstrapServers: []string{"localhost:9092"}, Auth: Auth{Mechanism: "none"}}
	s.File.Active = "dev"
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}

	assertMode(t, path, 0o600)
	assertMode(t, filepath.Dir(path), 0o700)

	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.File.Active != "dev" || got.File.Profiles["dev"].BootstrapServers[0] != "localhost:9092" {
		t.Errorf("round trip mismatch: %+v", got.File)
	}
	if got.File.Version != CurrentVersion {
		t.Errorf("version = %d, want %d", got.File.Version, CurrentVersion)
	}
}

func TestSaveTightensLooseMode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not apply on Windows")
	}
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, []byte(`{"version":1,"profiles":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if mode, bad := InsecureMode(path); !bad || mode != 0o644 {
		t.Fatalf("InsecureMode = %o, %v; want 644, true", mode, bad)
	}
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	if _, bad := InsecureMode(path); bad {
		t.Error("file still insecure after Save")
	}
}

func TestLoadMissing(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.json"))
	if !errors.Is(err, ErrNoProfiles) {
		t.Fatalf("err = %v, want ErrNoProfiles", err)
	}
}

func TestLoadRejectsNewerVersion(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, []byte(`{"version":99,"profiles":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected an error for a newer file version")
	}
}

func TestResolvePath(t *testing.T) {
	t.Setenv("NTK_PROFILE_PATH", "")
	t.Setenv("XDG_CONFIG_HOME", "/xdg")
	t.Setenv("HOME", "/home/test")

	if got, _ := ResolvePath("/flag.json"); got != "/flag.json" {
		t.Errorf("flag: got %s", got)
	}
	if got, _ := ResolvePath(""); got != filepath.Join("/xdg", "ntk", "profiles.json") {
		t.Errorf("xdg: got %s", got)
	}
	t.Setenv("NTK_PROFILE_PATH", "/env.json")
	if got, _ := ResolvePath(""); got != "/env.json" {
		t.Errorf("env: got %s", got)
	}
	t.Setenv("NTK_PROFILE_PATH", "")
	t.Setenv("XDG_CONFIG_HOME", "")
	if runtime.GOOS != "windows" {
		if got, _ := ResolvePath(""); got != "/home/test/.config/ntk/profiles.json" {
			t.Errorf("home: got %s", got)
		}
	}
}

func TestSelect(t *testing.T) {
	s := &Store{Path: "/p/profiles.json", File: &File{
		Active:   "a",
		Profiles: map[string]*Profile{"a": {}, "b": {}, "c": {}},
	}}
	t.Setenv("NTK_PROFILE", "")
	if name, _, _ := s.Select(""); name != "a" {
		t.Errorf("active: got %s", name)
	}
	t.Setenv("NTK_PROFILE", "b")
	if name, _, _ := s.Select(""); name != "b" {
		t.Errorf("env: got %s", name)
	}
	if name, _, _ := s.Select("c"); name != "c" {
		t.Errorf("flag: got %s", name)
	}
	if _, _, err := s.Select("zzz"); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing: err = %v, want ErrNotFound", err)
	}
}

func TestResolveFile(t *testing.T) {
	s := &Store{Path: filepath.Join("/repo", "sandbox", "profiles.json")}
	if got := s.ResolveFile("certs/ca.crt"); got != filepath.Join("/repo", "sandbox", "certs", "ca.crt") {
		t.Errorf("relative: got %s", got)
	}
	if got := s.ResolveFile("/abs/ca.crt"); got != "/abs/ca.crt" {
		t.Errorf("absolute: got %s", got)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := fi.Mode().Perm(); got != want {
		t.Errorf("%s: mode %04o, want %04o", path, got, want)
	}
}

func TestSaveKeepsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	in := `{"version":1,"active":"a","future_setting":{"x":1},"profiles":{"a":{"bootstrap_servers":["h:1"],"auth":{"mechanism":"none"},"added_later":"keep me"}}}`
	if err := os.WriteFile(path, []byte(in), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	s.File.Profiles["a"].Description = "changed"
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	for _, want := range []string{`"future_setting"`, `"added_later": "keep me"`, `"description": "changed"`} {
		if !strings.Contains(string(b), want) {
			t.Errorf("saved file lost %s:\n%s", want, b)
		}
	}
}
