// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package profile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strings"
)

var (
	ErrNoProfiles = errors.New("no profile file")
	ErrNotFound   = errors.New("profile not found")
)

func ResolvePath(flag string) (string, error) {
	if flag != "" {
		return flag, nil
	}
	if p := os.Getenv("NTK_PROFILE_PATH"); p != "" {
		return p, nil
	}
	if x := os.Getenv("XDG_CONFIG_HOME"); x != "" {
		return filepath.Join(x, "ntk", "profiles.json"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("locating profile file: %w", err)
	}
	return filepath.Join(home, ".config", "ntk", "profiles.json"), nil
}

type Store struct {
	Path string
	File *File

	// Fields ntk doesn't know (at the file level and in each profile), kept on Save.
	unknown         map[string]json.RawMessage
	unknownProfiles map[string]map[string]json.RawMessage
}

func knownKeys(v any) map[string]bool {
	keys := map[string]bool{}
	t := reflect.TypeOf(v)
	for i := range t.NumField() {
		name, _, _ := strings.Cut(t.Field(i).Tag.Get("json"), ",")
		keys[name] = true
	}
	return keys
}

func (s *Store) keepUnknown(b []byte) {
	var top map[string]json.RawMessage
	if json.Unmarshal(b, &top) != nil {
		return
	}
	known := knownKeys(File{})
	s.unknown = map[string]json.RawMessage{}
	for k, v := range top {
		if !known[k] {
			s.unknown[k] = v
		}
	}
	var profiles map[string]map[string]json.RawMessage
	if json.Unmarshal(top["profiles"], &profiles) != nil {
		return
	}
	pk := knownKeys(Profile{})
	s.unknownProfiles = map[string]map[string]json.RawMessage{}
	for name, fields := range profiles {
		for k, v := range fields {
			if !pk[k] {
				if s.unknownProfiles[name] == nil {
					s.unknownProfiles[name] = map[string]json.RawMessage{}
				}
				s.unknownProfiles[name][k] = v
			}
		}
	}
}

// RenameUnknown moves a profile's unknown fields along when it is renamed or copied.
func (s *Store) RenameUnknown(from, to string, copyOnly bool) {
	if f, ok := s.unknownProfiles[from]; ok {
		s.unknownProfiles[to] = f
		if !copyOnly {
			delete(s.unknownProfiles, from)
		}
	}
}

func (s *Store) marshal() ([]byte, error) {
	if len(s.unknown) == 0 && len(s.unknownProfiles) == 0 {
		return json.MarshalIndent(s.File, "", "  ")
	}
	out := map[string]any{}
	for k, v := range s.unknown {
		out[k] = v
	}
	out["version"] = s.File.Version
	if s.File.Active != "" {
		out["active"] = s.File.Active
	}
	profiles := map[string]any{}
	for name, p := range s.File.Profiles {
		b, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(b, &fields); err != nil {
			return nil, err
		}
		for k, v := range s.unknownProfiles[name] {
			fields[k] = v
		}
		profiles[name] = fields
	}
	out["profiles"] = profiles
	return json.MarshalIndent(out, "", "  ")
}

func Load(path string) (*Store, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w at %s (create one with `ntk profile create`)", ErrNoProfiles, path)
	}
	if err != nil {
		return nil, err
	}
	var f File
	dec := json.NewDecoder(bytes.NewReader(b))
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}
	if f.Version > CurrentVersion {
		return nil, fmt.Errorf("%s has version %d; this ntk supports up to %d (upgrade ntk)", path, f.Version, CurrentVersion)
	}
	if f.Profiles == nil {
		f.Profiles = map[string]*Profile{}
	}
	s := &Store{Path: path, File: &f}
	s.keepUnknown(b)
	return s, nil
}

func LoadOrEmpty(path string) (*Store, error) {
	s, err := Load(path)
	if errors.Is(err, ErrNoProfiles) {
		return &Store{Path: path, File: &File{Version: CurrentVersion, Profiles: map[string]*Profile{}}}, nil
	}
	return s, err
}

func (s *Store) Save() error {
	s.File.Version = CurrentVersion
	b, err := s.marshal()
	if err != nil {
		return err
	}
	b = append(b, '\n')

	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".profiles-*.json") // mode 0600, kept by the rename
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.Path)
}

func InsecureMode(path string) (fs.FileMode, bool) {
	if runtime.GOOS == "windows" {
		return 0, false
	}
	fi, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	mode := fi.Mode().Perm()
	return mode, mode&0o077 != 0
}

func (s *Store) Names() []string {
	names := make([]string, 0, len(s.File.Profiles))
	for n := range s.File.Profiles {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

func (s *Store) Select(flag string) (string, *Profile, error) {
	name, source := flag, "-p/--profile"
	if name == "" {
		name, source = os.Getenv("NTK_PROFILE"), "$NTK_PROFILE"
	}
	if name == "" {
		name, source = s.File.Active, "the active profile"
	}
	if name == "" {
		return "", nil, fmt.Errorf("no active profile in %s (set one with `ntk profile set <name>` or pass -p)", s.Path)
	}
	p, ok := s.File.Profiles[name]
	if !ok {
		err := fmt.Errorf("%w: %q (from %s) in %s (have: %s)", ErrNotFound, name, source, s.Path, strings.Join(s.Names(), ", "))
		if strings.ContainsAny(name, `/\`) || strings.HasSuffix(name, ".json") {
			err = fmt.Errorf("%w; %q looks like a file path: the profile file is set with --profile-path or $NTK_PROFILE_PATH", err, name)
		}
		return "", nil, err
	}
	return name, p, nil
}

func (s *Store) ResolveFile(p string) string {
	if p == "" || filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(filepath.Dir(s.Path), p)
}
