// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/prefs"
	"github.com/ntailio/ntk/topics"
)

type fetchFunc func(ctx context.Context, s *session) ([]string, error)

type cacheFile struct {
	At    time.Time `json:"at"`
	Items []string  `json:"items"`
}

func cacheDir() string {
	if x := os.Getenv("XDG_CACHE_HOME"); x != "" {
		return filepath.Join(x, "ntk", "completion")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cache", "ntk", "completion")
}

// cached returns completion items ("name\tdescription") for kind, from a
// short-lived per-profile cache or the cluster.
func (a *app) cached(ctx context.Context, kind string, fetch fetchFunc) []string {
	p, err := prefs.Load()
	if err != nil || !p.CompletionEnabled() {
		return nil
	}
	s, err := a.loadStore()
	if err != nil {
		return nil
	}
	name, _, err := s.Select(a.flags.profile)
	if err != nil {
		return nil
	}
	path := filepath.Join(cacheDir(), name, kind+".json")
	var c cacheFile
	if b, err := os.ReadFile(path); err == nil && json.Unmarshal(b, &c) == nil && time.Since(c.At) < p.CompletionTTL() {
		return c.Items
	}

	sess, err := a.session()
	if err != nil {
		return c.Items
	}
	defer sess.Close()
	ctx, cancel := context.WithTimeout(ctx, p.CompletionTimeout())
	defer cancel()
	items, err := fetch(ctx, sess)
	if err != nil {
		return c.Items
	}
	if b, err := json.Marshal(cacheFile{At: time.Now(), Items: items}); err == nil {
		if os.MkdirAll(filepath.Dir(path), 0o700) == nil {
			_ = os.WriteFile(path, b, 0o600)
		}
	}
	return items
}

func filterPrefix(items []string, prefix string, exclude []string) []string {
	var out []string
	for _, it := range items {
		name, _, _ := strings.Cut(it, "\t")
		if strings.HasPrefix(name, prefix) && !slices.Contains(exclude, name) {
			if !strings.HasPrefix(prefix, "_") && strings.HasPrefix(name, "_") {
				continue
			}
			out = append(out, it)
		}
	}
	return out
}

func fetchTopics(ctx context.Context, s *session) ([]string, error) {
	return topics.Names(ctx, s.cl.Admin, "")
}

func (a *app) complete(kind string, fetch fetchFunc, multi bool) cobra.CompletionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
		if !multi && len(args) > 0 {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		return filterPrefix(a.cached(cmd.Context(), kind, fetch), toComplete, args), cobra.ShellCompDirectiveNoFileComp
	}
}

func (a *app) completeTopicArg(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return a.complete("topics", fetchTopics, false)(cmd, args, toComplete)
}

func (a *app) completeTopicArgs(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	return a.complete("topics", fetchTopics, true)(cmd, args, toComplete)
}

// completeTopicList completes comma-separated topic lists, e.g. "--topics a,b<tab>".
func (a *app) completeTopicList(cmd *cobra.Command, _ []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
	head, last := "", toComplete
	if i := strings.LastIndexByte(toComplete, ','); i >= 0 {
		head, last = toComplete[:i+1], toComplete[i+1:]
	}
	done := strings.Split(strings.TrimSuffix(head, ","), ",")
	var out []string
	for _, it := range filterPrefix(a.cached(cmd.Context(), "topics", fetchTopics), last, done) {
		name, _, _ := strings.Cut(it, "\t")
		out = append(out, head+name)
	}
	return out, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
}

// unixPaths completes the path part of unix:<path>; bash can't file-complete behind the prefix.
func unixPaths(prefix string) []string {
	matches, _ := filepath.Glob(prefix + "*")
	out := make([]string, 0, len(matches))
	for _, m := range matches {
		if fi, err := os.Stat(m); err == nil && fi.IsDir() {
			m += string(filepath.Separator)
		}
		out = append(out, "unix:"+m)
	}
	return out
}
