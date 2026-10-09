// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/factualtech/ntk/configs"
	"github.com/factualtech/ntk/exitcode"
	"github.com/factualtech/ntk/kafka"
	"github.com/factualtech/ntk/prefs"
)

type configRows struct {
	entries []configs.Entry
	docs    bool
}

func (r configRows) Header(bool) []string {
	h := []string{"KEY", "VALUE", "SOURCE", "DEFAULT"}
	if r.docs {
		h = append(h, "DESCRIPTION")
	}
	return h
}

func (r configRows) Rows(bool) [][]string {
	var rows [][]string
	for _, e := range r.entries {
		row := []string{e.Key, e.Human(), e.Source, e.Default()}
		if e.ReadOnly {
			row[2] += " (read-only)"
		}
		if r.docs {
			doc, _, _ := strings.Cut(e.Documentation, ". ")
			row = append(row, doc)
		}
		rows = append(rows, row)
	}
	return rows
}

func (r configRows) Names() []string {
	var n []string
	for _, e := range r.entries {
		n = append(n, e.Key)
	}
	return n
}

type configView struct {
	all, overridesOnly, docs bool
}

func (v *configView) flags(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&v.all, "all", false, "show every key")
	cmd.Flags().BoolVar(&v.overridesOnly, "overrides-only", false, "only keys set on this resource")
	cmd.Flags().BoolVar(&v.docs, "show-docs", false, "add a one-line description per key")
}

func (v *configView) filter(entries []configs.Entry, kind configs.Kind) []configs.Entry {
	if v.all {
		return entries
	}
	important := configs.Important
	if p, err := prefs.Load(); err == nil && len(p.ImportantKeys) > 0 {
		important = p.ImportantKeys
	}
	return slices.DeleteFunc(slices.Clone(entries), func(e configs.Entry) bool {
		if e.Overridden(kind) {
			return false
		}
		return v.overridesOnly || kind != configs.Topic || !slices.Contains(important, e.Key)
	})
}

func (a *app) newTopicConfigCmd() *cobra.Command {
	var view configView
	cmd := &cobra.Command{
		Use:   "config <topic>",
		Short: "Show and change topic configuration",
		Long: "Without a subcommand, shows the topic's configuration: overrides plus important keys,\n" +
			"with where each value comes from.",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.showConfig(cmd, configs.Topic, args[0], view)
		},
	}
	view.flags(cmd)

	get := &cobra.Command{
		Use:               "get <topic> <key>",
		Short:             "Print one config value",
		Args:              usageArgs(cobra.ExactArgs(2)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.getConfig(cmd, configs.Topic, args[0], args[1])
		},
	}
	set := &cobra.Command{
		Use:     "set <topic> key=value...",
		Short:   "Set config values (incremental; k+=v / k-=v append to / subtract from lists)",
		Example: "  ntk topic config set orders retention.ms=14d max.message.bytes=4MiB",
		Args:    usageArgs(cobra.MinimumNArgs(2)),
		RunE: func(cmd *cobra.Command, args []string) error {
			ops, err := configs.ParseAssignments(args[1:])
			if err != nil {
				return exitcode.With(exitcode.Usage, err)
			}
			return a.alterConfig(cmd, configs.Topic, args[0], ops)
		},
		ValidArgsFunction: a.completeConfigKeys(configs.Topic),
	}
	unset := &cobra.Command{
		Use:               "unset <topic> <key>...",
		Short:             "Remove overrides (revert to the default)",
		Args:              usageArgs(cobra.MinimumNArgs(2)),
		ValidArgsFunction: a.completeConfigKeys(configs.Topic),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.alterConfig(cmd, configs.Topic, args[0], deleteOps(args[1:]))
		},
	}
	var file string
	var prune bool
	apply := &cobra.Command{
		Use:     "apply <topic> -f file.json",
		Short:   "Make the topic's overrides match a JSON file of key: value",
		Example: "  ntk topic config export orders > orders.json && $EDITOR orders.json && ntk topic config apply orders -f orders.json",
		Args:    usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.applyConfigFile(cmd, configs.Topic, args[0], file, prune)
		},
		ValidArgsFunction: a.completeTopicArg,
	}
	apply.Flags().StringVarP(&file, "file", "f", "", "JSON object of key: value (\"-\" for stdin)")
	apply.Flags().BoolVar(&prune, "prune", false, "also remove overrides that are not in the file")
	_ = apply.MarkFlagRequired("file")

	var profileB string
	diff := &cobra.Command{
		Use:               "diff <topicA> <topicB>",
		Short:             "Compare two topics' effective configs (optionally across profiles)",
		Args:              usageArgs(cobra.ExactArgs(2)),
		ValidArgsFunction: a.completeTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			left, err := configs.Describe(cmd.Context(), s.cl.Client, configs.Topic, args[0])
			if err != nil {
				return err
			}
			sb := s
			if profileB != "" {
				if sb, err = a.sessionFor(profileB); err != nil {
					return err
				}
				defer sb.Close()
			}
			right, err := configs.Describe(cmd.Context(), sb.cl.Client, configs.Topic, args[1])
			if err != nil {
				return err
			}
			return a.render(diffConfigs(left, right, labelFor(s, args[0], profileB != ""), labelFor(sb, args[1], profileB != "")))
		},
	}
	diff.Flags().StringVar(&profileB, "profile-b", "", "profile for the second topic")
	_ = diff.RegisterFlagCompletionFunc("profile-b", a.completeProfiles)

	export := &cobra.Command{
		Use:               "export <topic>...",
		Short:             "Print overrides as JSON (the format `apply -f` reads)",
		Args:              usageArgs(cobra.MinimumNArgs(1)),
		ValidArgsFunction: a.completeTopicArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			all := map[string]map[string]string{}
			for _, t := range args {
				entries, err := configs.Describe(cmd.Context(), s.cl.Client, configs.Topic, t)
				if err != nil {
					return fmt.Errorf("topic %q: %w", t, err)
				}
				all[t] = configs.Overrides(entries, configs.Topic)
			}
			enc := json.NewEncoder(a.stdout)
			enc.SetIndent("", "  ")
			if len(args) == 1 {
				return enc.Encode(all[args[0]])
			}
			return enc.Encode(all)
		},
	}
	cmd.AddCommand(get, set, unset, apply, diff, export)
	return cmd
}

func labelFor(s *session, topic string, withProfile bool) string {
	if withProfile {
		return s.name + ":" + topic
	}
	return topic
}

func deleteOps(keys []string) []configs.Op {
	ops := make([]configs.Op, len(keys))
	for i, k := range keys {
		ops[i] = configs.Op{Key: k, Op: "delete"}
	}
	return ops
}

func (a *app) showConfig(cmd *cobra.Command, kind configs.Kind, name string, view configView) error {
	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	entries, err := configs.Describe(cmd.Context(), s.cl.Client, kind, name)
	if err != nil {
		return err
	}
	shown := view.filter(entries, kind)
	if a.flags.output == "json" || a.flags.output == "jsonl" {
		return a.render(shown)
	}
	return a.render(configRows{entries: shown, docs: view.docs})
}

func (a *app) getConfig(cmd *cobra.Command, kind configs.Kind, name, key string) error {
	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	entries, err := configs.Describe(cmd.Context(), s.cl.Client, kind, name)
	if err != nil {
		return err
	}
	e, ok := configs.Find(entries, key)
	if !ok {
		msg := fmt.Sprintf("unknown config %q", key)
		if sug := configs.Suggest(key, entries); sug != "" {
			msg += fmt.Sprintf(" (did you mean %q?)", sug)
		}
		return exitcode.With(exitcode.NotFound, fmt.Errorf("%s", msg))
	}
	if a.flags.output == "json" {
		return a.render(e)
	}
	fmt.Fprintln(a.stdout, e.String())
	return nil
}

func (a *app) alterConfig(cmd *cobra.Command, kind configs.Kind, name string, ops []configs.Op) error {
	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	pl, err := configs.PlanAlter(cmd.Context(), s.cl.Client, s.cl.Admin, kind, name, ops)
	if err != nil {
		return exitcode.With(exitcode.Usage, err)
	}
	return a.run(cmd.Context(), s, pl)
}

func (a *app) applyConfigFile(cmd *cobra.Command, kind configs.Kind, name, file string, prune bool) error {
	var b []byte
	var err error
	if file == "-" {
		b, err = readAll(a.stdin)
	} else {
		b, err = os.ReadFile(file)
	}
	if err != nil {
		return err
	}
	var desired map[string]any
	if err := json.Unmarshal(b, &desired); err != nil {
		return usageErr("%s: expected a JSON object of key: value: %v", file, err)
	}
	values := map[string]string{}
	for k, v := range desired {
		values[k] = fmt.Sprint(v)
	}
	s, err := a.session()
	if err != nil {
		return err
	}
	defer s.Close()
	entries, err := configs.Describe(cmd.Context(), s.cl.Client, kind, name)
	if err != nil {
		return err
	}
	pl, err := configs.PlanAlter(cmd.Context(), s.cl.Client, s.cl.Admin, kind, name, configs.FileOps(values, entries, kind, prune))
	if err != nil {
		return exitcode.With(exitcode.Usage, err)
	}
	return a.run(cmd.Context(), s, pl)
}

type configDiff struct {
	Key   string `json:"key"`
	Left  string `json:"left"`
	Right string `json:"right"`
}

type configDiffRows struct {
	left, right string
	diffs       []configDiff
}

func (r configDiffRows) Header(bool) []string {
	return []string{"KEY", strings.ToUpper(r.left), strings.ToUpper(r.right)}
}

func (r configDiffRows) Rows(bool) [][]string {
	var rows [][]string
	for _, d := range r.diffs {
		rows = append(rows, []string{d.Key, d.Left, d.Right})
	}
	return rows
}

func (r configDiffRows) MarshalJSON() ([]byte, error) { return json.Marshal(r.diffs) }

func diffConfigs(left, right []configs.Entry, ln, rn string) configDiffRows {
	l, rm := map[string]configs.Entry{}, map[string]configs.Entry{}
	for _, e := range left {
		l[e.Key] = e
	}
	for _, e := range right {
		rm[e.Key] = e
	}
	keys := slices.Sorted(maps.Keys(l))
	for k := range rm {
		if _, ok := l[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	out := configDiffRows{left: ln, right: rn}
	for _, k := range keys {
		a, b := l[k], rm[k]
		if a.String() != b.String() {
			out.diffs = append(out.diffs, configDiff{k, orDash(a.Human()), orDash(b.Human())})
		}
	}
	return out
}

func (a *app) completeConfigKeys(kind configs.Kind) cobra.CompletionFunc {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]cobra.Completion, cobra.ShellCompDirective) {
		if len(args) == 0 {
			if kind == configs.Topic {
				return a.completeTopicArg(cmd, args, toComplete)
			}
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		items := a.cached(cmd.Context(), "config-keys-"+strings.ToLower(kind.String()), func(ctx context.Context, s *session) ([]string, error) {
			entries, err := configs.Describe(ctx, s.cl.Client, kind, args[0])
			if err != nil {
				return nil, err
			}
			var keys []string
			for _, e := range entries {
				if !e.ReadOnly {
					keys = append(keys, e.Key)
				}
			}
			return keys, nil
		})
		var out []string
		for _, k := range filterPrefix(items, toComplete, nil) {
			if cmd.Name() == "set" {
				k += "="
			}
			out = append(out, k)
		}
		return out, cobra.ShellCompDirectiveNoFileComp | cobra.ShellCompDirectiveNoSpace
	}
}

func (a *app) sessionFor(profileName string) (*session, error) {
	s, err := a.loadStore()
	if err != nil {
		return nil, err
	}
	name, p, err := a.selectProfile(s, profileName)
	if err != nil {
		return nil, err
	}
	cl, err := kafka.NewClient(p, s.ResolveFile, a.clientOpts()...)
	if err != nil {
		return nil, err
	}
	return &session{cl: cl, name: name, prof: p, resolve: s.ResolveFile, opts: a.clientOpts()}, nil
}
