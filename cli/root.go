// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"charm.land/huh/v2"
	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/kafka"
	"github.com/ntailio/ntk/output"
	"github.com/ntailio/ntk/profile"
)

type globalFlags struct {
	profile     string
	profilePath string
	output      string
	noHeaders   bool
	noInput     bool
	outputSet   bool
	noColor     bool
	yes         bool
	confirm     string
	dryRun      bool
	timeout     time.Duration
	verbose     bool
	debug       bool
}

type app struct {
	flags  globalFlags
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer

	store  *profile.Store
	cancel context.CancelFunc
	lines  *bufio.Reader
}

func Execute(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	a := &app{stdin: stdin, stdout: stdout, stderr: stderr}
	root := a.newRootCmd()
	root.SetArgs(args)
	root.SetIn(stdin)
	root.SetOut(stdout)
	root.SetErr(stderr)

	cmd, err := root.ExecuteContextC(ctx)
	if a.cancel != nil {
		a.cancel()
	}
	if err == nil {
		return exitcode.OK
	}
	fmt.Fprintf(stderr, "error: %v\n", err)
	code := exitcode.Of(err)
	if code == exitcode.Usage {
		fmt.Fprintf(stderr, "Run '%s --help' for usage.\n", cmd.CommandPath())
	}
	return code
}

func (a *app) newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "ntk",
		Short: "Inspect and administer Kafka clusters",
		Long: "ntk is a CLI and TUI for Kafka: topics, consumer groups, consuming and producing,\n" +
			"ACLs, principals, quotas, brokers, and cluster health.",
		Args:          usageArgs(cobra.NoArgs),
		SilenceErrors: true,
		SilenceUsage:  true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			a.flags.outputSet = cmd.Flags().Changed("output") || os.Getenv("NTK_OUTPUT") != ""
			if a.flags.timeout > 0 {
				ctx, cancel := context.WithTimeout(cmd.Context(), a.flags.timeout)
				cmd.SetContext(ctx)
				a.cancel = cancel
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !a.interactive() {
				return cmd.Help()
			}
			return a.runTUI(cmd.Context(), "", "")
		},
	}
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return exitcode.With(exitcode.Usage, err)
	})
	root.SetGlobalNormalizationFunc(func(_ *pflag.FlagSet, name string) pflag.NormalizedName {
		if name == "out" {
			name = "output"
		}
		return pflag.NormalizedName(name)
	})

	f := root.PersistentFlags()
	f.StringVarP(&a.flags.profile, "profile", "p", "", "profile to use (env NTK_PROFILE)")
	f.StringVar(&a.flags.profilePath, "profile-path", "", "profile file (env NTK_PROFILE_PATH)")
	f.StringVarP(&a.flags.output, "output", "o", envOr("NTK_OUTPUT", "table"), "output format: table, wide, json, jsonl, name, template=...")
	f.BoolVar(&a.flags.noHeaders, "no-headers", false, "omit table headers")
	f.BoolVar(&a.flags.noInput, "no-input", os.Getenv("NTK_NO_INPUT") != "", "never prompt; fail when input is missing (env NTK_NO_INPUT)")
	f.BoolVar(&a.flags.noColor, "no-color", os.Getenv("NO_COLOR") != "", "disable color")
	f.BoolVarP(&a.flags.yes, "yes", "y", false, "skip confirmation prompts")
	f.StringVar(&a.flags.confirm, "confirm", "", "with -y on prod-labelled profiles: the resource name being destroyed")
	f.BoolVar(&a.flags.dryRun, "dry-run", false, "show what would change without changing anything")
	f.DurationVar(&a.flags.timeout, "timeout", 0, "overall command timeout (e.g. 30s)")
	f.BoolVar(&a.flags.verbose, "verbose", false, "verbose logs to stderr")
	f.BoolVar(&a.flags.debug, "debug", os.Getenv("NTK_DEBUG") != "", "debug logs to stderr, including Kafka requests")
	_ = root.RegisterFlagCompletionFunc("profile", a.completeProfiles)
	_ = root.RegisterFlagCompletionFunc("output", func(cmd *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		if cmd.Name() == "consume" {
			return completeConsumeOutput(toComplete)
		}
		return []string{"table", "wide", "json", "jsonl", "name", "template="}, cobra.ShellCompDirectiveNoSpace
	})

	root.AddCommand(
		a.newProfileCmd(),
		a.newTopicCmd(),
		a.newGroupCmd(),
		a.newConsumeCmd(),
		a.newProduceCmd(),
		a.newACLCmd(),
		a.newUserCmd(),
		a.newPrincipalCmd(),
		a.newWhoamiCmd(),
		a.newQuotaCmd(),
		a.newBrokerCmd(),
		a.newClusterCmd(),
		a.newHealthCmd(),
		a.newTxCmd(),
		a.newVersionCmd(),
		a.newApplyCmd(),
		a.newTUICmd(),
	)
	a.enablePickers(root)
	return root
}

// enablePickers makes commands that take a resource name open a fuzzy picker
// when the name is missing and ntk runs in a terminal (spec/cli-conventions.md#interactive-fallbacks).
func (a *app) enablePickers(c *cobra.Command) {
	for _, sub := range c.Commands() {
		a.enablePickers(sub)
	}
	if c.RunE == nil || c.ValidArgsFunction == nil || c.Args == nil || !strings.Contains(c.Use, " <") {
		return
	}
	args, run := c.Args, c.RunE
	c.Args = func(cmd *cobra.Command, got []string) error {
		if len(got) == 0 && a.canPick() {
			return nil
		}
		return args(cmd, got)
	}
	c.RunE = func(cmd *cobra.Command, got []string) error {
		if len(got) == 0 && a.canPick() {
			v, err := a.pick(cmd)
			if err != nil {
				return err
			}
			got = []string{v}
			if err := args(cmd, got); err != nil {
				return err
			}
		}
		return run(cmd, got)
	}
}

func (a *app) canPick() bool { return a.interactive() && !a.flags.noInput }

func (a *app) pick(cmd *cobra.Command) (string, error) {
	items, _ := cmd.ValidArgsFunction(cmd, nil, "")
	if len(items) == 0 {
		return "", usageErr("nothing to choose from: pass a name")
	}
	var opts []huh.Option[string]
	for _, it := range items {
		name, desc, _ := strings.Cut(string(it), "\t")
		label := name
		if desc != "" {
			label += "  " + desc
		}
		opts = append(opts, huh.NewOption(label, name))
	}
	placeholder := strings.Fields(cmd.Use)[1]
	var v string
	sel := huh.NewSelect[string]().Title("Choose " + strings.Trim(placeholder, "<>.")).Options(opts...).Value(&v).
		Filtering(true).Height(min(len(opts)+2, 15))
	if err := a.runForm(cmd.Context(), huh.NewForm(huh.NewGroup(sel))); err != nil {
		return "", err
	}
	return v, nil
}

func (a *app) loadStore() (*profile.Store, error) {
	if a.store != nil {
		return a.store, nil
	}
	path, err := profile.ResolvePath(a.flags.profilePath)
	if err != nil {
		return nil, err
	}
	s, err := profile.Load(path)
	if err != nil {
		return nil, err
	}
	if mode, bad := profile.InsecureMode(path); bad {
		fmt.Fprintf(a.stderr, "warning: %s has mode %04o; it contains secrets. Run `ntk profile fix-perms` to set 0600.\n", path, mode)
	}
	a.store = s
	return s, nil
}

func (a *app) connect() (*kafka.Client, string, *profile.Profile, error) {
	s, err := a.loadStore()
	if err != nil {
		return nil, "", nil, err
	}
	name, p, err := a.selectProfile(s, a.flags.profile)
	if err != nil {
		return nil, "", nil, err
	}
	if !a.flags.outputSet && p.Defaults != nil && p.Defaults.Output != "" {
		a.flags.output = p.Defaults.Output
	}
	cl, err := kafka.NewClient(p, s.ResolveFile, a.clientOpts()...)
	if err != nil {
		return nil, "", nil, fmt.Errorf("profile %q: %w", name, err)
	}
	return cl, name, p, nil
}

// clientOpts adds a franz-go logger for --verbose (info) and --debug (every request).
func (a *app) clientOpts() []kgo.Opt {
	switch {
	case a.flags.debug:
		return []kgo.Opt{kgo.WithLogger(kgo.BasicLogger(a.stderr, kgo.LogLevelDebug, func() string { return "[kafka] " }))}
	case a.flags.verbose:
		return []kgo.Opt{kgo.WithLogger(kgo.BasicLogger(a.stderr, kgo.LogLevelInfo, func() string { return "[kafka] " }))}
	}
	return nil
}

func (a *app) selectProfile(s *profile.Store, name string) (string, *profile.Profile, error) {
	name, p, err := s.Select(name)
	if errors.Is(err, profile.ErrNotFound) {
		err = exitcode.With(exitcode.NotFound, err)
	}
	return name, p, err
}

func (a *app) render(v any) error {
	f, err := output.Parse(a.flags.output)
	if err != nil {
		return exitcode.With(exitcode.Usage, err)
	}
	return output.Write(a.stdout, f, v, output.Options{NoHeaders: a.flags.noHeaders})
}

func groupCmd(use, short string, aliases ...string) *cobra.Command {
	return &cobra.Command{
		Use:     use,
		Aliases: aliases,
		Short:   short,
		Args:    cobra.ArbitraryArgs,

		SuggestionsMinimumDistance: 2,
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) == 0 {
				return cmd.Help()
			}
			msg := fmt.Sprintf("unknown command %q for %q", args[0], cmd.CommandPath())
			if s := cmd.SuggestionsFor(args[0]); len(s) > 0 {
				msg += fmt.Sprintf(" (did you mean %q?)", s[0])
			}
			return exitcode.With(exitcode.Usage, errors.New(msg))
		},
	}
}

func (a *app) interactive() bool {
	in, ok1 := a.stdin.(*os.File)
	out, ok2 := a.stdout.(*os.File)
	return ok1 && ok2 && term.IsTerminal(in.Fd()) && term.IsTerminal(out.Fd())
}

func usageArgs(fn cobra.PositionalArgs) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		return exitcode.With(exitcode.Usage, fn(cmd, args))
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
