// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/term"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/actions"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/kafka"
	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/profile"
)

type session struct {
	cl      *kafka.Client
	name    string
	prof    *profile.Profile
	resolve func(string) string
	opts    []kgo.Opt
}

func (a *app) session() (*session, error) {
	cl, name, p, err := a.connect()
	if err != nil {
		return nil, err
	}
	return &session{cl: cl, name: name, prof: p, resolve: a.store.ResolveFile, opts: a.clientOpts()}, nil
}

func newConsumerClient(s *session, opts ...kgo.Opt) (*kafka.Client, error) {
	return kafka.NewClient(s.prof, s.resolve, append(slices.Clone(s.opts), opts...)...)
}

func (s *session) Close() { s.cl.Close() }

// run shows, confirms, and applies a plan.
func (a *app) run(ctx context.Context, s *session, pl *plan.Plan) error {
	pl.Profile = s.name
	if pl.ClusterID == "" {
		ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if md, err := s.cl.Admin.BrokerMetadata(ctx); err == nil {
			pl.ClusterID = md.Cluster
		}
		cancel()
	}
	if pl.Empty() {
		fmt.Fprintln(a.stderr, "Nothing to change.")
		return nil
	}
	if a.flags.dryRun {
		return a.showDryRun(s, pl)
	}
	if s.prof.ReadOnly {
		return exitcode.With(exitcode.Refused, fmt.Errorf("refused: profile %q is read-only", s.name))
	}
	if err := a.confirmPlan(s, pl); err != nil {
		return err
	}
	if err := actions.Apply(ctx, s.cl, pl); err != nil {
		return err
	}
	fmt.Fprintf(a.stderr, "✓ %s\n", strings.TrimSuffix(pl.Summary, ":"))
	return nil
}

func (a *app) showDryRun(s *session, pl *plan.Plan) error {
	if a.flags.output == "json" {
		return a.render(pl)
	}
	fmt.Fprintf(a.stdout, "%s (dry run)\n%s", a.badge(s), indentPlan(pl))
	return nil
}

func (a *app) confirmPlan(s *session, pl *plan.Plan) error {
	if pl.Class == plan.SafeWrite {
		return nil
	}
	typed := pl.Class == plan.Destructive && pl.Confirm != "" && (pl.Typed || slices.Contains(s.prof.Labels, "prod"))
	if a.flags.yes {
		if pl.Typed {
			fmt.Fprintf(a.stderr, "%s %s", a.badge(s), indentPlan(pl))
		}
		if typed && a.flags.confirm != pl.Confirm {
			why := fmt.Sprintf("profile %q is labelled prod", s.name)
			if pl.Typed {
				why = "this operation always needs typed confirmation"
			}
			return exitcode.With(exitcode.Usage, fmt.Errorf("%s: -y also needs --confirm=%q", why, pl.Confirm))
		}
		return nil
	}
	if !a.canPrompt() {
		return exitcode.With(exitcode.Usage, errors.New("confirmation required but stdin is not a terminal; re-run with -y"))
	}
	fmt.Fprintf(a.stderr, "%s %s", a.badge(s), indentPlan(pl))
	if typed {
		answer := a.prompt(fmt.Sprintf("Type %q to confirm: ", pl.Confirm))
		if answer != pl.Confirm {
			return exitcode.With(exitcode.Refused, errors.New("confirmation did not match; nothing changed"))
		}
		return nil
	}
	answer := strings.ToLower(a.prompt("Apply? [y/N] "))
	if answer != "y" && answer != "yes" {
		return exitcode.With(exitcode.Refused, errors.New("aborted; nothing changed"))
	}
	return nil
}

func indentPlan(pl *plan.Plan) string { return pl.String() }

var badgeColors = map[string]string{"red": "1", "green": "2", "yellow": "3", "blue": "4", "magenta": "5", "cyan": "6"}

func (a *app) badge(s *session) string {
	b := "[" + s.name + "]"
	if a.flags.noColor || !stderrTTY(a) || s.prof.Color == "" {
		return b
	}
	c := s.prof.Color
	if code, ok := badgeColors[c]; ok {
		c = code
	}
	return lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color(c)).Render(b)
}

func (a *app) canPrompt() bool {
	if a.flags.noInput {
		return false
	}
	if accessible() {
		return true
	}
	f, ok := a.stdin.(*os.File)
	return ok && term.IsTerminal(f.Fd())
}

func (a *app) prompt(q string) string {
	fmt.Fprint(a.stderr, q)
	if a.lines == nil {
		a.lines = bufio.NewReader(a.stdin)
	}
	line, _ := a.lines.ReadString('\n')
	return strings.TrimSpace(line)
}

func (a *app) newApplyCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "apply <plan.json>",
		Short: "Apply a plan saved with --dry-run -o json",
		Args:  usageArgs(cobra.ExactArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			b, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			var pl plan.Plan
			if err := json.Unmarshal(b, &pl); err != nil {
				return exitcode.With(exitcode.Usage, fmt.Errorf("%s is not a plan: %w", args[0], err))
			}
			if !actions.Known(pl.Kind) {
				return exitcode.With(exitcode.Usage, fmt.Errorf("unknown plan kind %q", pl.Kind))
			}
			if a.flags.profile == "" {
				a.flags.profile = pl.Profile
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			ctx, cancel := context.WithTimeout(cmd.Context(), 10*time.Second)
			md, err := s.cl.Admin.BrokerMetadata(ctx)
			cancel()
			if err != nil {
				return err
			}
			if pl.ClusterID != "" && md.Cluster != pl.ClusterID {
				return exitcode.With(exitcode.Refused, fmt.Errorf("plan was made for cluster %s, but profile %q is cluster %s", pl.ClusterID, s.name, md.Cluster))
			}
			return a.run(cmd.Context(), s, &pl)
		},
	}
}
