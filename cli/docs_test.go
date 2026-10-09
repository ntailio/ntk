// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/ntailio/ntk/testkit"
)

var fence = regexp.MustCompile("(?s)```[a-z]*\n(.*?)```")

// TestDocCommands checks that every `ntk …` command in a code block of the
// README, the guide, and the spec exists, with the flags it uses.
func TestDocCommands(t *testing.T) {
	root := testkit.RepoRoot(t)
	files := []string{filepath.Join(root, "README.md")}
	for _, dir := range []string{"docs", "spec"} {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(p string, d os.DirEntry, err error) error {
			if err == nil && strings.HasSuffix(p, ".md") {
				files = append(files, p)
			}
			return err
		})
	}

	cmd := (&app{}).newRootCmd()
	cmd.InitDefaultHelpCmd()
	cmd.InitDefaultCompletionCmd()

	checked := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		rel, _ := filepath.Rel(root, f)
		for _, block := range fence.FindAllStringSubmatch(string(b), -1) {
			for _, line := range strings.Split(strings.ReplaceAll(block[1], "\\\n", " "), "\n") {
				for _, seg := range shellCommands(strings.TrimPrefix(strings.TrimSpace(line), "$ ")) {
					if len(seg) == 0 || seg[0] != "ntk" || (len(seg) > 1 && strings.HasPrefix(seg[1], "__")) {
						continue
					}
					checked++
					for _, p := range checkCommand(cmd, seg[1:]) {
						t.Errorf("%s: %s\n    %s", rel, p, strings.Join(seg, " "))
					}
				}
			}
		}
	}
	if checked < 100 {
		t.Errorf("only %d commands found in the docs; is the parser broken?", checked)
	}
}

// shellCommands splits a shell line into commands (at |, &&, ||, ;), each a
// list of words with quotes removed. A # comment or a redirection ends a command.
func shellCommands(line string) [][]string {
	var cmds [][]string
	var words []string
	var word strings.Builder
	inWord, quote, skip := false, byte(0), false
	flush := func() {
		if inWord && !skip {
			words = append(words, word.String())
		}
		word.Reset()
		inWord = false
	}
	end := func() {
		flush()
		cmds = append(cmds, words)
		words, skip = nil, false
	}
	for i := 0; i < len(line); i++ {
		c := line[i]
		switch {
		case quote != 0:
			if c == quote {
				quote = 0
			} else {
				word.WriteByte(c)
			}
		case c == '\'' || c == '"':
			quote, inWord = c, true
		case c == ' ' || c == '\t':
			flush()
		case c == '#' && !inWord:
			skip = true
		case c == '|' || c == ';' || c == '&':
			if i+1 < len(line) && line[i+1] == c {
				i++
			}
			end()
		case (c == '>' || c == '<') && !inWord:
			skip = true
		default:
			word.WriteByte(c)
			inWord = true
		}
	}
	end()
	return cmds
}

func checkCommand(root *cobra.Command, args []string) []string {
	var problems []string
	cmd, descending := root, true
	for i := 0; i < len(args); i++ {
		a := args[i]
		if strings.HasPrefix(a, "-") && len(a) > 1 && (a[1] < '0' || a[1] > '9') {
			name, _, hasValue := strings.Cut(strings.TrimLeft(a, "-"), "=")
			f := lookupFlag(cmd, name, strings.HasPrefix(a, "--"))
			switch {
			case name == "h" || name == "help":
			case f == nil:
				problems = append(problems, fmt.Sprintf("unknown flag %s on %q", a, cmd.CommandPath()))
			case !hasValue && f.NoOptDefVal == "":
				i++
			}
			continue
		}
		if !descending {
			continue
		}
		if sub := subcommand(cmd, a); sub != nil {
			cmd = sub
			continue
		}
		descending = false
		if cmd.HasAvailableSubCommands() && !strings.ContainsAny(cmd.Use, "<[") && !placeholder(a) {
			problems = append(problems, fmt.Sprintf("unknown command %q under %q", a, cmd.CommandPath()))
		}
	}
	return problems
}

func lookupFlag(cmd *cobra.Command, name string, long bool) *pflag.Flag {
	if long {
		return cmd.Flag(name)
	}
	if len(name) != 1 {
		return nil
	}
	for _, fs := range []*pflag.FlagSet{cmd.Flags(), cmd.PersistentFlags(), cmd.InheritedFlags()} {
		if f := fs.ShorthandLookup(name); f != nil {
			return f
		}
	}
	return nil
}

func subcommand(cmd *cobra.Command, name string) *cobra.Command {
	for _, c := range cmd.Commands() {
		if c.Name() == name || slices.Contains(c.Aliases, name) {
			return c
		}
	}
	return nil
}

// placeholder reports words like <topic>, [flags], … that stand for something.
func placeholder(w string) bool {
	return strings.ContainsAny(w, "<>[]…") || strings.Contains(w, "...") || strings.Contains(w, "|")
}

func TestCheckCommand(t *testing.T) {
	root := (&app{}).newRootCmd()
	for line, wantProblems := range map[string]int{
		"ntk topic list":                                    0,
		"ntk -p prod t ls 'orders.*' -o json":               0,
		"ntk c orders --from -1h -o 'exec:jq -c .' | wc -l": 0,
		"ntk topic create orders --bogus":                   1,
		"ntk topic lsit":                                    1,
		"ntk g describe billing -Z":                         1,
		"ntk topic describe <topic>":                        0,
	} {
		seg := shellCommands(line)[0]
		if got := checkCommand(root, seg[1:]); len(got) != wantProblems {
			t.Errorf("%s: %d problems %v, want %d", line, len(got), got, wantProblems)
		}
	}
	if got := shellCommands(`ntk a -m | ntk b 'x | y' # c`); len(got) != 2 || len(got[1]) != 3 || got[1][2] != "x | y" {
		t.Errorf("shellCommands = %q", got)
	}
}
