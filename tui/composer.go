// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"charm.land/bubbles/v2/textarea"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/factualtech/ntk/kafka"
	"github.com/factualtech/ntk/profile"
)

type composer struct {
	vid     int
	topic   string
	cl      *kafka.Client
	ctx     context.Context
	prof    *profile.Profile
	profile string

	key, headers, partition textinput.Model
	value                   textarea.Model
	focus                   int
	status                  string
	statusErr               bool
	history                 []historyEntry
	histPos                 int
	width, height           int
	sent                    int
}

type historyEntry struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Headers   string `json:"headers"`
	Partition string `json:"partition"`
}

type sentMsg struct {
	vid int
	r   *kgo.Record
	err error
}

type editedMsg struct {
	vid   int
	value string
	err   error
}

func newComposer(ctx context.Context, cl *kafka.Client, profileName string, p *profile.Profile, topic string, from *kgo.Record) *composer {
	c := &composer{vid: newID(), ctx: ctx, cl: cl, topic: topic, profile: profileName, prof: p}
	c.key, c.headers, c.partition = textinput.New(), textinput.New(), textinput.New()
	c.key.Placeholder, c.headers.Placeholder, c.partition.Placeholder = "no key", "k=v, k2=v2", "auto"
	c.value = textarea.New()
	c.value.Placeholder = "message value"
	c.value.ShowLineNumbers = false
	c.history = loadHistory(topic)
	c.histPos = len(c.history)
	if from != nil {
		c.key.SetValue(string(from.Key))
		c.value.SetValue(string(from.Value))
		var hs []string
		for _, h := range from.Headers {
			hs = append(hs, h.Key+"="+string(h.Value))
		}
		c.headers.SetValue(strings.Join(hs, ", "))
	}
	c.key.Focus()
	return c
}

func newComposerView(m *model, topic string, from *kgo.Record) view {
	return newComposer(m.ctx, m.client, m.profileName, m.prof, topic, from)
}

// RunComposer is the standalone `ntk produce -i` composer.
func RunComposer(ctx context.Context, cl *kafka.Client, profileName string, p *profile.Profile, topic string) error {
	c := newComposer(ctx, cl, profileName, p, topic, nil)
	_, err := tea.NewProgram(&composerApp{c}, tea.WithContext(ctx)).Run()
	return err
}

type composerApp struct{ c *composer }

func (a *composerApp) Init() tea.Cmd { return textinput.Blink }

func (a *composerApp) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	if k, ok := msg.(tea.KeyPressMsg); ok && (k.String() == "ctrl+c" || k.String() == "esc") {
		return a, tea.Quit
	}
	if ws, ok := msg.(tea.WindowSizeMsg); ok {
		a.c.setSize(ws.Width, ws.Height-3)
	}
	return a, a.c.update(nil, msg)
}

func (a *composerApp) View() tea.View {
	v := tea.NewView(styleTitle.Render("produce → "+a.c.topic) + " " + styleBadge(a.c.prof.Color).Render(" "+a.c.profile+" ") + "\n\n" +
		a.c.render() + "\n" + styleDim.Render("ctrl+s send  tab next field  ctrl+e $EDITOR  ctrl+o value from file  ctrl+h/ctrl+↓ history  esc quit"))
	v.AltScreen = true
	return v
}

func (c *composer) id() int                      { return c.vid }
func (c *composer) title() string                { return "produce → " + c.topic }
func (c *composer) load(*model) tea.Cmd          { return textinput.Blink }
func (c *composer) setFilter(string)             {}
func (c *composer) capturesKeys() bool           { return true }
func (c *composer) selectedJSON() (string, bool) { return "", false }

func (c *composer) cliCommand(*model) string {
	cmd := "ntk produce " + c.topic
	if k := c.key.Value(); k != "" {
		cmd += " -k " + strconv.Quote(k)
	}
	return cmd + " -v " + strconv.Quote(c.value.Value())
}

func (c *composer) setSize(w, h int) {
	c.width, c.height = w, h
	c.key.SetWidth(max(w-14, 10))
	c.headers.SetWidth(max(w-14, 10))
	c.partition.SetWidth(10)
	c.value.SetWidth(max(w-2, 10))
	c.value.SetHeight(max(h-8, 3))
}

func (c *composer) setFocus(i int) tea.Cmd {
	c.focus = (i + 4) % 4
	c.key.Blur()
	c.headers.Blur()
	c.partition.Blur()
	c.value.Blur()
	switch c.focus {
	case 0:
		return c.key.Focus()
	case 1:
		return c.headers.Focus()
	case 2:
		return c.partition.Focus()
	}
	return c.value.Focus()
}

func (c *composer) update(_ *model, msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case sentMsg:
		if msg.vid != c.vid {
			return nil
		}
		if msg.err != nil {
			c.status, c.statusErr = "✗ "+msg.err.Error(), true
		} else {
			c.sent++
			c.status, c.statusErr = fmt.Sprintf("✓ sent to %s/%d @%d (%d this session)", msg.r.Topic, msg.r.Partition, msg.r.Offset, c.sent), false
			c.remember()
		}
		return nil
	case editedMsg:
		if msg.vid != c.vid {
			return nil
		}
		if msg.err != nil {
			c.status, c.statusErr = "editor: "+msg.err.Error(), true
		} else {
			c.value.SetValue(msg.value)
		}
		return nil
	case tea.KeyPressMsg:
		switch msg.String() {
		case "ctrl+s":
			return c.send()
		case "tab":
			return c.setFocus(c.focus + 1)
		case "shift+tab":
			return c.setFocus(c.focus - 1)
		case "ctrl+e":
			return c.edit()
		case "ctrl+o":
			path := strings.TrimSpace(c.value.Value())
			b, err := os.ReadFile(path)
			switch {
			case path == "" || strings.Contains(path, "\n"):
				c.status, c.statusErr = "type a file path in the value box, then press ctrl+o", false
			case err != nil:
				c.status, c.statusErr = err.Error(), true
			case utf8.Valid(b):
				c.value.SetValue(string(b))
				c.status, c.statusErr = "loaded "+path, false
			default:
				c.status, c.statusErr = "binary files can't be edited here; use ntk produce -v @"+path, true
			}
			return nil
		case "ctrl+h", "ctrl+up":
			if c.histPos > 0 {
				c.histPos--
				c.apply(c.history[c.histPos])
			}
			return nil
		case "ctrl+down":
			if c.histPos < len(c.history)-1 {
				c.histPos++
				c.apply(c.history[c.histPos])
			}
			return nil
		}
	}
	var cmd tea.Cmd
	switch c.focus {
	case 0:
		c.key, cmd = c.key.Update(msg)
	case 1:
		c.headers, cmd = c.headers.Update(msg)
	case 2:
		c.partition, cmd = c.partition.Update(msg)
	default:
		c.value, cmd = c.value.Update(msg)
	}
	return cmd
}

func (c *composer) apply(h historyEntry) {
	c.key.SetValue(h.Key)
	c.headers.SetValue(h.Headers)
	c.partition.SetValue(h.Partition)
	c.value.SetValue(h.Value)
}

func (c *composer) record() (*kgo.Record, error) {
	r := &kgo.Record{Topic: c.topic, Partition: -1, Value: []byte(c.value.Value())}
	if k := c.key.Value(); k != "" {
		r.Key = []byte(k)
	}
	for _, h := range strings.Split(c.headers.Value(), ",") {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		k, v, ok := strings.Cut(h, "=")
		if !ok {
			return nil, fmt.Errorf("header %q is not key=value", h)
		}
		r.Headers = append(r.Headers, kgo.RecordHeader{Key: strings.TrimSpace(k), Value: []byte(strings.TrimSpace(v))})
	}
	if p := strings.TrimSpace(c.partition.Value()); p != "" && p != "auto" {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("partition %q is not a number", p)
		}
		r.Partition = int32(n)
	}
	return r, nil
}

func (c *composer) send() tea.Cmd {
	if c.prof.ReadOnly {
		c.status, c.statusErr = "refused: profile is read-only", true
		return nil
	}
	r, err := c.record()
	if err != nil {
		c.status, c.statusErr = err.Error(), true
		return nil
	}
	cl, ctx, id := c.cl, c.ctx, c.vid
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if r.Partition >= 0 {
			pcl, err := kgo.NewClient(append(cl.Opts(), kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
			if err != nil {
				return sentMsg{id, r, err}
			}
			defer pcl.Close()
			rec, err := pcl.ProduceSync(ctx, r).First()
			return sentMsg{id, rec, err}
		}
		rec, err := cl.ProduceSync(ctx, r).First()
		return sentMsg{id, rec, err}
	}
}

func (c *composer) edit() tea.Cmd {
	editor := os.Getenv("EDITOR")
	if editor == "" {
		editor = "vi"
	}
	f, err := os.CreateTemp("", "ntk-value-*.txt")
	if err != nil {
		c.status, c.statusErr = err.Error(), true
		return nil
	}
	_, _ = f.WriteString(c.value.Value())
	f.Close()
	id := c.vid
	cmd := exec.Command("/bin/sh", "-c", editor+` "$0"`, f.Name())
	return tea.ExecProcess(cmd, func(err error) tea.Msg {
		defer os.Remove(f.Name())
		if err != nil {
			return editedMsg{vid: id, err: err}
		}
		b, err := os.ReadFile(f.Name())
		return editedMsg{vid: id, value: strings.TrimSuffix(string(b), "\n"), err: err}
	})
}

func (c *composer) render() string {
	label := func(i int, s string) string {
		if c.focus == i {
			return styleKey.Render(s)
		}
		return styleDim.Render(s)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s\n", label(0, "Key       "), c.key.View())
	fmt.Fprintf(&b, "%s %s\n", label(1, "Headers   "), c.headers.View())
	fmt.Fprintf(&b, "%s %s\n", label(2, "Partition "), c.partition.View())
	fmt.Fprintf(&b, "%s %s\n", label(3, "Value"), styleDim.Render(fmt.Sprintf("(%d B)", len(c.value.Value()))))
	b.WriteString(c.value.View() + "\n")
	if c.status != "" {
		if c.statusErr {
			b.WriteString(styleError.Render(c.status))
		} else {
			b.WriteString(styleAccent.Render(c.status))
		}
	}
	return b.String()
}

func (c *composer) hints(*model) []hint {
	return []hint{{"ctrl+s", "send"}, {"tab", "field"}, {"ctrl+e", "$EDITOR"}, {"ctrl+o", "from file"}, {"ctrl+h", "history"}, {"esc", "back"}}
}

func historyPath(topic string) string {
	dir := os.Getenv("XDG_STATE_HOME")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".local", "state")
	}
	safe := strings.NewReplacer("/", "_", "\\", "_").Replace(topic)
	return filepath.Join(dir, "ntk", "history", safe+".jsonl")
}

func loadHistory(topic string) []historyEntry {
	f, err := os.Open(historyPath(topic))
	if err != nil {
		return nil
	}
	defer f.Close()
	var out []historyEntry
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64<<10), 16<<20)
	for sc.Scan() {
		var h historyEntry
		if json.Unmarshal(sc.Bytes(), &h) == nil {
			out = append(out, h)
		}
	}
	if len(out) > 100 {
		out = out[len(out)-100:]
	}
	return out
}

func (c *composer) remember() {
	h := historyEntry{Key: c.key.Value(), Value: c.value.Value(), Headers: c.headers.Value(), Partition: c.partition.Value()}
	c.history = append(c.history, h)
	c.histPos = len(c.history)
	path := historyPath(c.topic)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	b, _ := json.Marshal(h)
	_, _ = f.Write(append(b, '\n'))
}
