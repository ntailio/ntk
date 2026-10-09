// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"
	"charm.land/lipgloss/v2"

	"github.com/ntailio/ntk/actions"
	"github.com/ntailio/ntk/health"
	"github.com/ntailio/ntk/kafka"
	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/prefs"
	"github.com/ntailio/ntk/profile"
)

type view interface {
	id() int
	title() string
	load(m *model) tea.Cmd
	update(m *model, msg tea.Msg) tea.Cmd
	setFilter(f string)
	setSize(w, h int)
	render() string
	hints(m *model) []hint
	cliCommand(m *model) string
	selectedJSON() (string, bool)
}

type hint struct{ key, desc string }

type inputMode int

const (
	modeNormal inputMode = iota
	modeCommand
	modeFilter
)

var nextID int

func newID() int { nextID++; return nextID }

type (
	pushMsg          struct{ v view }
	switchProfileMsg struct{ name string }
	clusterMsg       struct {
		id      string
		brokers int
		err     error
	}
	healthMsg struct {
		level health.Level
		err   error
	}
	tickMsg  time.Time
	flashMsg struct {
		text string
		err  bool
	}
	planMsg struct {
		pl  *plan.Plan
		err error
		// after runs once the plan was applied successfully
		after func(m *model) tea.Cmd
	}
	appliedMsg struct {
		summary string
		err     error
		after   func(m *model) tea.Cmd
	}
)

type confirmState struct {
	pl    *plan.Plan
	typed bool
	input textinput.Model
	after func(m *model) tea.Cmd
}

type formState struct {
	form   *huh.Form
	title  string
	submit func(m *model) tea.Cmd
}

type Options struct {
	Store     *profile.Store
	Profile   string
	StartView string
	StartArg  string
}

type model struct {
	ctx   context.Context
	store *profile.Store
	prefs *prefs.Prefs

	profileName string
	prof        *profile.Profile
	client      *kafka.Client
	cluster     clusterMsg
	health      *healthMsg

	stack     []view
	mode      inputMode
	input     textinput.Model
	filter    string
	showHelp  bool
	status    string
	statusErr bool
	confirm   *confirmState
	form      *formState
	width     int
	height    int
	refresh   time.Duration
	wide      bool
	mouse     bool
}

func Run(ctx context.Context, o Options) error {
	p, err := prefs.Load()
	if err != nil {
		return err
	}
	m := &model{ctx: ctx, store: o.Store, prefs: p, input: textinput.New(), refresh: p.RefreshInterval(), width: 100, height: 30, mouse: p.TUI.Mouse}
	applyTheme(p.TUI.Colors)
	m.input.Prompt = ""
	if err := m.useProfile(o.Profile); err != nil {
		return err
	}
	start := o.StartView
	if start == "" {
		start = p.TUI.StartView
	}
	if start == "" {
		start = "topics"
	}
	v, err := m.viewFor(start, o.StartArg)
	if err != nil {
		return err
	}
	m.stack = []view{v}

	final, err := tea.NewProgram(m, tea.WithContext(ctx)).Run()
	if fm, ok := final.(*model); ok && fm.client != nil {
		fm.client.Close()
	}
	if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
		return nil
	}
	return err
}

func (m *model) useProfile(name string) error {
	p, ok := m.store.File.Profiles[name]
	if !ok {
		return fmt.Errorf("profile %q not found", name)
	}
	cl, err := kafka.NewClient(p, m.store.ResolveFile)
	if err != nil {
		return fmt.Errorf("profile %q: %w", name, err)
	}
	if m.client != nil {
		m.client.Close()
	}
	m.profileName, m.prof, m.client = name, p, cl
	m.cluster, m.health = clusterMsg{}, nil
	return nil
}

func (m *model) readOnly() bool { return m.prof.ReadOnly }

func (m *model) top() view { return m.stack[len(m.stack)-1] }

func (m *model) Init() tea.Cmd {
	return tea.Batch(m.loadCluster(), m.loadHealth(), m.top().load(m), m.tick())
}

func (m *model) tick() tea.Cmd {
	if m.refresh <= 0 {
		return nil
	}
	return tea.Tick(m.refresh, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m *model) timeout() (context.Context, context.CancelFunc) {
	return context.WithTimeout(m.ctx, 15*time.Second)
}

func (m *model) loadCluster() tea.Cmd {
	cl := m.client
	return func() tea.Msg {
		ctx, cancel := m.timeout()
		defer cancel()
		md, err := cl.Admin.BrokerMetadata(ctx)
		return clusterMsg{id: md.Cluster, brokers: len(md.Brokers), err: err}
	}
}

func (m *model) loadHealth() tea.Cmd {
	cl := m.client
	return func() tea.Msg {
		ctx, cancel := m.timeout()
		defer cancel()
		r, err := health.Run(ctx, cl.Client, cl.Admin, health.Options{Checks: []string{"brokers", "controller", "offline", "under-replicated", "under-min-isr"}})
		return healthMsg{level: r.Status, err: err}
	}
}

func (m *model) flash(text string, err bool) { m.status, m.statusErr = text, err }

// planThen builds a plan in the background, then asks for confirmation.
func (m *model) planThen(build func(ctx context.Context) (*plan.Plan, error), after func(m *model) tea.Cmd) tea.Cmd {
	if m.readOnly() {
		m.flash(fmt.Sprintf("refused: profile %q is read-only", m.profileName), true)
		return nil
	}
	m.flash("preparing…", false)
	return func() tea.Msg {
		ctx, cancel := m.timeout()
		defer cancel()
		pl, err := build(ctx)
		return planMsg{pl: pl, err: err, after: after}
	}
}

func (m *model) apply(pl *plan.Plan, after func(m *model) tea.Cmd) tea.Cmd {
	cl := m.client
	pl.Profile = m.profileName
	return func() tea.Msg {
		ctx, cancel := m.timeout()
		defer cancel()
		err := actions.Apply(ctx, cl, pl)
		return appliedMsg{summary: strings.TrimSuffix(pl.Summary, ":"), err: err, after: after}
	}
}

func (m *model) openForm(title string, f *huh.Form, submit func(m *model) tea.Cmd) tea.Cmd {
	f = f.WithShowHelp(true).WithWidth(min(max(m.width-4, 40), 100))
	m.form = &formState{form: f, title: title, submit: submit}
	return f.Init()
}

func (m *model) push(v view) tea.Cmd {
	m.filter = ""
	v.setSize(m.bodySize())
	m.stack = append(m.stack, v)
	return v.load(m)
}

func (m *model) replace(v view) tea.Cmd {
	m.filter = ""
	v.setSize(m.bodySize())
	m.stack = []view{v}
	return v.load(m)
}

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		for _, v := range m.stack {
			v.setSize(m.bodySize())
		}
		return m, nil
	case tea.KeyPressMsg:
		return m, m.handleKey(msg)
	case tea.MouseWheelMsg:
		if m.form != nil || m.confirm != nil {
			return m, nil
		}
		code := tea.KeyDown
		if msg.Button == tea.MouseWheelUp {
			code = tea.KeyUp
		}
		return m, m.top().update(m, tea.KeyPressMsg{Code: code})
	case clusterMsg:
		m.cluster = msg
		return m, nil
	case healthMsg:
		m.health = &msg
		return m, nil
	case tickMsg:
		return m, tea.Batch(m.loadCluster(), m.loadHealth(), m.top().load(m), m.tick())
	case pushMsg:
		return m, m.push(msg.v)
	case switchProfileMsg:
		return m, m.switchProfile(msg.name)
	case flashMsg:
		m.flash(msg.text, msg.err)
		return m, nil
	case planMsg:
		switch {
		case msg.err != nil:
			m.flash(msg.err.Error(), true)
		case msg.pl.Empty():
			m.flash("nothing to change", false)
		case msg.pl.Class == plan.SafeWrite:
			m.flash("applying…", false)
			return m, m.apply(msg.pl, msg.after)
		default:
			c := &confirmState{pl: msg.pl, after: msg.after, input: textinput.New()}
			c.typed = msg.pl.Class == plan.Destructive && msg.pl.Confirm != "" && (msg.pl.Typed || hasLabel(m.prof, "prod"))
			c.input.Prompt = ""
			m.confirm = c
			m.flash("", false)
			if c.typed {
				return m, c.input.Focus()
			}
		}
		return m, nil
	case appliedMsg:
		if msg.err != nil {
			m.flash(msg.err.Error(), true)
			return m, nil
		}
		m.flash("✓ "+msg.summary, false)
		cmds := []tea.Cmd{m.top().load(m)}
		if msg.after != nil {
			cmds = append(cmds, msg.after(m))
		}
		return m, tea.Batch(cmds...)
	}
	var cmds []tea.Cmd
	if m.form != nil {
		cmds = append(cmds, m.updateForm(msg))
	}
	for _, v := range m.stack {
		if cmd := v.update(m, msg); cmd != nil {
			cmds = append(cmds, cmd)
		}
	}
	return m, tea.Batch(cmds...)
}

func (m *model) updateForm(msg tea.Msg) tea.Cmd {
	f, cmd := m.form.form.Update(msg)
	if hf, ok := f.(*huh.Form); ok {
		m.form.form = hf
	}
	switch m.form.form.State {
	case huh.StateCompleted:
		submit := m.form.submit
		m.form = nil
		return tea.Batch(cmd, submit(m))
	case huh.StateAborted:
		m.form = nil
		m.flash("cancelled", false)
	}
	return cmd
}

func hasLabel(p *profile.Profile, l string) bool {
	for _, x := range p.Labels {
		if x == l {
			return true
		}
	}
	return false
}

// globalKeys maps actions to their default key; config.json tui.keys can remap them.
var globalKeys = map[string]string{
	"quit": "q", "command": ":", "filter": "/", "help": "?", "refresh": "r", "refresh-interval": "R",
	"wide": "w", "profiles": "ctrl+p", "yank": "y", "yank-command": "Y", "back": "esc",
}

func (m *model) remap(key string) string {
	for action, custom := range m.prefs.TUI.Keys {
		if custom == key {
			if def, ok := globalKeys[action]; ok {
				return def
			}
		}
	}
	return key
}

func (m *model) handleKey(msg tea.KeyPressMsg) tea.Cmd {
	key := msg.String()
	if key == "ctrl+c" && m.form == nil {
		return tea.Quit
	}
	if m.form != nil {
		if key == "esc" || key == "ctrl+c" {
			m.form = nil
			m.flash("cancelled", false)
			return nil
		}
		return m.updateForm(msg)
	}
	if m.confirm != nil {
		return m.handleConfirm(msg)
	}
	if m.showHelp {
		m.showHelp = false
		return nil
	}
	switch m.mode {
	case modeCommand, modeFilter:
		switch key {
		case "esc":
			if m.mode == modeFilter {
				m.applyFilter("")
			}
			m.mode = modeNormal
			m.input.Blur()
			return nil
		case "enter":
			mode, value := m.mode, strings.TrimSpace(m.input.Value())
			m.mode = modeNormal
			m.input.Blur()
			if mode == modeCommand {
				return m.runCommand(value)
			}
			return nil
		}
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		if m.mode == modeFilter {
			m.applyFilter(m.input.Value())
		}
		return cmd
	}

	if c, ok := m.top().(interface{ capturesKeys() bool }); ok && c.capturesKeys() {
		if key != "esc" {
			return m.top().update(m, msg)
		}
	}
	m.status = ""
	if b, ok := m.top().(interface{ binds(*model, string) bool }); ok && key != "esc" && b.binds(m, key) {
		return m.top().update(m, msg)
	}
	switch m.remap(key) {
	case "q":
		return tea.Quit
	case ":":
		return m.startInput(modeCommand, "")
	case "/":
		return m.startInput(modeFilter, m.filter)
	case "?":
		m.showHelp = true
		return nil
	case "r":
		return tea.Batch(m.loadCluster(), m.top().load(m))
	case "R":
		m.cycleRefresh()
		return m.tick()
	case "w":
		m.wide = !m.wide
		for _, v := range m.stack {
			if t, ok := v.(interface{ setWide(bool) }); ok {
				t.setWide(m.wide)
			}
		}
		return nil
	case "ctrl+p":
		return m.runCommand("ctx")
	case "y":
		if s, ok := m.top().selectedJSON(); ok {
			m.flash("copied row as JSON", false)
			return tea.SetClipboard(s)
		}
		return nil
	case "Y":
		c := m.top().cliCommand(m)
		m.flash("copied: "+c, false)
		return tea.SetClipboard(c)
	case "esc":
		if m.filter != "" {
			m.applyFilter("")
			return nil
		}
		if len(m.stack) > 1 {
			m.stack = m.stack[:len(m.stack)-1]
			m.top().setFilter("")
			return m.top().load(m)
		}
		return nil
	}
	return m.top().update(m, msg)
}

func (m *model) handleConfirm(msg tea.KeyPressMsg) tea.Cmd {
	c := m.confirm
	key := msg.String()
	if key == "esc" || (!c.typed && (key == "n" || key == "N")) {
		m.confirm = nil
		m.flash("aborted; nothing changed", false)
		return nil
	}
	if !c.typed {
		if key == "y" || key == "Y" {
			m.confirm = nil
			m.flash("applying…", false)
			return m.apply(c.pl, c.after)
		}
		return nil
	}
	if key == "enter" {
		m.confirm = nil
		if c.input.Value() != c.pl.Confirm {
			m.flash("confirmation did not match; nothing changed", true)
			return nil
		}
		m.flash("applying…", false)
		return m.apply(c.pl, c.after)
	}
	var cmd tea.Cmd
	c.input, cmd = c.input.Update(msg)
	return cmd
}

func (m *model) cycleRefresh() {
	steps := []time.Duration{0, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second}
	next := steps[0]
	for i, s := range steps {
		if s == m.refresh {
			next = steps[(i+1)%len(steps)]
		}
	}
	m.refresh = next
	if next == 0 {
		m.flash("auto-refresh off", false)
	} else {
		m.flash("auto-refresh every "+next.String(), false)
	}
}

func (m *model) startInput(mode inputMode, value string) tea.Cmd {
	m.mode = mode
	m.input.SetValue(value)
	m.input.CursorEnd()
	return m.input.Focus()
}

func (m *model) applyFilter(f string) {
	m.filter = f
	m.top().setFilter(f)
}

func (m *model) runCommand(line string) tea.Cmd {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return nil
	}
	arg := strings.Join(fields[1:], " ")
	switch fields[0] {
	case "q", "quit":
		return tea.Quit
	case "cmd":
		c := m.top().cliCommand(m)
		m.flash(c+"   (Y copies it)", false)
		return nil
	case "ctx", "profile", "profiles":
		if arg != "" {
			return m.switchProfile(arg)
		}
		return m.push(newProfilesView(m))
	}
	v, err := m.viewFor(fields[0], arg)
	if err != nil {
		m.flash(err.Error(), true)
		return nil
	}
	if isRoot(fields[0]) && arg == "" {
		return m.replace(v)
	}
	return m.push(v)
}

func isRoot(name string) bool {
	switch name {
	case "t", "topic", "topics", "g", "group", "groups", "b", "broker", "brokers", "acls", "acl", "principals", "principal",
		"users", "user", "quotas", "quota", "tx", "txn", "transactions", "health", "h", "cluster", "top", "gtop":
		return true
	}
	return false
}

func (m *model) viewFor(name, arg string) (view, error) {
	switch name {
	case "t", "topic", "topics":
		if arg != "" {
			return newTopicDetailView(m, arg), nil
		}
		return newTopicsView(m), nil
	case "g", "group", "groups":
		if arg != "" {
			return newGroupDetailView(m, arg), nil
		}
		return newGroupsView(m), nil
	case "b", "broker", "brokers":
		if arg != "" {
			return newBrokerDetailView(m, arg), nil
		}
		return newBrokersView(m), nil
	case "cluster":
		return newClusterView(m), nil
	case "acl", "acls":
		return newACLsView(m, ""), nil
	case "principal", "principals":
		if arg != "" {
			return newPrincipalDetailView(m, arg), nil
		}
		return newPrincipalsView(m, false), nil
	case "user", "users":
		return newPrincipalsView(m, true), nil
	case "quota", "quotas":
		return newQuotasView(m), nil
	case "tx", "txn", "transactions":
		return newTxView(m), nil
	case "h", "health":
		return newHealthView(m), nil
	case "c", "consume":
		if arg == "" {
			return nil, errors.New("usage: :consume <topic>")
		}
		return newConsumeView(m, arg, -1, "-100"), nil
	case "p", "produce":
		if arg == "" {
			return nil, errors.New("usage: :produce <topic>")
		}
		return newComposerView(m, arg, nil), nil
	case "topmon":
		if arg == "" {
			return nil, errors.New("usage: :topmon <topic>")
		}
		return newTopicMonitorView(m, arg), nil
	case "grpmon":
		if arg == "" {
			return nil, errors.New("usage: :grpmon <group>")
		}
		return newGroupMonitorView(m, arg), nil
	case "top":
		return newTopView(m), nil
	case "gtop":
		return newGroupTopView(m), nil
	case "ctx", "profiles":
		return newProfilesView(m), nil
	}
	return nil, fmt.Errorf("unknown view %q (try :topics, :groups, :brokers, :acls, :principals, :quotas, :tx, :health, :ctx)", name)
}

func (m *model) switchProfile(name string) tea.Cmd {
	if err := m.useProfile(name); err != nil {
		m.flash(err.Error(), true)
		return nil
	}
	return tea.Batch(m.loadCluster(), m.loadHealth(), m.replace(newTopicsView(m)))
}

const chromeLines = 3

func (m *model) bodySize() (int, int) { return m.width, max(m.height-chromeLines, 1) }

func (m *model) View() tea.View {
	var b strings.Builder
	b.WriteString(m.header())
	b.WriteByte('\n')
	switch {
	case m.form != nil:
		b.WriteString(styleTitle.Render(m.form.title) + "\n\n" + m.form.form.View())
	case m.confirm != nil:
		b.WriteString(m.confirmView())
	case m.showHelp:
		b.WriteString(m.helpScreen())
	default:
		b.WriteString(m.top().render())
	}
	body := lipgloss.NewStyle().Height(m.height - 1).MaxHeight(m.height - 1).MaxWidth(m.width).Render(b.String())
	v := tea.NewView(body + "\n" + m.footer())
	v.AltScreen = true
	v.WindowTitle = "ntk · " + m.profileName
	if m.mouse {
		v.MouseMode = tea.MouseModeCellMotion
	}
	return v
}

func (m *model) confirmView() string {
	c := m.confirm
	var b strings.Builder
	b.WriteString(styleBadge(m.prof.Color).Render(" "+m.profileName+" ") + " " + styleWarn.Render(c.pl.Summary) + "\n\n")
	for _, line := range c.pl.Changes {
		b.WriteString("  " + line + "\n")
	}
	for _, w := range c.pl.Warnings {
		b.WriteString(styleWarn.Render("  ! "+w) + "\n")
	}
	b.WriteString("\n")
	if c.typed {
		b.WriteString(fmt.Sprintf("Type %s to confirm: %s\n", styleKey.Render(c.pl.Confirm), c.input.View()))
		b.WriteString(styleDim.Render("enter confirm · esc cancel"))
	} else {
		b.WriteString(styleKey.Render("y") + " apply · " + styleKey.Render("n/esc") + " cancel")
	}
	return b.String()
}

func (m *model) header() string {
	badge := styleBadge(m.prof.Color).Render(" " + m.profileName + " ")
	parts := []string{styleTitle.Render("ntk"), badge}
	switch {
	case m.cluster.err != nil:
		parts = append(parts, styleError.Render("✗ "+m.cluster.err.Error()))
	case m.cluster.id != "":
		parts = append(parts, "cluster "+m.cluster.id, fmt.Sprintf("%d brokers", m.cluster.brokers))
	default:
		parts = append(parts, styleDim.Render("connecting…"))
	}
	if principal, _ := m.prof.ExpectedPrincipal(); principal != "" {
		parts = append(parts, principal)
	}
	if m.prof.ReadOnly {
		parts = append(parts, styleWarn.Render("read-only"))
	}
	if m.health != nil && m.health.err == nil {
		switch m.health.level {
		case health.Warn:
			parts = append(parts, styleWarn.Render("● WARN"))
		case health.Critical:
			parts = append(parts, styleError.Bold(true).Render("● CRITICAL"))
		default:
			parts = append(parts, styleOK.Render("● OK"))
		}
	}
	line1 := strings.Join(parts, styleDim.Render(" │ "))

	var crumbs []string
	for _, v := range m.stack {
		crumbs = append(crumbs, v.title())
	}
	line2 := styleDim.Render(strings.Join(crumbs, " › "))
	if m.filter != "" && m.mode != modeFilter {
		line2 += "  " + styleAccent.Render("/"+m.filter)
	}
	if m.refresh > 0 {
		line2 += styleDim.Render("  ↻ " + m.refresh.String())
	}
	return line1 + "\n" + line2
}

func (m *model) footer() string {
	switch m.mode {
	case modeCommand:
		return styleAccent.Render(":") + m.input.View()
	case modeFilter:
		return styleAccent.Render("/") + m.input.View()
	}
	if m.status != "" {
		if m.statusErr {
			return styleError.Render(m.status)
		}
		return styleAccent.Render(m.status)
	}
	if m.form != nil || m.confirm != nil {
		return ""
	}
	hs := append(m.top().hints(m), hint{":", "cmd"}, hint{"/", "filter"}, hint{"?", "help"}, hint{"q", "quit"})
	var parts []string
	for _, h := range hs {
		parts = append(parts, styleKey.Render(h.key)+" "+styleDim.Render(h.desc))
	}
	return lipgloss.NewStyle().MaxWidth(m.width).Render(strings.Join(parts, "  "))
}

func (m *model) helpScreen() string {
	rows := [][2]string{
		{":topics :groups :brokers", "resource views (also :acls :principals :users :quotas :tx :health :cluster)"},
		{":consume <t> :produce <t>", "message browser / composer"},
		{":top :gtop", "busiest topics / most lagging groups"},
		{":topmon <t> :grpmon <g>", "live monitors"},
		{":ctx [profile]", "switch profile (ctrl+p)"},
		{":cmd", "show the equivalent CLI command (Y copies it)"},
		{"/", "filter the table (/re:… for a regex; esc clears)"},
		{"enter / esc", "drill down / back"},
		{"tab / shift+tab", "switch tabs in detail views"},
		{"j/k ↑/↓ g/G", "move / top / bottom"},
		{"< >  s", "sort column left/right · reverse sort"},
		{"w", "toggle wide columns"},
		{"y / Y", "copy row as JSON / copy CLI command"},
		{"r / R", "refresh now / change auto-refresh"},
		{"q, ctrl+c", "quit"},
	}
	var b strings.Builder
	b.WriteString(styleTitle.Render("Keys") + "\n\n")
	for _, r := range rows {
		b.WriteString(fmt.Sprintf("  %s  %s\n", styleKey.Width(28).Render(r[0]), r[1]))
	}
	if hs := m.top().hints(m); len(hs) > 0 {
		b.WriteString("\n" + styleTitle.Render(m.top().title()) + "\n\n")
		for _, h := range hs {
			b.WriteString(fmt.Sprintf("  %s  %s\n", styleKey.Width(28).Render(h.key), h.desc))
		}
	}
	b.WriteString("\n" + styleDim.Render("press any key to close"))
	return b.String()
}
