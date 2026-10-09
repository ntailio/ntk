// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	tea "charm.land/bubbletea/v2"
	"charm.land/huh/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/consume"
	"github.com/ntailio/ntk/kafka"
	"github.com/ntailio/ntk/record"
	"github.com/ntailio/ntk/units"
)

func timeNow() time.Time { return time.Now() }

func parseTimeNow(s string) (time.Time, error) { return units.ParseTime(s, time.Now()) }

type recordsMsg struct {
	vid  int
	recs []*kgo.Record
	err  error
	done bool
}

type consumeView struct {
	tableView
	vid       int
	topic     string
	partition int32
	from      string
	buffer    int
	recs      []*kgo.Record
	paused    bool
	running   bool
	cancel    context.CancelFunc
	ch        chan recordsMsg
	detailTab int
	hexView   bool
	find      string
	total     int
	finished  bool
	cl        *kafka.Client
}

func newConsumeView(m *model, topic string, partition int32, from string) view {
	size := m.prefs.TUI.BufferSize
	if size <= 0 {
		size = 10000
	}
	return &consumeView{tableView: newTableView(), vid: newID(), topic: topic, partition: partition, from: from, buffer: size}
}

func (v *consumeView) id() int { return v.vid }

func (v *consumeView) title() string {
	t := "consume " + v.topic
	if v.partition >= 0 {
		t += fmt.Sprintf("/%d", v.partition)
	}
	return t
}

func (v *consumeView) close() {
	if v.cancel != nil {
		v.cancel()
	}
	if v.cl != nil {
		v.cl.Close()
		v.cl = nil
	}
	v.running = false
}

func (v *consumeView) load(m *model) tea.Cmd {
	if v.running {
		return nil
	}
	v.running = true
	v.ch = make(chan recordsMsg, 16)
	ctx, cancel := context.WithCancel(m.ctx)
	v.cancel = cancel
	id, topic, part, fromStr, ch := v.vid, v.topic, v.partition, v.from, v.ch
	go func() {
		defer close(ch)
		from, err := consume.ParsePosition(fromStr, time.Now())
		if err != nil {
			ch <- recordsMsg{vid: id, err: err}
			return
		}
		o := consume.Options{Topics: []string{topic}, From: from, Follow: true}
		if part >= 0 {
			o.Partitions = []int32{part}
		}
		rctx, rcancel := context.WithTimeout(ctx, 15*time.Second)
		pl, err := consume.Resolve(rctx, m.client.Admin, o)
		rcancel()
		if err != nil {
			ch <- recordsMsg{vid: id, err: err}
			return
		}
		cl, err := kafka.NewClient(m.prof, m.store.ResolveFile, consume.ClientOptions(pl, o)...)
		if err != nil {
			ch <- recordsMsg{vid: id, err: err}
			return
		}
		v.cl = cl
		for ctx.Err() == nil {
			fs := cl.PollRecords(ctx, 500)
			if ctx.Err() != nil {
				return
			}
			var recs []*kgo.Record
			fs.EachRecord(func(r *kgo.Record) {
				if !r.Attrs.IsControl() {
					recs = append(recs, r)
				}
			})
			var ferr error
			fs.EachError(func(_ string, _ int32, err error) {
				if ferr == nil && ctx.Err() == nil {
					ferr = err
				}
			})
			select {
			case ch <- recordsMsg{vid: id, recs: recs, err: ferr}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return v.wait()
}

func (v *consumeView) wait() tea.Cmd {
	ch, id := v.ch, v.vid
	return func() tea.Msg {
		msg, ok := <-ch
		if !ok {
			return recordsMsg{vid: id, done: true}
		}
		return msg
	}
}

func (v *consumeView) update(m *model, msg tea.Msg) tea.Cmd {
	switch msg := msg.(type) {
	case recordsMsg:
		if msg.vid != v.vid {
			return nil
		}
		if msg.done {
			v.finished = true
			return nil
		}
		if msg.err != nil {
			v.err = msg.err
		}
		if len(msg.recs) > 0 {
			v.total += len(msg.recs)
			v.recs = append(v.recs, msg.recs...)
			if over := len(v.recs) - v.buffer; over > 0 {
				v.recs = v.recs[over:]
			}
			v.rebuild()
		} else if !v.loaded {
			v.rebuild()
		}
		return v.wait()
	case tea.KeyPressMsg:
		if m.top() != view(v) {
			return nil
		}
		switch msg.String() {
		case "space":
			v.paused = !v.paused
			if v.cl != nil {
				if v.paused {
					v.cl.PauseFetchTopics(v.topic)
				} else {
					v.cl.ResumeFetchTopics(v.topic)
				}
			}
			return nil
		case "enter", "tab":
			v.detailTab = (v.detailTab + 1) % 4
			return nil
		case "x":
			v.hexView = !v.hexView
			return nil
		case "F":
			return v.restartForm(m)
		case "s":
			return v.saveForm(m)
		case "P":
			if r := v.record(); r != nil {
				return m.push(newComposerView(m, v.topic, r))
			}
		case "[", "]":
			v.jumpPartition(msg.String() == "]")
		case "n":
			v.findNext()
		default:
			_, cmd := v.handleKey(msg)
			return cmd
		}
	}
	return nil
}

func (v *consumeView) rebuild() {
	var rows []row
	for _, r := range v.recs {
		key := ""
		if r.Key != nil {
			key = preview(r.Key, 24)
		}
		rows = append(rows, row{cells: []string{fmt.Sprintf("%d:%d", r.Partition, r.Offset),
			r.Timestamp.Local().Format("15:04:05"), dash(key), units.Bytes(int64(len(r.Value))), preview(r.Value, 120)}, raw: r})
	}
	follow := v.current() == nil || v.table.Cursor() >= len(v.shown)-1
	v.headers = []string{"PART:OFFSET", "TIME", "KEY", "SIZE", "VALUE"}
	v.all, v.loaded = rows, true
	v.refillAll()
	if follow && !v.paused {
		v.table.GotoBottom()
	}
}

// refillAll shows every row: find jumps to matches instead of hiding the rest.
func (v *consumeView) refillAll() {
	saved := v.filter
	v.filter = ""
	v.tableView.refill()
	v.filter = saved
}

func (v *consumeView) setFilter(f string) {
	v.find = f
	v.findNext()
}

func (v *consumeView) findNext() {
	if v.find == "" {
		return
	}
	start := v.table.Cursor() + 1
	f := strings.ToLower(v.find)
	for i := 0; i < len(v.shown); i++ {
		idx := (start + i) % len(v.shown)
		r := v.shown[idx].raw.(*kgo.Record)
		if strings.Contains(strings.ToLower(string(r.Value)), f) || strings.Contains(strings.ToLower(string(r.Key)), f) {
			v.table.SetCursor(idx)
			return
		}
	}
}

func (v *consumeView) jumpPartition(next bool) {
	r := v.record()
	if r == nil {
		return
	}
	c := v.table.Cursor()
	for i := 1; i < len(v.shown); i++ {
		idx := c + i
		if !next {
			idx = c - i
		}
		if idx < 0 || idx >= len(v.shown) {
			return
		}
		if v.shown[idx].raw.(*kgo.Record).Partition != r.Partition {
			v.table.SetCursor(idx)
			return
		}
	}
}

func (v *consumeView) record() *kgo.Record {
	r := v.current()
	if r == nil {
		return nil
	}
	return r.raw.(*kgo.Record)
}

func (v *consumeView) setSize(w, h int) {
	v.width, v.height = w, h
	v.tableView.setSize(w, max(h*3/5-2, 3))
}

func preview(b []byte, n int) string {
	if !utf8.Valid(b) {
		s := hex.EncodeToString(b)
		if len(s) > n {
			s = s[:n] + "…"
		}
		return "0x" + s
	}
	s := strings.Map(func(r rune) rune {
		if r < 0x20 {
			return ' '
		}
		return r
	}, string(b))
	if utf8.RuneCountInString(s) > n {
		return string([]rune(s)[:n]) + "…"
	}
	return s
}

func (v *consumeView) render() string {
	state := styleOK.Render("● live")
	switch {
	case v.paused:
		state = styleWarn.Render("❚❚ paused")
	case v.finished:
		state = styleDim.Render("stopped")
	}
	part := "all partitions"
	if v.partition >= 0 {
		part = fmt.Sprintf("partition %d", v.partition)
	}
	head := fmt.Sprintf("%s · %s · from %s · %s · %s messages (buffer %s)", styleTitle.Render(v.topic), part, v.from, state,
		units.Count(int64(v.total)), units.Count(int64(len(v.recs))))
	if v.find != "" {
		head += "  " + styleAccent.Render("find /"+v.find+" (n next)")
	}
	body := v.tableView.render("waiting for messages…")
	return head + "\n" + body + "\n" + v.detail()
}

func (v *consumeView) detail() string {
	r := v.record()
	if r == nil {
		return ""
	}
	tabs := []string{"Value", "Headers", "Hex", "Metadata"}
	var bar []string
	for i, t := range tabs {
		if i == v.detailTab {
			bar = append(bar, styleTabOn.Render(t))
		} else {
			bar = append(bar, styleTab.Render(t))
		}
	}
	var content string
	switch v.detailTab {
	case 0:
		if v.hexView || !utf8.Valid(r.Value) {
			content = hex.Dump(r.Value)
		} else {
			content = string(r.Value)
		}
		if r.Value == nil {
			content = styleDim.Render("(null value: tombstone)")
		}
	case 1:
		if len(r.Headers) == 0 {
			content = styleDim.Render("no headers")
		}
		for _, h := range r.Headers {
			content += fmt.Sprintf("%s = %s\n", styleKey.Render(h.Key), preview(h.Value, 200))
		}
	case 2:
		content = hex.Dump(r.Value)
	case 3:
		b, _ := json.MarshalIndent(record.MetaOf(r), "", "  ")
		content = string(b)
	}
	lines := strings.Split(content, "\n")
	room := max(v.height-v.height*3/5-2, 3)
	if len(lines) > room {
		lines = append(lines[:room-1], styleDim.Render(fmt.Sprintf("… %d more lines", len(lines)-room+1)))
	}
	return strings.Join(bar, " ") + "\n" + strings.Join(lines, "\n")
}

func (v *consumeView) restartForm(m *model) tea.Cmd {
	from, part := v.from, ""
	if v.partition >= 0 {
		part = itoa(v.partition)
	}
	f := huh.NewForm(huh.NewGroup(
		huh.NewInput().Title("Start from").Description("latest, earliest, -N per partition, @offset, a time, -1h").Value(&from),
		huh.NewInput().Title("Partition").Description("empty = all partitions").Value(&part),
	))
	return m.openForm("Consume "+v.topic, f, func(m *model) tea.Cmd {
		v.close()
		v.from, v.partition = from, -1
		if n, err := strconv.Atoi(strings.TrimSpace(part)); err == nil {
			v.partition = int32(n)
		}
		v.recs, v.total, v.finished, v.err, v.loaded = nil, 0, false, nil, false
		return v.load(m)
	})
}

func (v *consumeView) saveForm(m *model) tea.Cmd {
	path := v.topic + ".jsonl"
	f := huh.NewForm(huh.NewGroup(huh.NewInput().Title("Save loaded messages (jsonl) to").Value(&path)))
	return m.openForm("Save", f, func(m *model) tea.Cmd {
		out, err := os.Create(path)
		if err != nil {
			m.flash(err.Error(), true)
			return nil
		}
		defer out.Close()
		enc := json.NewEncoder(out)
		for _, r := range v.recs {
			if err := enc.Encode(record.FullOf(r)); err != nil {
				m.flash(err.Error(), true)
				return nil
			}
		}
		m.flash(fmt.Sprintf("saved %d messages to %s", len(v.recs), path), false)
		return nil
	})
}

func (v *consumeView) hints(*model) []hint {
	return []hint{{"space", "pause"}, {"enter/tab", "detail tab"}, {"x", "hex"}, {"F", "start/partition"}, {"n", "find next"},
		{"[ ]", "partition"}, {"y", "copy record"}, {"s", "save"}, {"P", "produce copy"}}
}

func (v *consumeView) cliCommand(m *model) string {
	c := "ntk consume " + v.topic + " --from " + v.from
	if v.partition >= 0 {
		c += fmt.Sprintf(" -P %d", v.partition)
	}
	return c + " -m"
}

func (v *consumeView) selectedJSON() (string, bool) {
	r := v.record()
	if r == nil {
		return "", false
	}
	b, err := json.Marshal(record.FullOf(r))
	return string(b), err == nil
}
