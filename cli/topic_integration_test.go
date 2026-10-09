// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/testkit"
	"github.com/ntailio/ntk/topics"
)

func TestTopicLifecycle(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Name(t, "life")
	t.Cleanup(func() { e.run("topic", "delete", name, "-y") })

	out := e.ok("topic", "create", name, "-P", "4", "-r", "3", "--config", "retention.ms=2d", "--config", "max.message.bytes=2MiB")
	_ = out
	e.ok("topic", "create", name, "--if-not-exists")
	e.fails(exitcode.Error, "topic", "create", name)

	var listed []topics.Topic
	if err := json.Unmarshal([]byte(e.ok("topic", "list", name, "-o", "json")), &listed); err != nil || len(listed) != 1 {
		t.Fatalf("list json: %v %v", listed, err)
	}
	if l := listed[0]; l.Partitions != 4 || l.ReplicationFactor != 3 || l.RetentionMs == nil || *l.RetentionMs != 172800000 {
		t.Errorf("unexpected topic: %+v", l)
	}
	contains(t, e.ok("topic", "list", name, "-o", "wide"), "MIN-ISR", "LEADERS")
	contains(t, e.ok("t", "ls", "-o", "name", name), name)

	e.ok("topic", "add-partitions", name, "6", "-y")
	e.fails(exitcode.Error, "topic", "add-partitions", name, "3", "-y")
	eventually(t, "6 partitions", func() bool { return strings.Contains(e.out("topic", "describe", name), "Partitions:   6") })

	testkit.Produce(t, name, "a", "b", "c", "d", "e", "f")
	desc := e.ok("topic", "describe", name)
	contains(t, desc, "Partitions:   6", "Messages: 6", "Overrides:", "retention.ms=2d")
	contains(t, e.ok("topic", "describe", name, "--partitions", "0-1"), "PART")

	offs := e.ok("topic", "offsets", name, "--time", "-1h")
	contains(t, offs, "OFFSET@TIME")

	e.ok("topic", "truncate", name, "--all", "-y")
	eventually(t, "0 messages after truncate", func() bool {
		var d topics.Detail
		return json.Unmarshal([]byte(e.out("topic", "describe", name, "-o", "json")), &d) == nil && d.Messages == 0
	})
	e.fails(exitcode.Usage, "topic", "truncate", name, "--all", "--before-offset", "1")

	dry := e.ok("topic", "delete", name, "--dry-run")
	contains(t, dry, "(dry run)", "Delete 1 topic")
	e.ok("topic", "delete", name, "-y")
	e.gone(name)
}

func TestTopicDeleteRegexAndPresets(t *testing.T) {
	e := newEnv(t, nil)
	prefix := testkit.Name(t, "rx")
	a, b := prefix+"-a", prefix+"-b"
	e.ok("topic", "create", a, b, "-P", "1", "--preset", "compacted")
	contains(t, e.ok("topic", "config", a), "compact")
	contains(t, e.ok("topic", "config", b), "compact")
	e.ok("topic", "delete", "--regex", "^"+prefix, "-y")
	e.gone(a)
	e.gone(b)
	e.fails(exitcode.Usage, "topic", "create", testkit.Name(t, "x"), "--preset", "nope")
}

func TestTopicConfig(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "cfg", 1)
	other := testkit.Topic(t, e.adm, "cfg2", 1)

	e.ok("topic", "config", "set", name, "retention.ms=14d", "max.message.bytes=4MiB", "-y")
	eventually(t, "retention.ms=14d", func() bool {
		return strings.TrimSpace(e.out("topic", "config", "get", name, "retention.ms")) == "1209600000"
	})
	_, errOut := e.fails(exitcode.Usage, "topic", "config", "set", name, "retention.mss=1d")
	contains(t, errOut, `did you mean "retention.ms"`)
	e.fails(exitcode.Usage, "topic", "config", "set", name, "min.insync.replicas=9", "-y")

	eventually(t, "config view to show retention.ms 2w", func() bool {
		show := e.out("topic", "config", name)
		return strings.Contains(show, "2w") && strings.Contains(show, "dynamic-topic")
	})
	contains(t, e.ok("topic", "config", name, "--all"), "segment.index.bytes")
	contains(t, e.ok("topic", "config", name, "--show-docs"), "DESCRIPTION")

	var exported map[string]string
	eventually(t, "export with max.message.bytes", func() bool {
		exported = nil
		return json.Unmarshal([]byte(e.out("topic", "config", "export", name)), &exported) == nil && exported["max.message.bytes"] == "4194304"
	})
	contains(t, e.ok("topic", "config", "diff", name, other), "retention.ms", "max.message.bytes")

	file := filepath.Join(t.TempDir(), "cfg.json")
	os.WriteFile(file, []byte(`{"retention.ms": "3d"}`), 0o600)
	e.ok("topic", "config", "apply", name, "-f", file, "--prune", "-y")
	eventually(t, "only retention.ms=3d after apply --prune", func() bool {
		exported = nil
		json.Unmarshal([]byte(e.out("topic", "config", "export", name)), &exported)
		return len(exported) == 1 && exported["retention.ms"] == "259200000"
	})
	e.ok("topic", "config", "unset", name, "retention.ms", "-y")
	eventually(t, "no overrides after unset", func() bool { return strings.TrimSpace(e.out("topic", "config", "export", name)) == "{}" })
}

func TestReassignAndElect(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "move", 2)
	planOut := e.ok("topic", "reassign", "plan", "--topics", name, "--brokers", "1,2,3", "-o", "json")
	file := filepath.Join(t.TempDir(), "plan.json")
	var asg topics.Assignment
	if err := json.Unmarshal([]byte(planOut), &asg); err != nil || len(asg.Partitions) != 2 {
		t.Fatalf("plan: %s %v", planOut, err)
	}
	asg.Partitions[0].Replicas = []int32{asg.Partitions[0].Replicas[2], asg.Partitions[0].Replicas[0], asg.Partitions[0].Replicas[1]}
	b, _ := json.Marshal(asg)
	os.WriteFile(file, b, 0o600)
	contains(t, e.ok("topic", "reassign", "plan", "--topics", name, "--brokers", "1,2,3"), "CURRENT", "PROPOSED")
	e.ok("topic", "reassign", "apply", file, "--throttle", "50MiB/s", "-y")
	e.ok("topic", "reassign", "status", "--topics", name)
	e.ok("topic", "reassign", "cancel", "--topics", name, "-y")
	e.ok("topic", "elect-leaders", "--topics", name, "-y")
	e.fails(exitcode.Usage, "topic", "elect-leaders", "--type", "sideways")
}

func TestSavedPlanApply(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "saved", 1)
	file := filepath.Join(t.TempDir(), "plan.json")
	os.WriteFile(file, []byte(e.ok("topic", "delete", name, "--dry-run", "-o", "json")), 0o600)
	e.ok("apply", file, "-y")
	e.gone(name)

	bad := filepath.Join(t.TempDir(), "bad.json")
	os.WriteFile(bad, []byte(`{"kind":"topic.delete","cluster_id":"other","payload":{"topics":["x"]}}`), 0o600)
	e.fails(exitcode.Refused, "apply", bad, "-y")
}

func TestSafetyRules(t *testing.T) {
	ro := map[string]any{"bootstrap_servers": testkit.Bootstrap(), "auth": map[string]any{"mechanism": "none"}, "read_only": true}
	prod := map[string]any{"bootstrap_servers": testkit.Bootstrap(), "auth": map[string]any{"mechanism": "none"}, "labels": []string{"prod"}}
	e := newEnv(t, map[string]any{"ro": ro, "prod": prod})
	name := testkit.Topic(t, e.adm, "safe", 1)

	_, errOut := e.fails(exitcode.Refused, "-p", "ro", "topic", "delete", name, "-y")
	contains(t, errOut, "read-only")
	e.fails(exitcode.Refused, "-p", "ro", "topic", "create", testkit.Name(t, "ro"))
	e.ok("-p", "ro", "topic", "describe", name)

	_, errOut = e.fails(exitcode.Usage, "-p", "prod", "topic", "delete", name, "-y")
	contains(t, errOut, "--confirm")
	e.fails(exitcode.Usage, "-p", "prod", "topic", "delete", name)
	e.ok("-p", "prod", "topic", "delete", name, "-y", "--confirm", name)
	e.gone(name)
}

func TestCompletionTopics(t *testing.T) {
	e := newEnv(t, nil)
	name := testkit.Topic(t, e.adm, "compl", 1)
	out := e.ok("__complete", "topic", "list", name[:len(name)-2])
	contains(t, out, name)
	contains(t, e.ok("__complete", "c", name, "-P", ""), "0")
	contains(t, e.ok("__complete", "-p", "sand"), "sandbox")
}
