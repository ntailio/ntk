// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/acls"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/health"
	"github.com/ntailio/ntk/principals"
	"github.com/ntailio/ntk/quotas"
	"github.com/ntailio/ntk/testkit"
	"github.com/ntailio/ntk/tx"
)

func TestACLs(t *testing.T) {
	e := newEnv(t, nil)
	topic := testkit.Topic(t, e.adm, "acl", 1)
	user, _ := testkit.ScramUser(t, e.adm, "reader")
	principal := "User:" + user
	group := testkit.Name(t, "grp")

	e.ok("acl", "grant", "consumer", "--principal", principal, "--topic", topic, "--group", group)
	testkit.WaitACLs(t, e.adm, principal, 3)
	contains(t, e.ok("acl", "list", "--topic", topic, "--pattern", "match"), principal)

	e.ok("acl", "check", "--principal", principal, "--operation", "read", "--topic", topic)
	e.ok("acl", "check", "--principal", principal, "--operation", "describe", "--topic", topic)
	e.fails(exitcode.CheckFailed, "acl", "check", "--principal", principal, "--operation", "write", "--topic", topic)

	e.ok("acl", "create", "--principal", principal, "--operation", "write", "--topic", topic, "--deny")
	testkit.WaitACLs(t, e.adm, principal, 4)
	e.fails(exitcode.CheckFailed, "acl", "check", "--principal", principal, "--operation", "write", "--topic", topic)

	exported := filepath.Join(t.TempDir(), "acls.json")
	eventually(t, "export with 4 ACLs", func() bool {
		out := e.out("acl", "export", "--principal", principal)
		os.WriteFile(exported, []byte(out), 0o600)
		return strings.Contains(out, `"deny"`) && strings.Contains(out, `"READ"`)
	})
	eventually(t, "import of an unchanged export to be a no-op", func() bool {
		_, errOut, code := e.run("acl", "import", "-f", exported)
		return code == 0 && strings.Contains(errOut, "Nothing to change")
	})

	e.ok("acl", "delete", "--principal", principal, "--operation", "write", "--topic", topic, "-y")
	testkit.WaitACLs(t, e.adm, principal, 3)
	e.fails(exitcode.Usage, "acl", "delete", "-y")
	e.fails(exitcode.Usage, "acl", "import", "-f", exported, "--prune")

	empty := filepath.Join(t.TempDir(), "empty.json")
	os.WriteFile(empty, []byte(`[{"principal":"`+principal+`","allow":[{"operations":["READ"],"group":"`+group+`"}]}]`), 0o600)
	// The plan is built from one broker's view, which may lag, so re-apply until it converges.
	eventually(t, "only the group ACL after import --prune", func() bool {
		e.ok("acl", "import", "-f", empty, "--prune", "--scope", principal, "-y")
		var list []acls.ACL
		json.Unmarshal([]byte(e.out("acl", "list", "--principal", principal, "-o", "json")), &list)
		return len(list) == 1 && list[0].ResourceType == "GROUP"
	})
	testkit.WaitACLs(t, e.adm, principal, 1)
	e.ok("acl", "grant", "producer", "--principal", principal, "--topic", topic)
	testkit.WaitACLs(t, e.adm, principal, 3)
	e.ok("acl", "revoke", "producer", "--principal", principal, "--topic", topic, "-y")
	e.fails(exitcode.Usage, "acl", "grant", "streams-app", "--principal", principal)
	e.fails(exitcode.Usage, "acl", "grant", "nope", "--principal", principal)
}

func TestSelfLockoutWarning(t *testing.T) {
	e0 := newEnv(t, nil)
	user, pass := testkit.ScramUser(t, e0.adm, "self")
	self := map[string]any{"bootstrap_servers": []string{"localhost:19103"}, "auth": map[string]any{"mechanism": "scram-sha-512", "username": user, "password": pass}}
	e := &env{t: t, path: testkit.Profiles(t, map[string]any{"self": self}), adm: e0.adm}
	e.ok("acl", "create", "--principal", "User:"+user, "--operation", "describe", "--cluster")
	eventually(t, "self-lockout warning", func() bool {
		dry := e.out("-p", "self", "acl", "delete", "--principal", "User:"+user, "--cluster", "--operation", "describe", "--dry-run")
		return strings.Contains(dry, "the principal this profile uses")
	})
	e.ok("acl", "delete", "--principal", "User:"+user, "--cluster", "-y")
}

func TestUsersAndPrincipals(t *testing.T) {
	e := newEnv(t, nil)
	user := testkit.Name(t, "u")
	t.Cleanup(func() { e.run("user", "delete", user, "--with-acls", "-y") })

	e.okIn("first-password\n", "user", "create", user, "--password-stdin", "--mechanism", "scram-sha-256")
	e.fails(exitcode.Error, "user", "create", user, "--password-stdin", "--mechanism", "scram-sha-256")
	e.okIn("second\n", "user", "create", user, "--password-stdin")
	eventually(t, "both SCRAM mechanisms listed", func() bool {
		var us []principals.User
		json.Unmarshal([]byte(e.out("user", "list", "-o", "json")), &us)
		for _, u := range us {
			if u.Name == user && len(u.Credentials) == 2 {
				return true
			}
		}
		return false
	})
	generated := strings.TrimSpace(e.ok("user", "rotate", user, "--generate", "-y"))
	if len(generated) < 20 {
		t.Errorf("generated password %q", generated)
	}
	e.fails(exitcode.Usage, "user", "create", testkit.Name(t, "x"), "--dry-run", "-o", "json")

	login := map[string]any{"bootstrap_servers": []string{"localhost:19104"}, "tls": map[string]any{"ca_file": filepath.Join(testkit.RepoRoot(t), "sandbox/certs/ca.crt")},
		"auth": map[string]any{"mechanism": "scram-sha-512", "username": user, "password": generated}}
	le := &env{t: t, path: testkit.Profiles(t, map[string]any{"login": login}), adm: e.adm}
	eventually(t, "new SCRAM credential to work", func() bool { _, _, code := le.run("-p", "login", "profile", "test"); return code == 0 })
	contains(t, le.ok("-p", "login", "whoami"), "User:"+user)

	e.ok("acl", "grant", "producer", "--principal", "User:"+user, "--topic", "some-topic")
	e.ok("quota", "set", "--user", user, "produce=5MiB/s", "-y")
	contains(t, e.ok("principal", "list"), "User:"+user)
	eventually(t, "principal describe to show SCRAM, ACLs, and quota", func() bool {
		desc := e.out("principal", "describe", user)
		for _, want := range []string{"SCRAM-SHA-256", "SCRAM-SHA-512", "produce → some-topic", "5.0 MiB/s"} {
			if !strings.Contains(desc, want) {
				return false
			}
		}
		return true
	})
	e.ok("quota", "unset", "--user", user, "produce", "-y")
	e.ok("user", "delete", user, "--mechanism", "scram-sha-256", "-y")
	e.ok("user", "delete", user, "--with-acls", "-y")
}

func TestMTLSPrincipal(t *testing.T) {
	e := newEnv(t, nil)
	cert, key, principal := testkit.ClientCert(t, "mtls")
	mtls := map[string]any{"bootstrap_servers": []string{"localhost:19102"},
		"tls":  map[string]any{"ca_file": filepath.Join(testkit.RepoRoot(t), "sandbox/certs/ca.crt"), "cert_file": cert, "key_file": key},
		"auth": map[string]any{"mechanism": "none"}}
	me := &env{t: t, path: testkit.Profiles(t, map[string]any{"m": mtls}), adm: e.adm}
	me.ok("-p", "m", "profile", "test")
	contains(t, me.ok("-p", "m", "whoami"), "User:CN="+strings.TrimPrefix(principal, "User:"))
	topic := testkit.Topic(t, e.adm, "mtls", 1)
	if out := me.ok("-p", "m", "topic", "list", topic, "-o", "name"); strings.TrimSpace(out) != "" {
		t.Errorf("unauthorized principal sees topic: %q", out)
	}
	e.ok("acl", "create", "--principal", principal, "--operation", "describe", "--topic", topic)
	t.Cleanup(func() { e.run("acl", "delete", "--principal", principal, "-y") })
	eventually(t, "ACL to apply", func() bool {
		return strings.TrimSpace(me.out("-p", "m", "topic", "list", topic, "-o", "name")) == topic
	})
}

func TestQuotas(t *testing.T) {
	e := newEnv(t, nil)
	user, client := testkit.Name(t, "qu"), testkit.Name(t, "qc")
	t.Cleanup(func() {
		e.run("quota", "unset", "--user", user, "produce", "consume", "-y")
		e.run("quota", "unset", "--user", user, "--client-id", client, "produce", "-y")
	})
	e.ok("quota", "set", "--user", user, "produce=10MiB/s", "consume=50MiB/s", "-y")
	e.ok("quota", "set", "--user", user, "--client-id", client, "produce=2MiB/s", "-y")
	eventually(t, "user and user+client-id quota entities", func() bool {
		var qs []quotas.Quota
		json.Unmarshal([]byte(e.out("quota", "list", "--user", user, "-o", "json")), &qs)
		return len(qs) == 2 && qs[0].Values["producer_byte_rate"] == 10<<20 && qs[1].Values["producer_byte_rate"] == 2<<20
	})
	var eff []quotas.Effective
	json.Unmarshal([]byte(e.ok("quota", "effective", "--user", user, "--client-id", client, "-o", "json")), &eff)
	got := map[string]quotas.Effective{}
	for _, x := range eff {
		got[x.Key] = x
	}
	if got["producer_byte_rate"].Value != 2<<20 || !strings.Contains(got["producer_byte_rate"].From, "client-id=") ||
		got["consumer_byte_rate"].Value != 50<<20 {
		t.Errorf("effective: %+v", eff)
	}
	contains(t, e.ok("quota", "set", "--user", user, "produce=100", "--dry-run"), "very low")
	e.fails(exitcode.Usage, "quota", "set", "produce=1MiB", "-y")
	e.fails(exitcode.Usage, "quota", "set", "--user", user, "bogus=1", "-y")
}

func TestBrokersAndCluster(t *testing.T) {
	e := newEnv(t, nil)
	contains(t, e.ok("broker", "list"), "controller*")
	contains(t, e.ok("b", "describe", "1"), "Log dirs:", "Dynamic overrides:")
	e.fails(exitcode.NotFound, "broker", "describe", "99")
	contains(t, e.ok("broker", "config", "1", "--all"), "log.dirs")
	contains(t, e.ok("broker", "log-dirs"), "/var/lib/kafka/data")
	contains(t, e.ok("broker", "api-versions", "--broker", "1"), "Produce", "Fetch")
	contains(t, e.ok("cluster", "describe"), "KRaft:", "metadata.version")
	contains(t, e.ok("cluster", "quorum"), "VOTERS")
	contains(t, e.ok("cluster", "features"), "metadata.version")

	testkit.Serial(t)
	e.ok("broker", "config", "set", "--cluster-default", "log.cleaner.backoff.ms=16000", "-y")
	eventually(t, "cluster default to apply", func() bool {
		return strings.Contains(e.out("broker", "config", "--cluster-default"), "log.cleaner.backoff.ms")
	})
	e.ok("broker", "config", "unset", "--cluster-default", "log.cleaner.backoff.ms", "-y")
	e.fails(exitcode.Usage, "broker", "config", "set", "--cluster-default", "no.such.key=1", "-y")
}

func TestHealth(t *testing.T) {
	e := newEnv(t, nil)
	var r health.Report
	out, _, code := e.run("health", "-o", "json")
	if err := json.Unmarshal([]byte(out), &r); err != nil || len(r.Checks) < 10 || r.Brokers != 3 {
		t.Fatalf("health json (exit %d): %v %s", code, err, out)
	}
	contains(t, e.ok("health", "--checks", "brokers,controller"), "Status:")
	e.fails(exitcode.CheckFailed, "health", "--checks", "brokers", "--expect-brokers", "5")
	e.fails(exitcode.Usage, "health", "--fail-on", "sometimes")
}

func TestHangingTransaction(t *testing.T) {
	e := newEnv(t, nil)
	topic := testkit.Topic(t, e.adm, "hang", 1)
	txid := testkit.Name(t, "txn")
	cl, err := kgo.NewClient(kgo.SeedBrokers(testkit.Bootstrap()...), kgo.TransactionalID(txid), kgo.DefaultProduceTopic(topic),
		kgo.TransactionTimeout(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(t.Context(), &kgo.Record{Value: []byte("in-flight")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	var ts []tx.Transaction
	json.Unmarshal([]byte(e.ok("tx", "list", "--state", "Ongoing", "-o", "json")), &ts)
	var ours *tx.Transaction
	for i := range ts {
		if ts[i].TxnID == txid {
			ours = &ts[i]
		}
	}
	if ours == nil {
		t.Fatalf("ongoing transaction %s not listed: %+v", txid, ts)
	}
	contains(t, e.ok("tx", "describe", txid), "Ongoing")
	contains(t, e.ok("tx", "producers", topic), "open")
	contains(t, e.ok("tx", "hanging", "--topic", topic, "--older-than", "1ms"), txid)
	if n := len(lines(e.ok("c", topic, "--from", "earliest", "--isolation", "read_committed"))); n != 0 {
		t.Errorf("read_committed saw %d records of an open transaction", n)
	}

	var hs []tx.Hanging
	json.Unmarshal([]byte(e.ok("tx", "hanging", "--topic", topic, "--older-than", "1ms", "-o", "json")), &hs)
	if len(hs) != 1 {
		t.Fatalf("hanging: %+v", hs)
	}
	start := hs[0].TxnStartOffset
	_, errOut := e.fails(exitcode.Usage, "tx", "abort", "--topic", topic, "--partition", "0", "--start-offset", itoa(start), "-y")
	contains(t, errOut, "--confirm")
	e.ok("tx", "abort", "--topic", topic, "--partition", "0", "--start-offset", itoa(start), "-y", "--confirm", topic+"/0")
	eventually(t, "transaction aborted", func() bool {
		return !strings.Contains(e.out("tx", "producers", topic), "open")
	})
}

func TestDisruptiveHealthWithBrokerDown(t *testing.T) {
	testkit.Disruptive(t)
	e := &env{t: t, adm: testkit.Admin(t), path: testkit.Profiles(t, nil)}
	topic := testkit.Topic(t, e.adm, "down", 3)
	compose := func(args ...string) {
		t.Helper()
		cmd := exec.Command("docker", append([]string{"compose", "-f", filepath.Join(testkit.RepoRoot(t), "docker-compose.yml")}, args...)...)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("docker compose %v: %v\n%s", args, err, out)
		}
	}
	compose("stop", "kafka-3")
	t.Cleanup(func() {
		compose("start", "kafka-3")
		eventuallyWithin(t, 2*time.Minute, "all replicas back in sync", func() bool {
			return strings.Contains(e.out("health", "--checks", "under-replicated"), "✓ under-replicated")
		})
	})
	eventually(t, "health to report the missing broker", func() bool {
		out, _, code := e.run("health", "--fail-on", "warn", "--checks", "under-replicated,at-min-isr")
		return code == exitcode.CheckFailed && strings.Contains(out, "missing broker 3")
	})
	contains(t, e.ok("topic", "list", topic, "--under-replicated", "-o", "name"), topic)
}
