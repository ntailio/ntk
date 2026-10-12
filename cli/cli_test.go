// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/profile"
)

const testProfiles = `{
  "version": 1,
  "active": "dev",
  "profiles": {
    "dev":  {"bootstrap_servers": ["localhost:1"], "auth": {"mechanism": "none"}},
    "prod": {"bootstrap_servers": ["kafka:9093"], "auth": {"mechanism": "scram-sha-512", "username": "u", "password": "s3cret"},
             "read_only": true}
  }
}`

func run(t *testing.T, args ...string) (string, string, int) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, []byte(testProfiles), 0o600); err != nil {
		t.Fatal(err)
	}
	return runWith(t, path, args...)
}

func runWith(t *testing.T, path string, args ...string) (string, string, int) {
	t.Helper()
	t.Setenv("NTK_PROFILE", "")
	t.Setenv("NTK_OUTPUT", "")
	var stdout, stderr strings.Builder
	code := Execute(t.Context(), append([]string{"--profile-path", path}, args...), strings.NewReader(""), &stdout, &stderr)
	return stdout.String(), stderr.String(), code
}

func TestProfileListAndCurrent(t *testing.T) {
	out, _, code := run(t, "profile", "list", "-o", "name")
	if code != 0 || out != "dev\nprod\n" {
		t.Errorf("list: code %d, out %q", code, out)
	}
	out, _, _ = run(t, "profile", "current")
	if out != "dev\n" {
		t.Errorf("current: %q", out)
	}
	out, _, _ = run(t, "-p", "prod", "profile", "current")
	if out != "prod\n" {
		t.Errorf("current -p: %q", out)
	}
}

func TestProfileShowMasksSecrets(t *testing.T) {
	out, _, code := run(t, "profile", "show", "prod")
	if code != 0 || strings.Contains(out, "s3cret") || !strings.Contains(out, "********") {
		t.Errorf("show: code %d, out %s", code, out)
	}
	out, _, _ = run(t, "profile", "show", "prod", "--reveal")
	if !strings.Contains(out, "s3cret") {
		t.Errorf("show --reveal: %s", out)
	}
}

func TestProfileSet(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not apply on Windows")
	}
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, []byte(testProfiles), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, errOut, code := runWith(t, path, "profile", "set", "prod"); code != 0 {
		t.Fatalf("set: code %d: %s", code, errOut)
	}
	out, _, _ := runWith(t, path, "profile", "current")
	if out != "prod\n" {
		t.Errorf("after set: current = %q", out)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o600 {
		t.Errorf("mode after save = %04o, want 0600", fi.Mode().Perm())
	}
}

func TestExitCodes(t *testing.T) {
	tests := []struct {
		args []string
		want int
	}{
		{[]string{"profile", "set", "missing"}, exitcode.NotFound},
		{[]string{"-p", "missing", "topic", "list"}, exitcode.NotFound},
		{[]string{"topic", "list", "--bogus"}, exitcode.Usage},
		{[]string{"topic", "list", "a", "b"}, exitcode.Usage},
		{[]string{"frobnicate"}, exitcode.Usage},
		{[]string{"profile", "list", "-o", "xml"}, exitcode.Usage},
		{[]string{"consume", "t", "-o", `npipe:C:\x`}, exitcode.Usage},
		{[]string{"consume", "t", "--npipe-wait"}, exitcode.Usage},
		{[]string{"produce", "t", "--in", "npipe:"}, exitcode.Usage},
		{[]string{"produce", "t", "--in", "npipe:t", "-f", "x"}, exitcode.Usage},
	}
	for _, tt := range tests {
		t.Run(strings.Join(tt.args, " "), func(t *testing.T) {
			if _, errOut, code := run(t, tt.args...); code != tt.want {
				t.Errorf("code %d, want %d (stderr: %s)", code, tt.want, errOut)
			}
		})
	}
}

func TestInsecureModeWarning(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file modes do not apply on Windows")
	}
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, []byte(testProfiles), 0o644); err != nil {
		t.Fatal(err)
	}
	_, errOut, _ := runWith(t, path, "profile", "list")
	if !strings.Contains(errOut, "fix-perms") {
		t.Errorf("expected a permission warning, got %q", errOut)
	}
}

func TestOutAlias(t *testing.T) {
	out, _, code := run(t, "profile", "list", "--out", "name")
	if code != 0 || out != "dev\nprod\n" {
		t.Errorf("--out alias: code %d, out %q", code, out)
	}
}

func TestFromProperties(t *testing.T) {
	var in profileInput
	props := "bootstrap.servers=k1:9093,k2:9093\nsecurity.protocol=SASL_SSL\nsasl.mechanism=SCRAM-SHA-512\n" +
		"sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \\\n  username=\"bob\" password=\"p\\\"w\";\n" +
		"ssl.truststore.type=PEM\nssl.truststore.location=/etc/ca.pem\n# comment\n"
	if err := fromProperties(strings.NewReader(props), &in); err != nil {
		t.Fatal(err)
	}
	if in.bootstrap != "k1:9093,k2:9093" || in.security != secSASLTLS || in.mechanism != "scram-sha-512" ||
		in.username != "bob" || in.password != `p"w` || in.caFile != "/etc/ca.pem" {
		t.Errorf("parsed: %+v", in)
	}
	for _, bad := range []string{"security.protocol=SSL\nssl.keystore.type=JKS\nssl.keystore.location=/k.jks\nbootstrap.servers=a:1\n",
		"security.protocol=SASL_SSL\nsasl.mechanism=GSSAPI\nbootstrap.servers=a:1\n", "security.protocol=PLAINTEXT\n"} {
		var x profileInput
		if err := fromProperties(strings.NewReader(bad), &x); err == nil {
			t.Errorf("expected an error for %q", bad)
		}
	}
}

func TestProfileEditSet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	os.WriteFile(path, []byte(testProfiles), 0o600)
	if _, errOut, code := runWith(t, path, "profile", "edit", "dev", "--set", "read_only=true", "--set", "auth.mechanism=plain",
		"--set", "auth.username=u", "--set", "auth.password=p", "--set", "bootstrap_servers=h1:1,h2:2"); code != 0 {
		t.Fatalf("edit: %d %s", code, errOut)
	}
	out, _, _ := runWith(t, path, "profile", "show", "dev", "--reveal")
	for _, want := range []string{`"read_only": true`, `"plain"`, `"h2:2"`} {
		if !strings.Contains(out, want) {
			t.Errorf("show after edit missing %s:\n%s", want, out)
		}
	}
	if _, _, code := runWith(t, path, "profile", "edit", "dev", "--set", "nope=1"); code != exitcode.Usage {
		t.Errorf("unknown field: exit %d", code)
	}
	if _, _, code := runWith(t, path, "profile", "copy", "dev", "dev2"); code != 0 {
		t.Error("copy failed")
	}
	if _, _, code := runWith(t, path, "profile", "rename", "dev2", "dev3"); code != 0 {
		t.Error("rename failed")
	}
	if _, _, code := runWith(t, path, "profile", "delete", "dev3", "-y"); code != 0 {
		t.Error("delete failed")
	}
	names, _, _ := runWith(t, path, "profile", "list", "-o", "name")
	if names != "dev\nprod\n" {
		t.Errorf("profiles after copy/rename/delete: %q", names)
	}
}

func TestEveryCommandHasHelp(t *testing.T) {
	a := &app{stdin: strings.NewReader(""), stdout: &strings.Builder{}, stderr: &strings.Builder{}}
	root := a.newRootCmd()
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		for _, sub := range c.Commands() {
			path := strings.Fields(sub.CommandPath())[1:]
			var out strings.Builder
			code := Execute(t.Context(), append(path, "--help"), strings.NewReader(""), &out, &out)
			if code != 0 {
				t.Errorf("ntk %s --help: exit %d: %s", strings.Join(path, " "), code, out.String())
			}
			walk(sub)
		}
	}
	walk(root)
}

func TestParseIDs(t *testing.T) {
	got, err := parseIDs("5,0,3-4,4")
	if err != nil || fmt.Sprint(got) != "[0 3 4 5]" {
		t.Errorf("parseIDs: %v %v", got, err)
	}
	for _, bad := range []string{"a", "3-1", "1-x"} {
		if _, err := parseIDs(bad); err == nil {
			t.Errorf("%q: expected an error", bad)
		}
	}
}

func TestProfileColors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "profiles.json")
	for _, name := range []string{"one", "two"} {
		if _, errOut, code := runWith(t, path, "profile", "create", name, "--bootstrap", "localhost:1", "--no-test"); code != 0 {
			t.Fatalf("create %s: %d %s", name, code, errOut)
		}
	}
	if _, errOut, code := runWith(t, path, "profile", "copy", "one", "three"); code != 0 {
		t.Fatalf("copy: %d %s", code, errOut)
	}
	s, err := profile.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]string{}
	for _, name := range []string{"one", "two", "three"} {
		c := s.File.Profiles[name].Color
		if !slices.Contains(profile.Palette, c) {
			t.Errorf("%s: colour %q isn't from the palette", name, c)
		}
		if other, dup := seen[c]; dup {
			t.Errorf("%s and %s share colour %s", name, other, c)
		}
		seen[c] = name
	}
	if _, _, code := runWith(t, path, "profile", "create", "bad", "--bootstrap", "localhost:1", "--no-test", "--color", "purple"); code != exitcode.Usage {
		t.Errorf("an invalid colour: exit %d, want %d", code, exitcode.Usage)
	}
	if _, errOut, code := runWith(t, path, "profile", "create", "named", "--bootstrap", "localhost:1", "--no-test", "--color", "red"); code != 0 {
		t.Errorf("a named colour: %d %s", code, errOut)
	}
}

func TestOAuthFromPropertiesAndFlags(t *testing.T) {
	props := "bootstrap.servers=pkc.example.com:9092\nsecurity.protocol=SASL_SSL\nsasl.mechanism=OAUTHBEARER\n" +
		"sasl.oauthbearer.token.endpoint.url=https://idp.example.com/oauth2/token\n" +
		"sasl.jaas.config=org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required \\\n" +
		"  clientId=\"ntk\" clientSecret=\"s3cret\" scope=\"kafka\" extension_logicalCluster=\"lkc-1\" extension_identityPoolId=\"pool-1\";\n"
	var in profileInput
	if err := fromProperties(strings.NewReader(props), &in); err != nil {
		t.Fatal(err)
	}
	p, err := in.build()
	if err != nil {
		t.Fatal(err)
	}
	o := p.Auth.OAuth
	if p.Auth.Mechanism != "oauthbearer" || o == nil || o.TokenEndpoint != "https://idp.example.com/oauth2/token" || o.ClientID != "ntk" ||
		o.ClientSecret != "s3cret" || o.Scope != "kafka" || o.Extensions["logicalCluster"] != "lkc-1" || o.Extensions["identityPoolId"] != "pool-1" {
		t.Errorf("imported auth = %+v %+v", p.Auth, o)
	}

	path := filepath.Join(t.TempDir(), "profiles.json")
	var stdout, stderr strings.Builder
	code := Execute(t.Context(), []string{"--profile-path", path, "profile", "create", "cc", "--bootstrap", "k:9093", "--tls", "--no-test",
		"--sasl", "oauthbearer", "--oauth-token-endpoint", "https://idp/token", "--oauth-client-id", "ntk", "--oauth-client-secret-stdin"},
		strings.NewReader("s3cret\n"), &stdout, &stderr)
	if code != 0 {
		t.Fatalf("create: %d %s", code, stderr.String())
	}
	s, err := profile.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := s.File.Profiles["cc"].Auth.OAuth; got == nil || got.ClientSecret != "s3cret" || got.TokenEndpoint != "https://idp/token" {
		t.Errorf("created oauth = %+v", got)
	}
	if _, _, code := runWith(t, path, "profile", "create", "both", "--bootstrap", "k:9093", "--no-test", "--sasl", "oauthbearer",
		"--oauth-token-file", "/run/t", "--oauth-token-command", "x"); code != exitcode.Usage {
		t.Errorf("two token sources: exit %d, want %d", code, exitcode.Usage)
	}
}

func TestMSKIAMProfiles(t *testing.T) {
	props := "bootstrap.servers=b-1.demo.kafka.eu-west-1.amazonaws.com:9098\nsecurity.protocol=SASL_SSL\nsasl.mechanism=AWS_MSK_IAM\n" +
		"sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required awsProfileName=\"dev\" awsRoleArn=\"arn:aws:iam::123456789012:role/kafka\";\n" +
		"sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler\n"
	var in profileInput
	if err := fromProperties(strings.NewReader(props), &in); err != nil {
		t.Fatal(err)
	}
	p, err := in.build()
	if err != nil {
		t.Fatal(err)
	}
	if p.Auth.Mechanism != "aws-msk-iam" || p.Auth.AWS == nil || p.Auth.AWS.Profile != "dev" || p.Auth.AWS.RoleARN != "arn:aws:iam::123456789012:role/kafka" || !p.TLS.On() {
		t.Errorf("imported = %+v %+v", p.Auth, p.Auth.AWS)
	}

	path := filepath.Join(t.TempDir(), "profiles.json")
	if _, errOut, code := runWith(t, path, "profile", "create", "msk", "--bootstrap", "b-1.msk:9098", "--tls", "--no-test",
		"--sasl", "aws-msk-iam", "--aws-profile", "prod"); code != 0 {
		t.Fatalf("create: %d %s", code, errOut)
	}
	if _, _, code := runWith(t, path, "profile", "create", "plain-msk", "--bootstrap", "b-1.msk:9098", "--no-test", "--sasl", "aws-msk-iam"); code != exitcode.Usage {
		t.Errorf("aws-msk-iam without TLS: exit %d, want %d", code, exitcode.Usage)
	}
	if got := maskKey("AKIAIOSFODNN7EXAMPLE"); got != "AKIA…MPLE" {
		t.Errorf("maskKey = %q", got)
	}
}
