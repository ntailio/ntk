// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ntailio/ntk/profile"
)

const mskHost = "b-1.demo.abc123.c2.kafka.eu-west-1.amazonaws.com:9098"

// isolateAWS keeps the test away from real AWS config, credentials and instance metadata.
func isolateAWS(t *testing.T, sharedCredentials string) {
	dir := t.TempDir()
	creds := filepath.Join(dir, "credentials")
	os.WriteFile(creds, []byte(sharedCredentials), 0o600)
	empty := filepath.Join(dir, "config")
	os.WriteFile(empty, nil, 0o600)
	for k, v := range map[string]string{
		"AWS_SHARED_CREDENTIALS_FILE": creds, "AWS_CONFIG_FILE": empty, "AWS_EC2_METADATA_DISABLED": "true",
		"AWS_ACCESS_KEY_ID": "", "AWS_SECRET_ACCESS_KEY": "", "AWS_SESSION_TOKEN": "", "AWS_PROFILE": "", "AWS_REGION": "",
		"AWS_WEB_IDENTITY_TOKEN_FILE": "", "AWS_CONTAINER_CREDENTIALS_FULL_URI": "", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI": "",
	} {
		t.Setenv(k, v)
	}
}

func handshake(t *testing.T, a profile.Auth) (map[string]string, error) {
	t.Helper()
	m, err := awsMechanism(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, first, err := m.Authenticate(context.Background(), mskHost)
	if err != nil {
		return nil, err
	}
	var payload map[string]string
	if err := json.Unmarshal(first, &payload); err != nil {
		t.Fatalf("handshake isn't JSON: %s", first)
	}
	return payload, nil
}

func TestMSKIAMHandshake(t *testing.T) {
	isolateAWS(t, "")
	t.Setenv("AWS_ACCESS_KEY_ID", "AKIAFROMENVIRONMENT1")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "secret")
	p, err := handshake(t, profile.Auth{Mechanism: "aws-msk-iam"})
	if err != nil {
		t.Fatal(err)
	}
	if p["action"] != "kafka-cluster:Connect" {
		t.Errorf("action = %q", p["action"])
	}
	if c := p["x-amz-credential"]; !strings.HasPrefix(c, "AKIAFROMENVIRONMENT1/") || !strings.HasSuffix(c, "/eu-west-1/kafka-cluster/aws4_request") {
		t.Errorf("credential scope = %q", c)
	}
	if !strings.HasPrefix(p["user-agent"], "ntk/") {
		t.Errorf("user agent = %q", p["user-agent"])
	}
	if p["x-amz-signature"] == "" {
		t.Error("the handshake isn't signed")
	}
}

func TestMSKIAMNamedProfile(t *testing.T) {
	isolateAWS(t, "[dev]\naws_access_key_id = AKIAFROMDEVPROFILE01\naws_secret_access_key = secret\n")
	p, err := handshake(t, profile.Auth{Mechanism: "aws-msk-iam", AWS: &profile.AWS{Profile: "dev"}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(p["x-amz-credential"], "AKIAFROMDEVPROFILE01/") {
		t.Errorf("credentials didn't come from the dev profile: %q", p["x-amz-credential"])
	}
}

func TestMSKIAMWithoutCredentials(t *testing.T) {
	isolateAWS(t, "")
	_, err := handshake(t, profile.Auth{Mechanism: "aws-msk-iam"})
	if err == nil || !strings.Contains(err.Error(), "AWS credentials") {
		t.Errorf("no credentials: %v", err)
	}
}
