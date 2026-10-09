// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// Encrypted keys made with openssl, the way users (and the sandbox) create them.
func TestEncryptedKeys(t *testing.T) {
	if _, err := exec.LookPath("openssl"); err != nil {
		t.Skip("openssl not installed")
	}
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		if out, err := exec.Command("openssl", args...).CombinedOutput(); err != nil {
			t.Fatalf("openssl %v: %v\n%s", args, err, out)
		}
	}
	key, crt := filepath.Join(dir, "k.pem"), filepath.Join(dir, "c.pem")
	run("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=t", "-days", "1", "-keyout", key, "-out", crt)
	certPEM, _ := os.ReadFile(crt)
	cases := map[string][]string{
		"pkcs8 aes-256-cbc sha256": {"pkcs8", "-topk8", "-v2", "aes-256-cbc", "-v2prf", "hmacWithSHA256"},
		"pkcs8 aes-128-cbc sha1":   {"pkcs8", "-topk8", "-v2", "aes-128-cbc", "-v2prf", "hmacWithSHA1"},
		"pkcs8 des3":               {"pkcs8", "-topk8", "-v2", "des3"},
		"legacy rsa aes":           {"rsa", "-aes256", "-traditional"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			out := filepath.Join(dir, name+".key")
			run(append(args, "-in", key, "-out", out, "-passout", "pass:s3cret")...)
			keyPEM, _ := os.ReadFile(out)
			if _, err := keyPair(certPEM, keyPEM, "s3cret"); err != nil {
				t.Fatalf("decrypt: %v", err)
			}
			if _, err := keyPair(certPEM, keyPEM, "wrong"); err == nil {
				t.Error("wrong password accepted")
			}
			if _, err := keyPair(certPEM, keyPEM, ""); err == nil {
				t.Error("missing password accepted")
			}
		})
	}
	plain, _ := os.ReadFile(key)
	if _, err := keyPair(certPEM, plain, ""); err != nil {
		t.Errorf("unencrypted key: %v", err)
	}
}
