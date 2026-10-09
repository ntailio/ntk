// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package testkit

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func cleanupCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 15*time.Second)
}

// ScramUser creates a SCRAM-SHA-512 user named ntk-test-…-<purpose> and deletes it (and its ACLs) after the test.
func ScramUser(t testing.TB, adm *kadm.Client, purpose string) (user, password string) {
	t.Helper()
	user = Name(t, purpose)
	b := make([]byte, 18)
	_, _ = rand.Read(b)
	password = base64.RawURLEncoding.EncodeToString(b)
	resp, err := adm.AlterUserSCRAMs(t.Context(), nil, []kadm.UpsertSCRAM{{User: user, Mechanism: kadm.ScramSha512, Iterations: 4096, Password: password}})
	if err == nil {
		err = resp.Error()
	}
	if err != nil {
		t.Fatalf("creating SCRAM user %s: %v", user, err)
	}
	t.Cleanup(func() {
		ctx, cancel := cleanupCtx()
		defer cancel()
		_, _ = adm.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: user, Mechanism: kadm.ScramSha512}}, nil)
		deletePrincipalACLs(ctx, adm, "User:"+user)
	})
	return user, password
}

func deletePrincipalACLs(ctx context.Context, adm *kadm.Client, principal string) {
	b := kadm.NewACLs().AnyResource().ResourcePatternType(kadm.ACLPatternAny).Operations(kadm.OpAny).
		Allow(principal).Deny(principal).AllowHosts().DenyHosts()
	_, _ = adm.DeleteACLs(ctx, b)
}

// ClientCert mints a client certificate with CN=ntk-test-…-<purpose>, signed by
// the sandbox CA, into t.TempDir(). The principal is "User:" + CN.
func ClientCert(t testing.TB, purpose string) (certFile, keyFile, principal string) {
	t.Helper()
	dir := filepath.Join(RepoRoot(t), "sandbox", "certs")
	caCertPEM, err := os.ReadFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Skipf("sandbox CA not found (start the sandbox): %v", err)
	}
	caKeyPEM, err := os.ReadFile(filepath.Join(dir, "ca.key"))
	if err != nil {
		t.Skipf("sandbox CA key not found: %v", err)
	}
	cb, _ := pem.Decode(caCertPEM)
	kb, _ := pem.Decode(caKeyPEM)
	if cb == nil || kb == nil {
		t.Fatal("sandbox CA files are not PEM")
	}
	ca, err := x509.ParseCertificate(cb.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	caKey, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	cn := Name(t, purpose)
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: cn},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(24 * time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	pk, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	certFile, keyFile = filepath.Join(out, "client.crt"), filepath.Join(out, "client.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: pk}), 0o600); err != nil {
		t.Fatal(err)
	}
	return certFile, keyFile, "User:" + cn
}

// Group returns a unique consumer group name and deletes the group after the test.
func Group(t testing.TB, adm *kadm.Client, purpose string) string {
	t.Helper()
	name := Name(t, purpose)
	t.Cleanup(func() {
		ctx, cancel := cleanupCtx()
		defer cancel()
		_, _ = adm.DeleteGroups(ctx, name)
	})
	return name
}

// Produce writes values (keyed "k0", "k1", …) to topic and waits for them to be acknowledged.
func Produce(t testing.TB, topic string, values ...string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(Bootstrap()...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	var recs []*kgo.Record
	for i, v := range values {
		recs = append(recs, &kgo.Record{Key: []byte("k" + strconv.Itoa(i)), Value: []byte(v)})
	}
	if err := cl.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatalf("producing to %s: %v", topic, err)
	}
}

// Profiles writes a profile file for the sandbox into t.TempDir() and returns its path.
// Profile "sandbox" (plaintext, super user) is active; extra profiles can be added.
func Profiles(t testing.TB, extra map[string]any) string {
	t.Helper()
	ps := map[string]any{
		"sandbox": map[string]any{"bootstrap_servers": Bootstrap(), "auth": map[string]any{"mechanism": "none"}},
	}
	for k, v := range extra {
		ps[k] = v
	}
	b, err := json.MarshalIndent(map[string]any{"version": 1, "active": "sandbox", "profiles": ps}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "profiles.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Created returns when a test resource name was created, from its <created> part.
func Created(name string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(name, Prefix)
	if !ok {
		return time.Time{}, false
	}
	ts, _, _ := strings.Cut(rest, "-")
	secs, err := strconv.ParseInt(ts, 36, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(secs, 0), true
}

func stale(name string, olderThan time.Duration) bool {
	created, ok := Created(name)
	return ok && (olderThan <= 0 || time.Since(created) > olderThan)
}

// Janitor deletes ntk-test-* topics, groups, SCRAM users, ACLs, and quotas
// older than olderThan (0 = all). It returns what it removed.
func Janitor(ctx context.Context, cl *kgo.Client, olderThan time.Duration) ([]string, error) {
	adm := kadm.NewClient(cl)
	var removed []string
	var errs []error

	ts, err := adm.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	var topics []string
	for _, t := range ts {
		if stale(t.Topic, olderThan) {
			topics = append(topics, t.Topic)
		}
	}
	if len(topics) > 0 {
		if _, err := adm.DeleteTopics(ctx, topics...); err != nil {
			errs = append(errs, err)
		}
		for _, t := range topics {
			removed = append(removed, "topic "+t)
		}
	}

	if gs, err := adm.ListGroups(ctx); err == nil {
		var names []string
		for _, g := range gs.Groups() {
			if stale(g, olderThan) {
				names = append(names, g)
			}
		}
		if len(names) > 0 {
			_, _ = adm.DeleteGroups(ctx, names...)
			for _, g := range names {
				removed = append(removed, "group "+g)
			}
		}
	}

	if users, err := adm.DescribeUserSCRAMs(ctx); err == nil {
		for u, d := range users {
			if !stale(u, olderThan) {
				continue
			}
			for _, c := range d.CredInfos {
				_, _ = adm.AlterUserSCRAMs(ctx, []kadm.DeleteSCRAM{{User: u, Mechanism: c.Mechanism}}, nil)
			}
			removed = append(removed, "user "+u)
		}
	}

	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType, req.ResourcePatternType, req.Operation, req.PermissionType =
		kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny, kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
	if resp, err := req.RequestWith(ctx, cl); err == nil {
		del := kmsg.NewPtrDeleteACLsRequest()
		for _, r := range resp.Resources {
			for _, a := range r.ACLs {
				p, _ := strings.CutPrefix(a.Principal, "User:")
				if !stale(p, olderThan) && !stale(r.ResourceName, olderThan) {
					continue
				}
				f := kmsg.NewDeleteACLsRequestFilter()
				f.ResourceType, f.ResourceName, f.ResourcePatternType = r.ResourceType, kmsg.StringPtr(r.ResourceName), r.ResourcePatternType
				f.Principal, f.Host, f.Operation, f.PermissionType = kmsg.StringPtr(a.Principal), kmsg.StringPtr(a.Host), a.Operation, a.PermissionType
				del.Filters = append(del.Filters, f)
				removed = append(removed, fmt.Sprintf("acl %s %s on %s", a.Principal, a.Operation, r.ResourceName))
			}
		}
		if len(del.Filters) > 0 {
			if _, err := del.RequestWith(ctx, cl); err != nil {
				errs = append(errs, err)
			}
		}
	}

	for _, typ := range []string{"user", "client-id"} {
		qs, err := adm.DescribeClientQuotas(ctx, false, []kadm.DescribeClientQuotaComponent{{Type: typ, MatchType: 2}})
		if err != nil {
			continue
		}
		for _, q := range qs {
			hit := false
			for _, c := range q.Entity {
				if c.Name != nil && stale(*c.Name, olderThan) {
					hit = true
				}
			}
			if !hit {
				continue
			}
			var ops []kadm.AlterClientQuotaOp
			for _, v := range q.Values {
				ops = append(ops, kadm.AlterClientQuotaOp{Key: v.Key, Remove: true})
			}
			_, _ = adm.AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{Entity: q.Entity, Ops: ops}})
			removed = append(removed, "quota "+q.Entity.String())
		}
	}
	return removed, errors.Join(errs...)
}
