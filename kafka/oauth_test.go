// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntailio/ntk/profile"
)

func unsignedJWT(claims map[string]any) string {
	enc := func(v any) string {
		b, _ := json.Marshal(v)
		return base64.RawURLEncoding.EncodeToString(b)
	}
	return enc(map[string]string{"alg": "none"}) + "." + enc(claims) + "."
}

// tokenServer is a client-credentials endpoint that counts the tokens it hands out.
func tokenServer(t *testing.T, expiresIn int) (*httptest.Server, *atomic.Int32, *http.Request) {
	t.Helper()
	var hits atomic.Int32
	last := &http.Request{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		*last = *r.Clone(context.Background())
		id, secret, ok := r.BasicAuth()
		if !ok {
			id, secret = r.Form.Get("client_id"), r.Form.Get("client_secret")
		}
		if id != "ntk" || secret != "s3cret" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		n := hits.Add(1)
		tok := unsignedJWT(map[string]any{"sub": "svc-orders", "n": n, "exp": time.Now().Add(time.Duration(expiresIn) * time.Second).Unix()})
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"access_token": tok, "token_type": "Bearer", "expires_in": expiresIn})
	}))
	t.Cleanup(srv.Close)
	return srv, &hits, last
}

func withCacheDir(t *testing.T) {
	dir := t.TempDir()
	old := userCacheDir
	userCacheDir = func() (string, error) { return dir, nil }
	t.Cleanup(func() { userCacheDir = old })
}

func TestClientCredentialsAreCached(t *testing.T) {
	withCacheDir(t)
	srv, hits, last := tokenServer(t, 3600)
	o := &profile.OAuth{TokenEndpoint: srv.URL, ClientID: "ntk", ClientSecret: "s3cret", Scope: "kafka", Audience: "orders"}

	tok, err := NewTokenSource(o, nil).Token(context.Background(), false)
	if err != nil {
		t.Fatal(err)
	}
	if tok.Subject != "svc-orders" || tok.Cached || hits.Load() != 1 {
		t.Fatalf("first token: %+v, %d requests", tok, hits.Load())
	}
	if got := last.Form.Get("audience"); got != "orders" {
		t.Errorf("audience sent: %q", got)
	}
	if got := last.Form.Get("scope"); got != "kafka" {
		t.Errorf("scope sent: %q", got)
	}

	again, err := NewTokenSource(o, nil).Token(context.Background(), false) // as a later run would
	if err != nil || again.Value != tok.Value || !again.Cached || hits.Load() != 1 {
		t.Errorf("second run should reuse the cached token: cached=%v, %d requests, %v", again.Cached, hits.Load(), err)
	}

	if _, err := NewTokenSource(o, nil).Token(context.Background(), true); err != nil || hits.Load() != 2 {
		t.Errorf("fresh should skip the cache: %d requests, %v", hits.Load(), err)
	}

	other := *o
	other.Scope = "admin"
	if _, err := NewTokenSource(&other, nil).Token(context.Background(), false); err != nil || hits.Load() != 3 {
		t.Errorf("a different scope needs its own token: %d requests, %v", hits.Load(), err)
	}
}

func TestShortLivedTokensAreNotReused(t *testing.T) {
	withCacheDir(t)
	srv, hits, _ := tokenServer(t, 30) // less than the reuse slack
	o := &profile.OAuth{TokenEndpoint: srv.URL, ClientID: "ntk", ClientSecret: "s3cret"}
	for range 2 {
		if _, err := NewTokenSource(o, nil).Token(context.Background(), false); err != nil {
			t.Fatal(err)
		}
	}
	if hits.Load() != 2 {
		t.Errorf("a token about to expire was reused: %d requests", hits.Load())
	}
}

func TestTokenEndpointErrors(t *testing.T) {
	withCacheDir(t)
	srv, _, _ := tokenServer(t, 3600)
	_, err := NewTokenSource(&profile.OAuth{TokenEndpoint: srv.URL, ClientID: "ntk", ClientSecret: "wrong"}, nil).Token(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "401") || !strings.Contains(err.Error(), "invalid_client") {
		t.Errorf("wrong secret: %v", err)
	}
	_, err = NewTokenSource(&profile.OAuth{TokenEndpoint: "http://127.0.0.1:1/token", ClientID: "ntk", ClientSecret: "x"}, nil).Token(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "token endpoint") {
		t.Errorf("unreachable endpoint: %v", err)
	}
}

func TestTokenFileIsReadEachTime(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "token")
	os.WriteFile(path, []byte(unsignedJWT(map[string]any{"sub": "one"})+"\n"), 0o600)
	ts := NewTokenSource(&profile.OAuth{TokenFile: "token"}, func(p string) string { return filepath.Join(dir, p) })
	first, err := ts.Token(context.Background(), false)
	if err != nil || first.Subject != "one" || first.Source != "token file" {
		t.Fatalf("first: %+v %v", first, err)
	}
	os.WriteFile(path, []byte(unsignedJWT(map[string]any{"sub": "two"})), 0o600)
	if second, _ := ts.Token(context.Background(), false); second.Subject != "two" {
		t.Errorf("a rotated token file wasn't re-read: %+v", second)
	}
	os.WriteFile(path, []byte("  \n"), 0o600)
	if _, err := ts.Token(context.Background(), false); err == nil || !strings.Contains(err.Error(), "empty") {
		t.Errorf("empty file: %v", err)
	}
}

func TestTokenCommand(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses sh")
	}
	count := filepath.Join(t.TempDir(), "count")
	ts := NewTokenSource(&profile.OAuth{TokenCommand: "echo x >> " + count + "; echo plain-token"}, nil)
	for range 3 {
		tok, err := ts.Token(context.Background(), false)
		if err != nil || tok.Value != "plain-token" {
			t.Fatalf("%+v %v", tok, err)
		}
	}
	if b, _ := os.ReadFile(count); strings.Count(string(b), "x") != 1 {
		t.Errorf("the command ran %d times, want once per process", strings.Count(string(b), "x"))
	}
	_, err := NewTokenSource(&profile.OAuth{TokenCommand: "echo no such account >&2; exit 1"}, nil).Token(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "no such account") {
		t.Errorf("failing command: %v", err)
	}
}

func TestOAuthExtensionsReachTheHandshake(t *testing.T) {
	dir := t.TempDir()
	os.WriteFile(filepath.Join(dir, "t"), []byte("tok"), 0o600)
	a := profile.Auth{Mechanism: "oauthbearer", OAuth: &profile.OAuth{TokenFile: filepath.Join(dir, "t"), Extensions: map[string]string{"logicalCluster": "lkc-1"}}}
	m, err := oauthMechanism(a, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, first, err := m.Authenticate(context.Background(), "localhost:9092")
	if err != nil {
		t.Fatal(err)
	}
	if s := string(first); !strings.Contains(s, "auth=Bearer tok") || !strings.Contains(s, "logicalCluster=lkc-1") {
		t.Errorf("handshake = %q", s)
	}
}

func TestJWTClaims(t *testing.T) {
	sub, exp, ok := jwtClaims(unsignedJWT(map[string]any{"sub": "bob", "exp": 1900000000}))
	if !ok || sub != "bob" || exp.Unix() != 1900000000 {
		t.Errorf("got %q %v %v", sub, exp, ok)
	}
	if _, _, ok := jwtClaims("not-a-jwt"); ok {
		t.Error("an opaque token parsed as a JWT")
	}
}
