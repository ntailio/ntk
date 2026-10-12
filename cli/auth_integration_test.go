// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/testkit"
)

// OAUTH is the sandbox listener that accepts unsigned tokens.
var oauthBootstrap = []string{"localhost:19105", "localhost:19205", "localhost:19305"}

func TestOAuthClientCredentials(t *testing.T) {
	sub := testkit.Name(t, "oauth")
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		id, secret, ok := r.BasicAuth()
		if !ok {
			id, secret = r.Form.Get("client_id"), r.Form.Get("client_secret")
		}
		w.Header().Set("Content-Type", "application/json")
		if id != "ntk" || secret != "s3cret" {
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(`{"error":"invalid_client"}`))
			return
		}
		hits.Add(1)
		enc := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
		tok := enc(map[string]string{"alg": "none"}) + "." + enc(map[string]any{"sub": sub, "iat": time.Now().Unix(), "exp": time.Now().Add(time.Hour).Unix()}) + "."
		json.NewEncoder(w).Encode(map[string]any{"access_token": tok, "token_type": "Bearer", "expires_in": 3600})
	}))
	defer srv.Close()

	oauth := func(secret string) map[string]any {
		return map[string]any{"bootstrap_servers": oauthBootstrap, "auth": map[string]any{
			"mechanism": "oauthbearer",
			"oauth":     map[string]any{"token_endpoint": srv.URL, "client_id": "ntk", "client_secret": secret},
		}}
	}
	e := newEnv(t, map[string]any{"oauth": oauth("s3cret"), "wrong": oauth("nope")})

	contains(t, e.ok("-p", "oauth", "whoami"), "User:"+sub, "oauthbearer")
	if n := hits.Load(); n != 1 {
		t.Fatalf("token endpoint called %d times for the first run, want 1", n)
	}
	e.ok("-p", "oauth", "topic", "list")
	if n := hits.Load(); n != 1 {
		t.Errorf("a second run fetched a new token instead of using the cached one (%d requests)", n)
	}

	out := e.ok("profile", "test", "oauth")
	contains(t, out, "Token", "from the token endpoint", "sub "+sub, "oauthbearer as "+sub)
	if n := hits.Load(); n != 2 {
		t.Errorf("profile test should fetch a fresh token: %d requests", n)
	}

	out, _ = e.fails(exitcode.Connection, "profile", "test", "wrong")
	if !strings.Contains(out, "401") || !strings.Contains(out, "client_secret") {
		t.Errorf("wrong secret output:\n%s", out)
	}
}
