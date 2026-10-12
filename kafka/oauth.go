// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

	"github.com/ntailio/ntk/profile"
)

// A token is reused until this long before it expires.
const tokenSlack = 60 * time.Second

type Token struct {
	Value   string
	Expiry  time.Time // zero when unknown
	Subject string    // the JWT "sub" claim, unverified; empty if the token isn't a JWT
	Source  string    // "token endpoint", "token file" or "token command"
	Cached  bool      // true when it came from the token cache
}

type TokenSource struct {
	o       *profile.OAuth
	resolve func(string) string

	mu  sync.Mutex
	mem *Token // command tokens, kept for the life of the process
}

func NewTokenSource(o *profile.OAuth, resolve func(string) string) *TokenSource {
	return &TokenSource{o: o, resolve: resolve}
}

// fresh skips the caches, but still refreshes them.
func (s *TokenSource) Token(ctx context.Context, fresh bool) (Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case s.o.TokenFile != "":
		return s.fromFile()
	case s.o.TokenCommand != "":
		if !fresh && s.mem != nil && usable(s.mem.Expiry) {
			return *s.mem, nil
		}
		t, err := s.fromCommand(ctx)
		if err == nil {
			s.mem = &t
		}
		return t, err
	default:
		return s.fromEndpoint(ctx, fresh)
	}
}

func (s *TokenSource) fromFile() (Token, error) {
	path := s.o.TokenFile
	if s.resolve != nil {
		path = s.resolve(path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Token{}, fmt.Errorf("token file: %w", err)
	}
	return newToken(string(b), "token file", time.Time{})
}

func (s *TokenSource) fromCommand(ctx context.Context) (Token, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = exec.CommandContext(ctx, "cmd", "/C", s.o.TokenCommand)
	} else {
		cmd = exec.CommandContext(ctx, "sh", "-c", s.o.TokenCommand)
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return Token{}, fmt.Errorf("token command failed: %s", msg)
	}
	t, err := newToken(string(out), "token command", time.Time{})
	if err == nil && t.Expiry.IsZero() {
		t.Expiry = time.Now().Add(5 * time.Minute)
	}
	return t, err
}

func (s *TokenSource) fromEndpoint(ctx context.Context, fresh bool) (Token, error) {
	cache := cachePath(s.o)
	if !fresh {
		if t, ok := readCachedToken(cache); ok {
			return t, nil
		}
	}
	cfg := clientcredentials.Config{
		ClientID:     s.o.ClientID,
		ClientSecret: s.o.ClientSecret,
		TokenURL:     s.o.TokenEndpoint,
		Scopes:       strings.Fields(s.o.Scope),
	}
	if s.o.Audience != "" {
		cfg.EndpointParams = url.Values{"audience": {s.o.Audience}}
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	ot, err := cfg.Token(ctx)
	if err != nil {
		var re *oauth2.RetrieveError
		if errors.As(err, &re) && re.Response != nil {
			code := re.ErrorCode
			if code == "" {
				code = strings.TrimSpace(string(re.Body))
			}
			return Token{}, fmt.Errorf("token endpoint returned %d: %s", re.Response.StatusCode, code)
		}
		return Token{}, fmt.Errorf("token endpoint: %w", err)
	}
	t, err := newToken(ot.AccessToken, "token endpoint", ot.Expiry)
	if err != nil {
		return Token{}, err
	}
	writeCachedToken(cache, t)
	return t, nil
}

func newToken(raw, source string, expiry time.Time) (Token, error) {
	v := strings.TrimSpace(raw)
	if v == "" {
		return Token{}, fmt.Errorf("%s gave an empty token", source)
	}
	t := Token{Value: v, Source: source, Expiry: expiry}
	if sub, exp, ok := jwtClaims(v); ok {
		t.Subject = sub
		if t.Expiry.IsZero() {
			t.Expiry = exp
		}
	}
	return t, nil
}

func usable(expiry time.Time) bool {
	return expiry.IsZero() || time.Until(expiry) > tokenSlack
}

// jwtClaims reads sub and exp from a JWT without verifying it; only the broker can do that.
func jwtClaims(token string) (sub string, exp time.Time, ok bool) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", time.Time{}, false
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return "", time.Time{}, false
	}
	var c struct {
		Sub string  `json:"sub"`
		Exp float64 `json:"exp"`
	}
	if json.Unmarshal(b, &c) != nil {
		return "", time.Time{}, false
	}
	if c.Exp > 0 {
		exp = time.Unix(int64(c.Exp), 0)
	}
	return c.Sub, exp, true
}

var userCacheDir = os.UserCacheDir

func cachePath(o *profile.OAuth) string {
	dir, err := userCacheDir()
	if err != nil {
		return ""
	}
	h := sha256.Sum256([]byte(strings.Join([]string{o.TokenEndpoint, o.ClientID, o.Scope, o.Audience}, "\x00")))
	return filepath.Join(dir, "ntk", "tokens", hex.EncodeToString(h[:16])+".json")
}

type cachedToken struct {
	Token  string    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

func readCachedToken(path string) (Token, bool) {
	if path == "" {
		return Token{}, false
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Token{}, false
	}
	var c cachedToken
	if json.Unmarshal(b, &c) != nil || c.Token == "" || c.Expiry.IsZero() || !usable(c.Expiry) {
		return Token{}, false
	}
	t, err := newToken(c.Token, "token endpoint", c.Expiry)
	t.Cached = true
	return t, err == nil
}

// writeCachedToken stores a token for later runs. Failing to cache isn't an error.
func writeCachedToken(path string, t Token) {
	if path == "" || t.Expiry.IsZero() {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return
	}
	b, err := json.Marshal(cachedToken{Token: t.Value, Expiry: t.Expiry})
	if err != nil {
		return
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".token-*")
	if err != nil {
		return
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(b); err != nil {
		f.Close()
		return
	}
	if f.Close() == nil {
		_ = os.Rename(f.Name(), path)
	}
}

func oauthMechanism(a profile.Auth, resolve func(string) string) (sasl.Mechanism, error) {
	if a.OAuth == nil {
		return nil, errors.New("auth.oauth is required")
	}
	ts := NewTokenSource(a.OAuth, resolve)
	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		t, err := ts.Token(ctx, false)
		if err != nil {
			return oauth.Auth{}, err
		}
		return oauth.Auth{Token: t.Value, Extensions: a.OAuth.Extensions}, nil
	}), nil
}
