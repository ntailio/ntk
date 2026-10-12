// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/ntailio/ntk/brokers"
	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/kafka"
	"github.com/ntailio/ntk/profile"
)

func expiry(c *x509.Certificate) string {
	d := time.Until(c.NotAfter)
	if d < 0 {
		return "EXPIRED " + c.NotAfter.Format("2006-01-02")
	}
	return fmt.Sprintf("expires in %dd", int(d.Hours()/24))
}

func roughly(d time.Duration) string {
	switch {
	case d < 0:
		return "the past (expired)"
	case d >= 48*time.Hour:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	case d >= time.Hour:
		return fmt.Sprintf("%dh%02dm", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%dm", int(d.Minutes()))
}

func maskKey(k string) string {
	if len(k) <= 8 {
		return "****"
	}
	return k[:4] + "…" + k[len(k)-4:]
}

func tokenHint(o *profile.OAuth) string {
	switch {
	case o.TokenFile != "":
		return "check auth.oauth.token_file"
	case o.TokenCommand != "":
		return "run auth.oauth.token_command yourself to see what it prints"
	}
	return "check auth.oauth.token_endpoint, client_id and client_secret"
}

func (a *app) newProfileTestCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "test [name]",
		Short:             "Connect with a profile step by step and report what works",
		Args:              usageArgs(cobra.MaximumNArgs(1)),
		ValidArgsFunction: a.completeProfileArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			sel := a.flags.profile
			if len(args) == 1 {
				sel = args[0]
			}
			name, p, err := a.selectProfile(s, sel)
			if err != nil {
				return err
			}
			return a.diagnose(cmd.Context(), s, name, p)
		},
	}
}

func (a *app) diagnose(ctx context.Context, s *profile.Store, name string, p *profile.Profile) error {
	w := a.stdout
	ok := func(stage, format string, args ...any) {
		fmt.Fprintf(w, "✓ %-11s %s\n", stage, fmt.Sprintf(format, args...))
	}
	fail := func(code int, stage, msg, hint string) error {
		fmt.Fprintf(w, "✗ %-11s %s\n", stage, msg)
		if hint != "" {
			fmt.Fprintf(w, "  hint: %s\n", hint)
		}
		return exitcode.With(code, fmt.Errorf("profile %q: %s check failed", name, strings.ToLower(stage)))
	}
	if err := p.Validate(); err != nil {
		return fail(exitcode.Usage, "Profile", err.Error(), "fix it with `ntk profile edit "+name+"`")
	}
	addr := p.BootstrapServers[0]
	host, port, _ := net.SplitHostPort(addr)

	ips, err := net.DefaultResolver.LookupHost(ctx, host)
	if err != nil {
		return fail(exitcode.Connection, "DNS", err.Error(), "check the host name in bootstrap_servers")
	}
	ok("DNS", "%s → %s", host, strings.Join(ips, ", "))

	start := time.Now()
	conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, port), 10*time.Second)
	if err != nil {
		return fail(exitcode.Connection, "TCP", err.Error(), "the port is closed or filtered: check the listener port and firewalls")
	}
	ok("TCP", "%s (%s)", addr, time.Since(start).Round(time.Millisecond))

	if p.TLS.On() {
		cfg, err := kafka.TLSConfig(p.TLS, s.ResolveFile)
		if err != nil {
			conn.Close()
			return fail(exitcode.Usage, "TLS", err.Error(), "check the tls.* file paths and key_password")
		}
		if cfg.ServerName == "" {
			cfg.ServerName = host
		}
		tc := tls.Client(conn, cfg)
		tc.SetDeadline(time.Now().Add(10 * time.Second))
		if err := tc.Handshake(); err != nil {
			conn.Close()
			hint := "is this a TLS listener? (security protocol " + p.SecurityProtocol() + ")"
			var unknown x509.UnknownAuthorityError
			var host x509.HostnameError
			switch {
			case errors.As(err, &unknown):
				hint = "the server certificate is not signed by a trusted CA: set tls.ca_file"
			case errors.As(err, &host):
				hint = "the certificate does not match the host: set tls.server_name"
			case strings.Contains(err.Error(), "certificate required") || strings.Contains(err.Error(), "bad certificate"):
				hint = "the broker requires a client certificate (mTLS): set tls.cert_file and tls.key_file"
			}
			return fail(exitcode.Connection, "TLS", err.Error(), hint)
		}
		certs := tc.ConnectionState().PeerCertificates
		if len(certs) > 0 {
			ok("TLS", "server cert %s, issuer %s, %s", certs[0].Subject.CommonName, certs[0].Issuer.CommonName, expiry(certs[0]))
		}
		tc.Close()
		if p.TLS.HasClientCert() {
			c, err := kafka.LoadCertificate(p.TLS, s.ResolveFile)
			if err != nil {
				return fail(exitcode.Usage, "Client cert", err.Error(), "check tls.cert_file, tls.key_file, and tls.key_password")
			}
			ok("Client cert", "%s, %s", c.Subject.String(), expiry(c))
		}
	} else {
		conn.Close()
	}

	var tokenSub string
	if p.Auth.Mechanism == "oauthbearer" && p.Auth.OAuth != nil {
		t, err := kafka.NewTokenSource(p.Auth.OAuth, s.ResolveFile).Token(ctx, true)
		if err != nil {
			return fail(exitcode.Connection, "Token", err.Error(), tokenHint(p.Auth.OAuth))
		}
		detail := "from the " + t.Source
		if !t.Expiry.IsZero() {
			detail += ", expires in " + roughly(time.Until(t.Expiry))
		}
		if t.Subject != "" {
			detail += ", sub " + t.Subject
		}
		ok("Token", "%s", detail)
		tokenSub = t.Subject
	}

	if p.Auth.Mechanism == "aws-msk-iam" {
		creds, err := kafka.AWSCredentials(ctx, p.Auth)
		if err != nil {
			return fail(exitcode.Connection, "AWS", err.Error(), "check auth.aws.profile and auth.aws.role_arn, or log in with `aws sso login`")
		}
		ok("AWS", "credentials %s from %s", maskKey(creds.AccessKeyID), creds.Source)
	}

	cl, err := kafka.NewClient(p, s.ResolveFile)
	if err != nil {
		return fail(exitcode.Usage, "Client", err.Error(), "")
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	md, err := cl.Admin.BrokerMetadata(ctx)
	if err != nil {
		switch {
		case errors.Is(err, kerr.SaslAuthenticationFailed) && p.Auth.Mechanism == "aws-msk-iam":
			return fail(exitcode.Connection, "SASL", err.Error(), "MSK rejected the IAM credentials: the IAM policy must allow kafka-cluster:Connect, and the bootstrap servers must be MSK's IAM port (9098)")
		case errors.Is(err, kerr.SaslAuthenticationFailed) && p.Auth.Mechanism == "oauthbearer":
			return fail(exitcode.Connection, "SASL", err.Error(), "the broker rejected the token: check its scope or audience, and auth.oauth.extensions if the cluster needs them")
		case errors.Is(err, kerr.SaslAuthenticationFailed):
			other := "scram-sha-256"
			if p.Auth.Mechanism == "scram-sha-256" {
				other = "scram-sha-512"
			}
			return fail(exitcode.Connection, "SASL", err.Error(), fmt.Sprintf("check auth.username/password; the listener may expect %s instead of %s", other, p.Auth.Mechanism))
		case errors.Is(err, kerr.UnsupportedSaslMechanism) || errors.Is(err, kerr.IllegalSaslState):
			return fail(exitcode.Connection, "SASL", err.Error(), "the listener does not accept "+p.Auth.Mechanism)
		case strings.Contains(err.Error(), "certificate required") || strings.Contains(err.Error(), "bad certificate"):
			return fail(exitcode.Connection, "Kafka", err.Error(), "the broker requires a client certificate (mTLS): set tls.cert_file and tls.key_file")
		case strings.Contains(err.Error(), "EOF") || strings.Contains(err.Error(), "reset"):
			return fail(exitcode.Connection, "Kafka", err.Error(), "the broker closed the connection: the listener likely expects a different security protocol than "+p.SecurityProtocol())
		}
		return fail(exitcode.Of(err), "Kafka", err.Error(), "")
	}
	switch {
	case p.Auth.Mechanism == "oauthbearer" && tokenSub != "":
		ok("SASL", "%s as %s", p.Auth.Mechanism, tokenSub)
	case p.Auth.Mechanism == "oauthbearer" || p.Auth.Mechanism == "aws-msk-iam":
		ok("SASL", "%s", p.Auth.Mechanism)
	case p.Auth.Mechanism != "none":
		ok("SASL", "%s as %s", p.Auth.Mechanism, p.Auth.Username)
	}
	controller := md.Controller
	if q, err := brokers.DescribeQuorum(ctx, cl.Client); err == nil {
		controller = q.LeaderID
	}
	version := ""
	if vs, err := cl.Admin.ApiVersions(ctx); err == nil {
		for _, v := range vs {
			if v.Err == nil {
				version = " · Kafka " + strings.TrimPrefix(v.VersionGuess(), "v")
				break
			}
		}
	}
	ok("Metadata", "cluster %s · %d brokers · controller %d%s", md.Cluster, len(md.Brokers), controller, version)
	principal, derived := p.ExpectedPrincipal()
	how := "configured"
	if derived {
		how = "derived"
	}
	if principal == "" && p.TLS.HasClientCert() {
		if pr, err := kafka.CertPrincipal(p.TLS, s.ResolveFile); err == nil {
			principal, how = pr, "derived from the client certificate; ssl.principal.mapping.rules may change it"
		}
	}
	if principal == "" && p.Auth.Mechanism == "aws-msk-iam" {
		principal, how = "your IAM identity", "MSK authorizes it with IAM policies, not Kafka ACLs"
	}
	if principal == "" && tokenSub != "" {
		principal, how = "User:"+tokenSub, "derived from the token's sub claim; the broker's sasl.oauthbearer.sub.claim.name may differ"
	}
	if principal == "" {
		principal, how = "User:ANONYMOUS", "no authentication"
	}
	ok("Principal", "%s (%s)", principal, how)
	return nil
}
