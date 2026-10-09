// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"charm.land/huh/v2"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"

	"github.com/factualtech/ntk/exitcode"
	"github.com/factualtech/ntk/kafka"
	"github.com/factualtech/ntk/profile"
)

type profileInput struct {
	name        string
	description string
	bootstrap   string
	security    string
	caFile      string
	certFile    string
	keyFile     string
	keyPassword string
	serverName  string
	insecure    bool
	mechanism   string
	username    string
	password    string
	principal   string
	readOnly    bool
	color       string
	labels      string
}

const (
	secPlaintext     = "plaintext"
	secTLS           = "tls"
	secMTLS          = "mtls"
	secSASLTLS       = "sasl_tls"
	secSASLPlaintext = "sasl_plaintext"
)

func (in *profileInput) usesTLS() bool {
	return in.security == secTLS || in.security == secMTLS || in.security == secSASLTLS
}

func (in *profileInput) usesSASL() bool {
	return in.security == secSASLTLS || in.security == secSASLPlaintext
}

func (in *profileInput) build() (*profile.Profile, error) {
	p := &profile.Profile{
		Description:      in.description,
		BootstrapServers: splitList(in.bootstrap),
		Principal:        in.principal,
		ReadOnly:         in.readOnly,
		Color:            in.color,
		Labels:           splitList(in.labels),
		Auth:             profile.Auth{Mechanism: "none"},
	}
	if in.usesTLS() {
		t := &profile.TLS{Enabled: true, ServerName: in.serverName, InsecureSkipVerify: in.insecure, KeyPassword: in.keyPassword}
		var err error
		if t.CAFile, err = absPath(in.caFile); err != nil {
			return nil, err
		}
		if in.security == secMTLS {
			if t.CertFile, err = absPath(in.certFile); err != nil {
				return nil, err
			}
			if t.KeyFile, err = absPath(in.keyFile); err != nil {
				return nil, err
			}
		}
		p.TLS = t
	}
	if in.usesSASL() {
		p.Auth = profile.Auth{Mechanism: in.mechanism, Username: in.username, Password: in.password}
	}
	return p, p.Validate()
}

func (a *app) newProfileCreateCmd() *cobra.Command {
	var (
		in                              profileInput
		useTLS                          bool
		passwordStdin, keyPasswordStdin bool
		activate                        bool
		noTest                          bool
		properties                      string
	)
	cmd := &cobra.Command{
		Use:   "create [name]",
		Short: "Create a profile (interactive wizard, or flags for scripts)",
		Example: "  ntk profile create\n" +
			"  ntk profile create ci --bootstrap kafka:9093 --tls --ca-file ca.pem \\\n" +
			"      --sasl scram-sha-512 --username ci --password-stdin --read-only",
		Args: usageArgs(cobra.MaximumNArgs(1)),
		RunE: func(cmd *cobra.Command, args []string) error {
			path, err := profile.ResolvePath(a.flags.profilePath)
			if err != nil {
				return err
			}
			s, err := profile.LoadOrEmpty(path)
			if err != nil {
				return err
			}
			if len(args) == 1 {
				in.name = args[0]
			}
			if properties != "" {
				if in.name == "" {
					return usageErr("a profile name is required with --from-properties")
				}
				if err := readProperties(properties, &in); err != nil {
					return usageErr("%s: %v", properties, err)
				}
			}

			wizard := in.name == "" || in.bootstrap == ""
			if wizard {
				if !a.interactive() && !accessible() {
					return exitcode.With(exitcode.Usage, errors.New("name and --bootstrap are required when not running in a terminal"))
				}
				if err := a.profileWizard(cmd.Context(), s, &in, false); err != nil {
					return err
				}
			} else {
				if properties == "" {
					in.security = flagSecurity(useTLS, in)
				}
				if passwordStdin && keyPasswordStdin {
					return usageErr("--password-stdin and --key-password-stdin can't both read stdin")
				}
				if passwordStdin {
					if in.password, err = readSecret(a.stdin); err != nil {
						return err
					}
				} else if cmd.Flags().Changed("password") {
					fmt.Fprintln(a.stderr, "warning: --password may end up in your shell history; prefer --password-stdin")
				}
				if keyPasswordStdin {
					if in.keyPassword, err = readSecret(a.stdin); err != nil {
						return err
					}
				} else if cmd.Flags().Changed("key-password") {
					fmt.Fprintln(a.stderr, "warning: --key-password may end up in your shell history; prefer --key-password-stdin")
				}
			}

			if err := profile.ValidateName(in.name); err != nil {
				return exitcode.With(exitcode.Usage, err)
			}
			if _, exists := s.File.Profiles[in.name]; exists {
				return exitcode.With(exitcode.Usage, fmt.Errorf("profile %q already exists", in.name))
			}
			p, err := in.build()
			if err == nil {
				_, err = kafka.Options(p, s.ResolveFile)
			}
			if err != nil {
				return exitcode.With(exitcode.Usage, err)
			}

			if !noTest {
				fmt.Fprintf(a.stderr, "Testing connection to %s ...\n", p.BootstrapServers[0])
				md, err := testConnection(cmd.Context(), p, s.ResolveFile)
				switch {
				case err == nil:
					fmt.Fprintf(a.stderr, "✓ connected: cluster %s, %d brokers\n", md.Cluster, len(md.Brokers))
				case !wizard:
					return exitcode.With(exitcode.Connection, fmt.Errorf("connection test failed: %w (use --no-test to save anyway)", err))
				default:
					fmt.Fprintf(a.stderr, "✗ connection failed: %v\n", err)
					if !a.confirm(cmd.Context(), "Save the profile anyway?", false) {
						return exitcode.With(exitcode.Refused, errors.New("profile not saved"))
					}
				}
			}

			s.File.Profiles[in.name] = p
			switch {
			case s.File.Active == "" || activate:
				s.File.Active = in.name
			case wizard:
				if a.confirm(cmd.Context(), fmt.Sprintf("Make %q the active profile?", in.name), true) {
					s.File.Active = in.name
				}
			}
			if err := s.Save(); err != nil {
				return err
			}
			fmt.Fprintf(a.stderr, "Saved profile %q to %s", in.name, s.Path)
			if s.File.Active == in.name {
				fmt.Fprint(a.stderr, " (active)")
			}
			fmt.Fprintln(a.stderr)
			return nil
		},
	}
	f := cmd.Flags()
	f.StringVar(&in.bootstrap, "bootstrap", "", "bootstrap servers, host:port[,host:port...]")
	f.StringVar(&in.description, "description", "", "free-text description")
	f.BoolVar(&useTLS, "tls", false, "use TLS (implied by --ca-file and --cert-file)")
	f.StringVar(&in.caFile, "ca-file", "", "CA certificate file (default: system roots)")
	f.StringVar(&in.certFile, "cert-file", "", "client certificate file (mTLS)")
	f.StringVar(&in.keyFile, "key-file", "", "client key file (mTLS)")
	f.StringVar(&in.keyPassword, "key-password", "", "passphrase of an encrypted client key (prefer --key-password-stdin)")
	f.BoolVar(&keyPasswordStdin, "key-password-stdin", false, "read the client key passphrase from stdin")
	f.StringVar(&in.serverName, "server-name", "", "TLS server name override")
	f.BoolVar(&in.insecure, "insecure-skip-verify", false, "do not verify the server certificate")
	f.StringVar(&in.mechanism, "sasl", "", "SASL mechanism: "+strings.Join(kafka.Mechanisms()[1:], ", "))
	f.StringVar(&in.username, "username", "", "SASL username")
	f.StringVar(&in.password, "password", "", "SASL password (prefer --password-stdin)")
	f.BoolVar(&passwordStdin, "password-stdin", false, "read the SASL password from stdin")
	f.StringVar(&in.principal, "principal", "", "expected principal, e.g. User:bob")
	f.BoolVar(&in.readOnly, "read-only", false, "refuse all write operations with this profile")
	f.StringVar(&in.color, "color", "", "badge color: green, yellow, red, blue, magenta, or #hex")
	f.StringVar(&in.labels, "labels", "", `comma-separated labels ("prod" makes confirmations stricter)`)
	f.BoolVar(&activate, "activate", false, "make the new profile active")
	f.BoolVar(&noTest, "no-test", false, "skip the connection test")
	f.StringVar(&properties, "from-properties", "", "import a Java client properties file (bootstrap, security, SASL, PEM stores)")
	_ = cmd.RegisterFlagCompletionFunc("sasl", cobra.FixedCompletions(kafka.Mechanisms()[1:], cobra.ShellCompDirectiveNoFileComp))
	return cmd
}

func flagSecurity(useTLS bool, in profileInput) string {
	tls := useTLS || in.caFile != "" || in.certFile != "" || in.serverName != "" || in.insecure
	sasl := in.mechanism != "" && in.mechanism != "none"
	switch {
	case sasl && tls:
		return secSASLTLS
	case sasl:
		return secSASLPlaintext
	case in.certFile != "":
		return secMTLS
	case tls:
		return secTLS
	}
	return secPlaintext
}

func (a *app) profileWizard(ctx context.Context, s *profile.Store, in *profileInput, editing bool) error {
	if !editing {
		in.security, in.mechanism = secPlaintext, "scram-sha-512"
	}
	var fields0 []huh.Field
	if !editing {
		fields0 = append(fields0, huh.NewInput().Title("Profile name").Value(&in.name).Validate(func(v string) error {
			if err := profile.ValidateName(v); err != nil {
				return err
			}
			if _, ok := s.File.Profiles[v]; ok {
				return fmt.Errorf("profile %q already exists", v)
			}
			return nil
		}))
	}
	base := huh.NewGroup(append(fields0,
		huh.NewInput().Title("Bootstrap servers").Description("host:port, comma-separated").
			Value(&in.bootstrap).Validate(validateBootstrap),
		huh.NewInput().Title("Description").Description("optional").Value(&in.description),
		huh.NewSelect[string]().Title("Security").Options(
			huh.NewOption("Plaintext (no TLS, no auth)", secPlaintext),
			huh.NewOption("TLS (server verification only)", secTLS),
			huh.NewOption("mTLS (client certificate)", secMTLS),
			huh.NewOption("SASL over TLS", secSASLTLS),
			huh.NewOption("SASL without TLS (credentials sent in the clear)", secSASLPlaintext),
		).Value(&in.security),
	)...)
	if err := a.runForm(ctx, huh.NewForm(base)); err != nil {
		return err
	}

	// Built after the first form: huh's accessible mode ignores group hide funcs.
	var fields []huh.Field
	if in.usesTLS() {
		fields = append(fields,
			huh.NewInput().Title("CA certificate file").Description("leave empty to use the system roots").
				Value(&in.caFile).Validate(optionalFile),
			huh.NewInput().Title("TLS server name override").Description("optional").Value(&in.serverName),
			huh.NewConfirm().Title("Skip server certificate verification?").Description("insecure: only for testing").
				Value(&in.insecure))
	}
	if in.security == secMTLS {
		fields = append(fields,
			huh.NewInput().Title("Client certificate file").Value(&in.certFile).Validate(requiredFile),
			huh.NewInput().Title("Key passphrase").Description("only for an encrypted key").EchoMode(huh.EchoModePassword).Value(&in.keyPassword),
			huh.NewInput().Title("Client key file").Description("PEM: PKCS#8 (optionally encrypted), PKCS#1, or EC").
				Value(&in.keyFile).Validate(func(v string) error {
				if err := requiredFile(v); err != nil {
					return err
				}
				c, err := kafka.LoadCertificate(&profile.TLS{CertFile: in.certFile, KeyFile: v, KeyPassword: in.keyPassword}, func(s string) string { return s })
				if err != nil {
					return err
				}
				if time.Now().After(c.NotAfter) {
					return fmt.Errorf("the certificate expired on %s", c.NotAfter.Format("2006-01-02"))
				}
				return nil
			}))
	}
	if in.usesSASL() {
		fields = append(fields,
			huh.NewSelect[string]().Title("SASL mechanism").
				Options(huh.NewOptions("scram-sha-512", "scram-sha-256", "plain")...).Value(&in.mechanism),
			huh.NewInput().Title("Username").Value(&in.username).Validate(huh.ValidateNotEmpty()),
			huh.NewInput().Title("Password").EchoMode(huh.EchoModePassword).Value(&in.password).
				Validate(huh.ValidateNotEmpty()))
	}
	fields = append(fields,
		huh.NewInput().Title("Principal").Description("optional, e.g. User:bob (derived from the SASL username if empty)").
			Value(&in.principal),
		huh.NewConfirm().Title("Read-only?").Description("refuse all write operations with this profile").
			Value(&in.readOnly),
		huh.NewSelect[string]().Title("Badge color").Options(
			huh.NewOption("none", ""), huh.NewOption("green", "green"), huh.NewOption("yellow", "yellow"),
			huh.NewOption("red", "red"), huh.NewOption("blue", "blue"), huh.NewOption("magenta", "magenta"),
		).Value(&in.color),
		huh.NewInput().Title("Labels").Description(`optional, comma-separated; "prod" makes confirmations stricter`).
			Value(&in.labels))
	return a.runForm(ctx, huh.NewForm(huh.NewGroup(fields...)))
}

func (a *app) newProfileSwitchCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "switch",
		Short: "Pick the active profile interactively",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !a.interactive() && !accessible() {
				return exitcode.With(exitcode.Usage, errors.New("profile switch needs a terminal; use `ntk profile set <name>`"))
			}
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			if len(s.File.Profiles) == 0 {
				return fmt.Errorf("no profiles in %s (create one with `ntk profile create`)", s.Path)
			}
			selected := s.File.Active
			var opts []huh.Option[string]
			for _, name := range s.Names() {
				label := name
				if d := s.File.Profiles[name].Description; d != "" {
					label += "  " + d
				}
				opts = append(opts, huh.NewOption(label, name))
			}
			sel := huh.NewSelect[string]().Title("Switch profile").Options(opts...).Value(&selected).
				Filtering(true).Height(min(len(opts)+2, 15))
			if err := a.runForm(cmd.Context(), huh.NewForm(huh.NewGroup(sel))); err != nil {
				return err
			}
			s.File.Active = selected
			if err := s.Save(); err != nil {
				return err
			}
			fmt.Fprintf(a.stderr, "Active profile: %s\n", selected)
			return nil
		},
	}
}

func (a *app) runForm(ctx context.Context, form *huh.Form) error {
	err := form.WithInput(a.stdin).WithOutput(a.stderr).WithAccessible(accessible()).RunWithContext(ctx)
	if errors.Is(err, huh.ErrUserAborted) {
		return exitcode.With(exitcode.Refused, errors.New("aborted"))
	}
	return err
}

func (a *app) confirm(ctx context.Context, question string, def bool) bool {
	ok := def
	err := a.runForm(ctx, huh.NewForm(huh.NewGroup(huh.NewConfirm().Title(question).Value(&ok))))
	return err == nil && ok
}

// huh's line-based mode, for screen readers (and driving the prompts from tests).
func accessible() bool { return os.Getenv("ACCESSIBLE") != "" }

func testConnection(ctx context.Context, p *profile.Profile, resolve func(string) string) (kadm.Metadata, error) {
	cl, err := kafka.NewClient(p, resolve)
	if err != nil {
		return kadm.Metadata{}, err
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return cl.Admin.BrokerMetadata(ctx)
}

func validateBootstrap(v string) error {
	p := profile.Profile{BootstrapServers: splitList(v), Auth: profile.Auth{Mechanism: "none"}}
	return p.Validate()
}

func optionalFile(v string) error {
	if v == "" {
		return nil
	}
	return requiredFile(v)
}

func requiredFile(v string) error {
	if v == "" {
		return errors.New("required")
	}
	fi, err := os.Stat(v)
	if err != nil {
		return err
	}
	if fi.IsDir() {
		return fmt.Errorf("%s is a directory", v)
	}
	return nil
}

func absPath(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	return filepath.Abs(p)
}

func splitList(s string) []string {
	var out []string
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func readAll(r io.Reader) ([]byte, error) { return io.ReadAll(r) }

func readSecret(r io.Reader) (string, error) {
	b, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}
