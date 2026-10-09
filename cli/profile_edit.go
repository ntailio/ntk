// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/exitcode"
	"github.com/ntailio/ntk/kafka"
	"github.com/ntailio/ntk/profile"
)

func inputFromProfile(name string, p *profile.Profile) profileInput {
	in := profileInput{
		name: name, description: p.Description, bootstrap: strings.Join(p.BootstrapServers, ","), principal: p.Principal,
		readOnly: p.ReadOnly, color: p.Color, labels: strings.Join(p.Labels, ","),
		mechanism: p.Auth.Mechanism, username: p.Auth.Username, password: p.Auth.Password, security: secPlaintext,
	}
	if in.mechanism == "none" || in.mechanism == "" {
		in.mechanism = "scram-sha-512"
	}
	if t := p.TLS; t.On() {
		in.caFile, in.certFile, in.keyFile, in.serverName, in.insecure = t.CAFile, t.CertFile, t.KeyFile, t.ServerName, t.InsecureSkipVerify
		in.keyPassword = t.KeyPassword
	}
	switch p.SecurityProtocol() {
	case "SSL":
		in.security = secTLS
		if p.TLS.HasClientCert() {
			in.security = secMTLS
		}
	case "SASL_SSL":
		in.security = secSASLTLS
	case "SASL_PLAINTEXT":
		in.security = secSASLPlaintext
	}
	return in
}

func (a *app) newProfileEditCmd() *cobra.Command {
	var sets []string
	cmd := &cobra.Command{
		Use:               "edit <name>",
		Short:             "Edit a profile (wizard pre-filled with its values, or --set key=value)",
		Example:           "  ntk profile edit prod\n  ntk profile edit prod --set read_only=true --set tls.ca_file=/etc/kafka/ca.pem",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeProfileArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			name, p, err := a.selectProfile(s, args[0])
			if err != nil {
				return err
			}
			var np *profile.Profile
			if len(sets) > 0 {
				if np, err = applySets(p, sets); err != nil {
					return usageErr("%v", err)
				}
			} else {
				if !a.interactive() && !accessible() {
					return usageErr("use --set key=value when not in a terminal")
				}
				in := inputFromProfile(name, p)
				if err := a.profileWizard(cmd.Context(), s, &in, true); err != nil {
					return err
				}
				if np, err = in.build(); err != nil {
					return usageErr("%v", err)
				}
				np.ClientID, np.Timeouts, np.Defaults = p.ClientID, p.Timeouts, p.Defaults
				if np.TLS != nil && p.TLS != nil {
					np.TLS.CAPEM, np.TLS.CertPEM, np.TLS.KeyPEM = p.TLS.CAPEM, p.TLS.CertPEM, p.TLS.KeyPEM
				}
			}
			if _, err := kafka.Options(np, s.ResolveFile); err != nil {
				return usageErr("%v", err)
			}
			s.File.Profiles[name] = np
			if err := s.Save(); err != nil {
				return err
			}
			fmt.Fprintf(a.stderr, "Saved profile %q\n", name)
			return nil
		},
	}
	cmd.Flags().StringArrayVar(&sets, "set", nil, "field=value using profiles.json names, e.g. auth.username=bob (repeatable)")
	return cmd
}

// applySets edits a profile through its JSON form, so field names match profiles.json.
func applySets(p *profile.Profile, sets []string) (*profile.Profile, error) {
	b, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	for _, kv := range sets {
		key, val, ok := strings.Cut(kv, "=")
		if !ok {
			return nil, fmt.Errorf("--set %q is not key=value", kv)
		}
		parts := strings.Split(key, ".")
		cur := m
		for _, part := range parts[:len(parts)-1] {
			next, ok := cur[part].(map[string]any)
			if !ok {
				next = map[string]any{}
				cur[part] = next
			}
			cur = next
		}
		last := parts[len(parts)-1]
		switch {
		case val == "":
			delete(cur, last)
		case last == "bootstrap_servers" || last == "labels":
			var list []any
			for _, v := range splitList(val) {
				list = append(list, v)
			}
			cur[last] = list
		case val == "true" || val == "false":
			cur[last] = val == "true"
		default:
			cur[last] = val
		}
	}
	b, err = json.Marshal(m)
	if err != nil {
		return nil, err
	}
	var np profile.Profile
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&np); err != nil {
		return nil, fmt.Errorf("invalid --set: %w", err)
	}
	return &np, np.Validate()
}

func (a *app) newProfileDeleteCmd() *cobra.Command {
	return &cobra.Command{
		Use:               "delete <name>",
		Aliases:           []string{"rm"},
		Short:             "Delete a profile",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeProfileArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			name, _, err := a.selectProfile(s, args[0])
			if err != nil {
				return err
			}
			if !a.flags.yes {
				if !a.canPrompt() {
					return usageErr("confirmation required; re-run with -y")
				}
				if ans := strings.ToLower(a.prompt(fmt.Sprintf("Delete profile %q? [y/N] ", name))); ans != "y" && ans != "yes" {
					return exitcode.With(exitcode.Refused, errors.New("aborted"))
				}
			}
			delete(s.File.Profiles, name)
			if s.File.Active == name {
				s.File.Active = ""
			}
			if err := s.Save(); err != nil {
				return err
			}
			fmt.Fprintf(a.stderr, "Deleted profile %q\n", name)
			return nil
		},
	}
}

func (a *app) newProfileRenameCmd(copyOnly bool) *cobra.Command {
	use, short := "rename <old> <new>", "Rename a profile"
	if copyOnly {
		use, short = "copy <src> <dst>", "Duplicate a profile (e.g. to make a read-only variant)"
	}
	return &cobra.Command{
		Use:               use,
		Short:             short,
		Args:              usageArgs(cobra.ExactArgs(2)),
		ValidArgsFunction: a.completeProfileArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.loadStore()
			if err != nil {
				return err
			}
			src, p, err := a.selectProfile(s, args[0])
			if err != nil {
				return err
			}
			if err := profile.ValidateName(args[1]); err != nil {
				return usageErr("%v", err)
			}
			if _, exists := s.File.Profiles[args[1]]; exists {
				return usageErr("profile %q already exists", args[1])
			}
			cp := *p
			s.File.Profiles[args[1]] = &cp
			s.RenameUnknown(src, args[1], copyOnly)
			if !copyOnly {
				delete(s.File.Profiles, src)
				if s.File.Active == src {
					s.File.Active = args[1]
				}
			}
			if err := s.Save(); err != nil {
				return err
			}
			verb := "Renamed"
			if copyOnly {
				verb = "Copied"
			}
			fmt.Fprintf(a.stderr, "%s profile %q to %q\n", verb, src, args[1])
			return nil
		},
	}
}

var jaasKV = regexp.MustCompile(`(\w+)\s*=\s*"((?:[^"\\]|\\.)*)"`)

// fromProperties maps a Java client properties file to profile input.
func fromProperties(r io.Reader, in *profileInput) error {
	props := map[string]string{}
	sc := bufio.NewScanner(r)
	var cont string
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if cont != "" {
			line = cont + line
			cont = ""
		}
		if strings.HasSuffix(line, `\`) {
			cont = strings.TrimSuffix(line, `\`)
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "!") {
			continue
		}
		i := strings.IndexAny(line, "=:")
		if i < 0 {
			continue
		}
		props[strings.TrimSpace(line[:i])] = strings.TrimSpace(line[i+1:])
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if v := props["bootstrap.servers"]; v != "" {
		in.bootstrap = v
	}
	proto := strings.ToUpper(props["security.protocol"])
	switch proto {
	case "", "PLAINTEXT":
		in.security = secPlaintext
	case "SSL":
		in.security = secTLS
	case "SASL_SSL":
		in.security = secSASLTLS
	case "SASL_PLAINTEXT":
		in.security = secSASLPlaintext
	default:
		return fmt.Errorf("unsupported security.protocol %q", proto)
	}
	if strings.HasPrefix(proto, "SASL") {
		switch m := strings.ToUpper(props["sasl.mechanism"]); m {
		case "", "GSSAPI":
			return fmt.Errorf("sasl.mechanism %q is not supported yet (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", m)
		case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
			in.mechanism = strings.ToLower(m)
		default:
			return fmt.Errorf("sasl.mechanism %q is not supported yet", m)
		}
		for _, kv := range jaasKV.FindAllStringSubmatch(props["sasl.jaas.config"], -1) {
			v, _ := strconv.Unquote(`"` + kv[2] + `"`)
			switch kv[1] {
			case "username":
				in.username = v
			case "password":
				in.password = v
			}
		}
	}
	pem := func(typeKey string) error {
		if t := strings.ToUpper(props[typeKey]); t != "" && t != "PEM" {
			return fmt.Errorf("%s=%s: only PEM stores can be imported; convert with `keytool -importkeystore ... -deststoretype PKCS12` and `openssl pkcs12 -nodes`", typeKey, t)
		}
		return nil
	}
	if v := props["ssl.truststore.location"]; v != "" {
		if err := pem("ssl.truststore.type"); err != nil {
			return err
		}
		in.caFile = v
	}
	if v := props["ssl.keystore.location"]; v != "" {
		if err := pem("ssl.keystore.type"); err != nil {
			return err
		}
		in.certFile, in.keyFile, in.keyPassword = v, v, props["ssl.key.password"]
		if in.security == secTLS {
			in.security = secMTLS
		}
	}
	if in.bootstrap == "" {
		return errors.New("bootstrap.servers is missing")
	}
	return nil
}

func readProperties(path string, in *profileInput) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return fromProperties(f, in)
}
