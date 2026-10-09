// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"

	"github.com/ntailio/ntk/profile"
)

func tlsConfig(t *profile.TLS, resolve func(string) string) (*tls.Config, error) {
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         t.ServerName, // if empty, kgo sets it to each broker's host when dialing
		InsecureSkipVerify: t.InsecureSkipVerify,
	}

	ca, err := pemInput(t.CAFile, t.CAPEM, resolve)
	if err != nil {
		return nil, fmt.Errorf("tls CA: %w", err)
	}
	if ca != nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("tls CA: no PEM certificates found")
		}
		cfg.RootCAs = pool
	}

	if t.HasClientCert() {
		certPEM, err := pemInput(t.CertFile, t.CertPEM, resolve)
		if err != nil {
			return nil, fmt.Errorf("tls client cert: %w", err)
		}
		keyPEM, err := pemInput(t.KeyFile, t.KeyPEM, resolve)
		if err != nil {
			return nil, fmt.Errorf("tls client key: %w", err)
		}
		cert, err := keyPair(certPEM, keyPEM, t.KeyPassword)
		if err != nil {
			return nil, fmt.Errorf("tls client cert/key: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func pemInput(file, inline string, resolve func(string) string) ([]byte, error) {
	if inline != "" {
		return []byte(inline), nil
	}
	if file == "" {
		return nil, nil
	}
	return os.ReadFile(resolve(file))
}

// CertPrincipal is the principal Kafka's default mapping gives the client
// certificate ("User:" + subject); ssl.principal.mapping.rules may change it.
func CertPrincipal(t *profile.TLS, resolve func(string) string) (string, error) {
	certPEM, err := pemInput(t.CertFile, t.CertPEM, resolve)
	if err != nil || certPEM == nil {
		return "", err
	}
	block, _ := pem.Decode(certPEM)
	if block == nil {
		return "", errors.New("no PEM certificate found")
	}
	c, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", err
	}
	return "User:" + c.Subject.String(), nil
}

// TLSConfig is the TLS config a profile uses (for connection diagnostics).
func TLSConfig(t *profile.TLS, resolve func(string) string) (*tls.Config, error) {
	return tlsConfig(t, resolve)
}

// LoadCertificate reads a profile's client certificate and key, and returns the parsed leaf.
func LoadCertificate(t *profile.TLS, resolve func(string) string) (*x509.Certificate, error) {
	certPEM, err := pemInput(t.CertFile, t.CertPEM, resolve)
	if err != nil {
		return nil, err
	}
	keyPEM, err := pemInput(t.KeyFile, t.KeyPEM, resolve)
	if err != nil {
		return nil, err
	}
	pair, err := keyPair(certPEM, keyPEM, t.KeyPassword)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(pair.Certificate[0])
}
