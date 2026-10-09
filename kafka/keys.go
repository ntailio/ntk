// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/des"
	"crypto/pbkdf2"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"encoding/asn1"
	"encoding/pem"
	"errors"
	"fmt"
	"hash"
)

var (
	oidPBES2      = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 13}
	oidPBKDF2     = asn1.ObjectIdentifier{1, 2, 840, 113549, 1, 5, 12}
	oidHMACSHA1   = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 7}
	oidHMACSHA256 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 9}
	oidHMACSHA384 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 10}
	oidHMACSHA512 = asn1.ObjectIdentifier{1, 2, 840, 113549, 2, 11}
	oidAES128CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 2}
	oidAES192CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 22}
	oidAES256CBC  = asn1.ObjectIdentifier{2, 16, 840, 1, 101, 3, 4, 1, 42}
	oidDESEDE3CBC = asn1.ObjectIdentifier{1, 2, 840, 113549, 3, 7}
)

type algorithmIdentifier struct {
	Algorithm  asn1.ObjectIdentifier
	Parameters asn1.RawValue `asn1:"optional"`
}

type encryptedPrivateKeyInfo struct {
	Algorithm     algorithmIdentifier
	EncryptedData []byte
}

type pbes2Params struct {
	KeyDerivationFunc algorithmIdentifier
	EncryptionScheme  algorithmIdentifier
}

type pbkdf2Params struct {
	Salt           []byte
	IterationCount int
	KeyLength      int                 `asn1:"optional"`
	PRF            algorithmIdentifier `asn1:"optional"`
}

// decryptPKCS8 decrypts a PBES2 "ENCRYPTED PRIVATE KEY" (what `openssl pkcs8 -topk8 -v2` writes).
func decryptPKCS8(der []byte, password string) ([]byte, error) {
	var info encryptedPrivateKeyInfo
	if _, err := asn1.Unmarshal(der, &info); err != nil {
		return nil, fmt.Errorf("parsing encrypted key: %w", err)
	}
	if !info.Algorithm.Algorithm.Equal(oidPBES2) {
		return nil, fmt.Errorf("unsupported key encryption %v (only PBES2 is supported)", info.Algorithm.Algorithm)
	}
	var params pbes2Params
	if _, err := asn1.Unmarshal(info.Algorithm.Parameters.FullBytes, &params); err != nil {
		return nil, fmt.Errorf("parsing PBES2 parameters: %w", err)
	}
	if !params.KeyDerivationFunc.Algorithm.Equal(oidPBKDF2) {
		return nil, fmt.Errorf("unsupported key derivation %v (only PBKDF2 is supported)", params.KeyDerivationFunc.Algorithm)
	}
	var kdf pbkdf2Params
	if _, err := asn1.Unmarshal(params.KeyDerivationFunc.Parameters.FullBytes, &kdf); err != nil {
		return nil, fmt.Errorf("parsing PBKDF2 parameters: %w", err)
	}
	prf := func() hash.Hash { return sha1.New() }
	switch {
	case kdf.PRF.Algorithm == nil || kdf.PRF.Algorithm.Equal(oidHMACSHA1):
	case kdf.PRF.Algorithm.Equal(oidHMACSHA256):
		prf = sha256.New
	case kdf.PRF.Algorithm.Equal(oidHMACSHA384):
		prf = sha512.New384
	case kdf.PRF.Algorithm.Equal(oidHMACSHA512):
		prf = sha512.New
	default:
		return nil, fmt.Errorf("unsupported PBKDF2 PRF %v", kdf.PRF.Algorithm)
	}
	var iv []byte
	if _, err := asn1.Unmarshal(params.EncryptionScheme.Parameters.FullBytes, &iv); err != nil {
		return nil, fmt.Errorf("parsing cipher IV: %w", err)
	}
	var keyLen int
	var newCipher func([]byte) (cipher.Block, error)
	switch alg := params.EncryptionScheme.Algorithm; {
	case alg.Equal(oidAES128CBC):
		keyLen, newCipher = 16, aes.NewCipher
	case alg.Equal(oidAES192CBC):
		keyLen, newCipher = 24, aes.NewCipher
	case alg.Equal(oidAES256CBC):
		keyLen, newCipher = 32, aes.NewCipher
	case alg.Equal(oidDESEDE3CBC):
		keyLen, newCipher = 24, des.NewTripleDESCipher
	default:
		return nil, fmt.Errorf("unsupported key cipher %v", alg)
	}
	key, err := pbkdf2.Key(prf, password, kdf.Salt, kdf.IterationCount, keyLen)
	if err != nil {
		return nil, err
	}
	block, err := newCipher(key)
	if err != nil {
		return nil, err
	}
	data := info.EncryptedData
	if len(data) == 0 || len(data)%block.BlockSize() != 0 || len(iv) != block.BlockSize() {
		return nil, errors.New("malformed encrypted key")
	}
	out := make([]byte, len(data))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(out, data)
	pad := int(out[len(out)-1])
	if pad == 0 || pad > block.BlockSize() || pad > len(out) {
		return nil, errors.New("wrong key_password (or corrupt key)")
	}
	for _, b := range out[len(out)-pad:] {
		if int(b) != pad {
			return nil, errors.New("wrong key_password (or corrupt key)")
		}
	}
	out = out[:len(out)-pad]
	if _, err := x509.ParsePKCS8PrivateKey(out); err != nil {
		return nil, errors.New("wrong key_password (or corrupt key)")
	}
	return out, nil
}

// keyPair builds a TLS certificate from PEM, decrypting the key with password
// when it is an encrypted PKCS#8 or legacy encrypted PEM key.
func keyPair(certPEM, keyPEM []byte, password string) (tls.Certificate, error) {
	rest := keyPEM
	for {
		var block *pem.Block
		block, rest = pem.Decode(rest)
		if block == nil {
			break
		}
		switch {
		case block.Type == "ENCRYPTED PRIVATE KEY":
			if password == "" {
				return tls.Certificate{}, errors.New("the private key is encrypted: set tls.key_password")
			}
			der, err := decryptPKCS8(block.Bytes, password)
			if err != nil {
				return tls.Certificate{}, err
			}
			return tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}))
		//lint:ignore SA1019 legacy OpenSSL "Proc-Type: 4,ENCRYPTED" keys; the padding-oracle risk needs a remote decryptor, not a local key file
		case x509.IsEncryptedPEMBlock(block):
			if password == "" {
				return tls.Certificate{}, errors.New("the private key is encrypted: set tls.key_password")
			}
			//lint:ignore SA1019 see above
			der, err := x509.DecryptPEMBlock(block, []byte(password))
			if err != nil {
				return tls.Certificate{}, fmt.Errorf("decrypting key: %w", err)
			}
			return tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: der}))
		case block.Type == "PRIVATE KEY" || block.Type == "RSA PRIVATE KEY" || block.Type == "EC PRIVATE KEY":
			return tls.X509KeyPair(certPEM, keyPEM)
		}
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}
