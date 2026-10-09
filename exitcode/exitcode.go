// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package exitcode

import (
	"context"
	"errors"
	"io/fs"
	"net"

	"github.com/twmb/franz-go/pkg/kerr"
)

const (
	OK           = 0
	Error        = 1
	Usage        = 2
	Connection   = 3
	NotFound     = 4
	CheckFailed  = 5
	Refused      = 6
	Unauthorized = 7
	Unsupported  = 8
	OutputTarget = 9
)

type codedError struct {
	code int
	err  error
}

func (e *codedError) Error() string { return e.err.Error() }
func (e *codedError) Unwrap() error { return e.err }

func With(code int, err error) error {
	if err == nil {
		return nil
	}
	return &codedError{code: code, err: err}
}

func Of(err error) int {
	if err == nil {
		return OK
	}
	if ce, ok := errors.AsType[*codedError](err); ok {
		return ce.code
	}
	switch {
	case errors.Is(err, kerr.TopicAuthorizationFailed),
		errors.Is(err, kerr.GroupAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed),
		errors.Is(err, kerr.TransactionalIDAuthorizationFailed),
		errors.Is(err, kerr.DelegationTokenAuthorizationFailed):
		return Unauthorized
	case errors.Is(err, kerr.SaslAuthenticationFailed),
		errors.Is(err, context.DeadlineExceeded):
		return Connection
	case errors.Is(err, kerr.UnknownTopicOrPartition),
		errors.Is(err, kerr.GroupIDNotFound),
		errors.Is(err, fs.ErrNotExist):
		return NotFound
	case errors.Is(err, kerr.UnsupportedVersion):
		return Unsupported
	}
	if _, ok := errors.AsType[net.Error](err); ok {
		return Connection
	}
	return Error
}
