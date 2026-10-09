// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/ntailio/ntk/cli"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// The first signal starts a graceful stop; restoring default handling lets a second one kill ntk.
	context.AfterFunc(ctx, stop)
	code := cli.Execute(ctx, os.Args[1:], os.Stdin, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}
