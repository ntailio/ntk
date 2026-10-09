// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

// Command janitor removes ntk-test-* resources left behind by crashed test runs.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/ntailio/ntk/testkit"
)

func main() {
	all := flag.Bool("all", false, "remove every ntk-test-* resource regardless of age (only when no tests are running)")
	older := flag.Duration("older-than", time.Hour, "remove resources created longer ago than this")
	flag.Parse()
	if *all {
		*older = 0
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(testkit.Bootstrap()...))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	removed, err := testkit.Janitor(ctx, cl, *older)
	for _, r := range removed {
		fmt.Println("removed", r)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Fprintf(os.Stderr, "%d resource(s) removed\n", len(removed))
}
