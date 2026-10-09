// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"slices"
	"testing"

	"github.com/ntailio/ntk/testkit"
)

func TestList(t *testing.T) {
	adm := testkit.Admin(t)
	name := testkit.Topic(t, adm, "list", 4)

	got, err := List(t.Context(), adm, ListOptions{Pattern: name})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d topics, want 1: %+v", len(got), got)
	}
	tp := got[0]
	if tp.Name != name || tp.Partitions != 4 || tp.ReplicationFactor != 3 || tp.Internal {
		t.Errorf("unexpected topic: %+v", tp)
	}
	if tp.ID == "" {
		t.Error("missing topic id")
	}
}

func TestListRegexAndGlob(t *testing.T) {
	adm := testkit.Admin(t)
	a := testkit.Topic(t, adm, "match-a", 1)
	b := testkit.Topic(t, adm, "match-b", 1)

	glob, err := List(t.Context(), adm, ListOptions{Pattern: a[:len(a)-1] + "*"})
	if err != nil {
		t.Fatal(err)
	}
	if len(glob) != 1 || glob[0].Name != a {
		t.Errorf("glob: got %+v", glob)
	}

	re, err := List(t.Context(), adm, ListOptions{Pattern: `^ntk-test-.*-match-[ab]$`, Regex: true})
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tp := range re {
		names = append(names, tp.Name)
	}
	if !slices.Contains(names, a) || !slices.Contains(names, b) {
		t.Errorf("regex: %v does not contain %s and %s", names, a, b)
	}
}

func TestNames(t *testing.T) {
	adm := testkit.Admin(t)
	name := testkit.Topic(t, adm, "complete", 1)

	got, err := Names(t.Context(), adm, name[:len(name)-3])
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(got, name) {
		t.Errorf("Names(%q) = %v, want it to contain %s", name[:len(name)-3], got, name)
	}
}

func TestInvalidPattern(t *testing.T) {
	if _, err := matcher(ListOptions{Pattern: "[", Regex: true}); err == nil {
		t.Error("expected a regex error")
	}
	if _, err := matcher(ListOptions{Pattern: "["}); err == nil {
		t.Error("expected a glob error")
	}
}
