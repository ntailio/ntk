// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package testkit

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const Prefix = "ntk-test-"

const DefaultBootstrap = "localhost:19101,localhost:19201,localhost:19301"

func Bootstrap() []string {
	s := os.Getenv("NTK_TEST_BOOTSTRAP")
	if s == "" {
		s = DefaultBootstrap
	}
	return strings.Split(s, ",")
}

func Admin(t testing.TB) *kadm.Client {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: skipped with -short")
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(Bootstrap()...), kgo.ClientID("ntk-test"))
	if err != nil {
		t.Fatalf("creating client: %v", err)
	}
	t.Cleanup(cl.Close)
	adm := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := adm.BrokerMetadata(ctx); err != nil {
		t.Skipf("sandbox not reachable at %s (start it with `docker compose up -d --wait`): %v",
			strings.Join(Bootstrap(), ","), err)
	}
	return adm
}

func Name(t testing.TB, purpose string) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("random name: %v", err)
	}
	r := strings.ToLower(base32.StdEncoding.EncodeToString(b))[:6]
	return Prefix + strconv.FormatInt(time.Now().Unix(), 36) + "-" + r + "-" + purpose
}

func Topic(t testing.TB, adm *kadm.Client, purpose string, partitions int32) string {
	t.Helper()
	name := Name(t, purpose)
	resp, err := adm.CreateTopic(t.Context(), partitions, 3, nil, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		t.Fatalf("creating topic %s: %v", name, err)
	}
	waitVisible(t, adm, name)
	t.Cleanup(func() {
		// t.Context() is already canceled when cleanups run.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		resp, err := adm.DeleteTopic(ctx, name)
		if err == nil {
			err = resp.Err
		}
		if err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Logf("cleanup: deleting topic %s: %v", name, err)
		}
	})
	return name
}

func RepoRoot(t testing.TB) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the working directory")
		}
		dir = parent
	}
}

// waitVisible waits until every broker's metadata has the topic's leaders.
func waitVisible(t testing.TB, adm *kadm.Client, name string) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(Bootstrap()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	md, err := adm.BrokerMetadata(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, b := range md.Brokers {
			req := kmsg.NewPtrMetadataRequest()
			rt := kmsg.NewMetadataRequestTopic()
			rt.Topic = kmsg.StringPtr(name)
			req.Topics = append(req.Topics, rt)
			resp, err := req.RequestWith(t.Context(), cl.Broker(int(b.NodeID)))
			if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
				ok = false
				break
			}
			for _, p := range resp.Topics[0].Partitions {
				if p.Leader < 0 {
					ok = false
				}
			}
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("topic %s did not reach every broker", name)
}
