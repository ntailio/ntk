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
	"slices"
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

// Disruptive skips unless NTK_TEST_DISRUPTIVE=1, then runs the test serially.
// Stopping a broker breaks any test running alongside, so disruptive tests are
// named TestDisruptive… and run on their own: go test -run '^TestDisruptive' ./...
func Disruptive(t testing.TB) {
	t.Helper()
	if !strings.HasPrefix(t.Name(), "TestDisruptive") {
		t.Fatalf("disruptive test %s must be named TestDisruptive… so it can run on its own", t.Name())
	}
	if os.Getenv("NTK_TEST_DISRUPTIVE") != "1" {
		t.Skip("disruptive test (stops brokers): set NTK_TEST_DISRUPTIVE=1")
	}
	Serial(t)
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

// onEveryBroker polls each broker directly until check passes on all of them:
// metadata, ACLs and credentials reach the brokers asynchronously.
func onEveryBroker(t testing.TB, adm *kadm.Client, within time.Duration, what string, check func(ctx context.Context, b *kgo.Broker) bool) {
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
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		ok := true
		for _, b := range md.Brokers {
			if !check(t.Context(), cl.Broker(int(b.NodeID))) {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s did not reach every broker", what)
}

func waitVisible(t testing.TB, adm *kadm.Client, name string) {
	t.Helper()
	onEveryBroker(t, adm, 10*time.Second, "topic "+name, func(ctx context.Context, b *kgo.Broker) bool {
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(name)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, b)
		if err != nil || len(resp.Topics) != 1 || resp.Topics[0].ErrorCode != 0 {
			return false
		}
		for _, p := range resp.Topics[0].Partitions {
			if p.Leader < 0 {
				return false
			}
		}
		return true
	})
}

// WaitACLs waits until every broker reports exactly want ACLs for principal.
func WaitACLs(t testing.TB, adm *kadm.Client, principal string, want int) {
	t.Helper()
	onEveryBroker(t, adm, 10*time.Second, "ACLs for "+principal, func(ctx context.Context, b *kgo.Broker) bool {
		req := kmsg.NewPtrDescribeACLsRequest()
		req.ResourceType, req.ResourcePatternType, req.Operation, req.PermissionType =
			kmsg.ACLResourceTypeAny, kmsg.ACLResourcePatternTypeAny, kmsg.ACLOperationAny, kmsg.ACLPermissionTypeAny
		req.Principal = kmsg.StringPtr(principal)
		resp, err := req.RequestWith(ctx, b)
		if err != nil || resp.ErrorCode != 0 {
			return false
		}
		n := 0
		for _, r := range resp.Resources {
			n += len(r.ACLs)
		}
		return n == want
	})
}

// WaitSCRAMUsers waits until every broker has SCRAM-SHA-512 credentials for
// users. The sandbox's init container adds them after the brokers report
// healthy, so `docker compose up --wait` can return before they exist.
func WaitSCRAMUsers(t testing.TB, adm *kadm.Client, users ...string) {
	t.Helper()
	onEveryBroker(t, adm, 60*time.Second, "SCRAM users", func(ctx context.Context, b *kgo.Broker) bool {
		req := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest()
		for _, u := range users {
			ru := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
			ru.Name = u
			req.Users = append(req.Users, ru)
		}
		resp, err := req.RequestWith(ctx, b)
		if err != nil || resp.ErrorCode != 0 || len(resp.Results) != len(users) {
			return false
		}
		for _, r := range resp.Results {
			if r.ErrorCode != 0 || !slices.ContainsFunc(r.CredentialInfos, func(c kmsg.DescribeUserSCRAMCredentialsResponseResultCredentialInfo) bool {
				return c.Mechanism == int8(kadm.ScramSha512)
			}) {
				return false
			}
		}
		return true
	})
}
