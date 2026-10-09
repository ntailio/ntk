// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package topics

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// WaitPropagated waits (up to timeout) until every broker's metadata shows the
// topics as present with leaders (exist=true) or absent (exist=false). Kafka
// applies topic creation and deletion to brokers asynchronously.
func WaitPropagated(ctx context.Context, cl *kgo.Client, names []string, exist bool, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	md, err := kadm.NewClient(cl).BrokerMetadata(ctx)
	if err != nil {
		return
	}
	for {
		done := true
		for _, b := range md.Brokers {
			req := kmsg.NewPtrMetadataRequest()
			for _, n := range names {
				t := kmsg.NewMetadataRequestTopic()
				t.Topic = kmsg.StringPtr(n)
				req.Topics = append(req.Topics, t)
			}
			resp, err := req.RequestWith(ctx, cl.Broker(int(b.NodeID)))
			if err != nil {
				return
			}
			for _, t := range resp.Topics {
				present := t.ErrorCode == 0 && len(t.Partitions) > 0
				for _, p := range t.Partitions {
					if p.Leader < 0 {
						present = false
					}
				}
				if present != exist {
					done = false
				}
			}
		}
		if done {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
}
