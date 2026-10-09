// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package tx

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/ntailio/ntk/plan"
	"github.com/ntailio/ntk/units"
)

type Transaction struct {
	TxnID       string   `json:"transactional_id"`
	ProducerID  int64    `json:"producer_id"`
	Epoch       int16    `json:"producer_epoch"`
	State       string   `json:"state"`
	Coordinator int32    `json:"coordinator"`
	StartMs     int64    `json:"start_timestamp_ms"`
	TimeoutMs   int32    `json:"timeout_ms"`
	Partitions  []string `json:"partitions"`
}

func (t Transaction) Duration() time.Duration {
	if t.StartMs <= 0 || (t.State != "Ongoing" && !strings.HasPrefix(t.State, "Prepare")) {
		return 0
	}
	return time.Since(time.UnixMilli(t.StartMs))
}

func List(ctx context.Context, adm *kadm.Client, states []string, producerIDs []int64, longerThan time.Duration) ([]Transaction, error) {
	listed, err := adm.ListTransactions(ctx, producerIDs, states)
	if err != nil {
		return nil, err
	}
	var ids []string
	for id := range listed {
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return []Transaction{}, nil
	}
	described, err := adm.DescribeTransactions(ctx, ids...)
	if err != nil {
		return nil, err
	}
	var out []Transaction
	for _, id := range ids {
		l := listed[id]
		t := Transaction{TxnID: id, ProducerID: l.ProducerID, State: l.State, Coordinator: l.Coordinator}
		if d, ok := described[id]; ok && d.Err == nil {
			t.Epoch, t.StartMs, t.TimeoutMs, t.State = d.ProducerEpoch, d.StartTimestamp, d.TimeoutMillis, d.State
			d.Topics.Each(func(topic string, p int32) { t.Partitions = append(t.Partitions, fmt.Sprintf("%s/%d", topic, p)) })
			slices.Sort(t.Partitions)
		}
		if longerThan > 0 && t.Duration() < longerThan {
			continue
		}
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b Transaction) int { return strings.Compare(a.TxnID, b.TxnID) })
	return out, nil
}

func Describe(ctx context.Context, adm *kadm.Client, id string) (Transaction, error) {
	described, err := adm.DescribeTransactions(ctx, id)
	if err != nil {
		return Transaction{}, err
	}
	d, ok := described[id]
	if !ok {
		return Transaction{}, fmt.Errorf("transactional id %q: %w", id, kerr.TransactionalIDNotFound)
	}
	if d.Err != nil {
		return Transaction{}, fmt.Errorf("transactional id %q: %w", id, d.Err)
	}
	t := Transaction{TxnID: id, ProducerID: d.ProducerID, Epoch: d.ProducerEpoch, State: d.State, Coordinator: d.Coordinator,
		StartMs: d.StartTimestamp, TimeoutMs: d.TimeoutMillis}
	d.Topics.Each(func(topic string, p int32) { t.Partitions = append(t.Partitions, fmt.Sprintf("%s/%d", topic, p)) })
	slices.Sort(t.Partitions)
	return t, nil
}

type Producer struct {
	Topic            string `json:"topic"`
	Partition        int32  `json:"partition"`
	Leader           int32  `json:"leader"`
	ProducerID       int64  `json:"producer_id"`
	Epoch            int16  `json:"producer_epoch"`
	LastSequence     int32  `json:"last_sequence"`
	LastTimestamp    int64  `json:"last_timestamp_ms"`
	CoordinatorEpoch int32  `json:"coordinator_epoch"`
	TxnStartOffset   int64  `json:"txn_start_offset"`
}

func (p Producer) Open() bool { return p.TxnStartOffset >= 0 }

func Producers(ctx context.Context, adm *kadm.Client, topics []string, partitions []int32) ([]Producer, error) {
	var set kadm.TopicsSet
	md, err := adm.Metadata(ctx, topics...)
	if err != nil {
		return nil, err
	}
	for _, t := range md.Topics {
		if t.Err != nil {
			if len(topics) > 0 {
				return nil, fmt.Errorf("topic %q: %w", t.Topic, t.Err)
			}
			continue
		}
		for p := range t.Partitions {
			if len(partitions) == 0 || slices.Contains(partitions, p) {
				set.Add(t.Topic, p)
			}
		}
	}
	if len(set) == 0 {
		return nil, nil
	}
	described, err := adm.DescribeProducers(ctx, set)
	if err != nil {
		return nil, err
	}
	var out []Producer
	described.EachProducer(func(d kadm.DescribedProducer) {
		out = append(out, Producer{d.Topic, d.Partition, d.Leader, d.ProducerID, d.ProducerEpoch, d.LastSequence, d.LastTimestamp, d.CoordinatorEpoch, d.CurrentTxnStartOffset})
	})
	slices.SortFunc(out, func(a, b Producer) int {
		return cmp.Or(strings.Compare(a.Topic, b.Topic), cmp.Compare(a.Partition, b.Partition), cmp.Compare(a.ProducerID, b.ProducerID))
	})
	return out, nil
}

type Hanging struct {
	Producer
	OpenSince time.Duration `json:"open_ns"`
	TxnID     string        `json:"transactional_id,omitempty"`
	TxnState  string        `json:"coordinator_state,omitempty"`
	Reason    string        `json:"reason"`
}

// FindHanging returns open transactions older than olderThan (default: the
// broker's transaction.max.timeout.ms) or unknown to their coordinator.
func FindHanging(ctx context.Context, adm *kadm.Client, topics []string, broker int32, olderThan time.Duration) ([]Hanging, error) {
	if olderThan <= 0 {
		olderThan = 15 * time.Minute
	}
	ps, err := Producers(ctx, adm, topics, nil)
	if err != nil {
		return nil, err
	}
	txns, err := List(ctx, adm, nil, nil, 0)
	if err != nil && !errors.Is(err, kerr.UnsupportedVersion) {
		return nil, err
	}
	byPID := map[int64]Transaction{}
	for _, t := range txns {
		byPID[t.ProducerID] = t
	}
	var out []Hanging
	for _, p := range ps {
		if !p.Open() || (broker >= 0 && p.Leader != broker) {
			continue
		}
		h := Hanging{Producer: p}
		if p.LastTimestamp > 0 {
			h.OpenSince = time.Since(time.UnixMilli(p.LastTimestamp))
		}
		t, known := byPID[p.ProducerID]
		if known {
			h.TxnID, h.TxnState = t.TxnID, t.State
			if d := t.Duration(); d > h.OpenSince {
				h.OpenSince = d
			}
		}
		switch {
		case !known || t.State != "Ongoing" && !strings.HasPrefix(t.State, "Prepare"):
			h.Reason = "coordinator has no ongoing transaction for this producer"
		case h.OpenSince >= olderThan:
			h.Reason = "open longer than " + units.Duration(olderThan)
		default:
			continue
		}
		out = append(out, h)
	}
	return out, nil
}

const KindAbort = "txn.abort"

type AbortSpec struct {
	Topic            string `json:"topic"`
	Partition        int32  `json:"partition"`
	Leader           int32  `json:"leader"`
	ProducerID       int64  `json:"producer_id"`
	Epoch            int16  `json:"producer_epoch"`
	CoordinatorEpoch int32  `json:"coordinator_epoch"`
}

type AbortTarget struct {
	Topic            string
	Partition        int32
	StartOffset      *int64
	ProducerID       *int64
	Epoch            *int16
	CoordinatorEpoch *int32
	Force            bool
}

func PlanAbort(ctx context.Context, adm *kadm.Client, t AbortTarget) (*plan.Plan, error) {
	ps, err := Producers(ctx, adm, []string{t.Topic}, []int32{t.Partition})
	if err != nil {
		return nil, err
	}
	var p *Producer
	for i := range ps {
		if !ps[i].Open() {
			continue
		}
		switch {
		case t.StartOffset != nil && ps[i].TxnStartOffset == *t.StartOffset:
			p = &ps[i]
		case t.ProducerID != nil && ps[i].ProducerID == *t.ProducerID:
			p = &ps[i]
		}
	}
	spec := AbortSpec{Topic: t.Topic, Partition: t.Partition}
	switch {
	case p != nil:
		spec.Leader, spec.ProducerID, spec.Epoch, spec.CoordinatorEpoch = p.Leader, p.ProducerID, p.Epoch, p.CoordinatorEpoch
	case t.ProducerID != nil && t.Epoch != nil && t.CoordinatorEpoch != nil:
		md, err := adm.Metadata(ctx, t.Topic)
		if err != nil {
			return nil, err
		}
		spec.Leader = md.Topics[t.Topic].Partitions[t.Partition].Leader
		spec.ProducerID, spec.Epoch, spec.CoordinatorEpoch = *t.ProducerID, *t.Epoch, *t.CoordinatorEpoch
	case t.StartOffset != nil:
		return nil, fmt.Errorf("no open transaction starts at offset %d on %s/%d", *t.StartOffset, t.Topic, t.Partition)
	default:
		return nil, fmt.Errorf("producer %d has no open transaction on %s/%d (pass --producer-epoch and --coordinator-epoch to abort anyway)", *t.ProducerID, t.Topic, t.Partition)
	}
	txns, _ := List(ctx, adm, nil, []int64{spec.ProducerID}, 0)
	state, txnID := "unknown", ""
	if len(txns) > 0 {
		state, txnID = txns[0].State, txns[0].TxnID
	}
	if state == "PrepareCommit" && !t.Force {
		return nil, errors.New("the coordinator is committing this transaction (PrepareCommit); aborting would race with it (use --force to abort anyway)")
	}
	where := fmt.Sprintf("%s/%d", t.Topic, t.Partition)
	pl, err := plan.New(KindAbort, plan.Destructive, "Abort transaction on "+where+":", spec)
	if err != nil {
		return nil, err
	}
	open := "?"
	if p != nil {
		open = fmt.Sprintf("open since %s (start offset %s)", units.Ago(time.UnixMilli(p.LastTimestamp), time.Now()), units.Count(p.TxnStartOffset))
	}
	pl.Add("producer %d epoch %d, %s", spec.ProducerID, spec.Epoch, open)
	if txnID != "" {
		pl.Add("transactional id: %s (coordinator state %s)", txnID, state)
	}
	pl.Add("effect: records from this transaction become aborted; read_committed consumers resume")
	pl.Warn("writing abort markers needs CLUSTER_ACTION on the cluster")
	pl.Confirm = where
	return pl, nil
}

// ApplyAbort writes an abort marker to the partition leader (like kafka-transactions.sh abort).
func ApplyAbort(ctx context.Context, cl *kgo.Client, pl *plan.Plan) error {
	var spec AbortSpec
	if err := pl.Decode(&spec); err != nil {
		return err
	}
	req := kmsg.NewPtrWriteTxnMarkersRequest()
	m := kmsg.NewWriteTxnMarkersRequestMarker()
	m.ProducerID, m.ProducerEpoch, m.CoordinatorEpoch, m.Committed = spec.ProducerID, spec.Epoch, spec.CoordinatorEpoch, false
	t := kmsg.NewWriteTxnMarkersRequestMarkerTopic()
	t.Topic, t.Partitions = spec.Topic, []int32{spec.Partition}
	m.Topics = append(m.Topics, t)
	req.Markers = append(req.Markers, m)
	resp, err := req.RequestWith(ctx, cl.Broker(int(spec.Leader)))
	if err != nil {
		return err
	}
	for _, mk := range resp.Markers {
		for _, tp := range mk.Topics {
			for _, p := range tp.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					return fmt.Errorf("%s/%d: %w", tp.Topic, p.Partition, err)
				}
			}
		}
	}
	return nil
}
