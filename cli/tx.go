// Copyright 2026 Factual Tech AB
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/ntailio/ntk/tx"
	"github.com/ntailio/ntk/units"
)

type txnRows []tx.Transaction

func (r txnRows) Header(bool) []string {
	return []string{"TRANSACTIONAL ID", "PRODUCER ID", "EPOCH", "STATE", "DURATION", "PARTITIONS", "COORDINATOR"}
}

func (r txnRows) Rows(bool) [][]string {
	var rows [][]string
	for _, t := range r {
		dur := "-"
		if d := t.Duration(); d > 0 {
			dur = roundDur(d)
			if t.TimeoutMs > 0 && d > time.Duration(t.TimeoutMs)*time.Millisecond {
				dur += " ← exceeds timeout"
			}
		}
		rows = append(rows, []string{t.TxnID, strconv.FormatInt(t.ProducerID, 10), itoa(t.Epoch), t.State, dur, itoa(len(t.Partitions)), itoa(t.Coordinator)})
	}
	return rows
}

func (r txnRows) Names() []string {
	var n []string
	for _, t := range r {
		n = append(n, t.TxnID)
	}
	return n
}

type producerRows []tx.Producer

func (r producerRows) Header(bool) []string {
	return []string{"TOPIC", "PART", "PRODUCER ID", "EPOCH", "LAST SEQ", "LAST TIMESTAMP", "COORDINATOR EPOCH", "TXN START OFFSET"}
}

func (r producerRows) Rows(bool) [][]string {
	var rows [][]string
	now := time.Now()
	for _, p := range r {
		ts := "-"
		if p.LastTimestamp > 0 {
			t := time.UnixMilli(p.LastTimestamp)
			ts = fmt.Sprintf("%s (%s)", t.Format("15:04:05"), units.Ago(t, now))
		}
		start := "-"
		if p.Open() {
			start = units.Count(p.TxnStartOffset) + " ← open"
		}
		rows = append(rows, []string{p.Topic, itoa(p.Partition), strconv.FormatInt(p.ProducerID, 10), itoa(p.Epoch), itoa(p.LastSequence), ts, itoa(p.CoordinatorEpoch), start})
	}
	return rows
}

type hangingRows []tx.Hanging

func (r hangingRows) Header(bool) []string {
	return []string{"TOPIC", "PART", "PRODUCER ID", "EPOCH", "OPEN SINCE", "START OFFSET", "TXN ID", "REASON"}
}

func (r hangingRows) Rows(bool) [][]string {
	var rows [][]string
	for _, h := range r {
		id := orDash(h.TxnID)
		if h.TxnState != "" {
			id += " (" + h.TxnState + ")"
		}
		rows = append(rows, []string{h.Topic, itoa(h.Partition), strconv.FormatInt(h.ProducerID, 10), itoa(h.Epoch), roundDur(h.OpenSince), units.Count(h.TxnStartOffset), id, h.Reason})
	}
	return rows
}

func fetchTxnIDs(ctx context.Context, s *session) ([]string, error) {
	ts, err := tx.List(ctx, s.cl.Admin, nil, nil, 0)
	if err != nil {
		return nil, err
	}
	return txnRows(ts).Names(), nil
}

func (a *app) newTxCmd() *cobra.Command {
	cmd := groupCmd("tx", "Inspect producers and transactions; abort hanging ones", "transactions")

	var states, pids string
	var longer time.Duration
	list := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List transactions",
		Args:    usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			var ids []int64
			for _, p := range splitList(pids) {
				n, err := strconv.ParseInt(p, 10, 64)
				if err != nil {
					return usageErr("--producer-id %q is not a number", p)
				}
				ids = append(ids, n)
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			ts, err := tx.List(cmd.Context(), s.cl.Admin, splitList(states), ids, longer)
			if err != nil {
				return err
			}
			return a.render(txnRows(ts))
		},
	}
	list.Flags().StringVar(&states, "state", "", "only these states, e.g. Ongoing,PrepareCommit")
	list.Flags().StringVar(&pids, "producer-id", "", "only these producer ids")
	list.Flags().DurationVar(&longer, "duration-gt", 0, "only transactions open longer than this")

	describe := &cobra.Command{
		Use:               "describe <transactional-id>",
		Aliases:           []string{"get"},
		Short:             "Describe a transaction",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.complete("txn-ids", fetchTxnIDs, false),
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			t, err := tx.Describe(cmd.Context(), s.cl.Admin, args[0])
			if err != nil {
				return err
			}
			if a.flags.output != "table" && a.flags.output != "wide" {
				return a.render(t)
			}
			if err := a.render(txnRows{t}); err != nil {
				return err
			}
			if len(t.Partitions) > 0 {
				fmt.Fprintf(a.stdout, "\nPartitions: %s\n", strings.Join(t.Partitions, ", "))
			}
			return nil
		},
	}

	var partition int32
	producers := &cobra.Command{
		Use:               "producers <topic>",
		Short:             "Active producers per partition (open transactions marked)",
		Args:              usageArgs(cobra.ExactArgs(1)),
		ValidArgsFunction: a.completeTopicArg,
		RunE: func(cmd *cobra.Command, args []string) error {
			var parts []int32
			if cmd.Flags().Changed("partition") {
				parts = []int32{partition}
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			ps, err := tx.Producers(cmd.Context(), s.cl.Admin, []string{args[0]}, parts)
			if err != nil {
				return err
			}
			return a.render(producerRows(ps))
		},
	}
	producers.Flags().Int32Var(&partition, "partition", 0, "only this partition")

	var topic string
	var broker int32
	var older time.Duration
	hanging := &cobra.Command{
		Use:   "hanging",
		Short: "Find open transactions that block the last stable offset",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			var ts []string
			if topic != "" {
				ts = []string{topic}
			}
			hs, err := tx.FindHanging(cmd.Context(), s.cl.Admin, ts, broker, older)
			if err != nil {
				return err
			}
			if len(hs) == 0 && !a.streaming() {
				fmt.Fprintln(a.stderr, "No hanging transactions.")
				return nil
			}
			return a.render(hangingRows(hs))
		},
	}
	hanging.Flags().StringVar(&topic, "topic", "", "only this topic")
	hanging.Flags().Int32Var(&broker, "broker", -1, "only partitions led by this broker")
	hanging.Flags().DurationVar(&older, "older-than", 15*time.Minute, "open longer than this counts as hanging")
	_ = hanging.RegisterFlagCompletionFunc("topic", a.completeTopicArg)

	var t tx.AbortTarget
	var startOffset, producerID int64
	var epoch int16
	var coordEpoch int32
	abort := &cobra.Command{
		Use:   "abort --topic t --partition N (--start-offset O | --producer-id P [--producer-epoch E --coordinator-epoch C])",
		Short: "Abort a hanging transaction by writing an abort marker",
		Args:  usageArgs(cobra.NoArgs),
		RunE: func(cmd *cobra.Command, _ []string) error {
			fl := cmd.Flags()
			if t.Topic == "" || !fl.Changed("partition") {
				return usageErr("--topic and --partition are required")
			}
			if fl.Changed("start-offset") {
				t.StartOffset = &startOffset
			}
			if fl.Changed("producer-id") {
				t.ProducerID = &producerID
			}
			if fl.Changed("producer-epoch") {
				t.Epoch = &epoch
			}
			if fl.Changed("coordinator-epoch") {
				t.CoordinatorEpoch = &coordEpoch
			}
			if t.StartOffset == nil && t.ProducerID == nil {
				return usageErr("use --start-offset, or --producer-id")
			}
			s, err := a.session()
			if err != nil {
				return err
			}
			defer s.Close()
			pl, err := tx.PlanAbort(cmd.Context(), s.cl.Admin, t)
			if err != nil {
				return err
			}
			pl.Typed = true
			return a.run(cmd.Context(), s, pl)
		},
	}
	af := abort.Flags()
	af.StringVar(&t.Topic, "topic", "", "topic")
	af.Int32Var(&t.Partition, "partition", 0, "partition")
	af.Int64Var(&startOffset, "start-offset", 0, "the transaction's first offset (from `ntk tx hanging`)")
	af.Int64Var(&producerID, "producer-id", 0, "producer id")
	af.Int16Var(&epoch, "producer-epoch", 0, "producer epoch")
	af.Int32Var(&coordEpoch, "coordinator-epoch", 0, "coordinator epoch")
	af.BoolVar(&t.Force, "force", false, "abort even while the coordinator is committing")
	_ = abort.RegisterFlagCompletionFunc("topic", a.completeTopicArg)

	cmd.AddCommand(list, describe, producers, hanging, abort)
	return cmd
}
