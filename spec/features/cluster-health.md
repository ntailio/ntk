# Cluster overview & health

## Purpose

One place that answers "is the cluster OK?" in seconds, for both humans (the TUI dashboard) and scripts (`ntk health` with an exit code). Without it, you'd run `kafka-topics.sh --under-replicated-partitions`, `--unavailable-partitions`, `kafka-metadata-quorum.sh`, `kafka-reassign-partitions.sh --verify`, and `kafka-transactions.sh find-hanging` separately.

## Checks

| Check | Source | Warn | Critical |
|---|---|---|---|
| Brokers reachable | `Metadata` | a broker in metadata is not responding | fewer brokers than `--expect-brokers` |
| Controller | `DescribeCluster` | - | no active controller |
| KRaft quorum | `DescribeQuorum` | voter lag > 1000 records or last fetch > 5s | a voter is unreachable and the quorum is at its minimum |
| Offline partitions | `Metadata` | - | any partition without a leader |
| Under-replicated partitions | `Metadata` (ISR < replicas) | any | - |
| Under min ISR | `Metadata` + topic `min.insync.replicas` | - | any (producers with acks=all fail) |
| At min ISR | same | any (one more failure blocks writes) | - |
| Preferred leader imbalance | `Metadata` | > 10% of partitions not on the preferred leader | - |
| Reassignments in progress | `ListPartitionReassignments` | any running > 1h | - |
| Log dir errors | `DescribeLogDirs` | - | any offline log dir |
| Disk usage | `DescribeLogDirs` total/usable (KIP-827) | > 80% | > 90% |
| Hanging transactions | `ListTransactions` / `DescribeProducers` ([transactions](transactions.md)) | open > `transaction.max.timeout.ms` | blocks the LSO for > 15m |
| Consumer groups (opt-in `--groups pattern`) | [group monitoring](group-monitoring.md) | falling behind | stuck |

Thresholds can be overridden by flags and in `config.json`.

## CLI

```
ntk health                     [--checks all|quorum,urp,...] [--expect-brokers N] [--groups 'svc-*'] [--watch]
ntk cluster describe           # static overview (also in brokers.md)
```

```
$ ntk health
Cluster lkc-9f2 · Kafka 3.8 · 3 brokers · KRaft (3 voters)
✓ brokers            3/3 reachable
✓ controller         2
✓ quorum             leader 2, max voter lag 2
✓ under-min-isr      0
! at-min-isr         1 partition: orders.state/1 (ISR 2,3; min.insync.replicas 2)
! under-replicated   1 partition: orders.state/1 missing broker 1
✓ offline            0
✓ leader imbalance   2.1%
✓ reassignments      none
✓ disk               max 71% (broker 3)
✓ transactions       no hanging transactions
Status: WARN
$ echo $?
5
```

- Exit code **0** if everything is OK. **5** if any check reaches the chosen `--fail-on` level (default `critical`; use `--fail-on warn` for stricter CI). **3** if the cluster is unreachable.
- `-o json` gives the full report with a status per check and the affected resources, suitable for feeding into monitoring.
- `--watch` re-runs the checks every interval and prints only changes (for example, `10:14:03 under-replicated 0 → 1 (orders.state/1)`).

## TUI

`:health` (optionally the start view, configurable) shows a dashboard:

```
┌ health ─ lkc-9f2 ─ WARN ─ 10s ─────────────────────────────────────────────┐
│ Brokers  ● 1  ● 2 (ctrl)  ● 3          Quorum  leader 2 · lag ≤ 2           │
│ Disk     b1 ▇▇▇▇▇▇▁▁ 68%  b2 ▇▇▇▇▇▇▁▁ 69%  b3 ▇▇▇▇▇▇▇▁ 71%                   │
│ Partitions  4,812 total · 0 offline · 1 URP · 1 at min ISR · imbalance 2.1%  │
├────────────────────────────────────────────────────────────────────────────┤
│ !  under-replicated  orders.state/1  ISR 2,3  missing 1   since 10:14:03    │
│ !  at-min-isr        orders.state/1  min.insync=2                          │
└────────────────────────────────────────────────────────────────────────────┘
```

- `enter` on an issue jumps to the related resource (topic partition, broker, transaction).
- The app header badge shows the worst current status from any view, so problems stay visible while you work elsewhere ([TUI](../tui.md#layout)).
- `E` shows the recent event list (status changes since the TUI started).

## Kafka APIs

`Metadata`, `DescribeCluster`, `DescribeQuorum`, `DescribeConfigs` (topic `min.insync.replicas`, batched), `ListPartitionReassignments`, `DescribeLogDirs`, `ListTransactions`, `DescribeProducers`, plus the [group monitoring](group-monitoring.md) calls when `--groups` is set.

## Safety

Read-only. Fix actions suggested in the output (e.g. "run preferred leader election") are shown as commands to run. They're never executed automatically.

## Open questions

- Should the health report include a "suggested fix" command for each issue (e.g. `ntk topic elect-leaders --type preferred`)?
- Should health checks be pluggable (user-defined checks in `config.json`, e.g. "topic X must receive data at least every 5m")?
