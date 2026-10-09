# Producers & transactions

## Purpose

Finding and fixing transaction problems. A **hanging transaction** (a producer that began a transaction and never committed or aborted it, e.g. after a broker bug or a zombie producer) pins the partition's **last stable offset (LSO)**. Every `read_committed` consumer on that partition then stops making progress, while its lag keeps growing and there's no obvious error. Diagnosing this with `kafka-transactions.sh` requires several commands and broker ids. ntk surfaces it in [health](cluster-health.md), [group monitoring](group-monitoring.md) (as the cause of `stuck`), and here.

## CLI

```
ntk tx list                         [--state Ongoing,PrepareCommit,...] [--producer-id N] [--duration-gt 10m]
ntk tx describe <transactional-id>
ntk tx producers <topic>            [--partition N]    # active producers per partition
ntk tx hanging                      [--topic t] [--broker id] [--older-than 15m]
ntk tx abort                        (--topic t --partition N --producer-id P --producer-epoch E --coordinator-epoch C
                                     | --start-offset O --topic t --partition N)
```

### `tx list`

```
$ ntk tx list --state Ongoing
TRANSACTIONAL ID          PRODUCER ID  EPOCH  STATE    DURATION  PARTITIONS  COORDINATOR
payments-processor-0      4001         12     Ongoing  0.4s      3           1
settlement-batch          3877         2      Ongoing  47m       1           3   ← exceeds timeout
```

### `tx producers`

```
$ ntk tx producers orders.state --partition 1
PRODUCER ID  EPOCH  LAST SEQ  LAST TIMESTAMP   COORDINATOR EPOCH  TXN START OFFSET
3877         2      1,204     09:27:14 (47m)   5                  98,410    ← open
4001         12     88,120    10:14:02         7                  -
```

### `tx hanging`

This checks every partition (or the selected ones) for producers with an open transaction (`TXN START OFFSET` set) that either:

- are older than `transaction.max.timeout.ms` (or `--older-than`), or
- have a transactional id the coordinator doesn't know as Ongoing (for example, a transaction that was never registered with the coordinator).

```
$ ntk tx hanging
TOPIC          PART  PRODUCER ID  EPOCH  OPEN SINCE  START OFFSET  LSO BLOCKED FOR  TXN ID
orders.state      1  3877         2      47m         98,410        47m              settlement-batch (Ongoing)
```

### `tx abort`

This writes an abort marker for the hanging transaction (the `kafka-transactions.sh abort` equivalent). With `--start-offset`, ntk finds the producer id/epoch itself via `DescribeProducers`, so you don't have to copy ids around.

```
$ ntk tx abort --topic orders.state --partition 1 --start-offset 98410
[prod] Abort transaction on orders.state/1:
  producer 3877 epoch 2, open since 47m (start offset 98,410)
  transactional id: settlement-batch (coordinator state Ongoing)
  Effect: records from this transaction become aborted; read_committed consumers resume.
Type "orders.state/1" to confirm:
```

## TUI

`:tx` has tabs: Transactions, Hanging, Producers (by topic).

| Key | Action |
|---|---|
| `enter` | Describe transaction / producer |
| `A` | Abort selected hanging transaction (plan + typed confirmation) |
| `t` | Jump to the topic partition |

## Kafka APIs

- `ListTransactions` (KIP-664; kadm `ListTransactions`)
- `DescribeTransactions` (kadm `DescribeTransactions`)
- `DescribeProducers` (kadm `DescribeProducers`)
- `WriteTxnMarkers` sent to the partition leader for abort (requires `CLUSTER_ACTION` ACL; ntk warns if the current principal likely lacks it)

## Safety

- Everything is read-only except `abort`, which is **Destructive** and always requires typed confirmation, regardless of labels.
- ntk refuses to abort a transaction whose coordinator state is `PrepareCommit` unless `--force`. The coordinator is already completing that commit, and aborting would race with it.

## Open questions

- Should `tx hanging` run automatically as part of `group watch` when a `read_committed` group is stuck, and show the culprit inline?
- Is support for Kafka < 3.0 (no `ListTransactions`/`DescribeProducers`) worth any fallback?
