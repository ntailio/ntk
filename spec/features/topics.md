# Topics & partitions

## Purpose

Listing, inspecting, creating, and deleting topics, plus partition-level operations: adding partitions, truncating, reassigning replicas, and electing leaders. With the stock tools this is spread across `kafka-topics.sh`, `kafka-delete-records.sh`, `kafka-reassign-partitions.sh`, and `kafka-leader-election.sh`, and each takes JSON files in a different format. kcat only shows metadata.

## CLI

Alias: `ntk t …` ([short aliases](../cli-conventions.md#short-aliases)).

```
ntk topic list [pattern]            [--internal] [--regex] [--under-replicated] [--no-leader] [--under-min-isr]
ntk topic describe <topic>...       [--partitions 0,1]
ntk topic create <topic>            [-P partitions] [-r replication-factor] [--config k=v]... [--preset name] [--if-not-exists]
ntk topic delete <topic>...         [--regex]
ntk topic add-partitions <topic> <total>
ntk topic truncate <topic>          (--before-offset N | --before-time T | --all) [--partitions 0,1]
ntk topic offsets <topic>           [--time T]            # earliest/latest/offset-for-time per partition
ntk topic reassign plan             --topics a,b --brokers 1,2,3 [--rack-aware] [-o json > plan.json]
ntk topic reassign apply <plan.json> [--throttle 50MiB/s]
ntk topic reassign status           [--topics a,b]
ntk topic reassign cancel           [--topics a,b]
ntk topic elect-leaders             [--topics a,b] [--partitions ...] [--type preferred|unclean]
```

### `topic list`

```
$ ntk topic list 'orders*'
NAME           PARTITIONS  RF  URP  SIZE       RETENTION  CLEANUP
orders                 12   3    0  48.2 GiB   7d         delete
orders.dlq              3   3    0  120 MiB    30d        delete
orders.state           12   3    1  2.1 GiB    ∞          compact
```

`-o wide` adds `min.insync.replicas`, message count estimate (sum of high − low watermarks), leader distribution, and topic id. Sizes come from `DescribeLogDirs` and are skipped with `--fast`.

### `topic describe`

```
$ ntk topic describe orders.state
Topic:       orders.state (id 8Zq2…)
Partitions:  12   Replication: 3   min.insync.replicas: 2
Cleanup:     compact   Retention: ∞   Size: 2.1 GiB
Overrides:   cleanup.policy=compact, segment.ms=3600000   (see: ntk topic config orders.state)

PART  LEADER  REPLICAS  ISR     LOW      HIGH      MSGS     SIZE      STATUS
   0       1  1,2,3     1,2,3   0        104,233   104k     180 MiB   ok
   1       2  2,3,1     2,3     0         99,874   99.9k    171 MiB   under-replicated (missing 1)
  ...
Consumers:   svc-state-builder (lag 42), audit-sink (lag 0)
```

### `topic create`

```
$ ntk topic create payments -P 6 -r 3 --config retention.ms=7d --config min.insync.replicas=2
Created topic payments (6 partitions, RF 3)
```

- Durations and sizes in `--config` are parsed and converted: `retention.ms=7d` becomes `604800000`, and `retention.bytes=10GiB` becomes bytes.
- `-P`/`-r` default to the broker defaults (`num.partitions`, `default.replication.factor`) if omitted.
- **Presets** are named config bundles in `config.json`, e.g. `compacted` (cleanup.policy=compact, min.compaction.lag.ms=…) or `durable` (min.insync.replicas=2, unclean.leader.election.enable=false). `--preset compacted --config segment.ms=1h` layers flags over the preset.
- Without a name on a TTY, the `huh` form opens.

### `topic truncate`

This wraps `DeleteRecords`. `--before-time` resolves the offset for each partition with `ListOffsets` first. The plan shows how many messages each partition loses.

### Reassignment

`reassign plan` generates a balanced assignment (like `--generate`), optionally rack-aware, and prints a diff of replica changes per partition, including an estimated data movement in bytes. `reassign apply` sets an optional replication throttle (`leader/follower.replication.throttled.rate` + throttled replicas config) and removes it automatically when `status` reports completion. A plan file can also be written by hand in the stock `kafka-reassign-partitions.sh` JSON format, and that format is accepted as input.

## TUI

Topic list view (`:topics`):

| Key | Action |
|---|---|
| `enter` | Topic detail (tabs: Partitions, Config, Consumers, ACLs) |
| `c` / `p` | Consume from / produce to the topic |
| `m` | Topic monitor ([topic monitoring](topic-monitoring.md)) |
| `e` | Edit config ([topic config](topic-config.md)) |
| `n` | New topic form |
| `+` | Add partitions |
| `ctrl-d` | Delete (typed confirmation) |
| `i` | Toggle internal topics |
| `u` | Filter to topics with problems (URP / no leader / under min ISR) |

Partitions tab: per-partition leader, replicas, ISR (out-of-sync replicas highlighted), low/high watermarks, size. `c` on a partition consumes from that partition only. `T` truncates the selected partitions.

## Kafka APIs

- `Metadata` (kadm `ListTopics`, `Metadata`)
- `CreateTopics`, `DeleteTopics`, `CreatePartitions` (kadm `CreateTopics`, `DeleteTopics`, `CreatePartitions`)
- `ListOffsets` (kadm `ListStartOffsets`, `ListEndOffsets`, `ListOffsetsAfterMilli`)
- `DeleteRecords` (kadm `DeleteRecords`)
- `DescribeLogDirs` (kadm `DescribeAllLogDirs`) for sizes
- `AlterPartitionReassignments`, `ListPartitionReassignments` (kadm `AlterPartitionAssignments`, `ListPartitionReassignments`)
- `ElectLeaders` (kadm `ElectLeaders`)
- `IncrementalAlterConfigs` for throttles
- `DescribeGroups` / `OffsetFetch` for the "Consumers" section (see [consumer groups](consumer-groups.md))

## Safety

| Operation | Class |
|---|---|
| create, add-partitions | Safe write / Change. Adding partitions warns that key→partition mapping changes for keyed topics. |
| delete, truncate | Destructive. The plan shows size and message counts. |
| reassign apply | Change. The plan shows data movement. Warns if no throttle is set and movement > 10 GiB. |
| elect-leaders `unclean` | Destructive. Warns about possible data loss. |

`topic delete --regex` always lists the matched topics and requires confirmation, even with `-y` on `prod`-labelled profiles.

## Open questions

- Should we support "topic copy" (create with the same config and partition count on another profile)? Could be combined with the roadmap's cross-cluster diff.
- Reassignment balancing algorithm: simple round-robin (like the stock tool) vs size-aware placement?
