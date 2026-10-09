# Brokers

## Purpose

Inspecting brokers and the controller quorum, and managing dynamic broker configs, without SSH to brokers or reading `server.properties`. This covers `kafka-broker-api-versions.sh`, `kafka-log-dirs.sh`, `kafka-metadata-quorum.sh`, `kafka-features.sh`, and the broker part of `kafka-configs.sh`.

## CLI

Alias: `ntk b …` for `broker` commands. `cluster` has no short alias ([short aliases](../cli-conventions.md#short-aliases)).

```
ntk broker list
ntk broker describe <id>
ntk broker config <id|--cluster-default>         [--all] [--overrides-only]
ntk broker config set <id|--cluster-default> k=v...
ntk broker config unset <id|--cluster-default> key...
ntk broker log-dirs                               [--broker id] [--topic t]
ntk broker api-versions                           [--broker id]
ntk cluster describe                              # id, controller, quorum, features
ntk cluster quorum                                # KRaft quorum status
ntk cluster features                              # finalized feature levels (metadata.version, ...)
```

### `broker list`

```
$ ntk broker list
ID  HOST              PORT  RACK   ROLE                 LEADERS  REPLICAS  DISK USED   VERSION
 1  kafka-1.prod      9093  eu-1a  broker               412      1,236     1.2 TiB     3.8
 2  kafka-2.prod      9093  eu-1b  broker, controller*  410      1,236     1.2 TiB     3.8
 3  kafka-3.prod      9093  eu-1c  broker               414      1,236     1.3 TiB     3.8
```

`controller*` marks the active controller. Version is inferred from `ApiVersions` (the protocol doesn't report an exact version, so ntk shows the "≥" version).

### `broker describe`

This shows host, rack, leader and replica counts, partitions for which the broker is out of the ISR, log dirs with usage and errors, and the dynamic config overrides.

### Dynamic broker configs

```
$ ntk broker config set --cluster-default log.retention.hours=168
[prod] Change cluster-wide default broker config:
  log.retention.hours   (static: 168) → 168 (dynamic cluster default)
Apply? [y/N]
```

- Shows the source per key (`static-broker`, `dynamic-broker`, `dynamic-default-broker`, `default`) just like [topic config](topic-config.md).
- Rejects keys that are read-only (can't be updated dynamically) before sending anything. `broker config` marks them `(read-only)` in the SOURCE column.
- Sensitive values (`ssl.keystore.password`, …) are shown as `(sensitive)`.

### `broker log-dirs`

```
$ ntk broker log-dirs --topic orders
BROKER  DIR                 TOPIC   PART  SIZE      OFFSET-LAG  FUTURE
     1  /data/kafka-1       orders     0  4.0 GiB            0  -
     1  /data/kafka-1       orders     3  4.1 GiB            0  -
```

It also shows total/usable bytes per log dir (KIP-827) where the broker supports it.

### `cluster quorum`

```
$ ntk cluster quorum
Leader: 2 · Epoch 17 · High watermark 1,203,992
VOTER  LOG END     LAG  LAST FETCH  LAST CAUGHT UP
    1  1,203,992     0  0.1s ago    0.1s ago
    2  1,203,992     0  leader      -
    3  1,203,990     2  0.3s ago    0.3s ago
OBSERVERS: 4, 5, 6 (brokers)
```

## TUI

`:brokers` lists brokers. `enter` opens the detail view (tabs: Overview, Partitions, Log dirs, Config). `:cluster` opens a cluster overview (id, controller, quorum, features), which is shared with [cluster health](cluster-health.md).

| Key | Action |
|---|---|
| `e` | Edit broker config (staged, then applied with a diff) |
| `D` | Toggle cluster-default config |
| `p` | Show partitions led / replicated by this broker |

## Kafka APIs

- `DescribeCluster`, `Metadata` (kadm `BrokerMetadata`, `DescribeCluster`)
- `DescribeConfigs`, `IncrementalAlterConfigs` for BROKER resources (kadm `DescribeBrokerConfigs`, `AlterBrokerConfigs`)
- `DescribeLogDirs` (kadm `DescribeAllLogDirs`)
- `ApiVersions`
- `DescribeQuorum` (raw `kmsg` request)
- `UpdateFeatures` / `ApiVersions` finalized features (describe only; changing features is in the [roadmap](../roadmap.md))

## Safety

- Broker config set/unset are **Change**. Cluster-default changes are labelled "affects all brokers".
- Keys that affect availability or durability (`min.insync.replicas`, `unclean.leader.election.enable`, `num.replica.fetchers`, listener configs) add warnings to the plan.
- ntk doesn't restart, stop, or decommission brokers. For decommissioning, see reassignment in [topics](topics.md#reassignment).

## Open questions

- Should we add a "drain broker" helper (a reassignment plan that moves all replicas off one broker) on top of the reassignment feature?
- Should `metadata.version` upgrades (`UpdateFeatures`) be supported, or are they too risky for a general tool?
