# Consumer groups

## Purpose

Listing groups, seeing who consumes what and how far behind they are, and fixing offsets. The stock `kafka-consumer-groups.sh` works but is slow, its output is dense, and offset resets are error-prone. kcat has no group management. ntk supports both **classic** groups and **KIP-848 `consumer` protocol** groups, and shows share groups (KIP-932) as read-only where the cluster supports them.

For lag over time and alerting, see [group monitoring](group-monitoring.md).

## CLI

Alias: `ntk g …` ([short aliases](../cli-conventions.md#short-aliases)).

```
ntk group list [pattern]            [--state stable,empty,...] [--type classic|consumer|share] [--topic t] [--lag]
ntk group describe <group>          [--members] [--topic t]
ntk group lag <group>...            [--topic t] [--threshold N]
ntk group reset <group>             --topic t[:partitions] (--to-earliest | --to-latest | --to-offset N
                                    | --to-time T | --shift-by ±N | --from-file f.json) [--execute]
ntk group delete <group>...
ntk group delete-offsets <group> --topic t[:partitions]
```

### `group list`

```
$ ntk group list --lag
GROUP               TYPE      STATE    MEMBERS  TOPICS               LAG      COORDINATOR
svc-orders          consumer  Stable        6   orders, payments     1,204    2
audit-sink          classic   Stable        2   orders               0        1
old-reporting       classic   Empty         0   orders               8.2M     3
```

`--lag` fetches committed and end offsets for all listed groups, which costs one extra round trip per coordinator. `--topic orders` shows only groups with committed offsets for that topic.

### `group describe`

```
$ ntk group describe svc-orders
Group:        svc-orders   Type: consumer (KIP-848)   State: Stable   Epoch: 41
Coordinator:  broker 2     Assignor: uniform
Members:      6            Total lag: 1,204

TOPIC     PART  COMMITTED   END        LAG    MEMBER                    HOST          CLIENT-ID
orders       0  1,044,211   1,044,215    4    svc-orders-7d9f…-a1b2     10.2.0.14     svc-orders-0
orders       1  1,039,872   1,040,970  1,098  svc-orders-7d9f…-c3d4     10.2.0.15     svc-orders-1
...
payments     0     88,120      88,120    0    svc-orders-7d9f…-a1b2     10.2.0.14     svc-orders-0
```

`--members` shows members with their assigned partitions (grouped by member) plus, for classic groups, the protocol and assignor. Partitions without committed offsets are shown as `-` with lag relative to the start offset.

### `group reset`

```
$ ntk group reset svc-orders --topic orders --to-time 2026-09-30T08:00:00Z
[prod] Reset offsets for group "svc-orders" (dry run; add --execute to apply)
TOPIC   PART  CURRENT     NEW         DIFF
orders     0  1,044,211   1,002,113   −42,098
orders     1  1,039,872     998,410   −41,462
...
Total: 12 partitions, −501,332 messages (will be re-consumed)
```

- **Dry run is the default.** `--execute` applies the reset (after confirmation unless `-y`).
- The group must be **Empty** (no active members). If it isn't, ntk refuses and lists the active members. There's no force option, because committing offsets to an active group is overwritten by its members anyway.
- `--to-time` accepts RFC 3339, relative (`-2h`), or epoch ms.
- `--topic orders:0,3-5` selects partitions. `--all-topics` resets every topic the group has offsets for.
- `--from-file` accepts ntk's JSON plan format or the stock tool's CSV (`topic,partition,offset`).
- `--dry-run -o json > plan.json`, then `ntk apply plan.json` later (see [CLI conventions](../cli-conventions.md#safety)).

## TUI

Group list (`:groups`):

| Key | Action |
|---|---|
| `enter` | Group detail (tabs: Offsets & lag, Members, Topics) |
| `m` | Lag monitor ([group monitoring](group-monitoring.md)) |
| `R` | Reset offsets form → shows the plan → confirm |
| `ctrl-d` | Delete group |
| `f` | Filter by state / type |
| `t` | Jump to topic under cursor |

In the Offsets tab, lag cells are colored by threshold (configurable). Pressing `c` on a partition row opens the consumer at the group's committed offset, which is handy for finding the message a consumer is stuck on.

## Kafka APIs

- `ListGroups` with type/state filters (kadm `ListGroups`)
- `DescribeGroups` for classic, `ConsumerGroupDescribe` for KIP-848 (kadm `DescribeGroups`, raw `kmsg.ConsumerGroupDescribeRequest` where needed)
- `OffsetFetch`, `ListOffsets` (kadm `FetchOffsets`, `Lag`, `ListEndOffsets`)
- `OffsetCommit` for resets (kadm `CommitOffsets`)
- `DeleteGroups`, `OffsetDelete` (kadm `DeleteGroups`, `DeleteOffsets`)
- `FindCoordinator` (implicit)

## Safety

| Operation | Class |
|---|---|
| reset | Change. Dry run by default and requires an Empty group. |
| delete, delete-offsets | Destructive. Refused if the group has active members. |

## Open questions

- Show share groups (KIP-932) fully, including share-partition state, or only list them?
- Should `group reset` support "reset to the offset of another group" (copy offsets between groups)?
