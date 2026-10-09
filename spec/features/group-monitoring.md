# Consumer group monitoring

## Purpose

Answering "is my consumer keeping up?" Current lag alone is misleading. You need the **trend**, the **consume rate vs produce rate**, and whether the group is **stuck** or **rebalancing**. ntk calculates these from offsets alone, and the same logic works as a scriptable check for CI, cron, or on-call runbooks.

For static group management (describe, reset, delete), see [consumer groups](consumer-groups.md).

## CLI

```
ntk group watch <group>...          [--interval 5s] [--by-partition] [--topic t]
ntk group lag <group>...            [--threshold N] [--max-time-lag 5m] [--fail-on-stuck]
ntk group top                       [--sort lag|time-lag|rate] [-n 20] [pattern]
```

### `group watch`

```
$ ntk group watch svc-orders
svc-orders · consumer · Stable · 6 members · interval 5s
TOPIC     LAG     TREND      CONSUME/S  PRODUCE/S  TIME LAG  ETA      STATUS
orders    1,204   ▅▄▃▃▂▂ ↓   1,410      1,204      ~1s       ~6s      catching up
payments      0   ▁▁▁▁▁▁ =     120        120      0s        -        ok
```

Metrics per topic (and per partition with `--by-partition`):

| Metric | How it's computed |
|---|---|
| **Lag** | end offset − committed offset |
| **Consume rate** | Δ committed offset / Δt |
| **Produce rate** | Δ end offset / Δt |
| **Time lag** | Estimated age of the committed position: lag ÷ produce rate. With `--exact-time-lag`, ntk instead fetches the record at the committed offset and uses its timestamp (one extra fetch per partition). |
| **ETA** | Lag ÷ (consume rate − produce rate), shown only when it's positive |
| **Status** | `ok` · `catching up` · `falling behind` (lag rising over the window) · `stuck` (lag > 0 and committed offset unchanged for ≥ `--stuck-after`, default 2m) · `rebalancing` (group state PreparingRebalance/CompletingRebalance or an epoch change) · `no members` (Empty with lag > 0) |

### Scriptable checks

`group lag` is a one-shot check. The `--threshold`/`--max-time-lag`/`--fail-on-stuck` flags turn it into an assertion that exits with **code 5** when breached ([exit codes](../cli-conventions.md#exit-codes)):

```
$ ntk group lag svc-orders --threshold 10000 --max-time-lag 5m
svc-orders  orders    lag 1,204   time-lag ~1s   ok
svc-orders  payments  lag 0       time-lag 0s    ok
$ echo $?
0
```

Stuck detection needs two samples, so `--fail-on-stuck` samples for `--stuck-after` before deciding. `-o json` emits the full metric set for other tools.

### `group top`

Ranks all groups (or those matching a pattern) by lag, time lag, or consume rate, to find the worst consumer in a cluster at a glance.

## TUI

`m` on a group (or `:grpmon <group>`), and `:gtop` for the ranking.

```
┌ monitor svc-orders ─ Stable ─ 6 members ─ epoch 41 ─ 5s ─────────────────┐
│ lag     ▅▄▃▃▂▂▁   1,204 (−3,100 in 1m)      status: catching up            │
│ rates   consume 1,410/s · produce 1,204/s · ETA ~6s                        │
├───────────────────────────────────────────────────────────────────────────┤
│ TOPIC/PART  MEMBER            LAG    TREND   CONS/S  TIME LAG  STATUS      │
│ orders/1    svc-orders-1      1,098  ▆▅▃▂    210     ~1s       catching up │
│ orders/7    svc-orders-4      0      ▁▁▁▁    118     0s        ok          │
│ orders/9    svc-orders-5      611    ▃▃▃▃      0     4m        stuck       │
└───────────────────────────────────────────────────────────────────────────┘
```

| Key | Action |
|---|---|
| `enter` on a stuck partition | Opens the consumer at the committed offset, showing the message the consumer is likely stuck on |
| `M` | Group by member instead of partition (spot a slow instance) |
| `R` | Reset offsets (goes to the [consumer groups](consumer-groups.md#group-reset) reset flow) |
| `+` / `-` | Change interval |

Events such as a rebalance starting, a member joining or leaving, or a partition becoming stuck are shown as a timestamped event strip under the header.

## Kafka APIs

- `OffsetFetch` (kadm `FetchOffsets`) and `ListOffsets` (kadm `ListEndOffsets`) each interval, batched per coordinator/leader
- `DescribeGroups` / `ConsumerGroupDescribe` for state, epoch, members
- `Fetch` of a single record for `--exact-time-lag` and the "open stuck message" action

## Safety

Read-only.

## Open questions

- Should thresholds be stored per group in `config.json` so `ntk group lag` with no flags uses them?
- Should we support a webhook/notify action on breach, or keep that to cron + exit codes?
