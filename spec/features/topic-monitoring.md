# Topic monitoring

## Purpose

Seeing what's happening on a topic right now: is data flowing, how fast, are partitions balanced, is anything under-replicated. Without a metrics stack you'd have to poll offsets by hand. ntk computes live rates from **offset deltas** and sizes from **log dirs**, so it needs no JMX or Prometheus. It's a short-window view (ntk doesn't store history; see [overview](../overview.md#non-goals-for-now)).

## CLI

```
ntk topic watch <topic>...          [--interval 5s] [--by-partition] [--window 1m]
ntk topic stats <topic>...          # one-shot snapshot
ntk topic top                       [--sort msgs|bytes|size|partitions] [-n 20] [pattern]
```

### `topic watch`

```
$ ntk topic watch orders --by-partition
orders · 12 partitions · RF 3 · 48.2 GiB · interval 5s · window 1m
PART  LEADER  ISR     MSG/S   Δ1m      SIZE      SKEW   STATUS
   0       1  1,2,3     102   6.1k     4.0 GiB   1.0×   ok
   1       2  2,3        98   5.9k     3.9 GiB   1.0×   under-replicated
   2       3  3,1,2      11   0.7k     4.1 GiB   0.1×   ← low throughput
  ...
TOTAL                 1,204   72.2k    48.2 GiB
```

- **msg/s** is the change in the high watermark between samples. It counts produce throughput including transaction markers. **bytes/s** is the change in partition size from `DescribeLogDirs`. Because segment deletion makes it noisy, it's shown only in `-o wide` and marked approximate.
- **Skew** is the partition's msg rate relative to the mean. Partitions below 0.25× or above 4× are flagged, which usually means key hotspots.
- The watch output updates in place on a TTY. Otherwise, and with `-o jsonl`, it emits one sample object per interval, so it's suitable for piping into other tools.

### `topic top`

A cluster-wide ranking of the busiest or largest topics, like `top` for topics. It takes the same metrics from one sample pair across all topics.

```
$ ntk topic top -n 5
TOPIC            MSG/S   SIZE       PARTS  URP
orders           1,204   48.2 GiB      12    0
clickstream        980   310 GiB       48    0
payments           120   6.0 GiB        6    0
...
```

## TUI

`m` on a topic (or `:topmon <topic>`), and `:top` for the cluster-wide ranking.

```
┌ monitor orders ─ 5s ─ window 1m ──────────────────────────────────────────┐
│ msg/s  ▁▂▃▅▆▇▆▅▆▇█▇▆▅   1,204 now · 1,150 avg · 1,480 max                  │
│ size   48.2 GiB (+312 MiB in window)                                       │
│ URP 0 · offline 0 · leaders: b1 4 · b2 4 · b3 4                            │
├───────────────────────────────────────────────────────────────────────────┤
│ PART  LEADER  ISR    MSG/S  ▁▂▃ trend     SIZE     SKEW  STATUS            │
│    0       1  1,2,3    102  ▃▄▄▅▅▄       4.0 GiB  1.0×  ok                 │
│    2       3  3,1,2     11  ▁▁▁▁▁▁       4.1 GiB  0.1×  low throughput     │
└───────────────────────────────────────────────────────────────────────────┘
```

| Key | Action |
|---|---|
| `+` / `-` | Change sampling interval |
| `W` | Change the window (1m, 5m, 15m; kept in memory only). `w` is the global wide toggle. |
| `g` | Show groups consuming this topic with their lag ([group monitoring](group-monitoring.md)) |
| `c` | Consume from the selected partition |

## Kafka APIs

- `ListOffsets` (latest) per partition every interval (kadm `ListEndOffsets`)
- `Metadata` for leader/ISR
- `DescribeLogDirs` for sizes (less often by default: every 30s, because it's heavier)

## Safety

Read-only. The minimum interval is 1s. With `topic top` on clusters with more than 5,000 partitions, ntk warns and defaults to a 15s interval.

## Open questions

- Optionally read broker JMX/Prometheus metrics (bytes in/out, request rates) when a metrics endpoint is configured in the profile? See [roadmap](../roadmap.md).
- Should we keep a small on-disk history so `watch` can show the last hour on startup?
