# Monitoring

ntk computes rates from offsets, so it works on any cluster without JMX or extra agents.

![topic watch with live traffic, then a health check](../assets/demos/monitor.gif)

## Topics

```sh
ntk topic watch orders                 # per partition: msg/s, size, skew, ISR, trend
ntk topic watch 'orders.*'             # several topics, one row each
ntk topic top                          # the busiest topics on the cluster
ntk topic stats orders --sample 10s    # one snapshot, scriptable with -o json
```

`watch` redraws in place on a terminal. Piped, or with `-o jsonl`, it prints one sample per interval, which is an easy way to log throughput over time.

## Consumer groups

```sh
ntk group watch billing
ntk group top                          # groups ranked by lag
```

For each group and topic you get lag, a trend sparkline, consume and produce rates, **time lag** (how old the oldest unconsumed message is), an **ETA** to catch up, and a status: `ok`, `catching up`, `falling behind`, `stuck` (lag but no commits), or `rebalancing`.

### In scripts and CI

```sh
ntk group lag billing --threshold 10000           # exit 5 if lag is higher
ntk group lag 'svc-*' --max-time-lag 5m
ntk group lag billing --fail-on-stuck --stuck-after 2m
```

## Cluster health

```sh
ntk health
```

```
Cluster abc123 · Kafka 4.3 · 3 brokers · KRaft (3 voters)
✓ brokers            3/3 reachable
✓ controller         2
✓ quorum             leader 2, max voter lag 0
✓ offline            0
✓ under-replicated   0
✓ under-min-isr      0
✓ at-min-isr         0
✓ leader-imbalance   0.0% of partitions not on their preferred leader
✓ reassignments      none
✓ log-dirs           3 dirs, no errors
✓ disk               max 31% (broker 1)
✓ transactions       no hanging transactions
Status: OK
```

`ntk health --fail-on warn` exits 5 when anything is amber, which makes it a one-line readiness check. `--watch` keeps running and prints changes, `--checks` picks a subset, and `-o json` gives the details.

## Brokers and the cluster

```sh
ntk broker list                        # id, host, rack, controller
ntk broker describe 1                  # partitions, log dirs, dynamic config
ntk broker log-dirs --topic orders     # where the bytes are
ntk cluster quorum                     # KRaft voters and their lag
ntk tx hanging                         # transactions blocking consumers
```

## In the TUI

`m` on a topic or group opens the same live views with sparklines. `:health` is the dashboard, and the header always shows the current status, whatever view you're in.
