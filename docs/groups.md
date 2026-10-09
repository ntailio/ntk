# Consumer groups

![consumer groups: lag, describe, a dry-run reset, and a lag check for scripts](../assets/demos/groups.gif)

## See what's going on

```sh
ntk group list --lag                   # or: ntk g ls --lag
ntk g list --state Stable --type consumer
ntk g describe billing                 # members, assignments, committed offsets, lag per partition
ntk g describe billing --members
```

ntk understands classic groups, the new KIP-848 consumer protocol, and share groups.

## Reset offsets

Resetting is a dry run by default. You see the plan, then add `--execute`.

```sh
ntk g reset billing --topic orders --to-earliest
ntk g reset billing --topic orders --to-time -2h
ntk g reset billing --topic orders:3 --to-offset 1044000
ntk g reset billing --all-topics --shift-by -100
ntk g reset billing --topic orders --to-earliest --execute
```

```
Reset offsets for group "billing":
  TOPIC     PART   CURRENT       NEW      DIFF
  orders       0        61         0       -61
  orders       1        35         0       -35
  ! 96 messages will be re-consumed
(dry run; add --execute to apply)
```

The group must have no active members. ntk checks first and tells you who's still connected.

## Clean up

```sh
ntk g delete billing.old
ntk g delete-offsets billing --topic orders.retired
```

## Watch lag live

```sh
ntk g watch billing
ntk g lag billing --threshold 10000 --max-time-lag 5m    # exit 5 if breached
```

See [Monitoring](monitoring.md) for rates, ETA and stuck detection.

## In the TUI

`:groups` lists groups; `enter` opens one with tabs for offsets, members, topics and ACLs. `R` resets offsets through a form, `m` opens the live lag monitor.
