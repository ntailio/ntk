# Topics

```sh
ntk topic list                  # or: ntk t ls
ntk topic list 'orders.*'       # glob; --regex for regular expressions
ntk topic describe orders
```

`describe` shows partitions, leaders, replicas, ISR, earliest and latest offsets, size, and the consumer groups reading the topic.

## Create

```sh
ntk topic create orders -P 12 -r 3
ntk topic create orders.state --preset compacted
ntk topic create events --config retention.ms=30d --config max.message.bytes=4MiB
```

Values take human units (`30d`, `4MiB`). `--preset compacted` and `--preset durable` bundle common settings, and you can define your own in `~/.config/ntk/config.json`.

## Configure

```sh
ntk topic config orders                     # the keys that matter, with source and default
ntk topic config orders --all               # everything
ntk topic config set orders retention.ms=14d
ntk topic config unset orders retention.ms  # back to the broker default
ntk topic config diff orders orders.v2      # compare two topics
```

Changes are validated by the broker before anything is applied, and risky ones come with a warning, for example lowering retention or switching `cleanup.policy`.

## Change shape

```sh
ntk topic add-partitions orders 24          # to a total of 24
ntk topic truncate orders --before-time -7d  # delete records older than 7 days
ntk topic reassign plan --topics orders --brokers 1,2,3 -o json > plan.json
ntk topic reassign apply plan.json --throttle 50MiB/s
ntk topic elect-leaders orders              # preferred leader election
```

## Delete

```sh
ntk topic delete orders.tmp
```

You see the plan first: partitions, size, message count, and which consumer groups still read the topic. On a `prod`-labelled profile you have to type the topic name.

## Everything can be a dry run

```sh
ntk topic create orders -P 12 --dry-run
ntk topic delete orders --dry-run -o json > plan.json
ntk apply plan.json                         # later, after review
```
