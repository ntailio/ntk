# Producing

`ntk produce` (or `ntk p`) sends bytes as you give them. No serialization, no schema, just Kafka.

## One message

```sh
ntk p orders -k order-1 -v '{"id":"order-1"}' -H tenant=acme
ntk p orders -k big -v @payload.bin            # value from a file
ntk p orders -k order-1 --tombstone            # null value, for compacted topics
```

## Many messages

```sh
cat values.txt | ntk p orders                  # one message per line
ntk p orders -f values.txt                     # same, from a file
ntk p orders --key-sep ':' < pairs.txt         # key:value per line
ntk p orders --delimiter '\0' < blob           # custom separator
```

### Load tests

```sh
ntk p orders -k 'order-{{.i}}' -v '{"id":{{.i}},"t":"{{now}}"}' --template --count 10000 --rate 500/s
```

`--repeat` runs until you stop it. `--rate` throttles.

## Replay and copy

```sh
ntk p orders --in jsonl -f yesterday.jsonl                # from consume -o jsonl
ntk p orders.retry --from-topic orders.dlq --from earliest   # topic to topic
ntk p orders --from-topic orders --from-profile prod         # cluster to cluster
```

`--in jsonl` reads the JSON Lines that `consume -o jsonl` writes. Keys, headers, and binary values come back exactly. By default records are re-partitioned by key and get a new timestamp; `--keep-partition` and `--keep-timestamp` keep the originals.

`--in raw -m` reads the output of `consume -m`, so a plain pipe copies everything:

```sh
ntk -p prod c orders --from -1h -m | ntk -p staging p orders -m
```

There's also `--in unix:<path>`, which listens on a socket, and `--in npipe:<name>`, which does the same with a named pipe on Windows. See [Mirroring](mirroring.md).

## Delivery settings

`--acks all|1|0`, `--compression gzip|snappy|lz4|zstd`, `-P` to force a partition, `--partitioner murmur2|round-robin|sticky` (murmur2 matches the Java client, so keys land where Java puts them), and `--transactional-id` to send everything in one transaction that's committed at the end.

## What you see

```
Produced 1,204 messages to orders in 2.1s (573 msg/s, 1.1 MiB) · 0 failed
```

Single messages print their partition and offset. `-o json` gives one delivery report per message.

## Interactive

`ntk p orders -i` opens the TUI composer: key, headers, a value editor (`ctrl+e` for `$EDITOR`), and a history of what you sent.
