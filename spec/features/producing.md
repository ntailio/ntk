# Producing

## Purpose

Sending test data, replaying exported messages, fixing up a stuck pipeline by re-publishing, or copying messages between topics or clusters. With kcat you have to get delimiter flags (`-K:`, `-H`) right. `kafka-console-producer.sh` can't easily set headers or keys. ntk supports keys, headers, and partitions from flags, stdin, files, or an interactive composer.

Like [consuming](consuming.md), producing never encodes or transforms data. Keys and values are sent as the exact bytes you provide.

## CLI

```
ntk produce <topic> [flags]
ntk p <topic> [flags]                  # short alias
```

### Input sources (exactly one)

| Source | Example |
|---|---|
| Flags (single message) | `ntk produce orders -k order-1 -v '{"id":"order-1"}' -H tenant=acme` |
| Stdin (`--in raw`, the default) | `cat values.txt \| ntk produce orders` (one message per line) |
| A file instead of stdin | `ntk produce orders -f values.txt`. The format still comes from `--in`. |
| ntk record format (jsonl) | `ntk produce orders --in jsonl -f records.jsonl` |
| Unix datagram socket | `ntk produce orders --in unix:/tmp/orders.sock` (one message per datagram) |
| From another topic/profile | `ntk produce orders.retry --from-topic orders.dlq [--from-profile prod] [--from … --until … -n …]` |
| Interactive | `ntk produce orders -i` (TUI composer) |
| Value from a file | `ntk produce orders -k big -v @payload.bin` (`@path` reads a file; `@-` reads stdin as one message) |

`-I, --in` is the inverse of consume's [`-o`](consuming.md#output): `raw`, `jsonl`, and `unix:<path>` read exactly what `ntk consume -o raw|jsonl|unix:<path>` writes. `-f <path>` only changes where the input comes from (a file instead of stdin), never its format.

| `--in` value | Reads | Framing | Input-specific flags |
|---|---|---|---|
| `raw` (default) | stdin or `-f` | values separated by the delimiter; with `-m`: `meta line \n` value, delimiter | `--delimiter`, `--key-sep`, `-m` |
| `jsonl` | stdin or `-f` | one ntk record per line | - |
| `unix:<path>` | a datagram socket ntk binds | one datagram per message; with `-m`: `meta line \n` value | `-m`, `--idle-timeout` |

Combinations that don't make sense are usage errors (exit code 2), e.g. `-f` with `--in unix:`, `--delimiter` with `--in jsonl`, or `--key-sep` with `-m`.

### Raw values (`--in raw`)

- Input is split on the delimiter (default `\n`), and the delimiter itself isn't sent. `--delimiter` takes the same ASCII syntax as [consume](consuming.md#raw-stdout--o-raw---delimiter), so `ntk c a --delimiter '\0' | ntk p b --delimiter '\0'` works as a pair.
- `--key-sep ':'` splits each message into `key:value`.
- Static `-H` headers are added to every message.

### Metadata line (`-m`)

With `-m`, every message starts with the single-line JSON [metadata line](consuming.md#metadata-line--m--meta) that `ntk consume -m` writes. ntk takes the key (`key`/`key_b64`), the headers, and tombstones (`value_null`) from it, and reads exactly `value_size` bytes of value. So it's the inverse of `consume -m`, with or without a socket:

```sh
ntk -p prod c orders --from -1h -m | ntk -p staging p orders -m
ntk c orders --from earliest -m --delimiter '\0' | ntk p orders.copy -m --delimiter '\0'
```

- On stdin each message is `meta line \n`, then `value_size` bytes, then the delimiter. Values may contain the delimiter or newlines; the size decides where the value ends. A different delimiter after the value, or input that ends inside a message, stops ntk with an error naming the message number.
- As with jsonl, `topic`, `partition`, `offset`, and `timestamp` are **ignored** unless you pass `--keep-topic`, `--keep-partition`, or `--keep-timestamp`.
- `-H` headers are added after the ones from the metadata line.

### Record file / replay (`--in jsonl`)

`--in jsonl` reads the **ntk record format** that [consume](consuming.md#jsonl-export--o-jsonl) writes with `-o jsonl`, so exporting and replaying works as a round trip:

```
ntk -p prod consume orders --from -1h --until -30m -o jsonl > window.jsonl
ntk -p staging produce orders --in jsonl -f window.jsonl
```

- By default `topic`, `partition`, `offset`, and `timestamp` from the file are **ignored**. The target topic comes from the command line and partitioning is done by key.
- `--keep-partition` and `--keep-timestamp` keep those fields. `--keep-topic` routes each record to its original topic name. These three flags need metadata input (`--in jsonl` or `-m`).
- `key_b64`, `value_b64`, and header `value_b64` fields are turned back into the original bytes, and a header without `value`/`value_b64` is a null header value, so the round trip is lossless for binary data too.
- You can select records with any line tool before replaying, e.g. `grep FAILED window.jsonl | ntk p orders --in jsonl`.

### Unix datagram socket (`--in unix:<path>`)

ntk **binds** a `SOCK_DGRAM` socket at `<path>` and produces one message per datagram it receives. This is the listener side of [`consume -o unix:`](consuming.md#unix-datagram-socket--o-unixpath), so external programs can push messages into Kafka, and two ntk processes can be connected:

```sh
ntk -p staging p orders --in unix:/tmp/orders.sock -m &
ntk -p prod c orders -f -m -o unix:/tmp/orders.sock --unix-wait
```

- **Framing:** without `-m` the whole datagram is the value (an empty datagram is an empty value), with no key; `-H` adds static headers. With `-m` the datagram is the metadata line, `\n`, then the value, exactly as `consume -m -o unix:` sends it. If `value_size` doesn't match the bytes that arrived, ntk stops with an error, which catches senders that truncate (for example socat with a `-b` smaller than the message).
- **The socket file:** if `<path>` holds a stale socket (nothing bound to it), ntk replaces it. If another process is listening there, or the path is a regular file, ntk refuses to start. The file is removed when ntk exits. Start order doesn't matter, since `consume --unix-wait` waits for the socket to appear.
- **Stopping:** ntk listens until it gets `SIGINT` (ctrl-c) or `SIGTERM`. `-n N` stops after N messages, and `--idle-timeout <dur>` stops after that long without a datagram. Neither is required.
- **Graceful shutdown:** on the first signal ntk stops listening, produces the datagrams already queued in the socket, waits for every in-flight message to be acknowledged, commits the transaction if `--transactional-id` is set, removes the socket, prints the summary, and exits 0 (non-zero if any message failed). A second signal quits immediately.
- **Backpressure:** while Kafka is slower than the sender, ntk stops reading, the socket's queue fills, and the sender's `send` blocks. Nothing is dropped.
- **Size:** datagrams up to 16 MiB are accepted. The practical limit is usually the sender's (see the [consume size limit](consuming.md#unix-datagram-socket--o-unixpath)).
- **Delivery:** a sender counts a datagram as delivered once the kernel accepts it. If ntk then fails to produce it, or is killed by a second signal, the sender doesn't find out (and a `consume -g` sender has already committed it). For a lossless copy between clusters, use `ntk p <topic> --from-topic <topic> --from-profile <profile>` instead.
- **Platforms:** Linux, macOS, and BSDs. Not available on Windows (exit code 8).

Any program can send to it. For example, with socat:

```sh
ntk p events --in unix:/tmp/events.sock -H source=socat &
printf 'hello' | socat -u STDIN UNIX-SENDTO:/tmp/events.sock
```

### Stopping bulk input

Stdin and `-f` input stop at end of input, the socket at a signal. `-n N` stops any bulk input after N messages. Ctrl-C on stdin input is a graceful stop like on the socket: what was read is produced and a transaction is committed. On `-f` and `--from-topic` input, ctrl-c aborts (and aborts the transaction).

### Keys and values

- `-k`/`-v` send the argument's bytes as-is. `@path` reads the bytes from a file, and `@-` reads all of stdin as a single message.
- There's no encoding, validation, or schema support. To send JSON, Avro, Protobuf, etc., produce the bytes with another tool and pipe them in.

### Delivery options

| Flag | Default | Description |
|---|---|---|
| `-P, --partition N` | by key (murmur2) / sticky | Force a partition |
| `--partitioner murmur2\|round-robin\|sticky` | murmur2 | Match the Java client by default |
| `--acks all\|1\|0` | all | |
| `--compression none\|gzip\|snappy\|lz4\|zstd` | none | |
| `--idempotent` | true | |
| `--transactional-id id` | - | Produce all input in one transaction, committed at the end and aborted on error |
| `--rate N/s` | unlimited | Throttle (useful for load tests and replays) |
| `--count N` / `--repeat` | - | Repeat the single flag-message N times; `{{.i}}`, `{{uuid}}`, `{{now}}` template functions available in `-k`/`-v` with `--template` |
| `--tombstone` | - | Produce a null value (for compacted topics); requires `-k` |

### Output

For each message ntk prints the partition and offset on stderr (`→ orders/3 @1,044,300`), or a summary for bulk input:

```
Produced 1,204 messages to orders in 2.1s (573 msg/s, 1.1 MiB) · 0 failed
```

`-o json` prints one delivery report per message to stdout (`topic`, `partition`, `offset`, `timestamp`, `error`).

## TUI

Composer, opened with `p` on a topic or `:produce <topic>`:

```
┌ produce → orders ─ [prod] ────────────────────────────────────────────────┐
│ Key      order-981                                                        │
│ Headers  tenant = acme                                  [+ add]           │
│ Partition auto ▾                                                          │
│ Value ────────────────────────────────────────────────────────── 58 B ───│
│ {"id": "order-981", "status": "RETRY"}                                    │
│                                                                           │
├───────────────────────────────────────────────────────────────────────────┤
│ ctrl-s send  ctrl-e edit in $EDITOR  ctrl-o value from file  ctrl-h history │
└───────────────────────────────────────────────────────────────────────────┘
```

- The value is sent exactly as typed, with no validation or reformatting.
- `ctrl-o` loads the value from a file (binary values are shown as hex).
- `ctrl-h` gives a history of recently sent messages per topic (stored locally in `~/.local/state/ntk/`, never in the profile file).
- From the message browser, `P` opens the composer pre-filled from a consumed message.

## Kafka APIs

- `Produce` via `kgo` (idempotent by default: `InitProducerId`)
- `InitProducerId`, `AddPartitionsToTxn`, `EndTxn` for `--transactional-id`

## Safety

- Produce is a **Safe write**, so it's refused on `read_only` profiles.
- Bulk input (stdin, `-f`, `--in unix:`, `--from-topic`) on `prod`-labelled profiles asks for confirmation first, showing where the messages come from and the target topic.
- `--tombstone` is shown as a delete in the confirmation on compacted topics.

## Open questions

- Should we add a built-in load-test mode (`ntk produce --bench`) with latency percentiles, or leave that to dedicated tools?
