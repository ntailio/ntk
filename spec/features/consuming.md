# Consuming

## Purpose

Reading messages to debug, inspect, or export them, and feeding them into other programs. kcat is the usual tool, but it has cryptic flags (`-C -o -10 -e -f '%k %s\n'`), and splitting its output is unreliable when messages contain newlines. `kafka-console-consumer.sh` is slow and joins a consumer group by default.

ntk's consume is **safe by default**: it uses no consumer group and never commits offsets, reads from the tail, and reads all partitions. It's also **Unix-friendly**. ntk never decodes or transforms message values. It writes the raw bytes and gives you three reliable ways to hand each message to another program: a custom delimiter, a Unix datagram socket, or one subprocess per message.

## CLI

```
ntk consume <topic>[,topic2] [flags]
ntk c <topic> [flags]                  # short alias
```

### Partitions

ntk consumes **all partitions** of every given topic by default. These flags narrow it down:

| Flag | Meaning |
|---|---|
| `-P, --partition 0,3-5` | Only these partitions. Allowed only with a single topic. |
| `--key-partition <key>` | Only the partition that `<key>` hashes to. The key bytes are hashed with murmur2, like the Java default partitioner. |

Messages from different partitions are interleaved in the order they're fetched. Ordering is only guaranteed within a partition, just like in Kafka itself.

### Where to start and stop

| Flag | Meaning |
|---|---|
| `--from latest` (default: profile `defaults.consume.start`) | Only new messages |
| `--from earliest` | From the start of each partition |
| `--from -N` | Last N messages **per partition** |
| `--from @offset` | Exact offset (use with `-P`) |
| `--from 2026-09-30T08:00:00Z` / `--from -1h` | Offset for timestamp (ListOffsets per partition) |
| `--until <same forms>` | Stop at offset/time (exclusive) |
| `-n, --limit N` | Stop after N messages in total |
| `-f, --follow` | Keep waiting for new messages. This is the default when `--from latest`. Otherwise ntk stops at the high watermark captured at start. |

### Output

The output target is chosen with `-o, --output` (`--out` also works). Since there's one flag, each message always goes to exactly **one** target:

| `-o` value | Target | Framing | Target-specific flags |
|---|---|---|---|
| `raw` (default) | stdout | `[meta line \n]` value, delimiter | `--delimiter` |
| `jsonl` | stdout | one JSON object per line, value embedded | - |
| `unix:<path>` | Unix datagram socket | one datagram per message: `[meta line \n]` value | `--unix-wait`, `--receipt` |
| `exec:<command>` | Subprocess per message | the command's stdin gets `[meta line \n]` value, then EOF | `--exec-timeout`, `--exec-parallel`, `--exec-continue`, `--receipt` |

- Everything after the first `:` is used as-is, so `-o 'exec:jq -c .'` runs `jq -c .`, and `-o unix:/run/a:b.sock` connects to `/run/a:b.sock`. Quote the value when the command contains spaces.
- Using a target-specific flag with a different target is a usage error (exit code 2), e.g. `--delimiter` with `-o jsonl`.
- consume doesn't accept the formats other commands use (`table`, `json`, `name`, …). It ignores `NTK_OUTPUT` and the profile's `defaults.output`, so the default is always `raw`.

**Values are raw bytes.** ntk writes them exactly as they're stored in Kafka. It doesn't decode, pretty-print, escape, or add a trailing newline. A null value (tombstone) is written as zero bytes, and the metadata line shows `"value_null": true`. The one exception is writing to a terminal: if stdout is a TTY, control bytes other than `\n` and `\t` are shown as `\xHH` so binary data can't garble the terminal. `--no-escape` turns this off.

#### Metadata line (`-m, --meta`)

It's off by default. With `-m`, a **single-line JSON** object describing the message comes before each value, followed by `\n`:

```json
{"topic":"orders","partition":3,"offset":1044211,"timestamp":"2026-09-30T08:14:02.118Z","timestamp_type":"create","key":"order-981","headers":[{"key":"tenant","value":"acme"}],"value_size":58,"value_null":false}
```

| Field | Notes |
|---|---|
| `topic`, `partition`, `offset` | |
| `timestamp`, `timestamp_type` | RFC 3339 UTC with ms precision; `create` or `log_append` |
| `key` / `key_b64` | `key` holds the key as a string if it's valid UTF-8, otherwise `key_b64` holds it in base64. A null key is `"key": null`. |
| `headers` | Array (keeps order and duplicate keys). Each header value uses the same `value` / `value_b64` rule; a null header value has neither field (`{"key":"trace"}`). |
| `value_size`, `value_null` | Size in bytes of the value that follows, and whether it's a tombstone |

JSON never contains a raw newline, so readers can always take the first line as metadata. The field names are part of the stable output interface ([CLI conventions](../cli-conventions.md#output)).

#### Raw stdout (`-o raw`, `--delimiter`)

```
[metadata line]\n        ← only with -m
<value bytes>
<delimiter>              ← default "\n"
```

- `--delimiter` takes an ASCII string, for example `'\0'`, `'\n---\n'`, or `'\x1e'` (record separator). The escapes `\n`, `\r`, `\t`, `\0`, `\\`, and `\xHH` (HH < 0x80) are supported. Non-ASCII input is rejected.
- It only applies to `-o raw`.
- If values can contain the delimiter, the stream can't be split reliably. Choose a delimiter that can't occur (e.g. `'\0'` for text), or use `-o exec:…`, `-o unix:…`, or `-o jsonl`, which never have this problem.

#### jsonl export (`-o jsonl`)

This is the lossless **ntk record format**. It has the metadata fields plus the value as `value` (valid UTF-8) or `value_b64` (anything else), all on one line per message. [Producing](producing.md#record-file--replay---in-jsonl) reads this format back with `--in jsonl` for replay and copying. `-m` is implied.

```json
{"topic":"orders","partition":3,"offset":1044211,"timestamp":"2026-09-30T08:14:02.118Z","timestamp_type":"create","key":"order-981","headers":[{"key":"tenant","value":"acme"}],"value":"{\"id\":\"order-981\",\"status\":\"FAILED\"}"}
```

The value is embedded as a JSON string. That's only a way of representing the bytes, not decoding them, so a JSON value appears escaped.

#### Unix datagram socket (`-o unix:<path>`)

Each message is sent as **one `SOCK_DGRAM` datagram** to the socket at `<path>`. The datagram contains the value, preceded by the metadata line and `\n` when `-m` is set. Because every datagram is exactly one message, no delimiter is needed.

- **The listener owns the socket.** The receiving program (the listener) creates the socket by binding the path. ntk only connects and sends. It never creates, removes, or "recreates" the socket file. The listener can be another ntk: [`ntk produce --in unix:<path>`](producing.md#unix-datagram-socket---in-unixpath) binds the socket and produces each datagram, so `-m` here and `-m` there copy keys, headers, and values exactly.
- **Missing socket:** if nothing is bound at `<path>`, ntk fails at startup. `--unix-wait` makes ntk wait instead, retrying until the socket appears.
- **Receiver goes away:** if a send fails with `ECONNREFUSED` (the receiver exited), ntk stops with an error and reports the last message it delivered.
- **Backpressure:** a connected Unix datagram send **blocks** while the receiver's queue is full, and ntk stops fetching until there's room. No messages are dropped.
- **Size limit:** if a message doesn't fit in one datagram (`EMSGSIZE`), ntk **stops with an error**. It never truncates or splits.
  ```
  error: orders/3 @1044211 is 312 KiB, larger than the unix datagram limit (212,992 bytes).
         Raise net.core.wmem_max (Linux) / net.local.dgram.maxdgram (macOS), or use -o exec:<command>.
  ```
  ntk sets `SO_SNDBUF` as high as the OS allows. On Linux the effective limit is about `net.core.wmem_max` (often around 208 KiB by default). On macOS the default `net.local.dgram.maxdgram` is only **2 KiB**. The receiver also needs a large enough receive buffer.
- **Platforms:** Linux, macOS, and BSDs. It's not available on Windows, which only supports `SOCK_STREAM` Unix sockets.

A listener with [socat](http://www.dest-unreach.org/socat/) that runs `./handle.sh` once per datagram, with the datagram on its stdin:

```sh
socat -u -b 1048576 UNIX-RECVFROM:/tmp/ntk.sock,unlink-early,rcvbuf=4194304,fork EXEC:./handle.sh &
ntk c orders -f -m -o unix:/tmp/ntk.sock --unix-wait
```

- `UNIX-RECVFROM` + `fork` starts one handler per datagram, so each handler sees exactly one message.
- `unlink-early` removes a stale socket file from a previous run before binding.
- **`-b` is required for messages over 8 KiB.** socat's default buffer is 8192 bytes, and it **silently truncates** larger datagrams. Set `-b` at least as large as your biggest message, and `rcvbuf` large enough to queue a few.
- To just watch the stream, use `socat -u -b 1048576 UNIX-RECV:/tmp/ntk.sock,unlink-early STDOUT`. Message boundaries are lost in that output.

#### Subprocess per message (`-o exec:<command>`)

ntk runs `<command>` once **per message** via `/bin/sh -c '<command>'`. It writes the message to the process's stdin (the metadata line and `\n` first if `-m` is set, then the value bytes), closes stdin, and waits for the process to exit before starting the next one. By default only one command runs at a time and each gets 30s (see `--exec-parallel` and `--exec-timeout` below). So message order is kept, and each program sees exactly one complete message regardless of its content.

- **No injection:** message data is only ever sent to stdin and environment variables. It's never inserted into the command string.
- **Environment variables** set for every message, with or without `-m`:

  | Variable | Value |
  |---|---|
  | `NTK_META` | The metadata line (JSON) |
  | `NTK_TOPIC`, `NTK_PARTITION`, `NTK_OFFSET` | |
  | `NTK_TIMESTAMP` | Epoch milliseconds |
  | `NTK_KEY` | The key, only if it's valid UTF-8 with no NUL bytes; unset otherwise |
  | `NTK_KEY_B64` | The key in base64; unset for null keys |

- **stdout/stderr** of the command go straight to ntk's stdout/stderr, and ntk adds no delimiters. With [`--receipt`](#receipt---receipt), the command's stdout goes to ntk's **stderr** instead, so ntk's stdout carries only receipt lines.
- **Failures:** a non-zero exit **stops** ntk with exit code 9 and reports the message's topic, partition, and offset. `--exec-continue` logs the failure to stderr and moves on.
- **`--exec-timeout <dur>`** (default **30s**) is how long one command may run. When it's exceeded, ntk sends `SIGTERM` to the command's process group (which includes anything `sh -c` started), then `SIGKILL` after 5s. The timeout counts as a failure. `--exec-timeout 0` turns it off.
- **`--exec-parallel N`** (default **1**: one command at a time, in order) runs up to N commands at once. With N > 1, **ordering is not guaranteed**, even within a partition. On failure ntk stops launching new commands and waits for the ones that are running.
- **Cost:** starting a process takes about 1–3 ms per message. That's fine for inspection and scripting. For bulk export, use `-o raw` or `-o jsonl`.

#### Receipt (`--receipt`)

With `-o unix:` and `-o exec:`, messages don't go to stdout. `--receipt` puts stdout to use: ntk writes **one single-line JSON object per message** to stdout after each delivery attempt. You can pipe it into anything, e.g. `wc -l` to count, `tail -1` to see the last position, or a monitoring script.

```json
{"topic":"orders","partition":3,"offset":1044211,"value_size":58,"status":"ok","duration_ms":4}
{"topic":"orders","partition":7,"offset":998120,"value_size":61,"status":"failed","exit_code":1,"duration_ms":12}
```

| Field | Notes |
|---|---|
| `topic`, `partition`, `offset`, `value_size` | Identify the message (no key, headers, or value) |
| `status` | `ok`, `failed` (non-zero exit, or a send error that `--exec-continue` skipped), or `timeout` |
| `exit_code` | `-o exec:` only |
| `duration_ms` | Time to deliver: the command's run time, or the `send` call for `unix:` |

- There's exactly one line per message, in delivery order, written only once the outcome is known. With `--exec-parallel N` > 1, lines come in completion order.
- A failure that stops ntk still gets its receipt line before ntk exits.
- It's only valid with `-o unix:` and `-o exec:`. `-o raw` and `-o jsonl` already write to stdout, so `--receipt` with them is a usage error.


### Consumer group mode (opt-in)

`-g, --group <name>` consumes as a member of a group and commits offsets, like a real consumer. It prints a notice on stderr that offsets will be committed, and `--no-commit` joins the group without committing. With an output target, the offset is only committed after the message was **delivered**: written to stdout, sent to the socket, or processed by a command that exited 0.

`--isolation read_committed|read_uncommitted` (default `read_uncommitted`) is shown on stderr at startup, so aborted transactional records don't surprise you.

### Progress

When stderr is a TTY, a status line shows the messages consumed, partitions, and current position, with a summary at exit (`Consumed 1,204 messages from 12 partitions`). The live position is only redrawn when message output (or command output, or receipts) isn't going to the same terminal, so the two never mix. `-q` turns it off. Only message output (or receipt lines) is ever written to stdout.

### Examples

```
ntk c orders --from -5                                  # last 5 values per partition
ntk c orders --from -5 -m                               # ...with the metadata line
ntk c orders -P 3 --from @1044211 -n 1 -m               # one exact message
ntk c orders --from -1h -o 'exec:jq -c "select(.status == \"FAILED\")"'
ntk c orders --from -1h -o 'exec:if grep -q FAILED; then echo "$NTK_PARTITION@$NTK_OFFSET"; fi'
ntk c orders --from earliest --delimiter '\0' | xargs -0 -n1 ./handle.sh
ntk c orders -f -m -o unix:/run/myapp/ingest.sock --unix-wait
ntk c orders --from earliest --until -1d -o 'exec:./handle.sh' --receipt | wc -l   # count processed
ntk c orders --from earliest --until -1d -o jsonl > old.jsonl   # export for replay (ntk p … --in jsonl -f old.jsonl)
ntk c orders -f -m -o unix:/tmp/o.sock --unix-wait   # into `ntk -p other p orders --in unix:/tmp/o.sock -m`
```

ntk has no filter flags. Filtering is done by the program you send messages to (`-o exec:…`, a pipe, or the socket receiver).

## TUI

Message browser, opened with `c` on a topic/partition or `:consume <topic>`:

```
┌ consume orders ─ all partitions ─ from -1h ─ ● live ─ 18,203 messages ──────────────────┐
│ PART  OFFSET      TIME       KEY          SIZE    VALUE (preview)                        │
│    3  1,044,211   10:14:02   order-981    58 B    {"id":"order-981","status":"FAILED",…  │
│    7    998,120   10:21:47   order-1002   61 B    {"id":"order-1002","status":"OK",…     │
├──────────────────────────────────────────────────────────────────────────────────────────┤
│ Value (text, 58 B)   Headers   Hex   Metadata                                            │
│ {"id":"order-981","status":"FAILED","amount":42.10}                                     │
```

The value is shown as UTF-8 text, or as a hex dump if it isn't valid UTF-8. It's never decoded or reformatted.

| Key | Action |
|---|---|
| `space` | Pause / resume live tail |
| `F` | Edit start position / partitions (form), then restart |
| `/` | Find: jump to the next loaded message containing the text (nothing is hidden) |
| `n` | Jump to the next match |
| `enter` / `tab` | Cycle the detail pane tab (Value, Headers, Hex, Metadata) |
| `x` | Toggle text / hex for the value |
| `y` | Yank the message as an ntk record (the jsonl format) |
| `s` | Save loaded messages to a file (jsonl) |
| `P` | Produce a copy (opens the [producer](producing.md) pre-filled; can target another topic) |
| `[` / `]` | Jump to previous/next partition |

The browser keeps a ring buffer (default 10,000 messages, configurable) so memory use stays flat on long tails.

## Kafka APIs

- `Metadata`, `ListOffsets` (start/end/time resolution)
- `Fetch` via `kgo` direct partition consumption (`kgo.ConsumePartitions`) when there's no group, or `kgo.ConsumerGroup` with `-g`
- `OffsetCommit` only with `-g` and without `--no-commit`

## Safety

- Read-only. It never commits offsets unless `-g` is given. It's allowed on `read_only` profiles, except `-g` with commits, which counts as a write and is refused.
- `--from earliest` without `--limit`/`--until` on a topic larger than 1 GiB asks for confirmation on a TTY (`this will read 48 GiB`), and warns otherwise.
- `-o exec:` runs only the command you give. ntk never inserts message data into it (see above).

## Open questions

- Should the TUI offer a display-only JSON indent toggle, or keep values strictly as-is?
- Should `-o unix:` also support `SOCK_SEQPACKET` or stream sockets for messages larger than the datagram limit?
- `-o exec:` on Windows: use `cmd /C`, or make it Unix-only like `-o unix:`?
- Should `-o exec:` have a batch mode (N messages per process, separated by a delimiter) for throughput?
