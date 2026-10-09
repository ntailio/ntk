# Consuming

`ntk consume` (or `ntk c`) reads messages and writes them out **exactly as stored**: no decoding, no pretty-printing, no consumer group unless you ask for one.

```sh
ntk c orders                     # follow new messages on all partitions
ntk c orders --from -10          # the last 10 per partition, then stop
ntk c orders --from earliest -n 100
ntk c orders --from -1h --until -30m
ntk c orders -P 3 --from @1044211 -n 1
ntk c orders --key-partition order-981   # only the partition that key hashes to
```

`--from` and `--until` take `earliest`, `latest`, `-N` (messages per partition), `@offset`, a time (`2026-09-30T08:00:00Z`), or a relative time (`-1h`).

## Show the metadata too

By default you get values only. `-m` prints a one-line JSON header before each message with the topic, partition, offset, timestamp, key, and headers:

```
{"topic":"orders","partition":3,"offset":1044211,"timestamp":"2026-09-30T08:14:02.118Z","timestamp_type":"create","key":"order-981","headers":[{"key":"tenant","value":"acme"}],"value_size":58,"value_null":false}
{"id":"order-981","status":"FAILED"}
```

## Output targets

`-o` chooses where messages go:

| `-o` | What happens |
|---|---|
| `raw` (default) | values to stdout, one per line (change the separator with `--delimiter '\0'`) |
| `jsonl` | one JSON object per message with everything in it, for export and replay |
| `exec:<command>` | run a command **per message**, with the message on its stdin |
| `unix:<path>` | send each message as one datagram to a Unix socket |
| `npipe:<path>` | send each message as one pipe message to a Windows named pipe |

### Export and replay

```sh
ntk c orders --from -1d -o jsonl > yesterday.jsonl
ntk -p staging p orders --in jsonl -f yesterday.jsonl
```

The JSON Lines format is lossless: binary keys and values are base64-encoded, headers and tombstones survive. See [Producing](producing.md).

### A command per message

![consume into a command per message](../assets/demos/exec.gif)

```sh
ntk c orders --from -1h -o 'exec:jq -c "select(.status == \"FAILED\")"'
ntk c orders --from -1h -o 'exec:if grep -q FAILED; then echo "$NTK_KEY at $NTK_PARTITION@$NTK_OFFSET"; fi'
ntk c orders -o 'exec:./handle.sh' --receipt | wc -l
```

The message goes to the command's stdin, never into the command line, so content can't break your shell. `NTK_TOPIC`, `NTK_PARTITION`, `NTK_OFFSET`, `NTK_KEY` and `NTK_META` are set in the environment. Commands run one at a time and in order, with a 30 second timeout; `--exec-parallel N` runs several, `--exec-continue` skips failures, and `--receipt` prints a JSON line per message with the outcome.

### A Unix socket

```sh
ntk c orders -f -m -o unix:/run/myapp/ingest.sock --unix-wait
```

Your program binds the socket and gets one datagram per message. Nothing is split or truncated: a message that doesn't fit is an error. This is also how two ntk processes mirror a topic, see [Mirroring](mirroring.md).

### A named pipe (Windows)

```powershell
ntk c orders -f -m -o npipe:ingest --npipe-wait
```

Windows has no datagram sockets, so `npipe:` does the same job over a named pipe. Your program creates `\\.\pipe\ingest` in message mode (`PIPE_TYPE_MESSAGE`, or `PipeTransmissionMode.Message` in .NET) and reads one pipe message per Kafka message. A bare name is short for `\\.\pipe\<name>`; a full pipe path works too.

## With a consumer group

```sh
ntk c orders -g svc-reporting
```

ntk joins the group and commits offsets after each message is delivered, so you can use it as a quick consumer in a pipeline. `--no-commit` joins without committing. `--isolation read_committed` hides aborted transactions.

## In the TUI

Press `c` on a topic for a live message browser: pause with `space`, switch between value, headers, hex and metadata with `tab`, find text with `/`, save what's loaded with `s`.
