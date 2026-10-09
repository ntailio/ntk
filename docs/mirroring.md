# Mirroring

Copying a topic from one place to another: between clusters, environments, or just topics. ntk has three ways, from simplest to most flexible.

## 1. Topic to topic

```sh
ntk p orders.retry --from-topic orders.dlq --from earliest
ntk p orders --from-topic orders --from-profile prod --from -1h
```

One command, one process. ntk consumes from the source (a different profile if you say so) and produces to the target, keeping keys and headers. `--from`, `--until` and `-n` bound the copy. This is the right choice for a one-off copy or a backfill.

## 2. A pipe

```sh
ntk -p prod c orders --from -1h -m | ntk -p staging p orders -m
```

`-m` on both sides carries the metadata (key, headers, tombstones) through the pipe. Values can contain newlines; the metadata line says how many bytes follow.

## 3. A Unix socket

![mirroring over a Unix socket: produce listens, consume sends, ctrl-c stops gracefully](../assets/demos/mirror.gif)

```sh
# the receiving side binds the socket
ntk -p staging p orders --in unix:/tmp/orders.sock -m

# the sending side connects (start it in any order; --unix-wait waits for the socket)
ntk -p prod c orders -f -m -o unix:/tmp/orders.sock --unix-wait
```

Each message is one datagram, so there's nothing to split and nothing to escape. The listener keeps running until you stop it; `ctrl-c` produces what it already received, flushes, and exits cleanly. `--idle-timeout 30s` stops it after a quiet period instead.

The socket isn't only for ntk. Any program can send datagrams to it, and any program can bind a socket and receive from `ntk consume -o unix:/path/to.sock`. That makes it a simple way to plug Kafka into something that speaks neither Kafka nor HTTP:

```sh
ntk p events --in unix:/tmp/events.sock &
printf 'hello' | socat -u STDIN UNIX-SENDTO:/tmp/events.sock
```

### On Windows: a named pipe

Windows has no Unix datagram sockets, so use a named pipe instead. It works the same way, one pipe message per Kafka message:

```powershell
# the receiving side creates the pipe
ntk -p staging p orders --in npipe:orders -m

# the sending side connects (--npipe-wait waits for the pipe)
ntk -p prod c orders -f -m -o npipe:orders --npipe-wait
```

`npipe:orders` is short for `\\.\pipe\orders`. Any program can open the pipe and write to it; each write is one message. To receive from `ntk consume -o npipe:`, a program creates the pipe in message mode (`PIPE_TYPE_MESSAGE`).

## What about MirrorMaker?

[MirrorMaker 2](https://kafka.apache.org/documentation/#georeplication) is the right tool for **continuous, production replication**: it runs as a long-lived service, tracks offsets, syncs consumer group positions and topic configs, and survives restarts.

ntk's mirroring is for the other cases: backfilling a topic into staging, replaying a window of production traffic locally, moving data between clusters once during a migration, or feeding a program that isn't a Kafka client. It's a single process you start and stop by hand, and it's running in a minute.

A few things to know:

- **Delivery.** Topic-to-topic (`--from-topic`) is the safest: the consumer and producer are in one process, and nothing is counted as copied until the target acknowledges it. Over a pipe or socket, the sender hands off a message and doesn't hear back, so a crash on the receiving side can lose what was in flight.
- **Order** is kept within a partition. With `--keep-partition` the target partition matches the source, otherwise keys are re-hashed, which gives the same layout if both topics have the same partition count.
- **Timestamps** are new by default; `--keep-timestamp` keeps the originals.
- **Transactions.** Add `--transactional-id` to the producer side to make a bounded copy all-or-nothing.
