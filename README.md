<h1>
<p align="center">
  <br>ntk
</h1>
  <p align="center">
    <a href="https://github.com/factualtech/ntk/actions/workflows/ci.yml"><img src="https://github.com/factualtech/ntk/actions/workflows/ci.yml/badge.svg?branch=trunk" alt="ci"></a>
    <br />
    A fast, friendly CLI and TUI for Apache Kafka.
    <br />
    Inspect clusters, move messages, watch lag, and fix things, from one binary.
    <br />
    <a href="#about">About</a>
    ·
    <a href="#install">Install</a>
    ·
    <a href="#documentation">Documentation</a>
    ·
    <a href="#contributing">Contributing</a>
    ·
    <a href="#status-and-roadmap">Status</a>
  </p>
</p>

![ntk: list topics, produce, consume, then browse the cluster in the TUI](assets/demos/demo.gif)

```sh
ntk topic list                       # what's here?
ntk consume orders --from -10        # the last 10 messages per partition
ntk produce orders -k k1 -v hello    # send one
ntk group watch billing              # live lag, rates, time to catch up
ntk                                  # or open the TUI
```

## About

ntk is the tool you reach for when you need to look at a Kafka cluster, right now, from a terminal. It replaces the stack of `kafka-*.sh` scripts and a kcat with one binary that knows your clusters, shows you what will change before it changes it, and treats messages as the bytes they are.

A few ideas run through the whole tool:

- **Profiles, not flags.** Connection details live in one file, with PLAINTEXT, TLS, mTLS, SASL PLAIN and SCRAM. `ntk -p prod` is the whole incantation, and a read-only or `prod`-labelled profile makes mistakes hard.
- **Bytes in, bytes out.** ntk never decodes, re-encodes or pretty-prints a message. What it gives you is what's in the log. For JSON, Avro or anything else, pipe it to the tool that understands it, or run one per message with `-o 'exec:jq .'`.
- **A plan before every change.** Creating, deleting, resetting, granting: you see the exact effect first. `--dry-run` works on every mutating command, and a saved plan can be applied later.
- **Watch without agents.** Throughput, lag, time lag and stuck consumers are computed from offsets, so monitoring works on any cluster, with nothing installed on the brokers.
- **The TUI is the CLI.** Every view in the terminal UI runs the same code as the command line, and will tell you the command it ran.

ntk is a single static Go binary with no JVM and no librdkafka. It sends no telemetry.

## Install

```sh
go install github.com/factualtech/ntk/cmd/ntk@latest
```

Or from a checkout, `make build` writes `bin/ntk`. Go 1.26 or newer is required. Linux and macOS are fully supported; Windows works for everything except the Unix socket outputs.

Then connect to a cluster and have a look around:

```sh
ntk profile create          # a short wizard: servers, security, test, save
ntk topic list
ntk                         # the TUI
```

Shell completion knows your topics, groups and principals:

```sh
ntk completion bash > ~/.local/share/bash-completion/completions/ntk    # zsh, fish, powershell too
```

Want somewhere safe to try it? The repo includes a 3-broker Kafka sandbox with plaintext, mTLS and SCRAM listeners: `docker compose up -d --wait`, then `export NTK_PROFILE_PATH=$PWD/sandbox/profiles.json`.

## Documentation

The [guide](docs/README.md) is a set of short chapters:

[Getting started](docs/getting-started.md) ·
[Profiles](docs/profiles.md) ·
[Topics](docs/topics.md) ·
[Consuming](docs/consuming.md) ·
[Producing](docs/producing.md) ·
[Consumer groups](docs/groups.md) ·
[Mirroring](docs/mirroring.md) ·
[Monitoring](docs/monitoring.md) ·
[Access control](docs/access.md) ·
[The TUI](docs/tui.md)

Every command has `--help`, and `ntk help <command>` works too. The exact behaviour of every flag and output field is written down in the [spec](spec/README.md), which is what ntk is built from.

## Contributing

Bug reports and feature ideas are very welcome as GitHub issues. ntk doesn't accept external pull requests at the moment; [CONTRIBUTING.md](CONTRIBUTING.md) explains why, and how to write an issue we can act on quickly.

## Status and Roadmap

ntk is young and under active development. It's in use against real clusters, and the command surface described in the spec is implemented and tested against the sandbox on every change. Expect occasional breaking changes to flags until a 1.0.

| Area | Status |
|---|---|
| Topics, partitions, configuration, reassignment | ✅ |
| Consuming and producing, replay, copy, Unix socket mirroring | ✅ |
| Consumer groups: lag, members, offset resets, KIP-848 and share groups | ✅ |
| Monitoring: watch, top, lag checks, cluster health | ✅ |
| ACLs with recipes, SCRAM users, quotas, principal view | ✅ |
| Transactions: hanging detection and abort | ✅ |
| Terminal UI | ✅ |
| Auth: PLAINTEXT, TLS, mTLS, SASL PLAIN, SCRAM-SHA-256/512 | ✅ |
| Auth: OAUTHBEARER, AWS MSK IAM, Kerberos | ❌ |

Message decoding and Schema Registry support are intentionally not on the roadmap. The full list of deferred features is in the [roadmap](spec/roadmap.md).

### Mirroring with a socket

![mirroring a topic over a Unix socket between two profiles](assets/demos/mirror.gif)

One side listens on a Unix datagram socket, the other sends one datagram per message:

```sh
ntk -p staging produce orders --in unix:/tmp/orders.sock -m
ntk -p prod consume orders --from -1h -m -o unix:/tmp/orders.sock --unix-wait
```

That's a topic mirror between two clusters with keys, headers and tombstones intact, and it stops cleanly on ctrl-c. The same socket works for any program that isn't a Kafka client. For continuous production replication you still want MirrorMaker; the [mirroring chapter](docs/mirroring.md) explains when to use which.

### A command per message

![consume into a command per message](assets/demos/exec.gif)

```sh
ntk consume orders --from -1h -o 'exec:jq -c "select(.status == \"FAILED\")"'
```

`-o exec:<command>` runs the command once per message, with the bytes on stdin and the key, offset and partition in `NTK_*` environment variables. It's the escape hatch for decoding, filtering and anything else ntk deliberately doesn't do itself.

### Monitoring and health

![topic watch with live traffic, then a health check](assets/demos/monitor.gif)

`topic watch` and `group watch` give you live per-partition rates, lag trends and an ETA to catch up. `ntk health` runs a dozen checks in one go and exits non-zero when something's wrong, which makes it a one-line readiness probe.

### Safety

![profiles: a staged connection test and a read-only profile refusing a delete](assets/demos/profiles.gif)

Profiles can be read-only, coloured, and labelled `prod`. Destructive commands show a plan and, on prod, require you to type the resource name even with `-y`. `ntk profile test` checks DNS, TCP, TLS, SASL and metadata one step at a time and tells you which one broke.

## License

ntk is released under the [Apache License 2.0](LICENSE). Copyright Factual Tech AB.
