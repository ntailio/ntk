# Getting started

## Install

ntk is a single static binary with no librdkafka or JVM. Pick one way:

**Download a binary** from the [releases page](https://github.com/ntailio/ntk/releases) for Linux, macOS, Windows or FreeBSD, then put it on your `PATH`:

```sh
sha256sum -c --ignore-missing ntk-*-checksums.txt     # optional: check the download
chmod +x ntk-*-linux-amd64 && sudo mv ntk-*-linux-amd64 /usr/local/bin/ntk
```

**Run it with Docker.** Mount your profiles, and use the host network to reach a cluster on `localhost`:

```sh
docker run --rm -it -v ~/.config/ntk:/home/ntk/.config/ntk ghcr.io/ntailio/ntk topic list
docker run --rm -it --network host -v ~/.config/ntk:/home/ntk/.config/ntk ghcr.io/ntailio/ntk
```

`:latest` is the newest stable release; every version also has its own tag, like `ghcr.io/ntailio/ntk:v1.0.0`.

**Build it with Go** 1.26 or newer:

```sh
go install github.com/ntailio/ntk/cmd/ntk@latest
```

From a checkout, `make build` writes `bin/ntk`.

## Connect to a cluster

ntk keeps connection details in **profiles**, so you never type bootstrap servers twice.

```sh
ntk profile create
```

The wizard asks for a name, bootstrap servers, the security setup (plaintext, TLS, mTLS, SASL PLAIN or SCRAM), tests the connection, and saves. Profiles live in `~/.config/ntk/profiles.json`, created with mode `0600` because it holds secrets.

Have a Java `client.properties` already? Import it:

```sh
ntk profile create prod --from-properties client.properties
```

More in [Profiles](profiles.md).

## First commands

```sh
ntk topic list                          # topics with partitions, size, retention
ntk topic describe orders               # partitions, leaders, ISR, offsets, consumers
ntk consume orders --from -5            # the last 5 messages per partition
ntk produce orders -k k1 -v '{"a":1}'   # one message
ntk group list --lag                    # consumer groups and their lag
ntk health                              # is the cluster OK?
```

Or open the TUI and look around:

```sh
ntk
```

Press `?` for keys, `:` to jump between views, `q` to quit. See [The TUI](tui.md).

## Shell completion

Completion covers commands, flags, and **live resources**: topic names, groups, brokers, principals, config keys. Results are cached briefly per profile so `<tab>` stays quick.

```sh
# bash
ntk completion bash > ~/.local/share/bash-completion/completions/ntk

# zsh (make sure $fpath has the directory and compinit runs)
ntk completion zsh > "${fpath[1]}/_ntk"

# fish
ntk completion fish > ~/.config/fish/completions/ntk.fish
```

Open a new shell, then try `ntk topic describe <tab>`.

## Output formats

Every listing command takes `-o`:

| `-o` | Use for |
|---|---|
| `table` (default) | reading |
| `wide` | every column |
| `json`, `jsonl` | scripts and `jq` |
| `name` | one name per line, for `xargs` |
| `template='{{.name}}'` | custom text |

Data goes to stdout and everything else (progress, warnings, prompts) to stderr, so piping is always safe. `consume` is the exception: its `-o` picks an output target instead, see [Consuming](consuming.md).

## Exit codes

Scripts can branch on them: `0` ok, `1` error, `2` usage, `3` can't connect, `4` not found, `5` a check failed (lag threshold, health), `6` refused (read-only profile or declined confirmation), `7` not authorized.
