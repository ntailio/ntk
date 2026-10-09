# Architecture

## Libraries

| Concern | Library | Notes |
|---|---|---|
| Kafka client | [franz-go](https://github.com/twmb/franz-go) `kgo` | Pure Go, no librdkafka/cgo. Supports every protocol request, consumer groups (classic + KIP-848), transactions. |
| Kafka admin | franz-go `kadm` | High-level admin helpers. For requests `kadm` doesn't wrap, use raw `kmsg` requests via `kgo.Client.Request`. |
| CLI | [Cobra](https://github.com/spf13/cobra) | Subcommands, flag parsing, shell completion. |
| TUI | [Bubble Tea](https://github.com/charmbracelet/bubbletea), [Bubbles](https://github.com/charmbracelet/bubbles), [Lip Gloss](https://github.com/charmbracelet/lipgloss) | Elm-style models; Bubbles provides textinput and textarea. The table is ntk's own (`tui/table.go`). |
| Prompts/forms | [huh](https://github.com/charmbracelet/huh) | Used for the profile wizard and CLI prompts. The same forms can be embedded in the TUI. |

ntk deliberately has **no message serde**: no decoding, encoding, or schema support. Keys and values are always raw bytes. Integration with other tools happens through Unix pipes, datagram sockets, and subprocesses (see [consuming](features/consuming.md#output)).

Target: Go 1.26+, static binaries for linux/darwin/windows × amd64/arm64.

## Package layout

Packages live at the repository root, one directory per concern. There's no `internal/` and no nesting beyond `cmd/`.

```
cmd/ntk/main.go   entry point: signal handling, calls cli.Execute, exits with its code
cli/              Cobra commands, one file per resource (profile.go, topic.go, ...); table adapters, safety prompts
tui/              Bubble Tea app: shell, table, views, composer, forms
profile/          profiles.json: types, validation, path resolution, atomic 0600 saves, selection
prefs/            config.json: presets, TUI settings, completion, lag thresholds
kafka/            builds kgo/kadm clients from a profile: TLS/mTLS (tls.go, keys.go), SASL registry (auth.go)
plan/             the Plan type shown by --dry-run, confirmations, and the TUI confirm dialog
actions/          applier registry: plan kind → function that executes it (shared by CLI, TUI, `ntk apply`)
output/           -o renderers: table, wide, json, jsonl, name, template
units/            durations, sizes, rates, times: parsing and formatting
exitcode/         exit codes (spec/cli-conventions.md#exit-codes) and error → code mapping
buildinfo/        version and VCS info
topics/           topics and partitions
configs/          topic and broker configs
groups/           consumer groups and offsets
monitor/          topic and group watchers (rates, lag, status)
consume/          positions, partition selection, the consume loop
produce/          record sources, rate limiting, transactions
record/           the jsonl record shapes shared by consume and produce
sink/             consume output targets: raw, jsonl, unix, exec
acls/             ACLs, recipes, checks, export/import
principals/       SCRAM users and principal views
quotas/           client quotas
brokers/          brokers, quorum, log dirs, features, API versions
health/           cluster health checks
tx/               producers and transactions
testkit/          integration test helpers for the sandbox: random names, cleanup, skipping, janitor
```

## Layering rule

```
cmd/ntk ──> cli ──┐
            tui ──┼──> topics, groups, acls, ... ──> kafka (kgo/kadm)
                  │         (typed results / plans)
                  ├──> profile
                  └──> output (cli) / components (tui)
```

- **Resource packages** (`topics`, `groups`, …) take a context and a `*kadm.Client`/`*kgo.Client` and return **typed Go structs**. They never print, prompt, read flags, or know about tables. Table adapters live in `cli` (e.g. `topicRows` in `cli/topic.go`).
- `cli` owns flags, profile selection, output, and exit codes. Commands stay thin: parse args, call a resource package, render.
- Mutations follow a **plan/apply** pattern: `topics.PlanDelete(...)` returns a `Plan` describing exactly what will change. The CLI renders the plan for `--dry-run` and confirmation, and `topics.ApplyDelete(plan)` executes it. The TUI shows the same plan in a confirm dialog. This keeps previews and executions identical.
- `cli/safety.go` decides if a plan may run. It checks `read_only`, whether the plan is destructive, and whether confirmation is required (see [CLI conventions](cli-conventions.md#safety)). Each plan kind has an applier registered in `actions`, so the CLI, the TUI, and `ntk apply plan.json` execute plans the same way.
- Long-running streams (consume, monitor) return a channel/iterator of events. The CLI prints them and the TUI turns them into `tea.Msg`s.
- Errors carry exit codes via `exitcode.With(code, err)`. Well-known Kafka errors (authorization, unknown topic, …) are mapped automatically by `exitcode.Of`.

## Auth mechanism registry

Auth is pluggable so new mechanisms don't change the rest of the code or the profile format. `kafka/auth.go` maps each `auth.mechanism` value to a function that builds the franz-go SASL mechanism:

```go
var saslMechanisms = map[string]func(profile.Auth) (sasl.Mechanism, error){
    "plain":         ...,
    "scram-sha-256": ...,
    "scram-sha-512": ...,
}
```

`none` means no SASL. TLS is configured separately from the SASL mechanism, which allows SASL_SSL and mTLS-only setups. Future mechanisms (`oauthbearer`, `aws-msk-iam`, `gssapi`) are added as entries here, with their fields added to `profile.Auth` ([roadmap](roadmap.md)).

## Capability detection

There's no up-front capability probe. When a broker doesn't support a request (for example `DescribeQuorum` on ZooKeeper clusters or `ListTransactions` before Kafka 3.0), franz-go returns `UNSUPPORTED_VERSION` and `exitcode.Of` maps it to exit code 8. `ntk broker api-versions` shows exactly which requests and versions each broker supports.

## Client lifecycle

- CLI: one client per invocation, closed on exit.
- TUI: one client per active profile, kept for the session. Switching profiles (`:ctx`) closes it and opens a new one. Each view fetches what it shows when it loads and on every refresh tick.
- Consumers created for `consume` default to **no consumer group** (direct partition assignment), so browsing never commits offsets or affects real groups.

## Testing strategy

- Unit tests for pure logic: parsing (`units`, `consume` positions, `sink` targets), config normalization, ACL checks, monitor status, key decryption, output renderers.
- Integration tests against the shared Docker Compose **sandbox** cluster (3 nodes, PLAINTEXT/mTLS/SASL/SCRAM listeners), isolated with randomly named resources. Most CLI tests run the real commands through `cli.Execute`. See [testing](testing.md).
- The TUI is smoke-tested by driving it in tmux.

## Licensing

ntk is licensed under [Apache-2.0](../LICENSE).

- Every Go source file starts with:
  ```go
  // Copyright 2026 Factual Tech AB
  // SPDX-License-Identifier: Apache-2.0
  ```
- Dependencies must use Apache-2.0-compatible licenses (MIT, BSD, Apache-2.0, ISC, MPL-2.0). The main dependencies are already compatible: franz-go (BSD-3-Clause), Cobra (Apache-2.0), and the Charm libraries (MIT). CI checks this with `go-licenses check ./...`.
- Release archives include `LICENSE` and `NOTICE`.
