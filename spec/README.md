# ntk design spec

These docs describe what `ntk` should do and how, before any code is written. They are the source of truth for scope and UX. If an implementation choice conflicts with a doc, update the doc first.

## Reading order

Foundations, which every feature doc builds on:

1. [Overview](overview.md): goals, non-goals, design principles
2. [Architecture](architecture.md): libraries, package layout, how the CLI and TUI share code
3. [Profiles](profiles.md): connection profiles, the profile file, interactive setup
4. [CLI conventions](cli-conventions.md): global flags, output formats, safety, exit codes
5. [TUI](tui.md): app shell, navigation, keymap, shared components

Guides:

- [Shell completion](completion.md): setting up bash (and zsh/fish) completion with live Kafka resource names
- [Testing & sandbox](testing.md): the local 3-node Docker Compose cluster, and how tests isolate themselves on it

Features:

| Area | Doc |
|---|---|
| Topics & partitions | [features/topics.md](features/topics.md) |
| Topic configuration | [features/topic-config.md](features/topic-config.md) |
| Consumer groups | [features/consumer-groups.md](features/consumer-groups.md) |
| Consuming | [features/consuming.md](features/consuming.md) |
| Producing | [features/producing.md](features/producing.md) |
| Topic monitoring | [features/topic-monitoring.md](features/topic-monitoring.md) |
| Consumer group monitoring | [features/group-monitoring.md](features/group-monitoring.md) |
| ACLs | [features/acls.md](features/acls.md) |
| Principals & SCRAM users | [features/principals.md](features/principals.md) |
| Quotas | [features/quotas.md](features/quotas.md) |
| Brokers | [features/brokers.md](features/brokers.md) |
| Cluster overview & health | [features/cluster-health.md](features/cluster-health.md) |
| Producers & transactions | [features/transactions.md](features/transactions.md) |

Later: [Roadmap](roadmap.md), which lists deferred features, future auth mechanisms, and open questions across docs.

## Feature doc template

Every doc under `features/` uses the same sections:

1. **Purpose**: what the user is trying to do, and where kcat or the stock `kafka-*.sh` tools fall short.
2. **CLI**: command synopsis, flags, examples, output samples.
3. **TUI**: view layout, key bindings, drill-down paths.
4. **Kafka APIs**: the protocol requests used (with `kadm` helpers where they exist).
5. **Safety**: confirmations, `--dry-run`, read-only profile behavior.
6. **Open questions**
