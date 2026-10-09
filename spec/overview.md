# Overview

`ntk` is a single-binary command-line tool and terminal UI for working with Apache Kafka. It covers the daily work of developers and operators: inspecting topics and groups, consuming and producing, monitoring lag, and administering ACLs, users, quotas, and brokers.

## Why another Kafka tool

| Pain with existing tools | What ntk does |
|---|---|
| The `kafka-*.sh` scripts need a JVM, are slow to start, and every script has its own flags and needs its own `--command-config` properties file. | One Go binary. Connection details live in named [profiles](profiles.md), so you don't repeat them on every call. |
| kcat is great for consume/produce but has almost no admin features. | Consume/produce plus full admin: topics, configs, groups, ACLs, SCRAM users, quotas, brokers, transactions. |
| Output is inconsistent and hard to script. | Every command supports `-o table|wide|json|jsonl|name|template=…` with stable field names. |
| Destructive operations run immediately (for example, `--reset-offsets --execute`). | Previews by default, typed confirmation for destructive actions, and read-only profiles for production. |
| There's no interactive way to explore a cluster. | A k9s-style [TUI](tui.md) that reuses the same logic as the CLI. |
| Common ACL setups ("let X produce to Y") take several commands and knowledge of the right operations. | ACL recipes such as `ntk acl grant producer --topic orders --principal User:svc-orders`. |

## Goals

- **Easy by default:** sensible defaults, interactive prompts when required input is missing (only on a TTY), and helpful errors that suggest the fix.
- **Safe by default:** preview before mutating, confirm before destroying, and honor `read_only` profiles.
- **Scriptable:** machine-readable output, meaningful [exit codes](cli-conventions.md#exit-codes), no prompts when not attached to a TTY or when `-y` is passed.
- **Same logic, two interfaces:** every CLI command and TUI action goes through the same service layer ([architecture](architecture.md)).
- **Broad coverage:** all the areas listed in the [docs index](README.md).
- **Extensible auth:** start with PLAINTEXT, TLS/mTLS, SASL PLAIN and SCRAM, and design so that OAUTHBEARER, AWS MSK IAM, and GSSAPI can be added without changing the profile format ([roadmap](roadmap.md)).

## Non-goals (for now)

- Being a metrics or alerting system. ntk shows live and short-window data it computes from Kafka APIs. It doesn't store history or replace Prometheus/Grafana.
- Managing ksqlDB or MirrorMaker ([roadmap](roadmap.md)).
- Provisioning clusters or brokers.
- A GitOps/declarative "apply the whole cluster state" engine. Import/export of individual resources (configs, ACLs) is in scope. Full reconciliation is not.

## Design principles

1. **Nouns, then verbs:** `ntk <resource> <action> [name] [flags]`, e.g. `ntk topic describe orders`. The TUI mirrors this: views are resources, keys are actions.
2. **The profile is the context:** like kubectl contexts. There's one active profile, it's easy to switch, and it's overridable per command with `-p`.
3. **Show the source of truth:** e.g. topic config shows whether a value is a default, a static broker setting, or a dynamic override.
4. **Never surprise:** any change that affects data or availability prints what will happen first.
5. **Minimal cluster assumptions:** work against plain Apache Kafka 3.x+ (KRaft or ZooKeeper-backed where APIs allow). Features that need newer brokers detect this via `ApiVersions` and degrade gracefully with a clear message.

## Target Kafka versions

- Primary target: Apache Kafka 3.6+ and 4.x (KRaft).
- Best-effort: 2.8+. Features that need newer APIs (e.g. `DescribeQuorum`, KIP-848 consumer groups, `ListTransactions`) are hidden or report "not supported by this cluster".
