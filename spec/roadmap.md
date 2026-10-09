# Roadmap

Features and ideas that are deliberately **out of the first scope**, plus questions that affect several docs. The in-scope areas are listed in the [docs index](README.md).

## Suggested milestones

1. **Foundation**: [profiles](profiles.md) (file, wizard, `set`/`switch`/`test`), client factory with PLAINTEXT/TLS/mTLS/PLAIN/SCRAM, [output](cli-conventions.md#output) renderers, safety/plan framework.
2. **Daily driver**: [topics](features/topics.md) (list/describe/create/delete), [consuming](features/consuming.md), [producing](features/producing.md), [consumer groups](features/consumer-groups.md) (list/describe/lag/reset).
3. **TUI shell**: [app shell](tui.md), topics/groups/consume/produce views.
4. **Admin**: [topic config](features/topic-config.md), [ACLs](features/acls.md), [principals](features/principals.md), [quotas](features/quotas.md), [brokers](features/brokers.md).
5. **Observability**: [topic monitoring](features/topic-monitoring.md), [group monitoring](features/group-monitoring.md), [cluster health](features/cluster-health.md), [transactions](features/transactions.md).
6. **Advanced topics**: reassignment, leader election, truncate.

## Deferred features

| Feature | Notes |
|---|---|
| **More auth mechanisms** | `oauthbearer` (client credentials against an OIDC IdP, token caching), `aws-msk-iam` (SigV4 via the AWS SDK credential chain), `gssapi` (Kerberos keytab/ticket cache). They plug into the [auth registry](architecture.md#auth-mechanism-registry) with no profile format change beyond new `auth.*` fields. |
| **Secret references** | Allow `env:VAR`, `file:/path`, `cmd:…`, `keyring:name` values in the profile file instead of inline secrets. For now secrets are inline only, protected by `0600`. |
| **Delegation tokens** | Create/renew/expire/describe. Probably belongs under [principals](features/principals.md). |
| **MirrorMaker 2 / cross-cluster** | Offset translation for failover, replication lag, checkpoint inspection. |
| **Cross-cluster diff** | Compare topics, configs, and ACLs between two profiles (e.g. staging vs prod). Partly covered by `topic config diff --profile-b`. |
| **Metrics integration** | Optional JMX/Prometheus endpoint per profile, for real bytes in/out, request latency, and per-client throughput in the monitoring views. |
| **Feature upgrades** | `UpdateFeatures` (e.g. `metadata.version`) with safety rails. |
| **Broker drain helper** | Reassignment plan to move all replicas off a broker. |
| **Plugin system** | External subcommands (`ntk-foo` on `$PATH` → `ntk foo`). |
| **Load test** | `produce --bench` with latency percentiles. |
| **Share groups (KIP-932)** | Full management once the API stabilizes. They're listed read-only in [consumer groups](features/consumer-groups.md). |
| **ksqlDB / Kafka Streams introspection** | Probably never. Streams apps show up as consumer groups and internal topics. |

## Explicitly out of scope

| Feature | Why |
|---|---|
| **Message decoding / encoding** (Avro, Protobuf, JSON Schema, pretty-printing) | Too messy to own. ntk works with raw bytes and hands messages to other tools through pipes, `-o exec:`, and `-o unix:` ([consuming](features/consuming.md#output)). |
| **Schema Registry** | Dropped along with serde, including the profile's `schema_registry` section. |

## Cross-cutting open questions

- **Distribution**: packaging (Homebrew, AUR, `go install`, GitHub releases, container image).
- **Telemetry**: none planned. State this explicitly in the README.
- **Config file split**: is `profiles.json` (secrets) + `config.json` (preferences) the right split, or should preferences go under a top-level key in `profiles.json`? The split keeps the secrets file small and `0600`, while preferences can be shared in dotfiles.
- **Windows support level**: fully supported, or best-effort?
- **Minimum Kafka version**: is 2.8 best-effort worth the extra code paths, or should we require 3.x?

Open questions specific to one area are at the bottom of each feature doc.
