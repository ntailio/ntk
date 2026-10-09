# Topic configuration

## Purpose

Viewing and changing topic-level configs. `kafka-configs.sh --describe` mixes defaults, broker static values, and overrides in a hard-to-read list, and changing a value takes the full `--entity-type topics --entity-name … --alter --add-config` incantation. ntk shows the effective value, **where it comes from**, and a diff before any change.

## CLI

```
ntk topic config <topic>                     [--all] [--overrides-only] [--show-docs]
ntk topic config get <topic> <key>
ntk topic config set <topic> k=v [k=v...]
ntk topic config unset <topic> <key>...      # revert to default
ntk topic config apply <topic> -f file.json  [--prune]
ntk topic config diff <topicA> <topicB>      [--profile-b other]
ntk topic config export <topic>...
```

### Show

```
$ ntk topic config orders
KEY                     VALUE        SOURCE            DEFAULT
cleanup.policy          delete       default           delete
retention.ms            7d           dynamic-topic     7d  (broker: log.retention.hours=168)
min.insync.replicas     2            dynamic-topic     1
max.message.bytes       2 MiB        dynamic-topic     1 MiB
segment.bytes           1 GiB        static-broker     1 GiB
```

- By default it shows only overrides plus a curated set of important keys (`cleanup.policy`, `retention.*`, `min.insync.replicas`, `max.message.bytes`, `compression.type`, `segment.*`). `--all` shows all keys.
- Values are humanized in tables (`7d`, `2 MiB`). `-o json` gives raw strings plus `source`, `is_default`, `is_sensitive`, `is_read_only`, and `synonyms`.
- `--show-docs` adds a one-line description per key (from `DescribeConfigs` documentation where the broker supplies it, otherwise a built-in table).

### Set / unset

```
$ ntk topic config set orders retention.ms=14d max.message.bytes=4MiB
[prod] Change config of topic "orders":
  retention.ms        7d     → 14d
  max.message.bytes   2 MiB  → 4 MiB
Apply? [y/N]
```

- Uses `IncrementalAlterConfigs` (SET/DELETE). It never does the legacy full replace that silently drops other overrides.
- Values are validated locally where possible (known key, type, range) before sending. Unknown keys give a "did you mean" suggestion.
- For list-type keys, `k+=v` / `k-=v` map to APPEND/SUBTRACT, e.g. `leader.replication.throttled.replicas+=0:1`.

### Apply from file

```json
{
  "retention.ms": "14d",
  "cleanup.policy": "delete",
  "min.insync.replicas": "2"
}
```

`apply` computes a diff against the current overrides. With `--prune`, overrides not in the file are unset. `export` writes this same format, so you can do `export | edit | apply`.

### Diff

`diff` compares two topics' effective configs, optionally across profiles (e.g. staging vs prod), and shows only keys that differ.

## TUI

Topic detail → **Config** tab, or `e` from the topic list.

| Key | Action |
|---|---|
| `a` | Toggle all keys / overrides + important |
| `enter` / `e` | Edit value inline (validated) |
| `u` | Unset (revert to default) |
| `D` | Show docs for key |
| `ctrl-s` | Review pending edits as a diff and apply |

Edits are staged: several keys can be edited, then applied together in one confirm dialog.

## Kafka APIs

- `DescribeConfigs` with `IncludeSynonyms`/`IncludeDocumentation` (kadm `DescribeTopicConfigs`)
- `IncrementalAlterConfigs` (kadm `AlterTopicConfigs`, `ValidateAlterTopicConfigs` for `--dry-run` server-side validation)

## Safety

All changes are **Change** class: diff + confirm. Certain keys add a warning to the plan:

- `cleanup.policy` delete→compact (or back): changes retention semantics.
- Lowering `retention.ms`/`retention.bytes`: shows an estimate of data that becomes eligible for deletion.
- `min.insync.replicas` > RF: refused.
- `unclean.leader.election.enable=true`: warns about possible data loss.

`--dry-run` also runs the broker-side `ValidateOnly` request, so broker policy violations surface before applying.

## Open questions

- Should curated "important keys" be configurable in `config.json`?
- Should ntk support config templates/policies (e.g. "all topics matching `svc-*` must have min.insync.replicas=2") with a `check` command? That may drift toward the declarative non-goal.
