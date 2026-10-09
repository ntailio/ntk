# CLI conventions

## Command shape

```
ntk <resource> <action> [args] [flags]
```

Resources are singular with plural aliases: `topic`/`topics`, `group`/`groups`, `acl`/`acls`, `broker`/`brokers`, `user`/`users`, `quota`/`quotas`, `tx`/`transactions`, `profile`/`profiles`. Common actions use the same names everywhere: `list` (alias `ls`), `describe` (alias `get`), `create`, `delete` (alias `rm`), `alter`/`set`, `watch`.

Top-level shortcuts for the most frequent tasks:

| Shortcut | Equivalent |
|---|---|
| `ntk consume <topic>` | [consuming](features/consuming.md) |
| `ntk produce <topic>` | [producing](features/producing.md) |
| `ntk health` | [cluster health](features/cluster-health.md) |
| `ntk whoami` | [principals](features/principals.md#whoami) |
| `ntk tui [view]` / `ntk` | [TUI](tui.md) |

With no arguments, `ntk` opens the TUI when stdout is a TTY, and prints help otherwise.

### Short aliases

The most-used commands have one-letter aliases (Cobra `Aliases`):

| Alias | Command | Example |
|---|---|---|
| `t` | `topic` | `ntk t describe orders` |
| `g` | `group` | `ntk g lag svc-orders` |
| `b` | `broker` | `ntk b list` |
| `c` | `consume` | `ntk c orders --from -5` |
| `p` | `produce` | `ntk p orders -k k1 -v hello` |

- The list is intentionally short. Other commands (`acl`, `user`, `quota`, `tx`, `profile`, `cluster`, …) have no one-letter alias, so `cluster` has none because `c` is `consume`.
- One-letter names are reserved for these aliases, so no future command or alias may use a single letter. This keeps `ntk c` from ever becoming ambiguous.
- Help output and shell completion show the aliases (`Aliases: topic, topics, t`).

## Global flags

| Flag | Env | Description |
|---|---|---|
| `-p, --profile <name>` | `NTK_PROFILE` | Profile to use (see [profiles](profiles.md#selecting-a-profile)). |
| `--profile-path <file>` | `NTK_PROFILE_PATH` | Profile file location. |
| `-o, --output <fmt>` (alias `--out`) | `NTK_OUTPUT` | `table` (default), `wide`, `json`, `jsonl`, `template=<go-template>`, `name` (names only, one per line). |
| `--no-color` | `NO_COLOR` | Disable color. Color is also off when stdout isn't a TTY. |
| `--no-headers` | | Omit table headers. |
| `-y, --yes` | | Skip confirmation prompts, **except** typed-name confirmation on `prod`-labelled profiles, which also needs `--confirm=<name>`. |
| `--confirm <name>` | | The typed confirmation for non-interactive runs. |
| `--no-input` | `NTK_NO_INPUT` | Never prompt; fail when input is missing. |
| `--dry-run` | | Print the plan and exit without changing anything. |
| `--timeout <dur>` | | Overall command timeout. |
| `--verbose` / `--debug` | `NTK_DEBUG` | Verbose logs to stderr; `--debug` also logs Kafka requests. (No `-v` shorthand: `produce -v` is the message value.) |

## Output

- Data goes to **stdout**, while logs, warnings, progress, and prompts go to **stderr**. So `ntk topic list -o name | xargs …` is always safe.
- There is no YAML output. Use `-o json` (and `jq`, `yq`, … if you need another format).
- `table` shows the most useful columns. `wide` adds everything.
- `json` output uses stable snake_case field names that are documented per command. These are part of the public interface. Renaming a field is a breaking change.
- `jsonl` is for streams (watch commands, and the [consume](features/consuming.md#jsonl-export--o-jsonl) record export). Lists also support it (one object per line).
- `consume` is the exception to `-o`: it writes raw message bytes, and its `-o` values are output targets: `raw` (default), `jsonl`, `unix:<path>`, and `exec:<command>`. See [consuming](features/consuming.md#output).
- `template=` uses Go `text/template` over the JSON form of each item, with the helpers `upper`, `lower`, `join`, `json`, and `default`: `-o template='{{.name}}\t{{.partitions}}'`.
- Sizes are shown in human units in tables (`1.2 GiB`) and in raw bytes in JSON. Durations and timestamps work the same way: relative/local in tables, RFC 3339 UTC in machine formats.

## Selecting resources

- Most `list` commands accept a glob or regex filter: `ntk topic list 'orders.*'`, `ntk group list --regex '^svc-'`.
- Internal topics (`__consumer_offsets`, `__transaction_state`, …) are hidden unless you pass `--internal` / `-a`.
- Many actions accept several names: `ntk topic delete a b c`.

## Safety

Every mutation is classified by the `safety` package ([architecture](architecture.md#layering-rule)):

| Class | Examples | Behavior |
|---|---|---|
| **Safe write** | create topic, add ACL, produce | Runs directly. |
| **Change** | alter config, reset offsets, alter quotas, add partitions | Shows the plan (a diff) and asks `Apply? [y/N]`. `-y` skips the prompt. |
| **Destructive** | delete topic/group/ACL/user, delete records, unclean leader election, abort transaction | Shows the plan and asks for confirmation. On `prod`-labelled profiles you must type the resource name (or pass `--confirm=<name>` with `-y`). |

Additional rules:

- **Read-only profiles** refuse every write and exit with code 6: `refused: profile "prod" is read-only (use -p prod-admin or remove read_only)`.
- **Non-TTY without `-y`**: commands that need confirmation fail with exit code 2 instead of hanging.
- **`--dry-run`** is available on every mutating command. It prints the same plan the confirmation shows, with no side effects.
- **Offset resets default to dry-run.** You need `--execute` to apply them (this matches the stock tool's behavior, so it's familiar).
- Plans can be saved and applied later: `--dry-run -o json > plan.json` and then `ntk apply plan.json`. The plan records the profile name and cluster id, and `apply` refuses to run it against a different cluster.
- The confirmation prompt shows the profile name in its `color`: `[prod] Delete topic "orders" (12 partitions, 48 GiB)?`

## Interactive fallbacks

If a required argument is missing and stdin/stdout is a TTY, ntk prompts for it instead of failing. For example, `ntk group describe` with no name opens a fuzzy picker of groups. Pass `--no-input` (or `NTK_NO_INPUT=1`) to always fail instead.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Success |
| 1 | General error |
| 2 | Usage error, or confirmation required but not possible (non-TTY without `-y`) |
| 3 | Connection or authentication failure |
| 4 | Resource not found |
| 5 | Check failed: a `--threshold`/health check condition was breached (see [monitoring](features/group-monitoring.md), [health](features/cluster-health.md)) |
| 6 | Refused by read-only profile or user declined confirmation |
| 7 | Authorization failure (Kafka `*_AUTHORIZATION_FAILED`) |
| 8 | Not supported by this cluster version |
| 9 | Output target failed: an `-o exec:` command exited non-zero or timed out, or `-o unix:` delivery failed (oversized datagram, receiver gone) |

## Shell completion

`ntk completion bash|zsh|fish|powershell` generates completion scripts. Completion is **dynamic**: topic, group, profile, and user names are fetched from the active profile, with a short cache (10s) and a 1s timeout so a slow cluster never blocks the shell. See the [shell completion guide](completion.md) for setup and details.

## Configuration beyond profiles

Non-connection preferences (TUI colors, refresh interval, key bindings, config presets, lag thresholds, completion) live in `~/.config/ntk/config.json`, separate from secrets. This file doesn't need `0600`.
