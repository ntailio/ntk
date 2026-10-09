# TUI

The TUI is a k9s-style resource browser built with Bubble Tea. It uses the same resource packages as the CLI ([architecture](architecture.md#layering-rule)), so every TUI action has an equivalent CLI command, and the TUI can show that command (`:cmd`, see below).

Launch with `ntk` or `ntk tui [view] [name]`, e.g. `ntk tui groups svc-orders`.

## Layout

```
┌ ntk ─ [prod] ● red ─ cluster lkc-9f2 ─ 3 brokers ─ User:bob ─ read-only ────────────┐
│ :topics  /orders                                                         ↻ 5s  ?help │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ NAME ▲              PARTS  RF  URP  SIZE      MSG/S   RETENTION   CLEANUP           │
│ orders                 12   3    0  48.2 GiB   1.2k   7d          delete            │
│ orders.dlq              3   3    0  120 MiB       0   30d         delete            │
│ orders.state           12   3    1  2.1 GiB      40   ∞           compact           │
│                                                                                      │
├──────────────────────────────────────────────────────────────────────────────────────┤
│ <enter> describe  <c> consume  <p> produce  <e> config  <g> groups  <ctrl-d> delete │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

- **Header**: profile name and color badge, cluster id, broker count, principal, and a `read-only` flag. It turns red when cluster health is degraded (see [cluster health](features/cluster-health.md)).
- **Command/filter line**: the current view, active filter, and refresh interval. Tables show `loading…` until their first load finishes.
- **Body**: the current view, usually a sortable table. Some views are split into table + detail pane.
- **Footer**: context-sensitive key hints. Press `?` for the full list.

## Views

| View | Command | Doc |
|---|---|---|
| Topics | `:topics` / `:t` | [topics](features/topics.md) |
| Topic detail (partitions, config, groups) | `enter` on topic | [topics](features/topics.md), [topic config](features/topic-config.md) |
| Consume / message browser | `c` on topic, `:consume <topic>` | [consuming](features/consuming.md) |
| Produce composer | `p` on topic, `:produce <topic>` | [producing](features/producing.md) |
| Consumer groups | `:groups` / `:g` | [consumer groups](features/consumer-groups.md) |
| Group lag monitor | `m` on group | [group monitoring](features/group-monitoring.md) |
| Topic monitor | `m` on topic | [topic monitoring](features/topic-monitoring.md) |
| Brokers | `:brokers` / `:b` | [brokers](features/brokers.md) |
| ACLs | `:acls` | [ACLs](features/acls.md) |
| Principals / SCRAM users | `:principals` / `:users` | [principals](features/principals.md) |
| Quotas | `:quotas` | [quotas](features/quotas.md) |
| Transactions / producers | `:tx` | [transactions](features/transactions.md) |
| Health dashboard | `:health` / `:h` (default start view is configurable) | [cluster health](features/cluster-health.md) |
| Profiles | `:ctx` / `:profiles` | [profiles](profiles.md) |

## Global keys

| Key | Action |
|---|---|
| `:` | Command palette: views (optionally with a name, e.g. `:topic orders`), `:ctx <profile>`, `:cmd`, `:q`. |
| `/` | Filter the current table (substring; `/re:` prefix for regex). `esc` clears. |
| `enter` | Drill down / describe |
| `esc` | Back (a view stack, like a browser history) |
| `tab` / `shift-tab` | Switch pane or tab inside a detail view |
| `j`/`k`, `↑`/`↓`, `g`/`G` or `home`/`end`, `ctrl-f`/`ctrl-b` | Move / top / bottom / page. Views that bind `g` or `G` to an action (e.g. `g` groups) keep `home`/`end` for top/bottom. |
| `<` / `>` | Sort by the previous / next column |
| `s` | Reverse the sort order |
| `space` | Select rows for bulk actions (e.g. delete several topics) |
| `r` | Refresh now |
| `R` | Change auto-refresh interval (off, 2s, 5s, 10s, 30s), except in views where `R` is an action (reset offsets) |
| `y` | Yank the selected row as JSON to the clipboard (OSC 52, so it works over SSH) |
| `Y` | Yank the equivalent CLI command |
| `:cmd` | Show the equivalent CLI command for the current view/filter |
| `ctrl-p` | Profile switcher (same as `:ctx`) |
| `?` | Help |
| `q` / `ctrl-c` | Quit. In the produce composer `q` is typed as text; use `esc` to leave it and `ctrl-c` to quit. |

View-specific keys are listed in each feature doc. A view's own keys take precedence over the global ones. Across views, `n` means new/create, `e` edit, `ctrl-d` delete, `m` monitor, `c` consume, and `p` produce.

## Shared components

- **Resource table**: sortable, filterable, wide mode (`w`), sticky header, and a mark on rows that changed since the last refresh.
- **Detail pane**: key/value sections with tabs, e.g. topic → Partitions | Config | Consumers | ACLs.
- **Plan/confirm dialog**: renders the same `Plan` the CLI shows for `--dry-run`. On `prod`-labelled profiles it requires typing the resource name. Mutations are hidden or disabled on `read_only` profiles.
- **Form**: embedded `huh` forms for create/edit (topic create, ACL grant, quota set, profile create).
- **Message viewer**: shows the value as-is, as UTF-8 text or a hex dump (never decoded or reformatted), plus the header list and metadata.
- **Sparkline/bar**: for throughput and lag in the monitoring views.
- **Status line**: non-blocking success/error messages. Errors include the Kafka error.

## Behavior

- **Async everything**: all Kafka calls run as `tea.Cmd`s, so the UI never blocks. `esc` leaves a view whose call is still running; the result is dropped.
- **Refresh**: tables auto-refresh at the chosen interval (default 5s, set in `config.json`), and the selection and scroll position are kept across refreshes.
- **Resize**: columns are sized to their content, and on narrow terminals the rightmost columns that don't fit are dropped.
- **Theme**: uses the terminal's 16 ANSI colors, so it follows light and dark terminal themes, and respects `NO_COLOR`. Colors can be overridden in `config.json` (`tui.colors`).
- **Mouse**: optional (`tui.mouse`, off by default) for wheel scrolling.
- **Key bindings**: global keys can be remapped in `config.json` (`tui.keys`, e.g. `{"quit": "ctrl+q"}`).

## Open questions

- Should the TUI support split views, e.g. consume in one pane and group lag in another?
- Should it support multiple profiles open at once (tabs per cluster)?
