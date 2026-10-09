# The TUI

Run `ntk` with no arguments. It opens on the topic list of your active profile.

```sh
ntk                          # topics
ntk tui groups               # start on a view
ntk tui consume orders       # or straight into a topic
```

## Moving around

| Key | Does |
|---|---|
| `:` | command line: `:topics`, `:groups`, `:brokers`, `:acls`, `:health`, `:consume orders`, `:ctx prod`, `:q` |
| `/` | filter the table (`/re:` for a regex) |
| `enter` / `esc` | open / back |
| `tab` | next tab in a detail view |
| `j` `k` or arrows, `g` `G`, `ctrl-f` `ctrl-b` | move, top, bottom, page |
| `<` `>` / `s` | sort by another column / reverse |
| `w` | wide: show every column |
| `r` / `R` | refresh now / change the auto-refresh interval |
| `y` / `Y` | copy the row as JSON / copy the CLI command for this view |
| `ctrl-p` | switch profile |
| `?` | help for the current view |
| `q` | quit |

Every view can tell you the equivalent CLI command (`Y` or `:cmd`), so the TUI doubles as a way to learn the CLI.

## Views

- **Topics** → `enter` for partitions, config, consumers and ACLs. `c` consume, `p` produce, `m` monitor, `e` edit config, `n` new, `ctrl-d` delete.
- **Consume** → a live message browser. `space` pauses, `tab` cycles value / headers / hex / metadata, `/` finds, `s` saves to a file, `P` re-sends a message through the composer.
- **Groups** → lag per partition, members, topics. `R` resets offsets, `m` watches lag live.
- **Brokers, ACLs, principals, quotas, transactions** → the same data as the CLI, with create and delete actions where they make sense.
- **Health** → the dashboard. The header shows the status in every view.

## Changing things

Every write shows the same plan the CLI would print and asks for confirmation. Read-only profiles hide the write actions. On a `prod`-labelled profile you type the name, just like the CLI.

## Settings

`~/.config/ntk/config.json` holds the refresh interval, the start view, mouse support, key remaps and colours:

```json
{ "tui": { "refresh": "5s", "start_view": "health", "mouse": true } }
```
