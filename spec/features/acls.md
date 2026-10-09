# ACLs

## Purpose

Seeing and managing who can do what. `kafka-acls.sh` needs you to know which operations each use case needs (a consumer needs READ on the topic **and** READ on the group, a transactional producer also needs WRITE+DESCRIBE on the transactional id, …). Its output is also hard to filter. ntk adds **recipes** for common roles, a principal-centric view, export/import, and an access check.

## CLI

```
ntk acl list                  [--principal User:x] [--topic t] [--group g] [--resource-type ...]
                              [--pattern literal|prefixed|match] [--operation read,...] [--host h]
ntk acl create                --principal P --operation OP[,OP] (--topic|--group|--cluster|--transactional-id|--delegation-token) NAME
                              [--prefixed] [--deny] [--host '*']
ntk acl delete                <same filter flags as list>          # deletes all matches (shown first)
ntk acl grant <recipe>        --principal P [recipe flags]
ntk acl revoke <recipe>       --principal P [recipe flags]
ntk acl check                 --principal P --operation OP --topic t     # "can P do OP on t?"
ntk acl export                [filter flags] > acls.json
ntk acl import -f acls.json   [--prune --scope 'User:svc-*']
```

### `acl list`

```
$ ntk acl list --principal User:svc-orders
PRINCIPAL         PERM   OPERATION  RESOURCE           PATTERN   NAME            HOST
User:svc-orders   ALLOW  WRITE      TOPIC              LITERAL   orders          *
User:svc-orders   ALLOW  DESCRIBE   TOPIC              LITERAL   orders          *
User:svc-orders   ALLOW  READ       TOPIC              PREFIXED  payments.       *
User:svc-orders   ALLOW  READ       GROUP              LITERAL   svc-orders      *
```

`--topic orders --pattern match` shows every ACL that **applies** to `orders`: literal, prefixed matches, and wildcard `*`. That's what you want when debugging.

### Recipes

| Recipe | Grants |
|---|---|
| `producer --topic T [--prefixed]` | WRITE, DESCRIBE on topic T |
| `consumer --topic T --group G [--prefixed]` | READ, DESCRIBE on topic T; READ on group G |
| `transactional-producer --topic T --txn-id ID` | producer + WRITE, DESCRIBE on transactional id ID (+ IDEMPOTENT_WRITE on cluster for old brokers) |
| `streams-app --app-id A --topic-in T... --topic-out T...` | consumer on inputs, producer on outputs, ALL on internal topics / group / txn id prefixed with `A` |
| `topic-admin --topic T [--prefixed]` | ALTER, ALTER_CONFIGS, CREATE, DELETE, DESCRIBE, DESCRIBE_CONFIGS on topic T |
| `read-only-cluster` | DESCRIBE, DESCRIBE_CONFIGS on cluster, all topics, all groups (monitoring accounts) |

```
$ ntk acl grant consumer --principal User:svc-reporting --topic orders --group svc-reporting
[prod] Create 3 ACLs:
  + ALLOW User:svc-reporting READ      TOPIC:LITERAL:orders         host=*
  + ALLOW User:svc-reporting DESCRIBE  TOPIC:LITERAL:orders         host=*
  + ALLOW User:svc-reporting READ      GROUP:LITERAL:svc-reporting  host=*
Apply? [y/N]
```

`revoke` removes exactly the ACLs the recipe would create, and nothing else.

### `acl check`

This evaluates the ACLs locally with Kafka's authorizer semantics (DENY wins; literal, prefixed, and wildcard matches; implied operations such as READ/WRITE → DESCRIBE; `super.users` isn't visible, so ntk says so):

```
$ ntk acl check --principal User:svc-reporting --operation READ --topic orders
ALLOWED by: ALLOW User:svc-reporting READ TOPIC:LITERAL:orders host=*
```

It evaluates the ACLs ntk can see. It can't account for custom authorizers or `super.users` and says so in the output.

### Export / import

The export format (JSON) groups ACLs by principal, so it diffs well in code review:

```json
[
  {
    "principal": "User:svc-orders",
    "allow": [
      { "operations": ["WRITE", "DESCRIBE"], "topic": "orders" },
      { "operations": ["READ"], "topic": "payments.", "pattern": "prefixed" },
      { "operations": ["READ"], "group": "svc-orders" }
    ]
  }
]
```

`import` computes the diff and applies additions. `--prune` also deletes ACLs within `--scope` that aren't in the file. The scope is required with `--prune` so that an import never deletes unrelated ACLs.

## TUI

`:acls` shows a table with filter chips (principal, resource type, resource, operation).

| Key | Action |
|---|---|
| `n` | Create ACL form |
| `G` | Grant recipe form (choose the recipe, then fill in the fields) |
| `ctrl-d` | Delete selected ACL(s) (multi-select with `space`) |
| `P` | Pivot to the principal under the cursor ([principals](principals.md)) |
| `enter` on a resource | Jump to the topic/group |
| `C` | Access check form |

Topic and group detail views have an **ACLs** tab that shows the ACLs matching that resource.

## Kafka APIs

- `DescribeAcls` (kadm `DescribeACLs`)
- `CreateAcls` (kadm `CreateACLs`)
- `DeleteAcls` (kadm `DeleteACLs`)
- `DescribeCluster` / `Metadata` for the authorized-operations view where available

## Safety

- create/grant/import additions are **Safe write** (shown as a plan).
- delete/revoke/import `--prune` are **Destructive**. The plan lists every ACL to be removed. `acl delete` with no filter is refused.
- If an ACL deletion would remove the current profile's own access (its `principal` loses DESCRIBE/ALTER on the cluster), ntk asks for an extra confirmation.

## Open questions

- Should the check command also call `DescribeCluster`/`Metadata` with `IncludeAuthorizedOperations` to show the broker's own answer for the current principal?
- Should recipe definitions be user-extensible in `config.json`?
