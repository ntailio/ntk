# Quotas

## Purpose

Viewing and setting client quotas: produce/fetch byte rates, request time percentage, controller mutation rate, and connection creation rate. With `kafka-configs.sh`, quotas are awkward because entities can be combined (user + client-id), have defaults (`<default>`), and follow a precedence order that the tool never explains. ntk shows **which quota actually applies** to a given user/client.

## CLI

```
ntk quota list                    [--user U|--user-default] [--client-id C|--client-default] [--ip IP|--ip-default]
ntk quota set   <entity> k=v...   # entity: --user U [--client-id C] | --client-id C | --ip IP | --*-default
ntk quota unset <entity> key...
ntk quota effective --user U --client-id C     # resolve precedence
```

Keys (with friendly aliases and unit parsing):

| Key | Alias | Example |
|---|---|---|
| `producer_byte_rate` | `produce` | `produce=10MiB/s` |
| `consumer_byte_rate` | `consume` | `consume=50MiB/s` |
| `request_percentage` | `request` | `request=200` |
| `controller_mutation_rate` | `mutations` | `mutations=10` |
| `connection_creation_rate` (IP entities) | `connections` | `connections=20` |

### `quota list`

```
$ ntk quota list
ENTITY                                 PRODUCE      CONSUME      REQUEST  MUTATIONS  CONNECTIONS
user=svc-orders                        10 MiB/s     -            -        -          -
user=svc-orders, client-id=backfill    2 MiB/s      5 MiB/s      -        -          -
user=<default>                         20 MiB/s     50 MiB/s     -        -          -
client-id=<default>                    -            -            100      -          -
ip=10.2.0.14                           -            -            -        -          20/s
```

### `quota effective`

This follows Kafka's precedence order (user+client-id → user+default client → user → default user+client-id → … → default client-id) and shows the winning entity per key:

```
$ ntk quota effective --user svc-orders --client-id backfill
KEY                  VALUE      FROM
producer_byte_rate   2 MiB/s    user=svc-orders, client-id=backfill
consumer_byte_rate   5 MiB/s    user=svc-orders, client-id=backfill
request_percentage   100        client-id=<default>
```

## TUI

`:quotas` shows a table of quota entities. Quotas also appear in the [principal](principals.md) detail view.

| Key | Action |
|---|---|
| `n` | New quota form (entity picker, keys with unit inputs) |
| `e` | Edit selected |
| `ctrl-d` | Remove selected quota entity |
| `E` | Effective quota form (user + client-id), shows the resolution |

## Kafka APIs

- `DescribeClientQuotas` (kadm `DescribeClientQuotas`)
- `AlterClientQuotas` (kadm `AlterClientQuotas`, with `ValidateOnly` for `--dry-run`)

## Safety

- set/unset are **Change** (diff + confirm).
- Setting a quota on a `<default>` entity warns that it affects **every** user/client without a more specific quota.
- Very low values (e.g. `produce` < 1 KiB/s) ask for extra confirmation, since typos in units are common.

## Open questions

- Should `quota effective` also sample live throughput for that client (not possible from Kafka APIs without JMX), or stay purely config-based?
