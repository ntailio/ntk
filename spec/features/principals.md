# Principals & SCRAM users

## Purpose

Kafka has **no principal registry**. A principal exists only as a name in ACLs, quotas, SCRAM credentials, or a client certificate. So "managing principals" in ntk covers two things:

1. **SCRAM user management**: create, rotate, and delete SCRAM-SHA-256/512 credentials stored in the cluster (the `kafka-configs.sh --entity-type users --alter --add-config 'SCRAM-SHA-512=[...]'` workflow).
2. **A principal-centric view**: everything ntk can find about one principal in one place (SCRAM mechanisms, ACLs, quotas, and groups whose client ids or members match), plus `whoami` for the current profile.

mTLS principals (certificate DNs) can't be created in Kafka. They appear in the principal view via their ACLs and quotas.

## CLI

```
ntk user list                               # SCRAM users
ntk user create <name>    [--mechanism scram-sha-512] [--iterations 8192] [--password-stdin]
ntk user rotate <name>    [--mechanism ...] [--password-stdin]
ntk user delete <name>    [--mechanism ...]           # default: all mechanisms
ntk principal list        [--source acl,quota,scram]
ntk principal describe <principal>                    # e.g. User:svc-orders, User:CN=bob,OU=eng
ntk whoami
```

### `user create`

```
$ ntk user create svc-reporting
Password: ********         (or: generated, see below)
Confirm:  ********
[prod] Create SCRAM credential:
  + User:svc-reporting  SCRAM-SHA-512  iterations=8192
Apply? [y/N] y
Created. Next: ntk acl grant consumer --principal User:svc-reporting --topic ... --group ...
```

- The password is prompted with masked input. It's never read from a flag without `--password-stdin`.
- `--generate` creates a strong random password and prints it **once** to stdout (everything else goes to stderr, so `ntk user create x --generate > secret.txt` works).
- `--profile-from <base>` also saves a new ntk profile `<base>-<user>`: a copy of profile `<base>` with the new SCRAM credentials (handy for testing the account right away).

### `user rotate`

Upserts a new credential for the same mechanism. Existing sessions stay connected until they re-authenticate. The output reminds you of this.

### `user list`

```
$ ntk user list
USER             MECHANISMS
bob              SCRAM-SHA-512 (8192)
svc-orders       SCRAM-SHA-256 (4096), SCRAM-SHA-512 (8192)
svc-reporting    SCRAM-SHA-512 (8192)
```

### `principal list`

Combines principals from ACLs, quota entities, and SCRAM users:

```
$ ntk principal list
PRINCIPAL                  SCRAM          ACLS  QUOTAS
User:bob                   sha-512           4  -
User:svc-orders            sha-256,512       6  producer_byte_rate=10MiB/s
User:CN=monitor,OU=ops     -                 3  -
User:*                     -                 1  -
```

### `principal describe`

```
$ ntk principal describe User:svc-orders
Principal:  User:svc-orders
SCRAM:      SCRAM-SHA-256 (4096), SCRAM-SHA-512 (8192)
Quotas:     user=svc-orders: producer_byte_rate=10 MiB/s
            (effective incl. defaults: consumer_byte_rate=∞, request_percentage=∞)
ACLs (6):   ALLOW WRITE,DESCRIBE  TOPIC:LITERAL:orders
            ALLOW READ            TOPIC:PREFIXED:payments.
            ALLOW READ            GROUP:LITERAL:svc-orders
Effective access (summary):
  produce → orders
  consume → payments.* via group svc-orders
Active groups with client-id svc-orders-*: svc-orders (6 members)
```

"Effective access" translates ACLs back into recipe terms ([ACLs](acls.md#recipes)), which is much easier to read than raw operation lists.

### `whoami`

```
$ ntk whoami
Profile:    prod
Principal:  User:bob (configured; SASL username bob)
Auth:       SASL_SSL scram-sha-512, client cert CN=bob,OU=eng
Cluster:    lkc-9f2 · authorized cluster operations: DESCRIBE, DESCRIBE_CONFIGS
```

It uses `IncludeClusterAuthorizedOperations` in `Metadata`/`DescribeCluster` to show what the broker says this principal may do at cluster level.

## TUI

`:principals` shows the principal list. `enter` opens a detail view with tabs: Overview, ACLs, Quotas, SCRAM.

| Key | Action |
|---|---|
| `n` | New SCRAM user form |
| `ctrl-r` | Rotate password |
| `ctrl-d` | Delete SCRAM credential |
| `G` | Grant ACL recipe to this principal |
| `Q` | Set quota for this principal ([quotas](quotas.md)) |

`:users` opens the same list filtered to principals with SCRAM credentials.

## Kafka APIs

- `DescribeUserScramCredentials` (kadm `DescribeUserSCRAMs`)
- `AlterUserScramCredentials` (kadm `AlterUserSCRAMs`). The salted password is computed client-side, following the Kafka protocol.
- `DescribeAcls`, `DescribeClientQuotas` for aggregation
- `DescribeCluster` / `Metadata` with authorized operations for `whoami`

## Safety

- create is a **Safe write**. rotate is a **Change**, and on `prod`-labelled profiles it warns that clients using the old password will fail on reconnect.
- delete is **Destructive**. If the principal still has ACLs, the plan lists them and offers to delete them too (`--with-acls`).
- Rotating or deleting the credential the **current profile uses** triggers an extra warning, and after a rotate ntk offers to update the profile's password.

## Open questions

- Should `principal list` also sample group member client ids/hosts to discover mTLS principals that have no ACLs (e.g. `super.users`)? Kafka doesn't expose the authenticated principal of group members, so this would only be a heuristic.
- Should we support delegation tokens here (create/renew/expire/describe) or keep them in the roadmap?
