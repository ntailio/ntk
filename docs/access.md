# Access control

ACLs, SCRAM users, and quotas, plus a view that puts them together per principal.

## ACLs

```sh
ntk acl list
ntk acl list --principal User:svc-orders
ntk acl list --topic orders --pattern match      # every ACL that applies to this topic
```

### Grant by role, not by operation

Nobody remembers that a consumer needs `READ` and `DESCRIBE` on the topic and `READ` on the group. Recipes do:

```sh
ntk acl grant producer --principal User:svc-orders --topic orders
ntk acl grant consumer --principal User:svc-reporting --topic orders --group svc-reporting
ntk acl grant transactional-producer --principal User:svc-pay --topic payments --txn-id pay-
ntk acl grant streams-app --principal User:svc-agg --app-id agg --topic-in orders --topic-out totals
ntk acl grant topic-admin --principal User:ops --topic 'orders.' --prefixed
ntk acl revoke consumer --principal User:svc-reporting --topic orders --group svc-reporting
```

You see the exact ACLs before they're created. `revoke` removes exactly what the recipe granted.

### Why can't it…?

```sh
ntk acl check --principal User:svc-reporting --operation write --topic orders
DENIED: no matching ALLOW ACL (super.users and custom authorizers are not visible to ntk)
```

`check` evaluates the rules the way the broker does, including `DENY` precedence, prefixes and wildcards, and tells you which rule decided. It exits 5 when denied, so it works in scripts too.

### Back up and sync

```sh
ntk acl export > acls.json
ntk acl import -f acls.json                                  # creates what's missing
ntk acl import -f acls.json --prune --scope 'User:svc-*'     # ...and removes extras in scope
```

## SCRAM users

```sh
ntk user list
ntk user create svc-orders --generate            # prints the password once
ntk user create svc-orders --password-stdin < pw.txt
ntk user rotate svc-orders --generate
ntk user delete svc-orders
```

`--profile-from local` also saves a profile that uses the new credentials, so you can test the account straight away. Passwords never appear in process lists or shell history unless you put them there.

## Quotas

```sh
ntk quota list
ntk quota set --user svc-orders produce=10MiB/s consume=50MiB/s
ntk quota set --user svc-orders --client-id backfill produce=2MiB/s
ntk quota set --user-default request=200
ntk quota effective --user svc-orders --client-id backfill   # which one wins?
ntk quota unset --user svc-orders produce
```

## One principal, everything

```sh
ntk principal describe User:svc-orders
```

SCRAM mechanisms, quotas, ACLs, and a plain-language summary of what the principal can do (`produce → orders`, `consume → orders via group svc-orders`). `ntk whoami` is the same view for yourself.

## Safety

ACL and user changes show a plan and ask first. Deleting ACLs that would lock *you* out gets an extra warning, and `prod`-labelled profiles require typing the principal name.
