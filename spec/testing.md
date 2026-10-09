# Testing & sandbox

ntk has two test layers:

- **Unit tests** with no external dependencies, for pure logic: parsing (`units`, `consume`, `sink`), config normalization (`configs`), ACL checks (`acls`), monitor status, key decryption (`kafka`), and `output` renderers.
- **Integration tests** against the **sandbox**: a long-lived 3-node Kafka cluster in Docker Compose that's also used for development and manual testing. Tests share this cluster, so each test isolates itself with [randomly named resources](#test-isolation) and cleans up after itself.

## Sandbox cluster

```sh
docker compose up -d --wait     # start; the first run generates certs into sandbox/certs/
docker compose ps -a            # certs and init show "Exited (0)" when done
docker compose down             # stop, keep all data
docker compose down -v          # stop and wipe all data (topics, groups, ACLs, SCRAM users)
```

Requires Docker with Compose v2. The files are [docker-compose.yml](../docker-compose.yml) and [sandbox/](../sandbox/).

### Topology

| | |
|---|---|
| Kafka | `apache/kafka:4.3.1`, KRaft |
| Nodes | 3, each broker + controller (`kafka-1`..`kafka-3`, node ids 1–3) |
| Replication | `default.replication.factor=3`, `min.insync.replicas=2`, internal topics RF 3 |
| Topics | `auto.create.topics.enable=false`, `num.partitions=3` |
| Authorization | `StandardAuthorizer`, `allow.everyone.if.no.acl.found=false` |
| Groups | `group.initial.rebalance.delay.ms=0` (fast tests), classic and KIP-848 protocols |

Data lives in named volumes, and the cluster id is fixed, so `down`/`up` keeps everything.

### Ports

Every port is in the 19000 range and bound to `127.0.0.1` only. The pattern is `19<node>0<listener>`:

| Listener | Security | kafka-1 | kafka-2 | kafka-3 | Principal |
|---|---|---|---|---|---|
| `CLIENT` | PLAINTEXT | 19101 | 19201 | 19301 | `User:ANONYMOUS` (super user) |
| `MTLS` | SSL, client cert required | 19102 | 19202 | 19302 | `User:<cert CN>` |
| `SASL` | SASL_PLAINTEXT | 19103 | 19203 | 19303 | `User:<username>` |
| `SASLTLS` | SASL_SSL (server TLS only) | 19104 | 19204 | 19304 | `User:<username>` |
| `INTERNAL` | PLAINTEXT, inter-broker | 19090 | 19090 | 19090 | container network only |
| `CONTROLLER` | PLAINTEXT, KRaft | 19091 | 19091 | 19091 | container network only |

Both SASL listeners accept `PLAIN`, `SCRAM-SHA-256`, and `SCRAM-SHA-512`. Advertised addresses are `localhost:<port>`, so clients must run **on the host**. A test running in a container needs `network_mode: host`.

### Built-in credentials

| Principal | Super user | PLAIN / SCRAM password | Client cert (MTLS) |
|---|---|---|---|
| `User:ANONYMOUS` | yes | - (CLIENT listener) | - |
| `User:admin` | yes | `admin-secret` | `sandbox/certs/admin.crt` + `admin.key` |
| `User:bob` | **no**, starts with no ACLs | `bob-secret` | `sandbox/certs/bob.crt` + `bob.key` |

- PLAIN users are static, in [sandbox/kafka_server_jaas.conf](../sandbox/kafka_server_jaas.conf).
- SCRAM users `admin` and `bob` are created by the compose `init` service (both mechanisms). Other SCRAM users are created at runtime, e.g. by tests.
- Client cert CNs map to the principal via `ssl.principal.mapping.rules`, so `CN=bob` becomes `User:bob`.

> The CLIENT listener gives **anyone** super-user access, and every secret here is public. That's fine on `127.0.0.1` for a sandbox. Never expose these ports or reuse this setup elsewhere.

### Certificates

`sandbox/certs/` is generated on the first `up` by the `certs` service ([gen-certs.sh](../sandbox/gen-certs.sh)) and is git-ignored.

| File | Purpose |
|---|---|
| `ca.crt`, `ca.key` | Sandbox CA. The key is kept so tests can **mint client certs for random principals**. |
| `kafka-N.keystore.pem` | Broker PEM keystore (encrypted PKCS#8 key, password `broker-secret`, + chain). SAN: `kafka-N`, `localhost`, `127.0.0.1`. |
| `admin.*`, `bob.*` | Client certs, unencrypted PKCS#8 keys |

To regenerate: `rm sandbox/certs/ca.crt && docker compose up -d --force-recreate --wait`.

### Using the sandbox with ntk

[sandbox/profiles.json](../sandbox/profiles.json) has a profile for each listener and user:

```sh
chmod 600 sandbox/profiles.json          # git doesn't keep the mode; avoids the permission warning
export NTK_PROFILE_PATH=$PWD/sandbox/profiles.json
ntk profile list
ntk -p sandbox-bob t list                # empty until bob is granted ACLs
```

| Profile | Listener | Principal |
|---|---|---|
| `sandbox` (active) | CLIENT | `User:ANONYMOUS` |
| `sandbox-mtls` / `sandbox-mtls-bob` | MTLS | `User:admin` / `User:bob` |
| `sandbox-plain` | SASL, PLAIN | `User:admin` |
| `sandbox-scram` / `sandbox-bob` | SASLTLS, SCRAM-SHA-512 | `User:admin` / `User:bob` |

Certificate paths in the file are relative to the profile file ([profiles](profiles.md#fields)).

Other tools work too, e.g. `kcat -b localhost:19101 -L`.

### Simulating failures

```sh
docker compose stop kafka-3     # partitions become under-replicated; ISR shrinks to 2 (= min ISR)
docker compose start kafka-3    # rejoins and catches up
```

## Test isolation

The sandbox is **shared and long-lived**. It's used by parallel tests, repeated runs, and developers poking around manually. So every integration test follows these rules:

1. **Never assume an empty cluster.** Only assert on resources the test created itself.
2. **Only touch your own resources.** Never list-then-delete anything a test didn't create.
3. **Clean up always**, including on failure, and let the janitor handle anything a crashed run left behind.

### Naming

Every resource a test creates gets a unique name from the test helper (package `testkit`):

```
ntk-test-<created>-<rand>-<purpose>
         │         │      └── readable hint, e.g. "orders"
         │         └── 6 random base32 chars, per test
         └── creation time, unix seconds in base36 (lets the janitor age-filter)
```

Example: `ntk-test-t3kq1c-7fz2qa-orders`.

| Resource | How it's isolated |
|---|---|
| Topics | `testkit.Topic(t, adm, "orders", partitions)` creates it, waits until every broker sees it, and registers deletion |
| Consumer groups | `testkit.Group(t, adm, "readers")` returns a unique name and deletes the group after the test |
| Transactional ids, client ids | `testkit.Name(t, …)` |
| SCRAM principals | `user, password := testkit.ScramUser(t, adm, "bob")` creates `ntk-test-…-bob` with a random password |
| mTLS principals | `cert, key, principal := testkit.ClientCert(t, "bob")` mints a cert with `CN=ntk-test-…-bob`, signed by `sandbox/certs/ca.key`, into `t.TempDir()` |
| ACLs | Only granted to test principals and/or on test resources, and removed in cleanup |
| Quotas | Only on test users/client ids, and removed in cleanup |
| Profiles | `testkit.Profiles(t, extra)` writes a profile file to `t.TempDir()` (active profile `sandbox`), never to the user's profile file |

`testkit.Produce(t, topic, values...)` writes records keyed `k0`, `k1`, … and waits for the acks. `testkit.Admin(t)` returns a super-user admin client, and skips the test when the sandbox isn't reachable.

### Cleanup

- Helpers register `t.Cleanup` **right after** creating a resource. Cleanup runs in reverse order: ACLs and quotas, then SCRAM users, groups, and finally topics.
- "Not found" is ignored during cleanup. Other cleanup errors are logged but don't fail the test.
- **Janitor:** `go run ./testkit/janitor` deletes `ntk-test-*` topics, groups, SCRAM users, ACLs, and quotas older than 1h (`--older-than` changes it; the age comes from `<created>` in the name). `TestMain` of the CLI suite runs it first. `--all` removes every `ntk-test-*` resource regardless of age, but only use it when no tests are running.

### Tests that can't be isolated

Some features affect the whole cluster. These tests call `testkit.Serial(t)` (a cross-process lock file), so they never run at the same time as each other. They must restore any state they change:

- dynamic broker configs, including cluster-wide defaults
- `<default>` quota entities
- cluster-wide health assertions (e.g. "no under-replicated partitions"). Where possible, scope assertions to the test's own topics.

**Disruptive tests** that stop brokers (URP, offline partitions, leader election, health) call `testkit.Disruptive(t)` and only run with `NTK_TEST_DISRUPTIVE=1`. They're serial, and in cleanup they restart the broker and wait until all ISRs are full again.

### Running

```sh
docker compose up -d --wait
go test ./...                       # unit + integration
go test -short ./...                # unit only; integration tests skip
NTK_TEST_DISRUPTIVE=1 go test ./... # also broker stop/start tests
```

Integration tests **skip** (not fail) with a clear message if the sandbox isn't reachable. `NTK_TEST_BOOTSTRAP` overrides the default `localhost:19101,localhost:19201,localhost:19301`.
