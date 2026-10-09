# Profiles

A profile is a named set of connection settings for one Kafka cluster: bootstrap servers, TLS/mTLS, SASL credentials, and safety settings. Exactly one profile is **active** at a time. Every command uses it unless overridden.

## Profile file location

The file path is resolved in this order (first match wins):

1. `--profile-path <file>` flag
2. `NTK_PROFILE_PATH` environment variable
3. `$XDG_CONFIG_HOME/ntk/profiles.json`
4. `~/.config/ntk/profiles.json`

### Permissions

Profiles contain secrets **inline** (passwords, key passphrases, inline private keys), so the file is protected by file permissions:

- When ntk creates the file, it's created with mode **`0600`**. If ntk creates the parent directory, it gets **`0700`**.
- Writes are atomic: write to a temp file in the same directory with mode `0600`, `fsync`, then `rename` over the original. The mode is kept, and a crash never leaves a half-written file.
- On every load, if the file is readable or writable by group/others (`mode & 0077 != 0`), ntk prints a warning to stderr:
  ```
  warning: /home/bob/.config/ntk/profiles.json has mode 0644; it contains secrets. Run `ntk profile fix-perms` to set 0600.
  ```
- `ntk profile fix-perms` runs `chmod 0600` on the file (and `0700` on the directory if ntk owns it).
- On Windows the mode check is skipped, and the file relies on the user profile directory's default ACL.

If the file doesn't exist, read-only commands fail with a hint to run `ntk profile create`. Running `ntk` or `ntk tui` with no profiles opens the create wizard (on a TTY).

## Selecting a profile

The profile used by a command is resolved in this order:

1. `-p, --profile <name>` flag
2. `NTK_PROFILE` environment variable
3. The `active` field in the profile file

`ntk profile set <name>` and `ntk profile switch` change `active`. The flag and environment variable never change the file.

## File format

```json
{
  "version": 1,
  "active": "dev",
  "profiles": {
    "dev": {
      "bootstrap_servers": ["localhost:9092"],
      "auth": { "mechanism": "none" }
    },
    "prod": {
      "description": "Production EU cluster",
      "bootstrap_servers": ["kafka-1.prod:9093", "kafka-2.prod:9093", "kafka-3.prod:9093"],
      "client_id": "ntk-bob",
      "principal": "User:bob",
      "tls": {
        "enabled": true,
        "ca_file": "/etc/kafka/ca.pem",
        "cert_file": "/home/bob/.kafka/bob.crt",
        "key_file": "/home/bob/.kafka/bob.key",
        "key_password": "",
        "server_name": "",
        "insecure_skip_verify": false
      },
      "auth": {
        "mechanism": "scram-sha-512",
        "username": "bob",
        "password": "s3cret"
      },
      "read_only": true,
      "color": "red",
      "labels": ["prod", "eu"],
      "timeouts": { "dial": "10s", "request": "30s" },
      "defaults": {
        "output": "table",
        "consume": { "start": "latest" }
      }
    }
  }
}
```

### Fields

| Field | Required | Description |
|---|---|---|
| `version` | yes | File format version. ntk migrates older versions in place (after writing a `.bak` copy). |
| `active` | no | Name of the active profile. |
| `profiles.<name>` | | Profile name: `[a-zA-Z0-9._-]+`. |
| `description` | no | Free text shown in `profile list`. |
| `bootstrap_servers` | yes | One or more `host:port` entries. |
| `client_id` | no | Kafka client id. Default `ntk`. Useful for quota matching and broker logs. |
| `principal` | no | The principal this profile is expected to authenticate as, e.g. `User:bob` or `User:CN=bob,OU=eng`. Used by `whoami`, "my ACLs" views, and ACL recipes. If unset, it's derived: SASL username → `User:<username>`, mTLS → `User:<cert subject DN>` (the broker's `ssl.principal.mapping.rules` may differ, so ntk labels derived values "derived"). |
| `tls.enabled` | no | Enables TLS. It's implied if any other `tls.*` field is set. |
| `tls.ca_file` / `tls.ca_pem` | no | CA bundle as a file path or inline PEM. If both are unset, the system roots are used. |
| `tls.cert_file` / `tls.cert_pem` | mTLS | Client certificate (chain), as a file or inline PEM. |
| `tls.key_file` / `tls.key_pem` | mTLS | Client private key, as a file or inline PEM. PKCS#1, PKCS#8, and EC keys are supported. |
| `tls.key_password` | no | Passphrase for an encrypted private key. |
| `tls.server_name` | no | SNI / verification hostname override. |
| `tls.insecure_skip_verify` | no | Disables verification. ntk prints a warning on every connect. |
| `auth.mechanism` | yes | `none`, `plain`, `scram-sha-256`, or `scram-sha-512`. It selects which other `auth.*` fields are valid (see [architecture](architecture.md#auth-mechanism-registry)). |
| `auth.username` / `auth.password` | SASL | Credentials for `plain` and `scram-*`. |
| `read_only` | no | If `true`, every mutating command is refused (exit code 6) and write actions are hidden in the TUI. |
| `color` | no | Badge color shown in the TUI header and in CLI confirmation prompts (`red`, `yellow`, `green`, `blue`, `magenta`, or a hex value). |
| `labels` | no | Free-form tags. The label `prod` makes confirmations stricter (you must type the resource name; see [CLI conventions](cli-conventions.md#safety)). |
| `timeouts` | no | `dial` and `request` durations. |
| `defaults` | no | Per-profile defaults for `output` and `consume.start` (`latest`). |

The security protocol is derived rather than stored:

| `tls.enabled` | `auth.mechanism` | Kafka security protocol |
|---|---|---|
| false | none | PLAINTEXT |
| true | none | SSL (mTLS if a client cert is set) |
| false | plain/scram-* | SASL_PLAINTEXT |
| true | plain/scram-* | SASL_SSL |

Relative file paths (`tls.ca_file`, `tls.cert_file`, `tls.key_file`) are resolved against the directory that contains the profile file, so a profile file can be shipped together with its certs (see the [sandbox profiles](testing.md#using-the-sandbox-with-ntk)).

Unknown top-level fields and unknown fields directly inside a profile are kept when rewriting the file, so newer ntk versions and hand edits survive. Unknown fields nested deeper (inside `tls`, `auth`, …) are dropped.

## Commands

```
ntk profile create [name]        Interactive wizard (or non-interactive with flags)
ntk profile list                 List profiles; marks the active one
ntk profile current              Print the active profile name
ntk profile show [name]          Show a profile; secrets masked unless --reveal
ntk profile edit <name>          Wizard pre-filled with existing values (or --set key=value)
ntk profile delete <name>        Delete a profile (asks for confirmation)
ntk profile rename <old> <new>
ntk profile copy <src> <dst>     Duplicate, e.g. to create a read-only variant
ntk profile set <name>           Set the active profile
ntk profile switch               Fuzzy picker to choose the active profile
ntk profile test [name]          Connect and report what works
ntk profile fix-perms            chmod 0600 the profile file
ntk profile path                 Print the resolved profile file path
```

### `profile list`

```
$ ntk profile list
   NAME     BOOTSTRAP                    AUTH            TLS   FLAGS
   dev      localhost:9092               none            -
 * prod     kafka-1.prod:9093 (+2)       scram-sha-512   mtls  read-only, prod
   staging  kafka.staging:9093           plain           tls
```

### `profile test`

This runs a staged check and stops at the first failure. Each stage's error comes with a hint for fixing it.

```
$ ntk profile test prod
✓ DNS        kafka-1.prod → 10.0.3.11
✓ TCP        kafka-1.prod:9093 (12ms)
✓ TLS        server cert CN=kafka-1.prod, issuer CN=Prod CA, expires in 212d
✓ Client cert CN=bob,OU=eng, expires in 41d
✓ SASL       scram-sha-512 as bob
✓ Metadata   cluster lkc-9f2 · 3 brokers · controller 2 · Kafka 3.8
✓ Principal  User:bob (configured)
```

Example failure hint: `✗ SASL: authentication failed. Check auth.username/password; the broker listener may expect scram-sha-256 instead of scram-sha-512.`

### `profile set` / `profile switch`

```
$ ntk profile set staging
Active profile: staging

$ ntk profile switch        # fuzzy picker (huh Select with filtering)
```

If stdout isn't a TTY, `profile switch` without an argument fails with a usage error. In the TUI, `:ctx` opens the same picker ([TUI](tui.md)).

## Interactive creation wizard

`ntk profile create` uses `huh` forms. Each step validates as you go, and choices change which steps appear later.

1. **Name**: validated for allowed characters and uniqueness.
2. **Bootstrap servers**: comma-separated. Each is checked for `host:port` format.
3. **Security**: choose one of
   - Plaintext (no TLS, no auth)
   - TLS (server verification only)
   - mTLS (client certificate)
   - SASL over TLS
   - SASL without TLS (warns that credentials are sent in the clear)
4. **TLS details** (if TLS): CA source (system roots / file / paste PEM), optional server name, optional skip-verify (with a warning).
5. **Client certificate** (if mTLS): cert file, key file, key passphrase (masked input). ntk checks that the files exist, the key matches the cert, and the cert hasn't expired.
6. **SASL** (if SASL): mechanism (PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512), username, password (masked). Future mechanisms show up here automatically via the auth registry.
7. **Principal**: pre-filled with the derived principal. You can accept it or override it.
8. **Safety**: read-only? color? labels (suggests `prod` if the name contains "prod")?
9. **Test connection**: runs `profile test`. On failure you can go back to edit, save anyway, or cancel.
10. **Save**: the file is created with `0600` if it's new. If this is the first profile, or you choose to, the profile is set active.

### Non-interactive creation

All wizard fields are also flags, for scripting and CI:

```
ntk profile create ci \
  --bootstrap kafka:9093 \
  --tls --ca-file /ci/ca.pem \
  --sasl scram-sha-512 --username ci --password-stdin \
  --read-only
```

`--password-stdin` (and `--key-password-stdin`) read the secret from stdin so it doesn't end up in shell history. Passing `--password` directly works but prints a warning.

`ntk profile create --from-properties client.properties` imports a Java client properties file: `bootstrap.servers`, `security.protocol`, `sasl.mechanism`, `sasl.jaas.config`, `ssl.truststore.*` / `ssl.keystore.*`, `ssl.key.password`. PEM keystore types can be imported directly. JKS/PKCS12 imports are [open questions](#open-questions).

## Open questions

- Should JKS/PKCS12 keystores be supported directly, or only converted to PEM at import time?
- Should `profile edit` support `$EDITOR` editing of the raw JSON for one profile, with validation on save?
- Should we allow a project-local profile file (e.g. `./.ntk/profiles.json`) that's merged on top of the user file?
