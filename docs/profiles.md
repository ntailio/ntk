# Profiles

A profile is a named connection: bootstrap servers, TLS and auth settings, and a few safety flags. One file holds all of them.

![profiles: list, staged connection test, whoami, and a read-only profile refusing a delete](../assets/demos/profiles.gif)

## Create one

```sh
ntk profile create                                   # interactive wizard
ntk profile create prod --from-properties client.properties
ntk profile create ci --bootstrap kafka:9093 --tls --ca-file ca.pem \
    --sasl scram-sha-512 --username ci --password-stdin < secret.txt
```

Supported: PLAINTEXT, TLS (with system or custom CA), mTLS with a client certificate (encrypted keys too), SASL PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512, in any combination with TLS.

The wizard tests the connection before saving. You can rerun that test any time, step by step:

```sh
ntk profile test prod
✓ DNS         kafka.example.com → 10.0.0.12
✓ TCP         kafka.example.com:9093 (14ms)
✓ TLS         server cert kafka.example.com, issuer Example CA, expires in 212d
✓ SASL        scram-sha-512 as svc-ops
✓ Metadata    cluster abc123 · 6 brokers · controller 2 · Kafka 3.9
✓ Principal   User:svc-ops (derived)
```

When something fails, the failing step says why and what to try.

## Switch between clusters

```sh
ntk profile list              # * marks the active one
ntk profile set staging       # make it the default
ntk -p prod topic list        # one-off
export NTK_PROFILE=prod       # for a whole shell session
```

`ntk whoami` shows which principal you are and what the cluster lets you do.

## Keep production safe

Two fields in a profile change how ntk behaves:

- **`read_only: true`** refuses every write. Good for the profile you use to look at prod.
- **label `prod`** makes destructive commands ask you to type the resource name, even with `-y`. The profile's colour also shows in every prompt and in the TUI header.

```sh
ntk profile copy prod prod-admin          # a second profile for the rare write
ntk profile edit prod --set read_only=true
ntk profile edit prod --set labels=prod --set color=red
```

## Where the file lives

`~/.config/ntk/profiles.json` (or `$XDG_CONFIG_HOME/ntk/profiles.json`). Point ntk somewhere else with `--profile-path` or `NTK_PROFILE_PATH`, which is handy for a repo-local file like the sandbox's.

ntk warns if the file is readable by others; `ntk profile fix-perms` fixes it. Certificate paths in the file can be relative to the file, so a profile file can travel with its certs.

## Other commands

`show` (secrets masked, `--reveal` to see them), `edit`, `rename`, `copy`, `delete`, `current`, `path`, `switch` (interactive picker).
