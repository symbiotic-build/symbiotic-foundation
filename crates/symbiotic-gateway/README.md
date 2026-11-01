# Symbiotic Gateway

`symbiotic-gateway` forwards trusted same-user HTTP requests from a Unix socket to
configured HTTPS destinations. It injects owner-only static credentials or renewable
login credentials. The caller's body and supported streaming responses keep their
existing passthrough behavior. Account admission, cooldowns, pacing and Chat
Completions spend accounting use `symbiotic-ai-runtime`.

When configured, the health file reports route login state, cooldowns, retries,
waiting requests, rate limits, cost and balance. Shutdown publishes `stopping`
before closing the listener. Login import converts a supported native login into
an owner-only renewable store.

## Configuration

The JSON configuration refuses unknown fields and has no `version` field:

- `log`: optional logging directives; `SYMBIOTIC_LOG` overrides them.
- `state_dir`: owner-only runtime directory containing the existing account database.
- `max_secret_bytes`: maximum credential bytes; at least 32.
- `max_header_bytes`: maximum HTTP request header bytes; at least 4 124, preserving the previous top-level minimum.
- `io_timeout_seconds`: positive caller socket I/O timeout.
- `passthrough`: required server configuration with `socket_path`, optional `health_path`,
  positive `max_connections`, `routes` and optional `retry_policies`.

The `passthrough` object retains its existing JSON shape. Routes retain their
method/path bindings, destinations, secret sources, stream dialects, header
allowlists, byte limits, account policies, retry settings and optional prices,
spend limits, balance polling and new-session settings. Secret backends are
`owner_only_file` and `login_file`. See the
[passthrough contract](../../docs/architecture/model-egress.md#passthrough-routes-and-login-accounts)
for the route fields and validation rules.

The gateway protects the process before reading configuration or secrets. It
retains `credential-process.lock` so a gateway and typed credential process cannot
open the same state directory concurrently. Provision the socket's parent as an
owner-only directory. The gateway does not open a typed socket or model registry.

## Command line

```sh
symbiotic-gateway <config>
symbiotic-gateway --child <config>
symbiotic-gateway login import --family <chatgpt_codex|xai_grok|kimi_code> --native <path> --store <path> [--replace]
```

`<config>` names a regular file owned by the gateway's user, with no group or other
permissions (`0600` or stricter), at most 1 MiB, and not a symbolic link as its last
path component. Any other file fails with `credential unavailable` before startup.

`--child` watches its parent through `symbiotic-supervise`. The server runs until
SIGTERM or SIGINT. Import refuses an existing store unless `--replace` is given.
