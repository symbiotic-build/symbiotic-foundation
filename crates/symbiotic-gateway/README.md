# Symbiotic Gateway

`symbiotic-gateway` forwards trusted same-user HTTP requests from a Unix socket to
configured HTTPS destinations. It injects owner-only static credentials or renewable
login credentials. The caller's body and supported streaming responses keep their
existing passthrough behavior. Account admission, cooldowns and pacing use `symbiotic-ai-runtime`.

When configured, the health file reports route login state, cooldowns, retries,
waiting requests, rate limits and balance. Shutdown publishes `stopping`
before closing the listener. Login import converts a supported native login into
an owner-only renewable store.

## Configuration

The JSON configuration refuses unknown fields and has no `version` field:

- `log`: optional logging directives; `SYMBIOTIC_LOG` overrides them.
- `state_dir`: owner-only runtime directory containing the account database and `outbox.sqlite`.
- `core_socket_path`: required absolute Unix socket path for the core service.
- `max_secret_bytes`: maximum credential bytes; at least 32.
- `max_header_bytes`: maximum HTTP request header bytes; at least 4 124, preserving the previous top-level minimum.
- `io_timeout_seconds`: positive caller socket I/O and core record-delivery timeout.
- `passthrough`: required server configuration with `socket_path`, optional `health_path`,
  positive `max_connections`, `routes` and optional `retry_policies`.

The `passthrough` object retains its existing JSON shape. Routes retain their
method/path bindings, destinations, secret sources, stream dialects, header
allowlists, byte limits, account policies, retry settings, balance polling and new-session settings. Secret backends are
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

## Request records and the quota gate

Every caller request that matches a route produces one record after its response
ends, including failures and local refusals. Retries share that record. Unmatched
requests produce none. The caller's `x-symbiotic-request-id` identifies the record;
requests without it receive a local UUID. `job_id` removes the final hyphen and
suffix, or keeps the whole identifier when it has no hyphen.

Records contain the matched account and route, served model (falling back to the
requested model), returned HTTP status and completion time in UTC. Successful
Responses and Chat Completions responses retain the provider's complete `usage`
object beside normalized input, cached input, output and reasoning token counts.
Missing counts stay absent. Missing usage and incomplete responses have null usage.
Quota headers remain intact in `meter.provider`; numeric readings use the
provider's unit. DeepSeek's latest successful balance poll adds its currency,
remaining balance and poll time. Optional `balance.allowance` is a decimal string
in that currency and supplies the reading's limit.

The gateway queues completed records to one SQLite writer after the response ends.
`state_dir/outbox.sqlite` uses WAL, `synchronous=FULL` and numbered migrations
through `user_version`. Its sole table orders records by `seq` and makes
`request_id` unique. This costs one native SQLite insert per completed request, an indexed head read,
and one delete per acknowledged delivery. The worker keeps disk writes off the
request handler. A failed write logs `outbox_write_failed` with the request id.

One drain task sends records in order over `core_socket_path` using the core's
`request.record` JSON-line protocol. It reuses its connection while draining. Each delivery uses `io_timeout_seconds`.
Success removes the row and updates the quota gate from the returned budgets.
`request_id_conflict` logs an error and removes the row. Other failures retain
rows and retry with delays doubling from 1 s to 30 s. New records wake the drain.
Startup drains records retained across restarts.

A Codex percent reading carries a limit of 100 only when its window is 10 080
minutes. Other header readings carry no limit. Before every upstream attempt,
the gate compares each account's latest local reading with the core's latest
budget, using reading time. Used quota at or above its limit, or a core budget
marked `exhausted`, closes the gate unless the core's latest budget permits
`over_limit_allowed`. A passed reset time opens it. Closed gates return local
HTTP 402 with `quota_exhausted`; a request refused before any attempt also records
`refused: "quota_exhausted"` and null usage. Every gate transition logs the
account, reading name and reason.

The gateway asks the core for `budget.get` at startup and at most once every 10 s
while closed gates receive requests. Each lookup times out after 2 s. A failed
lookup preserves the gate. An offline core does not prevent startup or forwarding
through open gates. Records accumulate in the outbox until delivery resumes.
Quota state stays in memory; the core owns budgets and credit decisions. The
health file adds no record or quota fields and omits the retired `cost` field.
