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

## Error classification, retries and concurrency

For HTTP error bodies and stream error events, the gateway selects a code in this order:

1. Use the provider's string `error.code`.
2. If no code exists, use the first matching `error_messages` entry's `code`.
3. If neither exists, use its string `error.type`.

Message rules come before `error.type` because Kimi Code sends its quota and
risk-control 429s with the generic type `rate_limit_error`.

Stream terminal events use the error under `response.error`; flat stream error codes
remain supported. Message rules match `error.message` by case-insensitive substring.
The selected code first uses the route's `error_classes` override, then the adapter's
built-in classification. Without an extension match, the adapter keeps its usual
fallback code and HTTP status classification. Provider differences belong in route
configuration. For Kimi and MiMo quota and risk-control failures, add these route fields:

```json
"error_classes": {"exceeded_current_quota_error": "final", "provider_quota_exhausted": "final", "provider_risk_control": "final"},
"error_messages": [
  {"contains": "usage limit", "code": "provider_quota_exhausted"},
  {"contains": "credit balance is insufficient", "code": "provider_quota_exhausted"},
  {"contains": "extra usage balance has run out", "code": "provider_quota_exhausted"},
  {"contains": "extra usage limit set for your account", "code": "provider_quota_exhausted"},
  {"contains": "token plan has been exhausted", "code": "provider_quota_exhausted"},
  {"contains": "concurrent request limit", "code": "provider_risk_control"}
]
```

The Kimi entries cover the seven account-limit messages the passthrough
recognized before this configuration existed; a Kimi route without them retries
those 429s as throttles, up to the throttle attempt limit.

`error_messages` is optional and defaults to an empty list. It accepts at most 32
entries. Each `contains` must be non-empty and at most 256 UTF-8 bytes. Each `code`
must contain 1–128 printable ASCII bytes. An explicit provider `error.code` takes
precedence over message rules, even when its message matches.

Default retries allow 10 total attempts for `throttle`, three for `server`, and
unlimited attempts for `network`; `final` fails immediately. Attempts include the
first send. A route's `retry_policy` overrides each class, including unlimited
throttle retries through an explicit rule with absent or null `max_attempts`.
Retry-After still takes precedence over account backoff. The attempt count is per
caller request, the first send included. When attempts run out, the caller gets the
same response as any `fail` decision:

- an HTTP failure returns the provider's status with the fixed local body
  `{"error":{"code":"upstream_status"}}`, for example 429 after ten throttles;
- a failure inside a stream returns HTTP 200 with the provider's own failure events,
  scrubbed of credentials and in their original order.

A cooldown is recorded only for a `retry_same` decision, so the final throttle sets
none. It still halves the account's adaptive concurrency limit.

Each account scope shares one memory-only adaptive concurrency limit across its
routes. It starts at `max_in_flight`, which remains its ceiling. Waiters get
permits in arrival order. A request acquires its adaptive permit after any
new-session hold or probe wait and before runtime account admission, and holds it,
with its runtime slot, through pacing waits and retries until the response ends. A
new-session request gives back its permit and its slot together only when it must
wait for a hold or the probe, so a new-session refusal never blocks sessions
already admitted, and no request waits for a permit while holding the slot another
waiter needs. Permit waiting uses the existing `slot` wait log
and health entry. A classified `throttle` halves the limit, rounding down with a
floor of one, even when retry policy says fail. Existing holders keep their permits;
the permits they return pay off the part of the decrease that could not take effect at once.
After 10 successful responses with no intervening throttle, the limit increases by
one up to its ceiling. The success counter resets on a throttle and after each
increase. Each change logs the route, account and new effective limit at info level.
The health file keeps its existing fields.

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

Every upstream attempt produces one record when that attempt ends, including
failures and continuations. Attempt 1 uses the caller's `x-symbiotic-request-id`;
attempt n ≥ 2 uses `<request_id>.<n>`. Requests without a caller id receive a local
UUID. Every attempt's `job_id` removes the caller id's final hyphen and suffix,
or keeps the whole caller id when it has no hyphen. A matched request that ends
before dispatch produces one local record. A quota refusal before a later attempt
uses that attempt's numbered id. Unmatched requests produce none. A job's cost in
the core is the sum of its attempts' records, so a retried or continued answer is
counted once per upstream call.

Records contain the attempt's account and route, served model (falling back to
the requested model), upstream HTTP status and end time in UTC. An attempt cut off
after its status arrived keeps that status; an attempt that failed before any
status records 502. Local records use the local response status.
An account is `shared:<key>` when its route has `account_sharing_key`; otherwise
it is `<tenant>/<account>`. Inside each part, `%` becomes `%25` and `/` becomes
`%2F`, so `org/team` + `prod` (`org%2Fteam/prod`) and `org` + `team/prod`
(`org/team%2Fprod`) stay distinct. Records, gates, `budget.get` arguments and the core's
`budgets[].account` all use this string. Independent tenants keep separate gates;
routes with the same sharing key share a gate.

Readable usage from each attempt retains the provider's complete `usage` object
beside normalized input, cached input, output and reasoning token counts, even
when the response fails or disconnects. Missing counts stay absent. Usage is null
only when the attempt reports no readable usage. Recognized headers, including
`retry-after`, `x-ratelimit-*` and Codex quota headers, remain in `meter.provider`.
Numeric readings use the provider's unit. DeepSeek's latest successful balance
poll adds its currency, remaining balance and poll time. Optional
`balance.allowance` is a decimal string in that currency and supplies the reading's limit.

The gateway queues records to one SQLite writer as each attempt ends. Recording
never blocks or fails the call. `state_dir/outbox.sqlite` uses WAL,
`synchronous=FULL` and `user_version` 1. Migration 1 creates `outbox`, which orders
records by `seq` and makes `request_id` unique, and `gate`, which stores each
account's latest local and core observations, including `over_limit_allowed`.
The native outbox cost is one insert per attempt, an indexed head read and one
delete per acknowledged delivery. Gate observations add one account upsert per
observation update. The writer keeps disk work off the request handler.
Failed writes log `outbox_write_failed` or `gate_write_failed`.

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
`over_limit_allowed`. Only Codex readings gate requests (operator decision 5,
2026-11-01); a DeepSeek budget at its allowance warns through the core and does
not refuse. A passed reset time opens the gate, and an `over_limit_allowed`
answer lapses with the reset of the window it answered, so a later window at
100 % refuses again even while the core is unreachable. Closed gates return local
HTTP 402 with `quota_exhausted`; every quota refusal also records
`refused: "quota_exhausted"` and null usage. Every gate transition logs the
account, reading name and reason.

The gateway asks the core for `budget.get` at startup and at most once every 10 s
while closed gates receive requests. Each lookup times out after 2 s. A failed
lookup preserves the gate. An offline core does not prevent startup or forwarding
through open gates. Records accumulate in the outbox until delivery resumes.
Opening restores gate observations before admission. An exhausted account stays
closed across restart during a core outage until reset or core permission.
The core owns budgets and credit decisions. The health file adds no record or quota fields and omits the retired `cost` field.
