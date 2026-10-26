---
status: approved
issue: 156
---

# 156 — Login accounts and agent passthrough

This record designs two additions to the credential process for
[#156](https://github.com/symbiotic-build/symbiotic-foundation/issues/156):

1. a **login-account credential source**, for a subscription login that renews its
   own access token;
2. a **passthrough route**, which forwards an agent tool's own streaming HTTP request
   through the account's existing queue limits without decoding it.

Nothing here is implemented yet. The operator approved this record on 2026-10-25
(§Operator decisions). It changes a security boundary and the
[boundary contract](../architecture/boundary.md), so only the operator could approve it.

## Operator decisions

The operator decided on 2026-10-25:

- **Login and passthrough in the credential process:** one credential process per
  account owns the login and forwards the account's agent traffic; the host relays to
  it over the passthrough socket (§Design).
- **No signed admission on passthrough routes:** the socket's same-UID peer check is
  the caller check (§What passthrough does not reuse).
- **Foundation waits out a rate limit for its callers:** during an account cooldown
  nothing goes out, and every caller waits in Foundation until the account-wide
  deadline. A request that itself got 429 is held and resent after the deadline. No
  429 reaches the caller. A maximum wait exists as a setting and is off by default.
  Slot waits are unbounded while the caller stays connected (key decisions 4 and 5).

## Lead decisions

The lead decided on 2026-10-26:

- **Shared account policy:** passthrough supports `max_in_flight` and
  `requests_per_minute`, with `rate_burst_seconds` controlling request pacing.
  Undecoded requests are not charged input units. Typed routes sharing the same
  `(tenant, account)` or sharing key must set `input_units_per_minute: null` and
  `provider_request_limit: null`. All bindings must agree on `max_in_flight`,
  `requests_per_minute` and `rate_burst_seconds`; otherwise startup returns
  `InvalidRequest`. Admission uses the existing runtime, with no new scheduler.
- **Cooldown and send ordering:** the existing per-account rate gate orders every
  send against cooldown recording. First sends and resends take the gate, re-read
  the cooldown, charge pacing, start the send and release the gate. Recording a
  cooldown after a 429 takes the same gate (§Admission).
- **Retention approach for this step:** the scrubber holds an open delta item's
  accumulated text until the item completes. Whole-item retention is already
  implemented and tested in the stream module. Holding only events that intersect
  an unresolved credential-length suffix is the target design, tracked as
  [#161](https://github.com/symbiotic-build/symbiotic-foundation/issues/161); it is
  not yet qualified for interleaved fields (§Scrubbing).
- **Hold limits are provisional route settings:** `max_held_text_bytes` defaults
  to 4 194 304 and `max_held_event_bytes` to 67 108 864. Both are positive byte
  limits per stream, with Serde defaults. They are provisional settings under the
  [boundary contract's bounds rule](../architecture/boundary.md#bounds-as-labelled-settings),
  not measured maxima. Exceeding either ends the stream with an error (§Scrubbing).
  `rate_burst_seconds` also defaults to 0 with `#[serde(default)]`, so existing
  configurations stay valid.

## Problem

Agent command-line tools such as the Codex CLI speak their provider's streaming
HTTP protocol themselves. A host that runs many of them on one account today keeps
its own token store and lets each tool's requests reach the provider on their own.
Foundation's shared queue never sees them.

The shared queue already does what such a host needs: per-account in-flight slots,
a cooldown after a rate limit, request pacing and usage receipts
([AI runtime §Current shared limits](../architecture/ai-runtime.md#current-shared-limits)).
Two parts are missing:

- **No login credential.** [Model egress §Deployment and credentials](../architecture/model-egress.md#deployment-and-credentials)
  allows three secret sources: `none`, an owner-only file with a fixed value, and an
  app-supplied resolver. None of them renews a token or stores a rotated one.
- **No undecoded route.** Every provider adapter sends one typed request and decodes
  one answer. The socket protocol has "no line framing, compression, stream
  multiplexing or raw HTTP forwarding" ([model egress §Wire format](../architecture/model-egress.md#wire-format)).

Without both, ten tools on one account that start a burst each receive their own
HTTP 429. Each tool backs off alone, and the next burst repeats it.

### The limit is on concurrent streams

Public reports indicate that subscription logins are limited by the number of
simultaneous streams, not only by usage:

- [openai/codex#41702](https://github.com/openai/codex/issues/41702) (open): repeated
  429s on one Pro login with four or five agents running across sessions.
  [#42685](https://github.com/openai/codex/issues/42685) (closed as its duplicate)
  reports failures beyond about two or three simultaneous streams.
- [#53978](https://github.com/openai/codex/issues/53978),
  [#54010](https://github.com/openai/codex/issues/54010) and
  [#55805](https://github.com/openai/codex/issues/55805) (open) report 429s with
  70–90% of the weekly allowance left. #55805 reports 155–288 s retry waits.

`ASSUMPTION` (reported by these issues and by an operator's own login, not a
provider specification): the same login answers single requests normally while
parallel requests get 429, and the `Retry-After` values of concurrent requests count
down to one shared deadline.

So the per-account in-flight limit is the main control, and `Retry-After` is an
account-wide deadline that every queued caller must respect. The design keeps
`max_in_flight` required, with no default, and the operator sets it low for a login
account (two or three streams to start).

## Terms

- **Login account:** a provider account reached through a subscription login, not
  an API key. It holds a short-lived **access token** and a **refresh token**. For
  example, a ChatGPT login used by the Codex CLI.
- **Renewal:** exchanging the refresh token for a new access token at the
  provider's token endpoint. The provider may rotate the refresh token too, and the
  old one then stops working.
- **Login family:** the fixed provider protocol of a login: issuer, client ID,
  token endpoint, renewal encoding and the headers that carry the token. Families are
  a closed list in code, like the existing provider list.
- **Login store:** the owner-only JSON file that holds one login account's tokens.
- **Passthrough route:** a configured pair of one caller path (for example
  `POST /v1/responses`) and one fixed provider URL. The credential process injects
  the account's credential and forwards the caller's request body unchanged.
- **Host:** the trusted program that connects to the passthrough socket on behalf of
  agent tools, for example a sandbox launcher's per-job proxy. Agent tools never
  connect to it directly.
- **Account cooldown:** the existing durable "do not send before" time for an account
  queue, stored by `QueueBackend::note_cooldown` and read by `cooldown_until`.
- **Maximum cooldown wait:** `max_queue_wait_seconds`, an optional route setting, off
  by default. When set, a request that has waited that long for cooldowns gets 429
  with the remaining cooldown. Waiting for a free slot never counts against it.
- **Stream dialect:** which provider event format a route streams, so the scrubber
  and usage reader know where text and usage are. Two exist: OpenAI Responses and
  OpenAI Chat Completions.

## Walkthrough

One credential process serves login account `acct-1` with `max_in_flight: 2` and no
maximum cooldown wait. It publishes health through `health_path`. Three agent
tools (A, B and C) run under one host.

**Procedure: three tools during a provider rate limit**

1. **A sends a turn:** `POST /v1/responses` with a placeholder token, to the host.
   - Before starting the tools, the host reads the published health file and checks
     its `updated_at` is no older than 30 s and the route's login is usable.
   - The host checks its own sandbox rules and forwards the turn over the passthrough socket.
   - The credential process takes one of the two slots and sees no cooldown.
2. **The credential process forwards A's request:** injects the access token and
   the family's account header, and sends it to the route's fixed URL.
   - The provider answers HTTP 429 with `Retry-After: 30`.
   - The credential process records a cooldown until now + 30 s on `acct-1`.
   - A's request keeps its slot and waits; A's tool receives nothing yet.
3. **B arrives 5 s later:** the cooldown has 25 s left.
   - B's request takes the second slot and waits; nothing is sent to the provider.
4. **C arrives 10 s later:** both slots are taken, so C waits for a slot.
5. **The cooldown ends:** A's request is resent with the same body, then B's.
   - A's and B's tools each see one slow response, not a 429.
6. **A's stream finishes:** the final `response.completed` event carries usage.
   - The credential process records A's input, cached, output and reasoning tokens
     in a settled spend receipt and releases the slot. C takes it.

The provider receives no request after the cooldown is recorded until it ends.
No tool sees a 429. If B had already been sent before A's cooldown was recorded, B
could also receive 429. Its request would wait out the shared cooldown and be resent.

## Design

### Login-account credential source

A new `SecretSource` variant names a login store and its family:

```rust
pub enum SecretSource {
    None,
    OwnerOnlyFile { path: PathBuf },
    /// Renewable login. Only this process reads, renews and rewrites the store.
    LoginFile { path: PathBuf, family: LoginFamily },
    #[serde(skip)]
    Resolver { name: String, resolve: Arc<SecretResolver> },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoginFamily {
    /// ChatGPT subscription login used by the Codex CLI.
    ChatgptCodex,
    /// xAI subscription login used by the Grok CLI.
    XaiGrok,
}
```

```json
{"backend":"login_file","path":"/private/egress/acct-1/login.json","family":"chatgpt_codex"}
```

`LoginFile` is valid only on passthrough routes. A typed route that names it is
refused by `validate_routes` with `InvalidRequest`, because these logins only
authorize the provider's own agent endpoints.

**Store format.** The login store is one owner-only regular JSON file, version 1:

```json
{
  "version": 1,
  "family": "chatgpt_codex",
  "issuer": "https://auth.openai.com",
  "client_id": "<family client id>",
  "access_token": "…",
  "refresh_token": "…",
  "id_token": "…",
  "account_id": "…",
  "principal_type": null,
  "principal_id": null,
  "expires_at": 1793000000
}
```

`id_token`, `account_id`, `principal_type` and `principal_id` are optional where the
family does not use them; `account_id` is required for `chatgpt_codex`. Unknown fields
are refused. `issuer` and `client_id` must equal the family's fixed values. The file
follows the existing owner-only file rules: owned by the process UID, regular, no
group or other permission bits, not a symlink, read with the `max_secret_bytes` bound.

**Families.** Each family fixes these values in code:

| Family | Token endpoint | Renewal body | Injected headers |
|---|---|---|---|
| `chatgpt_codex` | `{issuer}/oauth/token` | JSON | `Authorization: Bearer`, `ChatGPT-Account-ID`, `originator` |
| `xai_grok` | OIDC discovery at `{issuer}`, exact host only | form | `Authorization: Bearer`, `X-XAI-Token-Auth` |

Discovery accepts only a token endpoint on the issuer's exact HTTPS host. The token
endpoint is never caller-supplied and never follows redirects. Discovery and token
exchange each have a 30 s timeout, separate from the socket frame I/O timeout.
Renewal errors use static diagnostic codes, never provider or transport error text.

**Renewal.** The process renews when the access token expires within 300 s, or once
after the provider answers 401 to a forwarded request. Renewal runs under one async
mutex per login store, so concurrent requests near expiry cause one token exchange.
Requests that wait on that mutex use the renewed token.

**Procedure: one renewal**

1. **Write the renewal fence:** set a nonempty `<path>.renewal` file and sync it and
   its directory.
   - The fence means "an exchange may have rotated the refresh token".
2. **Exchange the refresh token:** one HTTPS request to the family's token endpoint.
3. **Install the result:** write the new store atomically (temporary file, sync,
   rename, directory sync).
4. **Clear the fence:** truncate `<path>.renewal` and sync it.

If the process stops between steps 1 and 4, the fence stays set. At the next start,
or the next renewal, a set fence refuses the account with `login_required` and sends
no token exchange. Reusing a refresh token that the provider may already have rotated
can make the provider revoke the whole login. A fresh login or import clears the
fence only after the replacement store is durable (§Import). A failed exchange that
the provider answered (for example 400 `invalid_grant`) leaves the fence set and the
account in `login_required`. A timeout or failure before any token-exchange request
bytes were written clears and syncs the fence; renewal may run at the next request.
This includes discovery failure or timeout, because no exchange has started. Once
exchange bytes may have been written, a timeout or failure leaves the fence set and
the account in `login_required`; no exchange is retried until a fresh import.

**One owner.** The credential process takes an exclusive, non-blocking `flock` on
`<path>.lock` at startup and holds it for its lifetime. The import command below takes
the same lock and is refused while the process runs. There is no shared lock, no
cross-process renewal lock and no adoption of another process's renewal: one process
owns each login store.

**Import.** `symbiotic-credential-process login import --family <family> --native <file> --store <path>`
reads the provider tool's own login file (the Codex CLI's `auth.json`, the Grok CLI's
session file), checks its family, issuer, client ID, scope and token expiry, and
writes a version-1 store. It refuses an existing store unless `--replace` is given.
It prints no token. It runs no provider tool and sends no request: the operator signs
in with the provider's own tool first. After import, only the credential process may
renew the login; the native file becomes stale at the first rotation.
Import installs the new store durably using a temporary file, file sync, rename and
directory sync. Only then does it clear and sync the renewal fence. A failure or
stop before the durable install leaves the fence set, so restart cannot renew with
the old store's possibly rotated refresh token.

**Health.** `PassthroughConfig.health_path` optionally names a sanitized JSON file
for hosts that must read health without access to the passthrough socket. When
configured, the process publishes one entry per route and a top-level `updated_at`
in Unix seconds:

```json
{"updated_at":1792000000,"routes":[{"name":"codex_responses","login":"ok","expires_at":1793000000,"cooldown_until":null}]}
```

`login` is `ok`, `expiring` (expires within two days), `renewal_failed` or
`login_required`; static-key routes report `static` with `expires_at: null`.
On a clean stop (SIGTERM or SIGINT), every route reports `stopping` before the
process closes its socket.
The file contains only route names, these fixed login states, expiry and cooldown
times, and `updated_at`. It contains no credential, path or provider text.

The process writes at startup, on every state change, and at least every 10 s.
Each publication uses a temporary file in the destination directory, sync, rename,
and directory sync. The file has mode 0640. Its group comes from the directory's
setgid bit, so the deployment chooses who can read it. The deployment provisions
that directory separately from the owner-only socket directory. The socket's
permissions and same-UID peer check stay unchanged. The socket has no health route.

Hosts read the file before starting work. A missing file, `updated_at` older than
30 s, or `stopping` means service unavailable. After a crash, the last healthy file
can stay fresh for up to 30 s. Work admitted in that window encounters the same
connection failure as work whose service fails mid-run.
A publication failure is reported with a sanitized diagnostic; readers reject the
previous file once it becomes stale.

### Passthrough route

`ProcessConfig` gains one optional section. Omitting it changes nothing, and the
configuration version stays 4.

```rust
pub struct ProcessConfig {
    // … existing fields …
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passthrough: Option<PassthroughConfig>,
}

pub struct PassthroughConfig {
    /// Socket inside an existing owner-only directory; distinct from `socket_path`.
    pub socket_path: PathBuf,
    /// Optional sanitized health file in a deployment-provisioned setgid directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub health_path: Option<PathBuf>,
    /// Maximum concurrent passthrough connections, including waiting ones.
    pub max_connections: usize,
    pub routes: Vec<PassthroughRoute>,
}

pub struct PassthroughRoute {
    /// Route name, unique in this process; reported by health and receipts.
    pub name: String,
    pub tenant: String,
    pub account: String,
    pub account_sharing_key: Option<symbiotic_ai_runtime::AccountSharingKey>,
    /// Exact caller method and origin-form path, for example POST /v1/responses.
    pub method: PassthroughMethod,
    pub path: String,
    /// Exact HTTPS provider URL; no userinfo, query, fragment or redirect.
    pub destination: String,
    /// `login_file` or `owner_only_file`.
    pub secret: SecretSource,
    pub stream: StreamDialect,
    /// Lowercase caller headers copied to the provider; all others are dropped.
    pub forward_headers: Vec<String>,
    /// Lowercase provider headers copied back only with a provider answer.
    /// See Returned headers for the status-specific allowlist and local responses.
    #[serde(default)]
    pub return_headers: Vec<String>,
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    /// Provisional hard byte limits per stream; both must be positive.
    #[serde(default = "default_max_held_text_bytes")]
    pub max_held_text_bytes: usize,
    #[serde(default = "default_max_held_event_bytes")]
    pub max_held_event_bytes: usize,
    /// Existing account policy fields, shared with typed routes on the same account.
    pub max_in_flight: usize,
    pub requests_per_minute: Option<u32>,
    /// Existing pacing burst duration; must match every shared binding.
    #[serde(default)]
    pub rate_burst_seconds: u64,
    /// Optional maximum total cooldown wait before answering 429; off when absent.
    /// Slot waits never count against it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queue_wait_seconds: Option<u64>,
    /// Longest silence between two upstream bytes; there is no total deadline.
    pub idle_timeout_seconds: u64,
}

#[serde(rename_all = "snake_case")]
pub enum PassthroughMethod { Get, Post }

#[serde(rename_all = "snake_case")]
pub enum StreamDialect { OpenaiResponses, OpenaiChatCompletions }
```

New public names follow the existing configuration conventions: lower snake case
fields, snake case enum tags, subject first (`max_queue_wait_seconds`, not
`queue_wait_max_seconds`). `health_path` follows the same field convention.
Public types are UpperCamelCase nouns: `PassthroughConfig`, `PassthroughRoute` and
`LoginFamily`. Error and diagnostic reasons are lower snake case, subject first:
`credential_unavailable`, `route_not_found`, `rate_limited` and `upstream_status`.
Command options are lower kebab case: `--family`, `--native`, `--store` and `--replace`.
External protocol names, including header names and provider fields, keep their
external spelling.

**Transport.** The passthrough socket speaks HTTP/1.1, one request per connection,
and answers with `Connection: close` and a chunked body. It uses the existing socket
rules: owner-only directory, the bind lock beside the socket, and a peer UID equal to
the process UID. A host therefore runs as the same UID as the credential process.
Request parsing is strict: one origin-form path without query, percent, fragment or
backslash; no duplicate headers; no `Transfer-Encoding` or `Expect`; a
`Content-Length` within `max_request_bytes`. The caller's `Authorization`,
cookies, `Host` and forwarding headers are never copied.

**Admission.** Each passthrough route registers its account with the runtime exactly
like a typed route. Typed and passthrough routes on the same `(tenant, account)` or
sharing key share one slot gate, one cooldown and one pacing bucket. Passthrough
supports only the account's `max_in_flight` and `requests_per_minute` limits, with
`rate_burst_seconds` controlling pacing burst duration; it charges no input units.
Typed routes may share the account only with `input_units_per_minute: null` and
`provider_request_limit: null`. Every binding must agree on `max_in_flight`,
`requests_per_minute` and `rate_burst_seconds`. Configuration validation refuses
incompatible sharing at startup with `InvalidRequest`.

The ordering point is the runtime's existing per-account rate gate, the lock held
from its pacing check through the claim
([AI runtime §Current shared limits](../architecture/ai-runtime.md#current-shared-limits)).
Typed sends pass this same gate. Every passthrough send, first or resent, takes the
gate, rechecks login state, re-reads the durable cooldown and checks pacing admission.
Only after those checks pass does it reserve one request in the spend ledger,
charge pacing, start the send and release the gate. A waiting request creates no
reservation; each actual send creates exactly one.
Recording a cooldown after a 429 takes that gate too. A send therefore either starts
before the cooldown is recorded and is already in flight, or sees the cooldown and
waits. The gate is released before any cooldown or pacing wait and before streaming
the answer. This extends the existing runtime mechanism; it adds no scheduler.

**Procedure: admitting one passthrough request**

1. **Match the route:** method and path select one route; anything else gets 404
   `route_not_found`.
2. **Check the login:** a route in `login_required` or `renewal_failed` answers 503
   `credential_unavailable` and sends nothing. Recheck after every wait and
   immediately before dispatch. This uses the process's current state; a host's
   earlier health-file check does not replace it.
3. **Wait for a slot:** FIFO, as long as the caller stays connected.
   - A slot wait is the normal state of a low in-flight limit, not a failure.
   - A caller that disconnects while waiting leaves the queue; nothing is sent.
4. **Wait out the cooldown:** while `cooldown_until` is in the future, send nothing.
   - The request keeps its slot; during a cooldown no slot could send anyway.
   - With `max_queue_wait_seconds` set, a request whose cooldown waits reach it gets
     a fixed 429 with a locally computed `Retry-After` for the remaining cooldown
     and no upstream header (§Returned headers).
5. **Keep the caller waiting:** see §Long waits.
6. **Renew if needed:** take a renewal when the access token expires within 300 s.
7. **Check send admission:** take the rate gate and recheck login state, cooldown
   and pacing budget before creating any spend reservation.
   - If the login is unusable, release the gate and answer 503
     `credential_unavailable` without sending.
   - If the cooldown is active, release the gate and return to step 4.
   - If pacing budget is unavailable, release the gate and wait with the slot held;
     then recheck login state and return to step 4.
8. **Reserve and send:** with the gate still held and all checks passed, inject the
   credential headers and reserve one request in the spend ledger. Immediately
   charge pacing, send the body unchanged and release the gate.
   - Every send is charged, including resends after a 429 or a login's first 401.
9. **On a 429 before any response byte reaches the caller:** take the rate gate and
   record the cooldown, then release the gate, record a `rate_limited` receipt, and
   return to step 4 with the same body and slot.

The slot is held until the response stream ends, fails or the caller disconnects.
A caller disconnect cancels the upstream request. Requests remain concurrent up to
`max_in_flight`; a limit of two permits two streams, without serialization.
Requests already sent before the cooldown is recorded may also receive 429. Each
such request follows step 9 and waits out the shared cooldown before being resent.

**Responses.**

- **2xx:** streamed to the caller through the scrubber. The process sets
  `Content-Type` to the validated format, `text/event-stream` or `application/json`;
  any other upstream type, or a compressed body, ends the response with an error.
  A family whose endpoint omits the type on its event stream (the ChatGPT Codex
  endpoint does) declares that, and the route reads it as `text/event-stream`.
- **401 on a login route:** the process renews once and resends once, before any
  byte reaches the caller. The resend returns to admission step 4 with the same
  slot, rechecking login state, cooldown and pacing. A second 401 marks the login
  `renewal_failed` and returns that request a fixed 401. The state persists until a
  fresh import; later requests get 503 `credential_unavailable` at admission.
- **401 on a static-key route:** returns a locally generated fixed 401, with no
  upstream header, renewal or resend.
- **429:** sets the account cooldown, and the request waits and is resent (step 9).
  This also applies to every request already in flight when the cooldown is recorded.
  After the cooldown is recorded, no new request is sent until it ends.
  The caller receives 429 only when its `max_queue_wait_seconds` is reached; then it
  has a fixed body and the locally computed `Retry-After` (§Returned headers).
- **Other non-2xx:** the status is forwarded with a fixed body. The upstream body is
  discarded because it can echo tokens. All headers follow §Returned headers.

**Forwarded request headers.** The route copies the caller headers named in
`forward_headers`, with their values unchanged, and drops all others. Agent tools put
their session and routing identity in request headers, and the provider uses it to
send the requests of one conversation to the same prompt cache. The Codex CLI, for
example, sends `session-id`, `thread-id`, `x-client-request-id`, `x-codex-turn-state`,
`x-codex-turn-metadata`, `x-codex-window-id` and `x-codex-parent-thread-id`. A route
that drops them still works, but each request lands on a cold cache, which costs more
input tokens and time. The host's route configuration must list every such header its
tool sends. Each value must be 1 to 8 192 bytes of printable ASCII, or the header is
dropped.
Configuration validation refuses `authorization`, `cookie`, `host`, `content-length`,
`transfer-encoding`, `connection`, `te`, `upgrade` and `proxy-authorization` in
`forward_headers`; the credential process owns those credential or transport headers.

**Returned headers.** A response carrying a provider answer, of any status, carries
only the route's `return_headers` that the provider sent, plus the rate-limit
allowlist below on 429 and 503. No other upstream header is relayed. Non-2xx bodies
are fixed. Agent tools read some of them back on their next request; for example the
Codex CLI keeps `x-codex-turn-state` and sends it again, so later requests of a turn
reach the same provider state. Each value must be 1 to 8 192 bytes of printable
ASCII and must not contain a credential the scrubber knows; otherwise that header
is dropped. The
response to a request that waited and was resent after a 429 carries the headers of
the final provider answer.

Allowlisted headers on 429 and 503 are `retry-after`, `x-ratelimit-*`, and the
family's documented rate-limit headers. Their values must be ASCII without control
characters and at most 256 bytes each, or they are dropped.

Locally generated responses carry no upstream header: 404 `route_not_found`,
503 `credential_unavailable`, 429 after `max_queue_wait_seconds`, and the fixed 401
for a static-key route. The local 429 carries only a locally computed `Retry-After`
for the remaining cooldown, in addition to locally generated transport headers.
The process generates its own framing and validated content type (§Transport and
§Responses); these are not relayed upstream headers.

Configuration validation refuses `set-cookie`, `cookie`, `authorization`,
`proxy-authenticate`, `www-authenticate`, `location`, `content-length`,
`transfer-encoding`, `connection`, `keep-alive`, `te`, `trailer`, `upgrade`,
`content-encoding` and `host` in `return_headers`.

**Long waits.** A request can wait minutes for a slot or a cooldown before its first
response byte. The credential process sends the caller nothing during the wait by
default. A host whose tool gives up on a silent request before its first byte measures
that limit (see §Open questions). If a tool has such a limit, the process sends a
keep-alive that does not change the final response. The first candidate is an HTTP/1.1
interim `102 Processing` response every 30 s, which HTTP clients skip before the final
status. This is chosen only after a measurement shows the tool needs it and skips it.

**Cooldown value.** A `Retry-After` header in seconds or as an HTTP date sets the
cooldown, parsed by the shared retry module from
[#155](https://github.com/symbiotic-build/symbiotic-foundation/issues/155). A 429
without a usable `Retry-After` uses the account's existing retry backoff
(`retry_base_delay_ms`, doubling, 120 s clamp). Response bodies are never parsed for
reset hints. The cooldown only moves forward, as today.

**Scrubbing.** Every response byte passes a scrubber before it reaches the caller.
The scrubber redacts, rather than refuses, every value the login has held in this
process (current and rotated tokens, account ID) and the static key. It matches the
encodings the existing credential boundary checks: exact bytes, JSON-escaped UTF-8,
percent-encoded UTF-8 (both hex cases), standard and URL-safe Base64 (padded and
unpadded). For event streams it also accumulates the dialect's delta text fields
(output text, reasoning text, tool-call arguments, names and IDs), so a token split
across deltas is redacted before release. This step keeps whole-item retention:
it holds each open delta item's accumulated text and event envelopes until that item
completes. The stream module already implements and tests this approach. The target
design holds only events that intersect an unresolved credential-length suffix;
[#161](https://github.com/symbiotic-build/symbiotic-foundation/issues/161) tracks
qualifying it for interleaved fields.

The provisional route settings `max_held_text_bytes` and `max_held_event_bytes`
default to 4 194 304 bytes of held text and 67 108 864 bytes of held event envelopes,
respectively. These defaults come from the predecessor agent proxy, not measured
maxima. Both settings must be positive; configuration validation refuses zero.
Each is a hard limit on the total held across all open items in one stream.
Before retaining text or an event envelope that would exceed either limit, the
scrubber ends the stream with a visible error. These are provisional settings under
the [boundary contract's bounds rule](../architecture/boundary.md#bounds-as-labelled-settings).

Redaction, not refusal, is the rule here because the stream has already reached
the caller in part. Refusal would cut every stream that quotes a token-shaped value.

**Usage.** At the end of a 2xx stream the process records the spend receipt:

- `openai_responses`: the `response.completed` event's `response.usage`
  (`input_tokens`, `input_tokens_details.cached_tokens`, `output_tokens`,
  `output_tokens_details.reasoning_tokens`), plus `response.id` and `response.model`.
- `openai_chat_completions`: the final chunk's `usage` object, present when the
  request asked for it.

The values map onto the existing `UsageTrace` fields and screening rules. A stream
that ends without a usage event, fails, or is cut by a disconnect or restart keeps
the receipt `Unknown`. A login account has no per-request price, so measured receipts
carry token counts and no `cost_micro_usd`.

The process also returns the measured usage to the host as an HTTP trailer on the
chunked response, when the host's request carried `TE: trailers`:

```text
symbiotic-usage: input_tokens=1200, cached_input_tokens=800, output_tokens=310, reasoning_tokens=96
```

A host can then count usage per job, or per anything it knows and Foundation does not,
without parsing the stream. A stream without a usage event gets no trailer. The
process keeps no per-caller totals and enforces no per-caller ceiling.

### What passthrough does not reuse

Passthrough requests have no invocation identity and no Memory data authorization.
The process therefore does not apply these typed-route mechanisms to them:

- signed attempts, permits and grant revisions;
- answer recovery and same-attempt status;
- the request failure budget and the unresolved-request block. An `Unknown`
  passthrough receipt does not block the next passthrough request;
- the response cache and request deduplication;
- the known-zero rule for automatic retry. A passthrough request is resent after a
  429 (step 9) and once after a login route's first 401. A provider that answers 429
  before any response byte reaches the caller has refused the request, and
  `Retry-After` is its instruction to send it again later; the receipt of the refused
  attempt still stays `Unknown`.

This extends the boundary contract: passthrough routes serve trusted same-UID hosts
outside Memory's data authorization. Approval of this record includes adding that
sentence to [boundary.md §Supported modes](../architecture/boundary.md#supported-modes-and-trusted-channels).

### Static-key accounts

A passthrough route may use `owner_only_file` instead of a login. The process then
injects `Authorization: Bearer <key>` and never renews. This covers API-key accounts
whose agent tools speak an OpenAI-compatible streaming protocol. A provider 401
returns a locally generated fixed 401 body without upstream headers; it causes no
renewal or resend.

## Failure modes

- **Credential process not running:** the socket connect fails. The host treats
  this as "service unavailable". A host using the health file also starts no new
  work when the file is missing, its `updated_at` is older than 30 s, or it reports
  `stopping`. On SIGTERM or SIGINT, the process publishes `stopping` before closing
  its socket. After a crash, the last healthy file can stay fresh for up to 30 s.
  Work admitted in that window encounters the same connection failure as work
  whose service fails mid-run.
- **Renewal failed:** admission refuses a route in `renewal_failed` or
  `login_required` with 503 `credential_unavailable`. The request whose second 401
  set `renewal_failed` receives the fixed 401 instead. An operator signs in again
  and imports to restore admission.
- **Restart during a stream:** the caller's stream ends without its final chunk. The
  receipt stays `Unknown`; the cooldown survives in the durable queue backend; slots
  start empty. The tool's own stream retry resends that step.
- **Restart during a renewal:** the fence stays set and the account is
  `login_required` until a fresh import.
- **Long cooldown:** callers wait without a deadline of their own. A host's job
  deadline, which disconnects the caller, bounds the wait; a host that needs a shorter
  bound sets `max_queue_wait_seconds`.
- **Upstream silence:** a gap longer than `idle_timeout_seconds` ends the stream with
  an error and an `Unknown` receipt.

## Key decisions

1. **One process owns each login store.** Renewal is an in-process single flight.
   - *Rejected:* several processes sharing one store under a shared lifetime lock and
     an exclusive renewal lock, with adoption of another process's renewal. That needs
     more locking and more failure states for no extra capability once one process
     serves every caller.
   - *Rejected:* the host keeping the login and passing the token per request. The
     token would leave the credential process.
2. **Passthrough speaks HTTP/1.1 over a second Unix socket.**
   - *Rejected:* stream frames inside the existing length-prefixed JSON protocol.
     Every host would need a client that re-encodes each event, and the existing
     protocol states it has no raw HTTP forwarding.
   - *Rejected:* a TCP listener. It has no peer UID check.
3. **Admission reuses the runtime's account gate, cooldown and pacing.** No second
   scheduler, as [AI runtime facade decision 1](8-ai-runtime.md#decisions) requires.
   - *Rejected:* a new passthrough queue. Typed and passthrough calls on one account
     would not share a cooldown.
4. **Every caller waits out a cooldown in Foundation; no 429 reaches it by default.**
   During a cooldown nothing is allowed to go out, even when a slot frees, so waiting
   is the only useful action. A maximum wait is an optional setting, off by default.
   Slot waits last while the caller stays connected.
   - *Rejected:* answering 429 at once. An agent tool configured without HTTP-level
     retries can treat a 429 like a dropped stream and retry with its own short
     backoff, so even a short cooldown would use up its retries.
   - *Rejected:* a default wait bound of 120 s (the earlier draft). Each longer
     cooldown would still end the tool's run and make the host reschedule it.
   - *Rejected:* bounding slot waits. With the two or three streams a login allows,
     waiting for a slot is routine.
5. **Foundation resends a request that got 429, after the cooldown.** The request has
   not reached the caller yet, its body is buffered within `max_request_bytes`, and the
   provider's `Retry-After` asks for exactly that.
   - *Rejected:* returning the first 429 and holding only later callers. That tool
     would still see a 429 and fail as above.
6. **Redaction on streams, with the credential boundary's encoding set.** See
   §Scrubbing for why refusal does not fit a stream.
7. **Login families are a closed list in code.** The injected headers decide where a
   token goes, so they are not free configuration, as with the existing provider list.

## Simplification pass

Each mechanism this design adds, and the requirement it serves:

| Mechanism | Serves |
|---|---|
| `LoginFile` source and store format | login accounts (#156 item 1) |
| Renewal single flight | one exchange per expiry across concurrent requests |
| Renewal fence | prevents reuse of a possibly rotated refresh token, which the provider can punish by revoking the login; no existing check covers a crash between exchange and install |
| Store lifetime lock | stops import from replacing a store the process is renewing |
| `login import` | the only way to create a store without copying tokens by hand |
| Health file | a host whose poller runs as another user must read health without reaching the passthrough socket |
| Passthrough socket and parser | undecoded forwarding (#156 item 2) |
| Shared admission | the account-wide slot limit and cooldown |
| `max_queue_wait_seconds`, off by default | hosts that need a hard limit on waiting (decision 4) |
| Resend after 429 | no 429 reaches the caller (decision 5) |
| `return_headers` | agent tools that read a routing header back would lose their turn state without it |
| Header allowlist on 429 and 503 | tools need the reset time; other headers can carry tokens or cookies |
| Scrubber | credentials must not reach the caller, which may be untrusted |
| Whole-item retention | reuses the tested stream module until suffix retention is qualified (#161) |
| `max_held_text_bytes`, `max_held_event_bytes` | hard per-stream retention limits with provisional route defaults |
| Usage reader | usage receipts (#156 item 3) |
| `symbiotic-usage` trailer | hosts that enforce their own per-job token ceilings need each response's usage; without it they would parse the stream a second time |
| 401 renewal and one resend | a token revoked before its expiry would otherwise fail every request until restart |

Removed while designing:

- cross-process shared and renewal locks, and renewal adoption (decision 1);
- a socket health route; the health file is the only health publication;
- parsing response bodies for reset hints; headers only;
- signed admission, answer recovery, request budgets and caching for passthrough
  (§What passthrough does not reuse);
- a total request deadline; the idle timeout fails on lack of progress instead.
- a bound on slot waits, and the invented `Retry-After: 1` it would have needed;
- a default cooldown bound; the setting stays, off by default;
- a keep-alive by default; it is added only for a tool measured to need it.

## Acceptance tests

All use loopback provider fixtures with synthetic tokens, in
`crates/symbiotic-credential-process/tests/passthrough.rs` and `login.rs`.

- **Renewal:** ten concurrent requests on a token expiring in 60 s cause one token
  exchange; all ten carry the new token.
- **Fence:** a process killed after the exchange and before install restarts with
  `login_required` and sends no exchange; import clears it.
- **Import ordering:** interrupt replacement import before the new store's directory
  sync; restart still sees the set fence and sends no exchange with the old token.
  An import that finishes installs the complete new store durably before clearing
  and syncing the fence; restart renews only with the imported token.
- **Renewal I/O bounds:** a discovery that never answers and a token exchange that
  never answers each fail at their own 30 s timeout. Discovery timeout or failure,
  and exchange timeout or failure proven to precede request bytes, clear and sync
  the fence. Restart permits renewal. When exchange bytes may have been written,
  timeout or failure keeps the fence set; restart reports `login_required` and
  sends no exchange. Caller errors and diagnostics contain static codes, never
  fixture response or transport error text.
- **Store checks:** group-readable, symlinked, wrong-owner, wrong-issuer and
  unknown-field stores are refused at startup.
- **Lock:** import is refused while the process runs.
- **Health heartbeat:** with `health_path` configured, startup publishes health and
  every state change updates it. Without state changes, publication intervals are
  at most 10 s and `updated_at` advances in Unix seconds.
- **Health staleness:** host admission reports service unavailable for a missing
  file, an `updated_at` older than 30 s, or `stopping`; a fresh usable entry permits
  work.
- **Health on clean shutdown:** SIGTERM makes the file show `stopping` for every
  route before the socket refuses connections. Host admission reports service
  unavailable even while that file is fresh.
- **Health replacement:** readers concurrent with publication see complete old or
  new JSON, never a partial file. Each replacement has mode 0640 and inherits the
  configured directory's group. Synthetic credentials, paths and provider text
  never appear in the file; only the declared fields do.
- **Forwarding:** the provider fixture receives the caller body byte for byte, the
  injected headers, only `forward_headers`, and no caller `Authorization`.
- **Forwarded headers:** a caller request with `session-id`, `x-codex-turn-state`,
  `x-codex-window-id`, `authorization` and `cookie` reaches the fixture with the first
  three unchanged, when they are in `forward_headers`, and without the last two.
- **Returned headers:** a fixture 200 with `x-codex-turn-state: abc` and
  `set-cookie: x` reaches the caller with `x-codex-turn-state: abc` only when it is in
  `return_headers`, and never with `set-cookie`; a value containing a known token is
  dropped.
- **Header configuration:** each forbidden name listed in §Forwarded request
  headers and §Returned headers is refused in its respective configuration list.
  Startup fails before any provider request.
- **Route defaults:** omitting `max_held_text_bytes`, `max_held_event_bytes` and
  `rate_burst_seconds` loads 4 194 304, 67 108 864 and 0, respectively. Explicit
  positive hold limits are preserved; zero for either hold limit refuses startup.
- **Route validation:** an unknown or invalid path, or a query string, is refused
  before any provider request; the fixture receives no request or credential.
- **Provider redirect:** a fixture redirect answer is not followed. The redirect
  target receives no request or credential; the caller receives the redirect status
  with a fixed body and only permitted returned headers, without `location`.
- **Response header rule:** provider answers with 200, a redirect, a login's second
  401, 500 and 503 carry the configured `return_headers` only. A 503 also carries
  the rate-limit allowlist. Unlisted upstream headers and non-2xx upstream bodies
  never reach the caller. After a 429 and resend, only the final provider answer's
  headers reach the caller, not the held 429's headers.
- **Local response headers:** route-not-found 404, credential-unavailable 503,
  maximum-wait 429 and static-key 401 carry no upstream headers, even when a prior
  provider answer supplied configured return headers or rate-limit headers. Only
  the maximum-wait 429 adds a locally computed `Retry-After`.
- **429 is waited out:** a fixture answers 429 with `Retry-After: 30`, then 200. The
  caller receives one 200 stream about 30 s later and no 429; the fixture sees the
  same body twice.
- **Shared cooldown:** a second caller arriving during that cooldown is sent only
  after it ends; the fixture sees no request during the cooldown.
- **Concurrent 429s:** with `max_in_flight: 2`, two requests are in flight before
  the first 429 arrives. Both receive 429 and are resent after the shared cooldown.
  The fixture receives no request during the cooldown, and both callers receive
  complete 200 streams without a 429.
- **Maximum wait:** with `max_queue_wait_seconds: 10` and `Retry-After: 600`, the
  caller gets a fixed 429 after about 10 s with locally computed `Retry-After`
  about 590 and no upstream header.
- **Typed and passthrough share:** a typed chat call on the same account waits out a
  cooldown set by a passthrough 429.
- **Shared policy validation:** for both `(tenant, account)` and sharing-key
  pooling, matching concurrency, request rate and burst duration with null typed
  input-unit and provider request limits permits startup. Each mismatched shared
  field, non-null `input_units_per_minute` or non-null `provider_request_limit`
  refuses startup with `InvalidRequest`. Passthrough sends charge no input units.
- **Pacing every send:** with a controlled clock, the first send, a 429 resend and
  a login's 401 resend each wait for request pacing budget and charge it once
  immediately before sending. During each resend wait the original slot stays
  held; the fixture receives the same body and no send before pacing permits it.
- **Reservation after admission:** with pacing exhausted and a controlled clock,
  a waiting request creates no spend reservation. Repeated admission checks while
  pacing is exhausted still create none. Once pacing permits a send, exactly one
  reservation exists for it. Repeat for a 429 resend and a login's 401 resend;
  each actual send has one reservation, with none added during its waits.
- **In-flight limit:** with `max_in_flight: 2`, five callers never have more than
  two concurrent requests at the fixture.
- **Slot wait is not a 429:** with `max_in_flight: 1`, `max_queue_wait_seconds: 5` and
  a fixture stream lasting 20 s, a second caller waits about 20 s and is forwarded.
- **Disconnect while waiting:** a caller that disconnects during a cooldown is never
  sent.
- **Cooldown set during a slot wait:** a caller waiting for a slot when another
  caller's request gets 429 with `Retry-After: 30` is sent only after that cooldown.
- **Cooldown recorded before dispatch:** pause a sender after its slot grant and
  before it takes the rate gate. Another caller's 429 records a cooldown under that
  gate. Resume the sender; the fixture sees no send from it until the cooldown ends.
- **Scrubbing:** Synthetic tokens split across every chunk boundary and across
  Responses and Chat Completions deltas are redacted in all listed encodings.
- **Stream hold limits:** for each configured hold limit, interleaved open items
  may total exactly the limit. The next text or event envelope that would exceed
  it ends the stream with an error before retention or release. Test text and
  event-envelope limits independently, keeping the other below its limit.
- **Usage:** a `response.completed` event produces a settled receipt with its token
  counts; a cut stream produces an `Unknown` receipt.
- **Usage trailer:** with `TE: trailers`, the same stream ends with a
  `symbiotic-usage` trailer carrying those counts; without a usage event, or without
  `TE: trailers`, no trailer is sent.
- **Restart:** a cooldown set before a restart still delays the first request after it.
- **Login 401:** one 401 causes one renewal and one resend with the held slot;
  two 401s mark `renewal_failed` and give that request a fixed 401. Later requests
  get 503 `credential_unavailable` with no send. The state survives later requests
  until a fresh import restores admission.
- **Unusable login admission:** both `login_required` and `renewal_failed` refuse
  new requests with 503 `credential_unavailable` and no provider request. A caller
  already waiting for a slot, cooldown, pacing or renewal when either state is set
  gets the same 503 after the wait and sends nothing, including before a resend.
- **Static-key 401:** a fixture 401 causes one provider request, no renewal and no
  resend. The caller gets a fixed 401 body without any upstream header.
- **Peer UID:** a connection from another UID is refused before parsing.

## Implementation size

`ESTIMATE`, from the closest existing code: about 3 500 lines in
`symbiotic-credential-process`. Login source, store and import about 700; passthrough
socket, parser and admission about 700; scrubber and dialects about 500; usage reader
about 150; configuration and validation about 150; tests about 1 300. Plus one
sentence in `boundary.md` and a new architecture section at closeout.

## Open questions

- How long each agent tool holds an unanswered request before its first byte. Each
  host measures it for its pinned tool version. If the tool gives up, the keep-alive in
  §Long waits is designed and measured against that tool.
- The right `max_in_flight` for a login. The public reports suggest two or three; a
  host raises it only on measured results.
