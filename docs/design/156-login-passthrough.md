---
status: draft
issue: 156
---

# 156 — Login accounts and agent passthrough

This record designs two additions to the credential process for
[#156](https://github.com/symbiotic-build/symbiotic-foundation/issues/156):

1. a **login-account credential source**, for a subscription login that renews its
   own access token;
2. a **passthrough route**, which forwards an agent tool's own streaming HTTP request
   through the account's existing queue limits without decoding it.

Nothing here is implemented. The operator approves this record before
implementation starts. It changes a security boundary and the
[boundary contract](../architecture/boundary.md), so delegated approval does not apply.

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
- **Queue wait bound:** `max_queue_wait_seconds`, the longest a passthrough request
  may wait for a slot or a cooldown before the credential process answers 429.
- **Stream dialect:** which provider event format a route streams, so the scrubber
  and usage reader know where text and usage are. Two exist: OpenAI Responses and
  OpenAI Chat Completions.

## Walkthrough

One credential process serves login account `acct-1` with `max_in_flight: 4` and
`max_queue_wait_seconds: 120`. Three agent tools (A, B and C) run under one host.

**Procedure: three tools during a provider rate limit**

1. **A sends a turn:** `POST /v1/responses` with a placeholder token, to the host.
   - The host checks its own sandbox rules and forwards it over the passthrough socket.
   - The credential process takes one of the four slots and sees no cooldown.
2. **The credential process forwards A's request:** injects the access token and
   the family's account header, and sends it to the route's fixed URL.
   - The provider answers HTTP 429 with `Retry-After: 30`.
   - The credential process records a cooldown until now + 30 s on `acct-1`.
   - A receives 429, `Retry-After: 30`, the allowlisted rate-limit headers and a fixed body.
3. **B arrives 5 s later:** the cooldown has 25 s left, within the 120 s bound.
   - B's request waits; nothing is sent to the provider.
   - At the end of the cooldown B takes a slot and is forwarded.
   - B's tool sees one slow response, not a 429.
4. **C arrives during a 583 s cooldown:** 583 s exceeds the 120 s bound.
   - The credential process answers 429 with `Retry-After` equal to the remaining
     cooldown, without contacting the provider.
   - The host can reschedule C's job instead of holding it idle for ten minutes.
5. **B's stream finishes:** the final `response.completed` event carries usage.
   - The credential process records B's input, cached, output and reasoning tokens
     in a settled spend receipt and releases the slot.

The provider sees one request during the cooldown (A's), not three.

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
endpoint is never caller-supplied and never follows redirects.

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
fence. A failed exchange that the provider answered (for example 400
`invalid_grant`) also leaves the account in `login_required`; a transport failure
before the request was written clears the fence and is retried at the next request.

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

**Health.** The passthrough socket answers `GET /health` locally with one entry per
route, and no credential value:

```json
{"routes":[{"name":"codex_responses","login":"ok","expires_at":1793000000,"cooldown_until":null}]}
```

`login` is `ok`, `expiring` (expires within two days), `renewal_failed` or
`login_required`; static-key routes report `static`. Hosts read it before starting
work.

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
    pub max_request_bytes: usize,
    pub max_response_bytes: usize,
    /// Existing account policy fields, shared with typed routes on the same account.
    pub max_in_flight: usize,
    pub requests_per_minute: Option<u32>,
    /// Longest wait for a slot or a cooldown before answering 429.
    pub max_queue_wait_seconds: u64,
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
`queue_wait_max_seconds`).

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
sharing key share one slot gate, one cooldown and one pacing bucket. Shared bindings
must agree on `max_in_flight` and `requests_per_minute`, or startup is refused, as for
typed routes.

**Procedure: admitting one passthrough request**

1. **Match the route:** method and path select one route; anything else gets 404.
2. **Check the login:** a route in `login_required` answers 503 with code
   `credential_unavailable` and sends nothing.
3. **Check the cooldown:** if `cooldown_until` is later than now + `max_queue_wait_seconds`,
   answer 429 at once.
   - `Retry-After` is the remaining whole seconds, rounded up.
4. **Wait:** wait for the cooldown to end, then for a slot (FIFO), then for pacing
   budget, all within `max_queue_wait_seconds` from arrival.
   - A wait that reaches the bound answers 429 with the remaining cooldown, or
     `Retry-After: 1` when only slots were busy.
   - A caller that disconnects while waiting leaves the queue; nothing is sent.
5. **Renew if needed:** take a renewal when the access token expires within 300 s.
6. **Reserve and send:** reserve one request in the spend ledger, inject the
   credential headers, and send the caller's body bytes unchanged.

The slot is held until the response stream ends, fails or the caller disconnects.
A caller disconnect cancels the upstream request.

**Responses.**

- **2xx:** streamed to the caller through the scrubber. `Content-Type` is forwarded
  when it is `text/event-stream` or `application/json`; any other type, or a
  compressed body, ends the response with an error. A family whose endpoint omits the
  type on its event stream (the ChatGPT Codex endpoint does) declares that, and the
  route reads it as `text/event-stream`.
- **401:** the process renews once and resends once, before any byte reaches the
  caller. A second 401 marks the login `renewal_failed` and returns 401.
- **429:** sets the account cooldown, then answers 429 with a fixed body and only the
  allowlisted headers below. The process never resends a 429 itself.
- **Other non-2xx:** the status is forwarded with a fixed body and no upstream
  header. The upstream body is discarded because it can echo tokens.

Allowlisted headers on 429 and 503 are `retry-after`, `x-ratelimit-*`, and the
family's documented rate-limit headers. Their values must be ASCII without control
characters and at most 256 bytes each, or they are dropped.

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
across deltas is redacted before the event that completes it is released. Held text
is bounded at 4 MiB and held event envelopes at 64 MiB; exceeding either ends the
stream with an error. These bounds are provisional settings, not measured maxima.

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
  passthrough receipt does not block the next passthrough request, because the agent
  tool, not Foundation, decides whether to resend;
- the response cache and request deduplication;
- automatic retry, except the single resend after a 401 described above.

This extends the boundary contract: passthrough routes serve trusted same-UID hosts
outside Memory's data authorization. Approval of this record includes adding that
sentence to [boundary.md §Supported modes](../architecture/boundary.md#supported-modes-and-trusted-channels).

### Static-key accounts

A passthrough route may use `owner_only_file` instead of a login. The process then
injects `Authorization: Bearer <key>` and never renews. This covers API-key accounts
whose agent tools speak an OpenAI-compatible streaming protocol.

## Failure modes

- **Credential process not running:** the socket connect fails. The host must treat
  this as "service unavailable" and not start new work; `GET /health` failing is the
  check.
- **Renewal failed:** the route reports `renewal_failed` or `login_required` and
  answers 503 `credential_unavailable`. An operator signs in again and imports.
- **Restart during a stream:** the caller's stream ends without its final chunk. The
  receipt stays `Unknown`; the cooldown survives in the durable queue backend; slots
  start empty. The tool's own stream retry resends that step.
- **Restart during a renewal:** the fence stays set and the account is
  `login_required` until a fresh import.
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
4. **A request waits up to `max_queue_wait_seconds`, then gets 429 with the remaining
   cooldown.**
   - *Rejected:* answering 429 at once during every cooldown. An agent tool configured without HTTP-level
     retries can treat a 429 like a dropped stream and retry with its own short
     backoff, so even a short cooldown would use up its retries.
   - *Rejected:* waiting without a bound. A ten-minute cooldown would hold every
     waiting connection, and its host's job, idle for ten minutes.
5. **Foundation never resends a 429.** The spend contract treats a 429 as unknown
   charge and allows automatic retry only with known-zero evidence. The tool owns its
   request.
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
| `GET /health` | hosts must not start work on a dead login or a stopped process |
| Passthrough socket and parser | undecoded forwarding (#156 item 2) |
| Shared admission | the account-wide slot limit and cooldown |
| `max_queue_wait_seconds` | decision 4 |
| Header allowlist on 429 and 503 | tools need the reset time; other headers can carry tokens or cookies |
| Scrubber | credentials must not reach the caller, which may be untrusted |
| Usage reader | usage receipts (#156 item 3) |
| `symbiotic-usage` trailer | hosts that enforce their own per-job token ceilings need each response's usage; without it they would parse the stream a second time |
| 401 renewal and one resend | a token revoked before its expiry would otherwise fail every request until restart |

Removed while designing:

- cross-process shared and renewal locks, and renewal adoption (decision 1);
- a separate health snapshot file; `GET /health` replaces it;
- parsing response bodies for reset hints; headers only;
- signed admission, answer recovery, request budgets and caching for passthrough
  (§What passthrough does not reuse);
- a total request deadline; the idle timeout fails on lack of progress instead.

## Acceptance tests

All use loopback provider fixtures with synthetic tokens, in
`crates/symbiotic-credential-process/tests/passthrough.rs` and `login.rs`.

- **Renewal:** ten concurrent requests on a token expiring in 60 s cause one token
  exchange; all ten carry the new token.
- **Fence:** a process killed after the exchange and before install restarts with
  `login_required` and sends no exchange; import clears it.
- **Store checks:** group-readable, symlinked, wrong-owner, wrong-issuer and
  unknown-field stores are refused at startup.
- **Lock:** import is refused while the process runs.
- **Forwarding:** the provider fixture receives the caller body byte for byte, the
  injected headers, only `forward_headers`, and no caller `Authorization`.
- **Routes:** an unknown path, a query string and a redirect answer are refused with
  no credential sent.
- **429:** a fixture 429 with `Retry-After: 30` reaches the caller as 429 with
  `retry-after: 30`, allowlisted rate-limit headers only, and a fixed body.
- **Shared cooldown:** after that 429, a second caller with a 120 s bound waits and
  is sent after the cooldown; the fixture sees no request during the cooldown. With
  `Retry-After: 600`, a second caller gets 429 with about 600 s at once.
- **Typed and passthrough share:** a typed chat call on the same account waits out a
  cooldown set by a passthrough 429.
- **In-flight limit:** with `max_in_flight: 2`, five callers never have more than
  two concurrent requests at the fixture.
- **Scrubbing:** Synthetic tokens split across every chunk boundary and across
  Responses and Chat Completions deltas are redacted in all listed encodings.
- **Usage:** a `response.completed` event produces a settled receipt with its token
  counts; a cut stream produces an `Unknown` receipt.
- **Usage trailer:** with `TE: trailers`, the same stream ends with a
  `symbiotic-usage` trailer carrying those counts; without a usage event, or without
  `TE: trailers`, no trailer is sent.
- **Restart:** a cooldown set before a restart still delays the first request after it.
- **401:** one 401 causes one renewal and one resend; two 401s mark `renewal_failed`.
- **Peer UID:** a connection from another UID is refused before parsing.

## Implementation size

`ESTIMATE`, from the closest existing code: about 3 500 lines in
`symbiotic-credential-process`. Login source, store and import about 700; passthrough
socket, parser and admission about 700; scrubber and dialects about 500; usage reader
about 150; configuration and validation about 150; tests about 1 300. Plus one
sentence in `boundary.md` and a new architecture section at closeout.

## Open questions

- Whether each agent tool holds an unanswered request for `max_queue_wait_seconds`
  without its own response timeout. The acceptance run of each host measures it for
  its pinned tool version; the host sets the bound below what it measures.
