# AI runtime

`symbiotic-ai-runtime` is the one way hosts run model calls. A host opens a
`Runtime` and gets ready providers back. Everything between the call and the
provider stays inside the runtime: queueing, retries and backoff, rate and
concurrency limits, cooldowns, attempt budgets, the response cache, traces,
usage receipts and persistence.

The [Foundation boundary contract](boundary.md) is authoritative for ownership,
provider-principal authorization, spend, storage and supported modes. This page
records current runtime behavior. Typed tenant/provider/configuration binding scopes execution and result reuse.
The canonical spend ledger owns reservations and recovery; admission/maintenance
bounds remain implementation work. This API supplies none of Memory's data authorization checks.

Design record: [docs/design/8-ai-runtime.md](../design/8-ai-runtime.md)
(issue #8).

## Use

```rust
use symbiotic_ai_runtime::{ModelBinding, ModelQueueConfig, Runtime, RuntimeConfig};

let runtime = Runtime::open(RuntimeConfig {
    state_dir: Some(data_dir.join("ai-runtime")), // required for dispatch
    ..RuntimeConfig::default()
})?;
let policy = ModelQueueConfig::default(); // explicit, conservative policy for this account
let identity = symbiotic_ai_runtime::BindingIdentity::new("tenant-a", "chat", "revision-1", "account-a");
let chat = runtime.chat(ModelBinding::new(raw_chat).with_identity(identity.clone()).with_policy(policy.clone()))?;          // Arc<dyn ChatProvider>
let embed = runtime.embedding(ModelBinding::new(raw_embedder).with_identity(identity.clone()).with_policy(policy.clone()))?; // Arc<dyn EmbeddingProvider>
let rerank = runtime.rerank(ModelBinding::new(raw_reranker).with_identity(identity.clone()).with_policy(policy.clone()))?;   // Arc<dyn RerankProvider>
let classify = runtime.classifier(ModelBinding::new(raw).with_identity(identity).with_policy(policy))?;      // Arc<dyn ClassifierProvider>
```

This example shows current runtime assembly, not complete authorized credential
dispatch. Credential-bearing transports stay inside Foundation; consumer adoption
follows [boundary.md](boundary.md#ownership).

A binding's `provider` is the raw transport: one of `symbiotic_model`'s HTTP
providers, or a host type implementing the provider trait
(`Arc<dyn ChatProvider>` and the other trait objects work too). Open the
runtime once per process or data root and share its clones. Every clone uses
the same state.

`ModelBinding` options:

| Field | Default | Meaning |
|---|---|---|
| `identity` | Required | Tenant, provider principal, configuration revision and concrete account |
| `account_sharing_key` | None | Tenant/account execution state; an explicit key pools accounts across bindings or tenants |
| `policy` | Required explicit policy, or the configured registry account | Concurrency, rate limits, retries, timeout |
| `response_cache` | `Default` | `Default`: the runtime's own cache when persistent, no cache in memory. `Off`: no response cache; explicit invocations can recover only their own retained answer. `Custom(cache)`: a host `ResponseCache` |
| `receipt_sink` / `trace_sink` | The runtime's sinks | Per-binding override |

## Configured registry

`ModelRegistry::from_json` validates the entire current-version configuration before
serving. Pass `Arc<ModelRegistry>` in `RuntimeConfig::registry`, then call
`Runtime::configured_provider(tenant, principal, credential_resolver)`. It returns
an installed chat, Gemini/OpenAI/Ollama embedding, Cohere rerank or Jev classifier adapter. Keyless bindings do
not call the credential resolver. Unknown tenants/principals and configuration,
transport or policy overrides are refused.

| Family | Required configuration |
|---|---|
| `models` | Unique ID and aliases, canonical model identity, installed adapter, supported operation, advisory capabilities; prices require provenance/date |
| `bindings` | Typed tenant/provider/revision/account identity, model ID or alias, endpoint, optional secret reference, account policy, explicit sharing key or null, finite request/response bytes and chat output tokens, effective settings |
| `accounts` | Named explicit execution policy: concurrency, timeout, attempts and optional pacing; no model-name fallback |

The [example catalogue](../../examples/model-registry.json) contains one synthetic
model with an alias and **no bindings or accounts**. It enables no provider.
Aliases use the canonical model on the wire and resolve the same capabilities.
Unsupported advertised operations/settings, ambiguous aliases, unusable limits,
endpoint credentials and conflicting shared-account policies refuse startup.
Credential-process deployment routes compile into this same validated registry;
they retain their existing permit protocol and single-attempt policy.

Every supported HTTP adapter requires finite nonzero encoded request and response
byte limits for success bodies, including chunked bodies. Non-success HTTP bodies
are discarded without decoding or retaining their bytes; the status determines the
error class, including for invalid UTF-8 bodies. Chat also requires a
finite output-token bound; requests above a configured bound are refused. Gemini
requires the exact configured dimension for every returned vector. Its adapter
refuses task options and conflicting per-request dimensions before dispatch.
Compatible embeddings require explicit `dimensions`, `embedding_full_dimensions`
and `embedding_input_tokens`. The last is the deployed model's usable per-input
token capacity after reserving special/template/task tokens. Each input's UTF-8
byte count must fit it, using the same conservative byte-level tokenizer bound
described for reranking below. This prevents silent context truncation, including
on compatible endpoints backed by Ollama. Unsupported tokenizers require another
adapter; byte transport limits alone do not establish complete-input processing.
For Memory's Qwen3-Embedding-8B profile, configure both as 1,024; reduced dimensions
are configurable per binding or OpenAI-compatible request, within the full ceiling.
OpenAI/OpenRouter uses `/embeddings` with `input`, `dimensions` and optional `input_type`
from request `task`; it restores batch input order and validates every vector.
Ollama uses `/api/embeddings` with `prompt`; batch, task and dimension overrides
are refused before HTTP. Foundation never splits one invocation into multiple calls.
Cohere/OpenRouter uses `/rerank` with `query`, `documents` and `top_n`. Required
`rerank_input_bytes` (sum of query/candidate UTF-8 bytes) and `rerank_candidates`
are hard admission limits, separate from encoded request/response bytes. Candidate
count is checked before scanning text. Required `rerank_context_tokens` is the
deployed provider/model's usable query-plus-document token capacity after reserving
special/template tokens; `rerank_query_tokens` is its query capacity and must not
exceed the usable context. Configure both from the deployed tokenizer and model,
not from HTTP byte limits. This adapter supports byte-level text tokenizers with at
most one token per UTF-8 byte: admission conservatively charges one token per byte
of each query/document pair and separately bounds the query. Deployments with other
tokenizers must not use this adapter. Over-capacity input is refused before HTTP,
never shortened, and the usable capacity is sent explicitly as `max_tokens_per_doc`
to avoid Cohere's default truncation. For Cohere rerank-v3.5, a usable budget no
larger than 4,093 tokens and query budget no larger than 2,048 reserve its three
special tokens; other models need their own configured capacities. See
[Cohere's API](https://docs.cohere.com/reference/rerank) and
[model limits](https://docs.cohere.com/docs/reranking-best-practices).
The response must contain exactly `min(top_k.unwrap_or(candidate_count),
candidate_count)` hits. Missing, extra, duplicate, invalid or non-finite hits refuse
the whole response; valid hits are sorted by score. OpenAI embedding batches also
require one uniquely indexed vector per input; Gemini requires one vector per input
and Ollama admits only one input and requires one vector. Provider-reported rerank
cost is retained in the usage trace before raw JSON is discarded.
These adapters use the same queue, credential boundary and receipt/accounting hook.
No provider binding, model default, separate scheduler or spend ledger is added.
Provider credentials and derived secret buffers use the shared non-Debug,
non-serializable `SecretValue` zeroizing container; `ResolvedAuth` is also non-Debug.

Without a registry, raw Foundation bindings require an explicit execution policy.
`default_model_queue_config` and `default_model_capabilities` are removed; consumers
must configure accounts rather than infer limits from a model/operator name.
Memory owns provider grants.

## Errors and credential boundary

`ModelError`, `QueueError` and `TraceError` carry closed `DiagnosticCode` values
(or a typed unsupported capability), never free-form strings. Adapter validation,
provider decoding, cache, storage and restored failures cannot attach provider or
credential text to an error. Diagnostics and logs retain static codes only. The
credential owner still checks successful raw and normalized outputs before results
reach runtime bookkeeping, and discards raw provider JSON.

## Persistence

| | `state_dir: None` | `state_dir: Some(dir)` |
|---|---|---|
| Dispatch | Refused: `SpendLedgerUnavailable` | Durable reservation before provider execution |
| Queue backend | `MemoryQueue` (in-process, bounded terminal history) | `SqliteQueue` at `dir/queue.sqlite` |
| Cooldowns, attempt budgets, deduplication | End with the process | Survive restarts |
| Response cache (`Default` mode) | None | `dir/responses/<kind>/<binding and transport hash>/<request hash>.json` |

**Private state.** Cached responses and queue state can hold private text,
so the state directory and everything in it are owner-only:

- A missing `state_dir` is created `0700`, with any missing parents.
- An existing `state_dir` must be a directory owned by the current user,
  not a symlink, and closed to group and others. Otherwise `Runtime::open`
  fails with a static queue diagnostic. State paths and underlying filesystem
  error text are never copied into runtime errors.
- Inside it, the runtime creates directories `0700` and files `0600`: the
  database (SQLite gives its journal files the database's mode) and cache
  entries, written through a temporary file and a rename.
- A symlink, or a component owned by another user, anywhere under
  `responses/` or at the database files, is refused. It is never followed.
  The current implementation also tightens existing component permissions at open.
  This does not establish a legacy-format compatibility requirement; state format
  and unresolved-attempt handling follow
  [boundary.md](boundary.md#storage-and-credentials).

The ledger durably reserves one provider request before dispatch and retains Unknown
charge after crash, timeout or missing usage; success settles measured usage.
`SpendReceiptRef::MAX_BYTES` is 256 UTF-8 bytes, giving generous headroom over
emitted `egress:` references (71 bytes) and UUID `runtime:` references (55 bytes
at `u32::MAX`). Construction and deserialization refuse longer references.
Every model enqueue result must have an item ID that fits `runtime:{item_id}:{attempt}`
for every `u32` attempt: at most 237 UTF-8 bytes. Oversized IDs are refused with
`SpendReceiptRefTooLong` before claiming; reference-construction failures after
claiming abort before dispatch, recording the refusal and releasing the lease.
Queue settlement errors propagate to the caller.
`QueueReceipt::spend_receipt` carries a typed `SpendReceiptRef`, looked up through
`Runtime::spend_receipt` even after queue retention. Explicit invocation recovery
is separate from the response cache. Implicit calls retain only a content-free
completion marker in the ledger and never recover ledger answers. Consumer commit
refusal never releases spend.
Queue claims that fail before reservation, or whose reservations are atomically
released before transport, do not consume the provider-attempt allowance. The
ledger records pre-dispatch release separately from ordinary `Released` accounting:
a known-zero provider failure still consumes an attempt. Followers use this evidence
even after lease reclaim marks the queue item dead. A later identical call can
reconsider a pre-dispatch storage failure once the ledger is available again.
`Runtime::reconcile_spend` requires external charge evidence. An unresolved reservation
counts against the absolute account request allowance until reconciliation; unknown
replay returns `SpendReconciliationRequired`. Typed-route money remains reporting. Passthrough can configure the rolling reservation budget described below.

Ledger accounting receipts survive queue retention. Only explicit invocations save
recovery answers, until completion time plus `RuntimeConfig::retention`, host acceptance
through `Runtime::discard_invocation_output`, or matching input erasure through
`Runtime::purge_responses`. Reads treat an expired answer as absent without writing.
The maintenance sweep clears the expired backlog through the recovery expiry
index in batches of at most 64 answers. Discard, expiry and erasure preserve
completion markers, receipts, usage and account spend. Replaying a completed invocation returns its retained answer,
then the typed `InvocationCompleted` diagnostic with its receipt once the answer is gone.

**Current retention settings.** At open, and on a runtime-owned background timer,
a persistent runtime retires state older than `RuntimeConfig::retention` (seven
days by default). `RuntimeConfig::maintenance_interval` defaults to 60 seconds
and must be nonzero; sweeps continue while idle. Runtime handles, returned
providers and active attempts share one maintenance owner per opened ledger.
The timer holds only a weak reference and ends when the last holder drops. Each sweep:

- calls orphaned by a crash are marked dead;
- finished calls' queue records are deleted;
- cached responses older than `RuntimeConfig::response_max_age` (30 days by
  default) are deleted, then the oldest ones until the rest fit in
  `response_max_bytes` (1 GiB by default). `None` disables either limit.

An expired response also misses on read, before any sweep removes it.
These are sweep-based soft cache limits, not hard byte admission bounds. Pending
count/bytes and per-batch/idle work remain unbounded by these settings; see
[boundary.md](boundary.md#bounds-as-labelled-settings).
Periodic sweeps run on a dedicated background thread. Maintenance failures are
logged at WARN and exposed through `Runtime::last_maintenance_error`, which retains
the most recent failure since open even if later sweeps succeed. Open-time
maintenance errors refuse the open.
Recovery expiry runs independently of cache-path validation; unsafe cache paths
keep their visible errors for cache operations. A purge checks the whole cache tree
before it deletes anything, including retained recovery answers. A purge refused
for a filesystem path (symlink or path outside the cache root) removes nothing.
If the root or any component in it is a symlink or belongs to another user,
it refuses and removes nothing, so it can never reach outside the cache.

**Purge.** `Runtime::purge_responses(|cached| ...)` removes the cached
responses whose recorded owner matches. It is the hook for erasure: when a
source or tenant is erased, the host purges its responses. Each entry is
matched by what its response's trace records: the request's `source` and
`role_binding`, model, and typed binding identity (`CachedResponse::binding`).
Tenant erasure matches `binding.tenant`, independently of free-text source labels. The purge reads every entry once, so it suits erasure, not a
hot path. Until the Foundation job queue implements its purge flag and settlement
without an answer (queue PR 3), the host drains in-flight calls for the affected
input before declaring erasure complete.

## Passthrough money accounting

Chat Completions POST sends use the canonical ledger's existing reservation and
settlement transactions. `AccountAdmission::reserve_passthrough_priced` records
`sent_at_ms` (UTC Unix milliseconds), the optional caller `caller_request_id`, and
the token rates selected at send time. Settlement uses those frozen rates even
if the stream crosses a peak boundary or the process configuration changes.
`SpendReceipt::cost_nano_usd` is absent until both price and complete input/output
usage are known, except that `Released` receipts have zero cost.
Complete 4xx refusals release at zero cost (401 without reading its body); 5xx,
incomplete bodies, transport failures, cancellation and missing successful usage
retain `Unknown` because provider work may have been charged.
An unpriced receipt remains unpriced after reconciliation.
Provider bodies and credentials never enter these fields.

One stored money unit is one billionth of USD (nano-USD). Configuration accepts
nonnegative decimal strings with at most nine fractional digits, bounded by
SQLite's signed 64-bit maximum. Each request sums token count times rate using
saturating integer arithmetic, divides by one million, and rounds up once.
The result saturates at `i64::MAX` nano-USD. Output includes reasoning tokens;
separately priced reasoning replaces that part of the output charge.
Missing cache splits charge all input at the miss price. One reported split
component derives the other from total input. Incomplete input/output usage
leaves the receipt `Unknown` on priced and unpriced routes. With a configured
spend limit, the receipt keeps its maximum-cost hold. It remains reconcilable
until complete usage arrives.
When output is positive, a distinct reasoning rate also requires a reasoning
count; otherwise complete usage settles with unknown cost.

`spend_limit` supplies a rolling account budget. Reservation takes SQLite's
IMMEDIATE write lock before it reads any costs. It admits a send only when
settled costs plus `max_request_usd` for every unresolved cost, including the
new send, fit `usd`. The window includes sends newer than
`sent_at_ms - window_seconds * 1000`; the lower bound is exclusive.
Released receipts do not count. A settled receipt whose cost remains unknown
also holds the maximum. Unknown sends survive restart and retain that hold
until settlement, release, or window expiry. `max_request_usd` must be a
conservative bound chosen for the route's requests; it does not rewrite or
limit caller output-token settings. The budget is guaranteed only when
`max_request_usd` bounds every request's real cost. A settled cost above
`max_request_usd` overshoots the reserved budget by the difference. Settlement
emits one `passthrough cost exceeded max_request_usd` warning with numeric
`cost_nano_usd` and `max_request_nano_usd` fields so the operator can raise the hold.
Admission refusal returns `BudgetExhausted(SpendBudgetExhausted)` before transport.

`Runtime::spend_cost(account, start, end, invocation, request_id_prefix)` reads
canonical receipts over the half-open UTC send-time window `[start, end)`.
Use `account_scope(identity, sharing)` for the account argument. Optional invocation
matches exactly; optional request ID matches a literal prefix, including `%`
and `_` without wildcard interpretation. The result contains
`settled_nano_usd`, `unknown_usage_count`, and `unpriced_count`. The two counts
are independent: partial usage leaves priced and unpriced receipts `Unknown`.
An unpriced receipt with partial usage contributes to both counts. Settled
priced receipts with unknown cost also contribute to `unknown_usage_count`. Released
receipts are excluded. Totals saturate at `u64::MAX`.

Schema 18 adds four nullable receipt columns: `sent_at_ms`, `caller_request_id`,
`price`, and `cost_nano_usd`. The frozen `price` JSON includes the per-request
`max_request_nano_usd` hold when configured, so settlement warnings survive restart.
One native `(account, sent_at_ms)` index bounds rolling budget reads and reports
to the requested account/window. The bare SQLite cost
is one index range scan and receipt updates in the existing transaction. There
is no new ledger, summary table, or accounting cache. Older schemas, including
version 17, are refused without migration. Deploying version 18 requires
rebuilding the queue database.

Verification: the `passthrough_money_*` tests in
[`spend_tests.rs`](../../crates/symbiotic-ai-runtime/src/spend_tests.rs)
cover cost arithmetic, UTC peak selection, reports, concurrent holds, expiry,
and refusal of the previous queue schema.

## Calls in flight

Once an attempt claims its queue item, the runtime owns it, not the caller.
The attempt runs as a task of its own, and the caller awaits its handle.
Dropping the caller's future (a job timeout, `tokio::time::timeout` around
`chat()`) therefore does not cancel the provider call. The attempt:

- finishes the provider call, bounded by `request_timeout_seconds`;
- records its receipts and trace, and stores the response in the cache when
  one applies;
- completes the item, or fails it with its error class and retry deadline;
- keeps its model slot until then, so an abandoned call still counts against
  `max_in_flight`.

Implicit calls share results only through the response cache: cache-off calls never
join another caller's running item or result, and cache hits resolve before provider
execution is registered. Accepted-handoff calls never join another call and each
consumes its own handoff through `acquire_handoff`, with identity mismatch and reuse
refused; explicit invocations deduplicate by their invocation identity.
A cache-off call still passes the accounting guards: while an identical call has an
Unknown reservation, the next identical call is refused until that reservation
settles, rather than dispatched.

The attempt renews its lease every third of `lease_seconds`, from its claim
until the item is completed or failed. That covers the provider call and
every receipt, trace, cache and cooldown write before the release, so a slow
sink cannot let the lease expire and hand the item to another caller. The
renewal is part of the attempt's own future, so it ends with the attempt and
cannot outlive it. It also stops once a renewal fails because the lease was
lost. Every exit of an attempt releases the lease, including a failed trace,
cache or cooldown write.

Renewal shares a task with the attempt, so nothing in the attempt may block
its thread. A `ResponseCache` is synchronous and may do file I/O, so every
cache read and write, the serialization of the stored response and the
`request_debug_dir` capture run on tokio's blocking pool. A slow disk
therefore delays only the call that is waiting for it.

There is no cancellation API. A provider that panics propagates the panic to
the waiting caller; its lease is not renewed and expires after
`lease_seconds`, as after a crash.

## Current shared limits

Execution state belongs to the runtime. By default queues and rate state are
keyed by typed tenant and concrete account; all models on that account share its
policy. An explicit `AccountSharingKey` pools execution across bindings or tenants.
Independent runtimes have separate in-process rate state. Persistent cooldowns
belong to their queue backend, so deliberately sharing a state directory shares
that durable account state. Poisoned rate/admission locks and closed gates return
visible `ModelError::Queue` errors; they never bypass pacing. Accounts share:

- one concurrency cap. Callers wait FIFO for a slot (`ModelAdmission`), and the
  backend enforces the same cap;
- one requests-per-minute bucket and one input-units bucket (text length ÷ 4;
  one request charge per call or batch);
- one cooldown after rate-limit, unavailable or timeout errors.

Only a provider attempt spends rate budget. Before its claim, a caller waits,
with its item still pending, until the buckets hold enough for one attempt.
It holds the queue's rate gate from that check through the claim, and the
attempt charges the buckets once its claim succeeds, immediately before the
provider call. A caller whose item is not claimable spends nothing: one
waiting on an identical call in flight, or one waiting out a retry backoff.
Each attempt, including each retry, is charged once, and pacing stays exact
because no two callers can be cleared for the same budget.
A caller waiting for budget gives up its model slot and sleeps in slices of
at most 250 ms. Between slices it looks at its item and the cache, so a
duplicate whose answer has arrived returns at once and spends nothing.

Pooling shares limits only. Deduplication, attempt budgets and results stay
per provider: the idempotency key is the queue, the tenant/provider/revision/account identity, effective provider descriptor,
credential generation and request hash. Endpoint, thinking and effort settings enter
the descriptor; custom providers must describe their effective configuration.

The key also includes the provider's credential generation,
`ModelProvider::credential_fingerprint`. The HTTP providers derive it from
their API key with `api_key_fingerprint`, a domain-separated SHA-256 of the
key. Rotating a key starts a fresh queue attempt allowance; the ledger still
refuses a new handoff until any uncertain prior charge is reconciled. The fingerprint is
one-way, and only a hash of it enters the queue's idempotency key. Neither
the key nor the fingerprint is written to traces, receipts or queue
payloads. A host provider without a credential returns `None`, and its
budgets are keyed as before. This fresh queue budget does not establish charge
certainty for an earlier attempt or authorize resubmitting an unknown charge;
admission and recovery follow the
[spend contract](boundary.md#spend-ledger-and-budgets). The ledger is consulted before any new handoff.

Raw bindings without a registry must agree on `max_in_flight`, `requests_per_minute`,
`input_units_per_minute`, `rate_burst_seconds` and `provider_request_limit`. A binding that disagrees
fails with `ModelError::InvalidRequest`. Retry and timeout settings may differ
per raw binding. Registry bindings of one account use an identical configured
execution policy, including timeout and retries.

## Policy knobs

Retry admission and recovery follow the
[spend contract](boundary.md#spend-ledger-and-budgets). The current `is_retryable`
policy considers `ModelError::Unavailable` (5xx, including 529)
and `ModelError::RateLimited` (429) only with explicit known-zero charge evidence. Opt-in
`retry_provider_errors` adds `ModelError::Provider` to that policy. These classes
now enforce charge certainty across the shared queued chat, embedding, rerank and classification
paths; timeout or unknown charge requires reconciliation.

`provider_request_limit` (default `None`) is an absolute account request allowance
with no implicit reset/window; `Some(0)` refuses dispatch. Typed-route money remains reporting.

`ModelQueueConfig` fields:

- `rate_burst_seconds` (default `0`): seconds of rate budget available as an
  initial burst. `0` paces from the first request. `60` admits one minute's
  budget at once, as per-minute metering allows.
- `retry_base_delay_ms` (default `1000`): the first retry delay. It doubles per
  attempt up to 32x, capped at 30 s (or at the base, if that is longer), plus
  up to `retry_jitter_seconds` of deterministic jitter. The total is at most
  two minutes. The backend stores the exact retry deadline
  (`QueueBackend::fail_with`), so no caller of the request retries earlier,
  and sub-second delays hold.
- `retry_provider_errors` (default `false`): also retry `ModelError::Provider`
  failures. `ModelQueueConfig::default()`
  allows three attempts when explicitly chosen; registry account policies specify
  their own finite total attempts. Retries require known-zero evidence. Provider errors
  never start a cooldown.
- `request_debug_dir`: write each serialized request to
  `{dir}/{kind}[/{scope}]/{request_hash}.json` before it is queued. For
  development builds only (`debug_assertions`): requests can contain sensitive
  text. Production builds refuse a policy containing this setting at bind time.
- `logical_retry_attempts` / `retry_attempts`: the request's total provider
  attempts across every retry layer, and the attempts per queue item. The
  logical budget is a cap: an item runs at most
  `min(retry_attempts, logical_retry_attempts)` attempts, so
  `logical_retry_attempts = 1` makes exactly one provider call. When the
  budget runs out, the error keeps the class of the last failure and says
  `attempt budget exhausted`. Queue items store only a typed diagnostic code
  (`last_error`) and typed class (`last_error_class`), so a later call or a
  restarted runtime rebuilds the same class without stored text.
- A lease that expires on an item's last allowed attempt, for example
  because the process crashed mid-call, ends the item as dead. A restarted
  runtime does not make another paid attempt from that item. On a non-final
  attempt, both queue backends allow another claim, but the ledger refuses a
  new dispatch until the uncertain charge is reconciled.

- `budget_renewal_seconds` (default `None`): applies to cache-enabled implicit
  calls. Once a request has exhausted its budget, later calls for the same
  request fail without a provider call while the queue remembers it.
  Cache-off implicit calls get independent items and fresh budgets, so this
  setting does not stop them; explicit invocations follow their frozen ceilings. On a persistent runtime that includes calls
  after a restart. `Some(n)` gives a new call a fresh budget after `n` seconds;
  `Some(0)` gives every call its own budget. These are current queue mechanics;
  renewal does not prove zero charge or authorize resending an uncertain attempt.
  Explicit invocation ceilings are frozen at first acceptance and never renew;
  their attempt count survives queue pruning and restart. Explicit calls carry no
  logical retry counter in queue payloads. Their latest receipt carries the original
  ceiling and cumulative provider attempts; confirmed pre-dispatch releases subtract
  the unused attempt, while known-zero provider failures consume one.
  Renewing (and continuing a retry chain) replaces the dead item
  only while it is still the newest for the request
  (`QueueBackend::enqueue_replacing`), so a delayed caller cannot start a
  budget over one another caller renewed in the meantime.

A classify request that fails validation returns `InvalidRequest` before it
takes a queue slot.

## Durable model jobs

`Runtime::model_jobs(scope, config)` opens the model job API on the canonical
spend ledger's SQLite connection. `model_job_payload` encodes the full request
and its frozen binding identity and account sharing setting, without credentials.
`ModelJobs::start_chat`, `start_embedding`, `start_rerank` and `start_classifier`
start workers for one handler kind. Each scoped kind has one canonical binding;
registration and enqueue reject a different binding before workers consume jobs.
`ModelJobs` accepts only Model execution; generic handlers use the generic job API.
Workers start without consumer resubmission;
`JobRunner::shutdown` drains sent calls and returns worker or monitoring failures.

The D1 job invocation identity is `(scope, job key)`.
`ModelJobs::invocation_key(key)` encodes that identity for direct `execute_*`
recovery; bare keys do not associate direct calls with scoped jobs.
ModelAdmission acquires the
same account slot used by direct calls before the job claim. There is no direct-call
queue item, nested admission or second slot. Claim and reservation commit in one
transaction, which returns the payload passed to transport. Jobs use the same rate
state, cooldown, finite timeout and credential sanitation as direct execution.
Paid settlement, the ledger's saved answer and job completion commit together.
The job record retains only a receipt reference; it never stores a paid answer.

Restart inspects expired claims and Uncertain jobs in bounded pages, using ledger
evidence for the recorded receipt before dispatch. Settled accounting without
an answer resolves to Failed, Cancelled or Purged; it does not remain Uncertain.
A committed answer from a direct `execute_*` under the
same scoped invocation key completes the job without another call.
A reservation without an answer
or trusted zero-charge evidence becomes Uncertain and is never resent blindly.
The frozen job claim ceiling is not renewed by a restart or a duplicate enqueue.

`ModelJobs::completions` delivers final jobs oldest first, within item and encoded
byte bounds, preflighting the ledger answer length before loading it. A single
oversized completion returns `CompletionTooLarge` without taking a lease. Confirm,
owner erasure and bounded maintenance delete ledger recovery and job copies in the
same transaction. Paid answers obey `max_result_bytes`, and maintenance counts
ledger recovery bytes alongside job-copy bytes. Oversized paid output is reported
as `queue_result_too_large` while preserving accounting.
Accounting and completion evidence remain. `recovery_until`
limits final answer availability only: expired Pending jobs can run, and expired
Uncertain jobs retain their input for reconciliation. Completion after the deadline
settles accounting without storing an answer. Owner erasure is sticky; an in-flight
purged call can settle but cannot save output, and an unresolved purged charge stays
Uncertain until reconciled.

Cancel observed before transport releases the reservation. A sent paid call
finishes under its normal timeout; cancellation keeps its answer and never retries.
Cancellation arrives with the lease heartbeat, using the runner's configured
interval. Version 1 has no job result cache, socket API or streaming provider
contract. Job attempt receipts use the configured receipt sink. A post-provider
trace failure reaches the runner error path while retaining the paid answer.
Acceptance tests are in `crates/symbiotic-ai-runtime/tests/jobs.rs`.

## Explicit invocation recovery

`Runtime::execute_chat`, `execute_embedding`, `execute_rerank` and
`execute_classifier` take a binding, an explicit logical invocation identity and
its request. Their `ExecutionResult` or `ExecutionError` includes its exact
accepted or recovered attempt's accounting state and canonical receipt reference.
Status lookup failures remain visible separately from the output or execution error.
`Runtime::invocation_status` discovers that receipt after a lost reply or restart
using the same binding identity, account sharing configuration and invocation.
These APIs require no telemetry sink. Caller invocation identities are scoped to
the full tenant/provider/configuration binding, including when accounts share quota.
Within that binding, reusing an invocation with different inputs or a changed
provider descriptor is refused. Explicit invocations, including bindings made with
`ModelBinding::with_invocation`, never read or write the response cache. They recover
only their own accepted attempt's retained recovery answer. Implicit calls retain caching.
Receipt acceptance order is SQLite rowid order: a predecessor must be released before
another attempt is accepted, and a completion is terminal. The latest receipt therefore
carries the original ceiling and cumulative attempts without scanning receipt history.
Request lookups use the receipt primary key, the latest-receipt index seek, or the exact
Unknown predicate on the unique partial index.

Every reservation transaction validates the immutable input/binding digest against
the invocation's latest receipt, including credential acceptance.
Released predecessors retain this binding, so concurrent delayed reservations cannot
change an invocation's inputs. Credential retries retain this invocation binding while
their ordinal and signed record establish distinct attempt identities.

For example, invocation `A` reserves input `X`, then is reconciled to Released.
A delayed reservation for `A` with `Y` is refused inside its transaction. Retrying
`A` with `X` may reserve a new attempt within its remaining budget, even if an
implicit call or invocation `P` has already completed `X`. That retry dispatches
under `A`; a later repeat of `A` recovers only `A`'s own saved output.

Credential acceptance records a handoff bound to the complete reservation,
account, operation, provider binding and exact queued input. Dispatch validates
that identity and atomically consumes its single-use dispatch owner. Reservation
and settlement run on the blocking pool under queue lease renewal; ownership is
checked again immediately before transport.

## Receipts

A `QueueReceiptSink` gets one `QueueReceipt` per step of a call:

- `Queued` (attempt 0);
- `Running`, with the queue and throttle waits;
- `Succeeded`, with usage, cache counts, the provider's receipt metadata and
  provider time;
- `Failed`, with the error, before any retry;
- `CacheHit`, which repeats the original usage and receipt and makes no
  provider call.

These usage receipts support telemetry and cost reporting. They are not the
canonical spend ledger or an enforceable monetary reservation. Accounting ownership
and budget guarantees are specified in
[boundary.md](boundary.md#spend-ledger-and-budgets). Receipt errors carry only
static diagnostic codes; they cannot contain provider response text.

## Current post-provider writes

The behavior below describes runtime cache/trace/queue writes. Foundation ledger
reservation, settlement and unknown-charge recovery are durable obligations under
[boundary.md](boundary.md#spend-ledger-and-budgets), not optional telemetry.

A ledger settlement-write failure returns an error and stops the queue item,
retaining its unknown charge for reconciliation.

Once the provider has answered, the call has been paid for, and the runtime
returns the answer. The writes that follow are best-effort: the response
cache, the trace, and the queue item's completion. The cache is an
optimisation, never a condition for returning a paid result. When one of
these writes fails:

- the response is returned, and its `Succeeded` usage receipt is recorded
  once;
- the failure is listed in the response trace's `metadata` under
  `RUNTIME_DIAGNOSTICS` (`"runtime_diagnostics"`), as
  `{"kind": ..., "error": ...}` entries, and the receipt's `metadata` carries
  the same list. The kinds are `response_cache_write_failed`,
  `trace_write_failed` and `queue_complete_failed`;
- it is logged as a `tracing` warning.

The same holds elsewhere. A cache hit whose trace write fails is still
returned, with the diagnostic. A failed failure-trace write is logged. A failed cooldown write returns a
queue error and persists a stopped item, because execution without its account
limiter is not allowed. Stopped items cannot be claimed directly. Genuine terminal refusals, including
cooldown-storage and pre-transport rate-state failures, cannot be continued or renewed. A later handoff may
reconsider an account-budget refusal or an uncertain-charge refusal whose receipt
has been reconciled to Released. Provider attempts still count toward the logical
attempt limit. Account reservation
denials and reservation storage errors that leave no receipt do not consume provider
attempts. A pre-transport release records durable dispatch-aborted evidence and stops
the queue claim when ownership permits; a later call can reopen that claim, including
an expired final claim. Reconciled provider attempts keep counting, and Unknown or
Settled accounting never restores an attempt.
Existing waiters return the recorded refusal. Durable successful output is checked
in every queue state and before dispatch, so interrupted queue completion cannot
hide a paid result. Failures without a
retry deadline also stop the item. Retryable failures with a deadline become failed
or dead according to their attempt budget. Retry also requires the known-zero
charge evidence described above.

## Custom response caches

`ResponseCache` is the seam for an alternate cache. `load` receives request kind,
scope, request hash and the serialized request; `Ok(None)` falls through to a
provider call. Custom caches must respect the result-identity and data-lifecycle
contract in [boundary.md](boundary.md#tenant-provider-bindings-and-data-access) and its
[storage rules](boundary.md#storage-and-credentials).

## Backends and conformance

`symbiotic-queue` ships `MemoryQueue`, the in-process backend with no storage
dependency. Its `conformance` feature exposes `queue_backend_conformance!`: 24
checks of the `QueueBackend` contract. Both `MemoryQueue` and `SqliteQueue` run
them in CI:

- deduplication dispositions;
- default attempts and schedule;
- per-queue idempotency;
- claim order and future items;
- `max_in_flight` for `claim` and `claim_item`;
- claim races;
- lease-owner and running checks;
- heartbeat;
- retry to dead;
- retry delay;
- expired-lease reclaim, with an expired final attempt ending dead;
- `fail_with` recording the error class and the exact retry deadline;
- `enqueue_replacing` superseding only the current newest item, atomically;
- cooldown monotonicity;
- unknown items.

A new backend passes the same macro. SQLite creates only the current schema;
queue files require schema version 18 and the current queue table layouts.
Older schemas are refused without migration; deploying version 18 requires
rebuilding the queue database. Queue and ledger mutations acquire
the SQLite write lock before reading state, so concurrent writers do not require
a read-to-write transaction upgrade. Unknown stored failure codes/classes
are refused with a static error. Terminal items without a recorded error class
return a queue error, without inferring a
class from provider text.

## Lower-level types

`symbiotic-model`'s `Queued*` providers, `ModelAdmission`, and constructing
queue backends for them stay public only because this crate composes them.
Consumers that build them directly are unsupported: behaviour such as shared
admission, cache scoping and retention is only guaranteed through the runtime.
`symbiotic_ai_runtime::model` re-exports the provider contracts and HTTP
providers for building raw transports.

Only `symbiotic-ai-runtime`, `symbiotic-queue-sqlite` and the WP14
`symbiotic-credential-process` implementation link SQLite. The credential process
extends the runtime database with permit replay protection and uses the same runtime
with response caching disabled; see [model egress](model-egress.md).
`crates/symbiotic-model/tests/feature_graph.rs` checks that `symbiotic-core`,
`symbiotic-queue`, `symbiotic-trace`, `symbiotic-model` and
`symbiotic-portability` do not.
