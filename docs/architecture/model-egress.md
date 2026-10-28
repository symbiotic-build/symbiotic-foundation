# Credential-owned model egress (WP14)

The [Foundation boundary contract](boundary.md) is authoritative for provider
principals, grant-revision dispatch ordering, spend ownership and supported modes.
This page describes the local credential backend and version-4 API implementing
Foundation-owned spend and ordered caller/provider grant-revision acceptance.

Memory consumes **`symbiotic-egress` 0.2.0** (Rust path `symbiotic_egress`).
The executable and implementation crate are **`symbiotic-credential-process` 0.2.0**
(`symbiotic_credential_process`). They reuse the Foundation runtime and its
operational database; there is no second provider scheduler or spend owner.

## Shared schema and Memory integration

The schema is defined in `crates/symbiotic-egress/src/lib.rs` and `jobs_client.rs`.
`PROTOCOL_VERSION = 4`. The credential operation also requires
`InjectProviderCredential.operation_version = 4`.

| Current type / API | Meaning |
| --- | --- |
| `EgressClient::exchange(Request) -> Result<Response, EgressError>` | Async, object-safe seam for Memory and its test doubles |
| `socket::UnixEgressClient` | Local socket implementation, configured path, frame bound and timeout |
| `Request { version, operation }` | Operations are `IssuePermit(Box<SignedAttempt>)`, `InjectProviderCredential(Box<InjectProviderCredential>)`, `PublishGrantRevision(SignedGrantRevision)`, `Receipt(SignedAttemptId)`, `AttemptStatus(SignedAttemptId)` |
| `Response { version, result }` | Result is `Result<Reply, EgressError>`; error codes contain no arbitrary provider strings |
| `Reply` | `Permit(PermitGrant)`, `Dispatched(DispatchResult)`, `GrantRevisionPublished`, `Receipt(Option<DispatchReceipt>)`, `AttemptStatus(AttemptStatus)` |
| `DurableAttempt` | Exact record binding described below |
| `AdmissionKey` | Non-Debug/non-Serialize, zeroized HMAC key; `sign_attempt`, `verify_attempt`, `sign_attempt_id`, `verify_attempt_id`, `sign_grant_revision`, `verify_grant_revision` |
| `ProviderPayload` | `Chat(ChatRequest)`, `Embedding(EmbeddingRequest)`, `Rerank(RerankRequest)` or `Classify(ClassifyRequest)`; use its `digest()` helper, never a separately implemented serialization |
| `DispatchPermit { token, attempt_digest }` | Opaque random capability, accepted exactly once, including across process restarts |
| `InjectProviderCredential` | `operation_version`, `admission`, `permit`, `payload` |
| `DispatchResult` | `receipt`, optional typed `output`, optional static `error: EgressError`, typed `diagnostics: Vec<DispatchDiagnostic>`, `receipt_persisted` |
| `DispatchReceipt` | `attempt_digest`, `AttemptId`, typed `SpendReceiptRef`, `DispatchStatus`, provider-reported `UsageTrace`, read-only `SpendState` |
| `ProviderOutput` | Chat text with optional typed finish reason, embedding vectors/dimensions, rerank hits or typed classifier answers; no raw provider response, raw error, credentials or trace metadata |

`DurableAttempt` and related wire types are defined in
[`crates/symbiotic-egress/src/lib.rs`](../../crates/symbiotic-egress/src/lib.rs).
Use those definitions for current version-4 serialization; this page does not
provide a replacement wire schema. `input_digest` hashes the exact typed
`ProviderPayload` JSON via its `digest()` helper. `input_manifest_digest` binds
Memory's manifest bytes; Foundation verifies the binding, not the manifest's
policy contents. Secret references are tenant scoped, not file paths.

All protocol timestamps are absolute Unix seconds (UTC), never milliseconds.
`recorded_at` is the serialization time. The required signed/digested `expires_at`
is the exclusive authority deadline, greater than `recorded_at` and at most
`i64::MAX`. Memory sets it to cover the earliest applicable expiry of caller or
provider input authority, including their authorization dependencies. Foundation
checks its own clock at acceptance as described below. This is a per-attempt
deadline; a newly authorized attempt may renew it without changing the invocation's
input/provider binding. The separate recovery deadline remains immutable across retries.

The replacement integration order is specified once in
[boundary.md](boundary.md#grant-revision-and-dispatch-ordering). The admission MAC
key is distinct from provider credentials and authenticates trusted admission;
it is not end-user authentication. Integration responsibilities are defined in
the [ownership contract](boundary.md#ownership).

The current `EgressClient` test seam supports unknown-charge results. It re-exports
the payload construction types; consumers do not import the credential-process
implementation.

## Signed model jobs (Memory FQ4)

`JobsClient<C>` offers `enqueue`, `admit`, `completions`, `ack`, `cancel`,
`purge_owner` and `status` on either `InProcessEgressClient` or `UnixEgressClient`.
The corresponding wire operations are `EnqueueJobs`, `AdmitJob`, `Completions`,
`AckJobs`, `CancelJobs`, `PurgeOwner` and `JobStatus`. Each carries a
`SignedJobsRequest`; the MAC covers the complete
`JobsRequest { scope, command }` under `symbiotic-egress/v4/jobs\0`. The outer
operation must match the signed command. Foreign job/token scopes are refused
before lookup or mutation. Replies use `Reply::Jobs(Result<JobsReply, JobError>)`.
No claim, heartbeat, checkpoint, worker completion or settlement operation is exposed.

An `EnqueueJob` contains a group, owners, a signed durable attempt and its exact
`ProviderPayload`. The signed invocation ID is the job key within
`JobScope { tenant, incarnation, queue }`. The frozen model binding, caller/manifest
binding and request participate in the payload's conflict digest. Group and
owners use the canonical job metadata; enqueue replays retain the original metadata.
Replaying enqueue joins the existing key, including after restart; it never
refreshes an admission or the route's frozen attempt ceiling. The signed job uses
that ceiling for reservation; the direct-egress one-send retry setting does not
reduce it. Each further claim still requires successor authority. `AdmitJob` updates
only the current signed authority, requiring the same immutable invocation binding
and increasing attempt ordinal and record sequence. An exact replay is idempotent.

Jobs use the existing model runner, account limiter and spend-ledger connection.
The account slot is acquired before the IMMEDIATE transaction that rechecks the
published grant revision and exclusive authority deadline, claims the job, reads
its waiting payload and reserves accounting. An invalidated admission moves an
unsent job to `AwaitingAdmission`, consumes no attempt or allowance, and retains
its input independently of recovery expiry. The shared egress payload preparation strips caller trace metadata and role labels
and uses the stable scoped invocation key for attribution. Provider secrets are resolved only
inside the claimed execution. No second scheduler, result copy or spend owner exists.
Local credential-resolution failures report authentication/configuration failure and
release the reservation without HTTP. Remote authentication rejection is not trusted
zero-charge evidence: its accounting and job remain uncertain.
Cancel before claim finalizes waiting work; cancel after claim arrives with the
lease heartbeat. Sent version-1 calls finish under their configured timeout.

`Completions { after, limit, max_bytes, wait_seconds }` long-polls up to
`io_timeout_seconds`; socket clients must configure a longer exchange timeout.
The byte bound covers the complete `JobsCompletions` body and must fit within
`max_frame_bytes` after subtracting the response envelope derived from the shared
wire serializer. Final deliveries
precede admission notices. A notice contains an ID/state/code and no delivery
fence; acknowledging an unfinished job returns `NotFinal`. The optional `after`
notice cursor is an exclusive ID bound in ascending ID order. Supply `after: ""`
to start pagination. When the request supplies a cursor, the response returns
`after` for the last notice actually included within the count and byte bounds,
or preserves the request cursor when no notice is included. Omitting `after`
polls the first notice page with the previous response encoding and byte accounting:
the response omits `after` and charges no cursor bytes. An empty page ends the
current paginated scan; restart with `after: ""` to discover jobs that entered
`AwaitingAdmission` at earlier IDs.
Memory's job adapter must pass the returned cursor at its next Foundation update.
Bare SQLite scans scoped unfinished rows through `jobs_claim`, filters by state
and `id > after`, then sorts by ID for the bounded page. The wire layer adds
opt-in cursor encoding and one empty-body serialization per paginated candidate
to keep exact byte counts; no stored cursor, new index or schema is needed.
An individually oversized completion returns `CompletionTooLarge` before taking a delivery lease.
`JobStatus` reads one job's metadata without loading input, admission or output.
Group summaries and diagnostics are outside this seven-operation subset.

`PurgeOwner(owner)` verifies the MAC and scope before any lookup, maintenance or
mutation, then calls the existing scoped `JobRequest::PurgeOwner`. It returns
`JobsReply::Purged(count)`. The store removes matching waiting inputs, admissions,
owner metadata and saved recovery answers in its existing SQLite transaction;
other owners and scopes are unaffected. Accounting remains intact. An erased
in-flight job can settle accounting but cannot persist an answer. This adds no
storage layer or schema: the cost is the existing scoped owner selection and
job/ledger updates, plus authenticated request encoding and verification.
Memory's job adapter must call `purge_owner` when erasing an input owner.

`ProcessConfig.jobs` and `.job_runner` use the existing versioned `JobConfig` and
`RunnerConfig` defaults. Admission bytes count against pending utilization and
follow the same confirmation, cancellation and erasure deletion rules as input.
Queue schema 17 adds the current admission column and `AwaitingAdmission` to the
existing bounded live-row indexes; older formats are refused without migration.
Workers attach to an authenticated scope on its first job request (also after
restart). Dropped consumer futures leave Foundation-owned execution running.
The process state lock remains held until its runners drain. Worker failures
are reported by subsequent job requests and a failed attachment stays refused.
Ledger-first recovery preserves `Uncertain` after a kill during dispatch and never
resends it; with `answer_recovery: "retain"`, final output is retained only by the
ledger's existing recovery owner.

The acceptance tests are `jobs_*` in
`crates/symbiotic-credential-process/tests/egress.rs`: both transports, keyed joins,
revocation during account waits, successor authority after recovery expiry,
foreign scopes/MAC tampering, and executable kill/restart without resending.
`jobs_only_local_credential_failures_are_known_zero_charge` covers local resolution
versus remote rejection; `admission_bytes_share_maintenance_budget` in the SQLite
job tests covers admission deletion under the existing maintenance byte budget.

## Shared direct-request failure budget

`RouteConfig.request_budget` optionally configures `{"attempts": 3,
"renewal_seconds": null}`. `attempts` must be positive. Omission preserves
per-call dispatch behavior and omits the field from route serialization, so
existing route revisions stay unchanged. A configured budget participates in
the route revision. This implements the 2026-10-05 revised SO-0 shared failure
budget decision; signed jobs keep their existing per-job allowance.

Direct egress counts failed and uncertain sends across invocations under a digest of
`(tenant, route, credential fingerprint, signed input_digest)`. The fingerprint
uses the shared model credential helper; keyless routes use its absent value.
The signed digest covers the exact typed payload before Foundation replaces
trace metadata/source labels. Credential rotation and different input digests
start separate budgets. No request or answer content enters the budget table.

`renewal_seconds: null` never renews, including across restart. A positive
interval renews after that many seconds since the last failure completion or
uncertain admission; clock rollback cannot renew early. Zero gives each call a
fresh budget. A successfully recorded response accepted by caller validation
(if supplied) deletes the key. Local credential failures and durably recorded
trusted pre-send failures do not consume allowance. HTTP failures keep their
existing unknown-charge accounting and count against this shared allowance;
the budget introduces no retry inside an invocation.

Thread-mode direct callers can attach
`InProcessEgressClient::with_answer_validation` before dispatch. Its synchronous,
prompt callback receives the normalized `ProviderOutput` and returns whether the
final answer is valid. Foundation invokes it on its owned dispatch task after
provider execution and before committing egress completion, while still holding
the configured per-key budget lock. Returning `false` reports
`EgressError::Provider { status: None }` with `ProviderFailed` receipt status and
no output. Rejection keeps the debit and updates the failure renewal timestamp
exactly like a failed send; measured usage still settles spend, and missing usage
keeps spend unknown with content-free output-received evidence. Under
`answer_recovery: "off"`, no rejected answer is persisted. A dropped caller future
does not cancel validation or completion; a crash before completion keeps the
debit. Answer validation leaves callers without this callback, socket dispatches
and signed jobs unchanged; completed calls retain unconfigured and zero-renewal
budget behavior. Answer validation adds no wire operation or persistent structure.

After single-use permit consumption and credential resolution, an exhausted
key returns `DispatchResult.error = RequestBudgetExhausted` (wire code
`request_budget_exhausted`), no output, and a released spend receipt: no HTTP
send or spend. With retained recovery the typed failure is recoverable; with
`answer_recovery: "off"` only the existing completion/receipt evidence persists.
The permit stays consumed. Budget admission failure refuses execution visibly.

The existing process state lock excludes other processes. One in-memory async
lock per request-budget key serializes admission through durable completion for
that key, after credential resolution supplies its fingerprint. Different keys
can execute and complete while another provider is blocked, including keys on the
same account when its configured concurrency permits. The lock map is protected
only during keyed lookup and retirement. The final holder or waiter removes its
own weak entry under that mutex; admission never scans unrelated keys. The
registry mutex protects synchronous SQLite operations and is never held across a
provider call or an async wait. Acquisition order is budget-key gate, then the
registry mutex for admission; execution reuses the runtime's existing account
concurrency slots, cooldown checks and rate gates; completion takes the registry
mutex while retaining the budget-key gate. Runtime rate gates protect allowance
checks through charging before a send, and account slots enforce configured
concurrency. No additional account lock is introduced. Unconfigured dispatches
retain their existing concurrency. Failure counts and last-failure Unix seconds
live in the single
`egress_request_failures` table in the existing SQLite operational database.
Admission checks renewal and commits one debit before provider execution; a failed
write prevents execution. Admission timestamps uncertain sends; a recorded failed
send updates the renewal timestamp at completion. Completion commits success
clearing or a trusted pre-send debit undo atomically with egress completion and
spend bookkeeping. Undo restores the previous renewal timestamp. A failed completion
write reports `receipt_persisted: false` and retains unknown spend and the debit;
a process crash before completion likewise preserves the debit. Remaining allowance
alone does not authorize resubmitting an unresolved request.

Before every direct or signed-job HTTP execution, including routes without a
request budget,
Foundation binds the existing request-key digest to the consumed canonical
`egress_permits` row. In the same immediate SQLite transaction, it checks for an
unfinished earlier send with that key whose canonical spend is neither Released
nor Settled, and commits any configured budget debit. Such a predecessor returns
`DispatchResult.error = ReconciliationRequired` without an HTTP send or another
debit; the new attempt's reservation is released and its permit remains consumed.
The original attempt identity, receipt and unknown reservation remain available
through authenticated same-attempt recovery after restart. Budget renewal,
a different invocation ID and answer-recovery expiry do not resolve it.
Reconciliation of the original receipt to Released or Settled removes this block.
Durably completed direct calls keep the shared failure-count behavior above;
signed jobs retain their existing per-job allowance and do not debit the direct
request budget. Job refusal before transport records `SpendReconciliationRequired`
and releases its new reservation. Different request keys proceed independently.

The nullable `egress_permits.request_key` field records this pre-send identity.
SQLite's native `egress_unresolved_request` partial index covers only consumed,
unfinished permits with `request_key IS NOT NULL`; unbound and completed permits
add no entries. The bare-engine admission cost is an indexed request-key lookup,
a receipt-reference lookup in the canonical ledger, and one permit update. The
index earns its write cost by excluding historical recovery rows from admission.
Signed-job admission also reads its claimed canonical job row by the existing
job ID to obtain the original signed input digest after trace replacement; that
read earns the shared request identity without storing another admission copy.
A second native partial expression index, `egress_request_receipt`, covers the
same live candidates by their structured receipt reference. It earns one entry
per bound request by letting the canonical ledger writer retire the matching
binding without scanning permits. The `egress_retire_request_binding` SQLite
trigger clears that binding when the canonical ledger transaction records Released
or Settled; an Unknown ledger update alone leaves it intact. Direct completion and
the signed-job completion callback also clear their bindings in the existing
completion transaction, including completed unknown-charge attempts. Rollback
preserves the binding.
Both indexes are rebuildable from canonical permits, and startup rebuilds the
obsolete predicate and retires its resolved bindings without changing receipts
or recovery. There is no separate unresolved-attempt table or cache and no
second execution-state owner.

Evidence: the `regression_request_budget_*` tests in
`crates/symbiotic-credential-process/tests/egress.rs` cover three sends for six
identical HTTP-400 classifier calls, restart, fresh-per-call renewal, separate
inputs and rotated credentials, two HTTP-401 sends, concurrent exhaustion,
positive renewal, success clearing, omission/validation, and no request,
answer or credential content in state under `off`. The
`regression_request_budget_answer_*` tests cover three sends for six rejected
HTTP-200 final answers (including token-limit termination), restart partway through
allowance consumption, rejection followed by acceptance, completion-time renewal,
measured and missing usage, and unchanged unconfigured/fresh-per-call dispatch.
The `regression_request_budget_durable_*` tests cover admission-write refusal,
completion-write failure after provider failure, caller rejection or success,
and crash/restart after a
send followed by a different invocation with identical input and two attempts
still unused. `regression_unresolved_request_different_input_proceeds_until_reconciliation`
covers different input, canonical reconciliation, omitted budgets and zero renewal.
`regression_jobs_share_unresolved_request_admission_and_completion` covers
unfinished direct and signed-job predecessors after restart, released refusal,
original receipt recovery, reconciliation and completed jobs with unknown usage.
The in-process `signed_jobs_refuse_unfinished_requests_and_retire_unknown_completions`
test exercises the signed-job runner and admission owner without an HTTP fixture,
including restart, refusal, receipt release, reconciliation, completed unknown
charge and preservation of the direct budget.
The registry `reconciled_request_bindings_retire_atomically_with_indexed_work`
test covers rollback, receipt preservation, 64 reconciliations and subsequent
admission under a 500-VM-instruction interruption bound;
`unresolved_request_index_rebuild_excludes_unbound_permits` covers the rebuilt
non-null predicate. The registry `unresolved_request_*` tests exercise the same
admission and restart
checks without network fixtures.
`regression_request_budget_unrelated_routes_complete_while_provider_is_blocked`
holds provider A behind an explicit response barrier and completes route B before
releasing A, with separate accounts and with a shared account.
`regression_request_budget_concurrent_dispatches_cannot_overspend` checks each
same-key admission debit before releasing its provider barrier: six concurrent
failing calls produce exactly three sends.
The registry test
`request_budget_pre_send_undo_restores_only_unexpired_consumption` covers renewal
timestamp restoration and expiry.

## Same-attempt recovery (v4)

V4 replaces earlier versions without aliases or fallback. Both request and credential-operation
versions, configuration version and HMAC domains are 4; the egress registry schema stamp is 9 and queue schema is 17.
Opening a registry with a different stamp fails with `Version`; no migration or reset is
performed. An incomplete registry refuses startup instead of recreating lost
canonical permit, grant-revision or request-budget tables. Its result-expiry index
remains rebuildable from the permit records. `lost_canonical_registry_tables_are_refused_without_recreation`
in the registry unit tests covers these source-table losses. Operators must reconcile any old live attempts before provisioning fresh state;
never delete active replay history.
The crate package version remains 0.2.0 on this unreleased branch.

The exact public API is:

```rust
DurableAttempt::attempt_id(&self) -> AttemptId
AdmissionKey::sign_attempt_id(&self, AttemptId) -> Result<SignedAttemptId, EgressError>
EgressClient::attempt_status(&self, SignedAttemptId) -> Result<AttemptStatus, EgressError> // async
```

`AttemptId { tenant, incarnation, invocation_id, attempt_ordinal }` is the durable
identity. Status requests use `Operation::AttemptStatus(SignedAttemptId { attempt_id,
authentication })`, authenticated over `b"symbiotic-egress/v4/attempt-status\0"` plus
its typed identity JSON. The reply is `Reply::AttemptStatus(AttemptStatus)`. Knowing an
identity alone does not authorize lookup; Memory signs it only for its trusted recovery
path and applies its own caller/output disclosure checks before releasing recovered output.

`IssuePermit(Box<SignedAttempt>)` returns `Reply::Permit(PermitGrant { permit, status })`.
For the same identity and exact authenticated attempt digest, the permit token and digest
are unchanged, including after restart, consumption, completion, revocation, or expiry.
A different signed record for the same identity returns `InvalidRequest`. Reattachment
runs before current route configuration/admission checks; it grants no second dispatch.
`InjectProviderCredential` still consumes a capability exactly once. Concurrent issue
requests converge on one permit, and concurrent injections admit at most one handoff.

`AttemptStatus` is tagged by `state` in snake case:

| State | Fields / meaning |
| --- | --- |
| `NotIssued` | No permit for this identity; lookup does not issue one |
| `Permitted` | Unconsumed permit under the published revision with unexpired authority |
| `Invalidated` | Unconsumed permit superseded by publication or authority expiry; see [revocation rules](#revocation-replay-and-unknown-charges) |
| `Dispatched { receipt }` | Consumed, with no durable completion; receipt retains unknown reservation |
| `Completed { result }` | `DispatchResult` with typed `ProviderOutput`, measured usage/charge, no error |
| `Failed { result }` | `DispatchResult` with static `EgressError`, no output, and known-zero or unknown charge |
| `Expired` | Terminal result recovery deadline elapsed; `Receipt` still returns accounting |

The required signed `DurableAttempt.recovery_expires_at` is an exclusive absolute Unix
second deadline, greater than the first attempt's `recorded_at` and at most `i64::MAX`.
A successor may be recorded after recovery expiry: its authority deadline still
controls execution, while the original recovery deadline still prevents storing output. It is immutable
across invocation retries, distinct from authority `expires_at`, and chosen by Memory
for its recovery window. Terminal results are unavailable at or after that deadline.
Completion after the deadline persists accounting but never stores recovery output.
Expiry neither permits a new dispatch nor deletes accounting or replay tombstones.
Pending permits and uncertain dispatched attempts retain their authority/execution
state independently of the recovery deadline.

Expired result rows are cleared incrementally at startup, before operations, and every
second during socket serving, even when idle or connection slots are occupied. Each call
clears at most 64 results selected by the partial deadline index, regardless of the
expired backlog. The hard-coded 64-result batch and one-second cadence are current
provisional values, pending replacement by versioned maintenance settings.
Status lookup enforces the deadline even before physical cleanup.
Embedded users must periodically call `CredentialProcess::purge_expired_results()` while idle. Cleanup failure
returns `StateUnavailable`; the daemon fails visibly. SQLite secure-delete is enabled;
this is logical retention, not a forensic erasure guarantee for WAL files, backups, or
filesystem snapshots. These recovery records are protected by filesystem permissions,
not encrypted at rest. Users of this local backend must keep the state directory
owner-only; these requirements do not prescribe a universal deployment topology.

## Wire format

One request/response per Unix connection. Each frame is a **big-endian u32 byte length**
followed by UTF-8 JSON. Zero/oversized frames are refused before body allocation.
There is no line framing, compression, stream multiplexing or raw HTTP forwarding.
Unknown protocol/operation versions refuse dispatch.

Enums use snake-case tags. `Operation` has `operation` / `body` fields; `ProviderPayload`
has `kind` / `request`; `Reply` has `reply` / `body`. Rust's `Result` is serialized as
`{"Ok": ...}` or `{"Err": "error_code"}`. For example a refusal is:

```json
{"version":4,"result":{"Err":"permit_refused"}}
```

`IssuePermit` requests have the outer form:

```json
{"version":4,"operation":{"operation":"issue_permit","body":{"attempt":{},"authentication":"..."}}}
```

The empty object above stands for **all** `DurableAttempt` fields, not a valid request.
Use the shared Rust types, which reject absent required fields.
`SignedAttempt.authentication` is HMAC-SHA256 over
`b"symbiotic-egress/v4/attempt\0" || serde_json::to_vec(attempt)`.
Revision publications use `b"symbiotic-egress/v4/grant-revision\0"` and the `GrantRevision` value.
The `AdmissionKey` helpers define serialization and constant-time verification.

## Revocation, replay and unknown charges

The required grant-revision ordering is in
[boundary.md](boundary.md#grant-revision-and-dispatch-ordering).
`PublishGrantRevision` authenticates a `GrantRevision { tenant, incarnation, revision }`.
The tenant/incarnation revision covers both caller and provider authorization,
including memberships and every input dependency. This backend uses one tenant-wide
revision: a change invalidates all pending admissions in that incarnation, and
Memory must reauthorize both principals before submitting a new admission.
The initial positive revision must be published before any new admission.
There is no implicit revision or timestamp fallback.

Memory's trusted integration serializes its grant changes and admission checks
through publication. Publication acknowledgement is the effective grant-change
point; until acknowledged, Memory keeps the change pending and does not expose it
as effective. It must not sign admissions from an older check under a newer revision.
An asynchronous notification after an effective Memory change does not satisfy
this contract. Memory adoption implements this integration.

Publication durably advances the revision and refuses rollback; an exact replay
is idempotent. Permit issuance checks the published revision but reserves nothing.
Dispatch acceptance rechecks exact equality in the same immediate transaction as
permit consumption and ledger reservation. Inside that transaction, after acquiring
the writer lock and before consuming or reserving, Foundation samples its own clock
and requires `now < expires_at`. That transaction is the acceptance point and is
serialized with publication. A delayed attempt at or past its exclusive deadline
returns `EgressError::AuthorityExpired` (wire code `authority_expired`), with no
permit consumption, reservation, receipt or charge. New permit issuance also refuses
expired authority; exact reattachment still returns the existing capability and status.

A permit invalidated by a grant-revision change or authority expiry before handoff has status
`Invalidated`: it is not a charge, does not occupy the invocation and does not consume
`max_attempts`. Its capability remains refused, even if its K record preceded the change.
After Memory reauthorizes both principals, the next admission under the published revision
is a new handoff for the same invocation, with the following ordinal and a higher record
sequence and a currently valid signed deadline; it cannot mutate an existing signed attempt.
Ordinals may have gaps when durable attempts were never issued, including expiry before
issuance; issued ordinals and record sequences must still strictly increase.
Issuing a successor permanently invalidates an unconsumed predecessor. Acceptance refuses
that predecessor and status remains `Invalidated` even if Foundation's clock rolls back.
Ordinary expiry needs no revision publication, so reauthorization after expiry can
use the same published revision. For example, ordinal 1 issued
under revision 10 and invalidated by revision 11 permits a newly signed ordinal 2 under
revision 11 even with `max_attempts = 1`. Accepted handoffs retain execution, accounting,
status and receipt recovery after publication or authority expiry; any retry must
match the current revision and carry its newly checked authority deadline.

The existing runtime `queue.sqlite` is extended with `egress_permits` and
`egress_grant_revisions` replay-protection tables. They store hashes, ordinal/sequence,
grant-revision bindings, authority deadlines, accepted-handoff counts, consumption status,
recoverable permit tokens and accounting receipts. Pending status is projected from the
stored permit revision, the durably published revision, indexed successor existence
and Foundation's clock, so publication and authority expiry need no permit-history scan. V3 also stores
safe typed results until the signed recovery deadline, never prompts or provider credentials.
The owner-only database and same-UID authenticated IPC protect these recovery values.
SQLite FULL synchronization (including macOS fullfsync) makes consumption precede
handoff. A process lock prevents two credential processes using one state directory.
These tables do not schedule jobs. The existing runtime remains the only model
queue/scheduler. Permit consumption reserves through the canonical Foundation ledger in the same
transaction; completion updates the ledger and recovery result atomically. Route
`provider_request_limit` configures an absolute account request allowance, distinct
from pacing or monetary observations. Each receipt carries its accepted `AttemptId` and typed `SpendReceiptRef`.
`SpendState` and usage are observations projected from the canonical ledger;
Memory retains the reference and cannot reserve, release or settle through this protocol.

Before handoff the consumed permit has durable `SpendState::Unknown` and its
Foundation-owned one-request reservation. Timeout, uncertain provider failure or crash leaves it reserved.
`Receipt(SignedAttemptId)` returns accounting only, never a cached output; it returns
`None` when no consumption record exists. Capability reuse follows the revocation rules above.
A lost issue-permit reply is recovered by replaying the exact signed `IssuePermit`:
it returns the same capability and current state without allocating another attempt.
A lost dispatch reply is recovered with `AttemptStatus`; never resend a consumed permit.
There is no exactly-once external execution claim. A process crash before durable
completion leaves `Dispatched` with unknown accounting, requiring reconciliation or
a visible stop.

A received success with measured usage reports `SpendState::Settled` and available measured
input/output/reasoning/media/cost fields. Missing usage remains `None`; it is never
invented. `UsageTrace.reported_cost_usd` preserves validated provider-reported USD cost
as an exact decimal string, including sub-micro-dollar precision, in immediate receipts
and recovered results. The same typed `UsageTrace` carries optional
`cache_hit_tokens`, `cache_miss_tokens`, `response_id`, `served_model` and `created`
(provider Unix seconds). OpenAI-compatible prompt counters include DeepSeek's
cache split and nested OpenAI cached-token counts. Anthropic input totals include
uncached input, cache reads and cache writes; misses include uncached input and
cache writes. Jev classification carries its response identity and reported cache
counts through the same usage owner. Missing fields stay absent; timestamps are
never synthesized. Response identity survives recovery even without token counts,
while spend remains unknown without measured usage. The complete credential
boundary checks these fields before any runtime or ledger write; reasoning text,
prompts, answers, raw responses and credentials are excluded from usage.
For all OpenAI-compatible and Anthropic calls, independently of HTTP observation
scope, `response_id` and `served_model` are screened against answer and reasoning
payload values in response choices and content blocks, including nested reasoning,
thinking, signatures and redacted thinking data. Protocol discriminators such as
roles, block types and finish reasons are excluded. An identity that occurs in an
answer or reasoning payload string, or contains a complete nonempty reasoning
payload string, is omitted from both typed usage and provider trace metadata,
with an `invalid_usage_identity` diagnostic; the paid answer is preserved.
An identity containing a short final answer remains valid unless it otherwise
fails screening.
No new persistent store is introduced: immediate replies and recovery derive
usage from the existing canonical spend receipt.
`reported_cost_usd` is separate from integer `cost_micro_usd`; the process neither
rounds it nor estimates prices, and does not establish a monetary ceiling.
Every failed dispatch returns a closed, credential-free `error` alongside
its receipt (`None` on success). Provider failures keep `rate_limited` with an
optional `retry_after_seconds`, `timeout`, `provider` with an optional HTTP
`status`, `invalid_provider_json`, or `transport`. Provider HTTP status and
Retry-After survive runtime attempt exhaustion and secondary bookkeeping diagnostics. HTTP-date Retry-After
values become a nonnegative delay relative to receipt time; malformed hints fail
visibly rather than being ignored. Send and response-body read timeouts share the
same timeout classification. Provider error bodies and raw header strings never
leave the transport. These classes are observations, not permission to retry:
unknown spend still requires reconciliation before another admitted attempt.
`EgressError::InvalidProviderJson` (wire code `invalid_provider_json`):
A successful HTTP response was not a valid provider answer: not JSON, or not the expected shape.
This preserves `ModelError::Unavailable(InvalidResponse)` separately from status-less
`provider` failures. The malformed body contributes no diagnostics or provider text. This
failure retains unknown charge and the existing request failure budget debit;
no parsed answer reaches caller answer validation.
A successful HTTP response refused for exceeding the route's `max_response_bytes`
bound keeps `EgressError::Provider { status: None }` and adds
`DispatchDiagnostic::MaxResponseBytesExceeded` (wire diagnostic
`max_response_bytes_exceeded`). The shared response reader identifies refusals
both from Content-Length and while reading a body without a known length. This
static diagnostic carries no provider body or response bytes and survives runtime
bookkeeping diagnostics. Dispatch evidence remains `may_have_dispatched = true`,
spend remains unknown, and each failed send debits the request failure budget
exactly once. No parsed answer reaches caller answer validation, so no additional
answer-validation debit occurs. The embedding batch and budget regressions in
[`embedding_route.rs`](../../crates/symbiotic-credential-process/tests/embedding_route.rs)
cover the default-bound refusal, three sends before budget exhaustion, and zero
validator calls; the projection regression in
[`provider.rs`](../../crates/symbiotic-credential-process/src/provider.rs)
covers dispatch evidence beneath bookkeeping diagnostics.
Credential-loading and setup/queue failures before
transport handoff report `SpendState::Released` and release that reservation for a
subsequent admitted attempt, while the attempt-count limit still applies. The shared
HTTP send boundary also records per-failed-call evidence in an opaque
`ModelError::HttpNotSent` error when reqwest reports a non-timeout connect-phase
failure (DNS, connection establishment or TLS setup), before any HTTP request
bytes are written. The shared `ModelProvider::failure_charge` default reads this
evidence, including beneath bookkeeping diagnostics, for every HTTP adapter
(including Jev); the error class alone never supplies it. Egress releases that
reservation and allows a new admitted attempt with an advancing ordinal, within
`max_attempts` and the existing provider cooldown rules.
HTTP error statuses (including 429 and 5xx), all timeouts (including connect
and TLS setup timeouts), failures after connection setup, and response-body read
errors retain `FailureCharge::Unknown` and the unknown spend reservation. This
repository holds no provider billing documentation supporting status exceptions.
Evidence lives only in the failed call's error; released accounting and retry
admission use the existing canonical spend ledger and attempt records.
Permit admission checks the latest attempt using the invocation/ordinal index. For a
consumed predecessor, its receipt's canonical ledger state must be Released;
an unconsumed predecessor follows the revocation rule above. Success is terminal,
and other charges require reconciliation. The latest row carries the cumulative accepted
handoff count, incremented only with atomic consumption/reservation and copied to the next
row; earlier invocation history needs no aggregate scan. Direct send admission
also checks unresolved predecessors across invocation IDs using the request identity
and canonical ledger as described above.
If the atomic final result/receipt write fails, the paid output still returns with
`receipt_persisted = false` and `SpendState::Unknown`; restart retains the earlier reservation and
`Dispatched` state. Receipt reconciliation and references follow the
[spend contract](boundary.md#spend-ledger-and-budgets). A paid output is not
evidence that canonical settlement was durably recorded.
Runtime queue-completion, trace-write and response-cache-write failures return the
static `queue_complete_failed`, `trace_write_failed` and `response_cache_write_failed`
diagnostics alongside the paid output and accounting state, even when the separate
registry write succeeds. Raw runtime diagnostic strings are never forwarded.

Memory supplies no budget unit, reservation, invocation spend limit, attempt
allowance or settlement instruction. The route owner configures a finite positive
`max_attempts` and an optional absolute account `provider_request_limit`.
Foundation reserves one provider request per accepted attempt and applies `max_attempts`
to accepted handoffs, rather than raw ordinals. After an accepted handoff, a logical invocation
is terminal on success; uncertain attempts stop retries, and only a known-zero
failure permits a new attempt within Foundation's allowance. HTTP protocol retries,
redirects and ambient proxies are disabled; runtime retry budgets are one.
No monetary reservation is supported or hard dollar ceiling promised; see
[the spend contract](boundary.md#spend-ledger-and-budgets).

## Per-route answer recovery

`RouteConfig.answer_recovery` accepts `"retain"` (the default when omitted) or
`"off"`. The setting belongs to the credential owner, participates in the frozen
route revision, and applies to every capability on that route. Omitting it also
omits it from route serialization, preserving the existing default route revision.
This implements the lead's 2026-10-05 “Egress answer persistence for SmartOffice”
decision: answers derived from messages must never be written to disk; permits,
attempt state and spend receipts remain durable.

With `off`, direct dispatch returns the paid answer in memory, including its live
usage and static diagnostics. Its completion transaction stores no `DispatchResult`
or answer-derived fields. `egress_permits.result` stays SQL NULL; the existing
`finished` state records 0 (not finished), 1 (default completion), or 2 (finished
without a retained answer). Recovery returns
`AttemptStatus::FinishedWithoutAnswer { receipt }`, including after its unused
answer-recovery deadline. The deadline remains part of the immutable signed
attempt but need not be later than `recorded_at` in this mode; the existing nonzero
and integer bounds remain. No new table, column, index or recovery store is added.

The receipt retains its identity, dispatch status, numeric token/media counts,
validated decimal cost and spend state. Provider-controlled response ID and
served-model text are omitted from durable usage. The canonical ledger retains
only its existing `output_received` completion evidence, never the answer or an
answer hash. Persistent trace/receipt sinks receive numeric usage and operational
state without response hashes or arbitrary response metadata. Live diagnostics
remain visible; persistence failures retain the existing conservative accounting
behavior. Stored invocation traces use a fresh Foundation-generated trace ID,
the model descriptor frozen before dispatch, and the Foundation queue item ID
or job-attempt receipt ID. Provider-returned trace IDs, model identities and
queue item IDs stay in memory and never reach persistent sinks under `off`.
`retain` preserves the existing trace behavior.
The setting is prospective: it does not erase answers previously
retained by a route configured with `retain`. An `off` runtime binding refuses direct
explicit-invocation replay of an earlier retained answer for that same invocation.
Previously retained job answers remain deliverable through job adoption and
completion delivery until their existing recovery deadline.

All answer-bearing write paths share this policy:

| Path | Behavior with `off` |
| --- | --- |
| Direct in-process and socket egress | Answer travels only in the live reply; registry result is NULL |
| AI runtime explicit invocation recovery | No saved answer; status has `output_available = false`; replay returns `InvocationCompleted` |
| Runtime default, configured-directory and custom response caches | Neither read nor written |
| Model job spend-ledger recovery | No saved response; numeric spend and completion evidence commit with final job state |
| `symbiotic-queue-sqlite` model job results | Model jobs pass no output to the queue; successful completion has `output = None`, `result_expired = true`, and its receipt reference |
| Trace and usage-receipt sinks | No response hash, provider identity text or response metadata |
| Queue waiting payload, admission, request hashes, permits and account pacing | Contain input or operational authority/accounting only; provider answers do not enter these fields |

Jobs newly executed under `off` deliver completion and spend references, not
answer content, even without a crash. Apps needing the live classifier/chat answer
use direct dispatch on that route. A crash after durable completion but before consuming a
live reply loses that answer. A caller still needing it starts a **new logical
invocation**, subject to the same admission, spend and retry rules. Neither
same-attempt replay nor uncertain-charge recovery resends the old request.
The equivalent runtime binding setting is
`ModelBinding::with_answer_recovery(AnswerRecovery::Off)`; egress supplies it from
the route for direct calls and durable job runners.

Evidence: `regression_answer_recovery_direct_never_writes_answers_or_usage_text`
checks chat/classifier answer markers in the database and WAL files, numeric
spend, restart recovery and unchanged default retention.
`regression_answer_recovery_jobs_never_write_paid_results` covers both queued
capabilities, reopen, content-free ledger evidence and once-only execution.
`regression_answer_recovery_crash_after_answer_before_consumption` kills the
executable after durable completion while the reply remains unread, then checks
finished-without-answer recovery and refusal to resend.
`regression_answer_recovery_off_disables_caches_recovery_and_answer_telemetry`
checks default/custom caches, explicit invocation restart, trace/receipt projection
and measured/missing-usage accounting. These are synthetic tests, not physical
power-loss qualification.

## Deployment and credentials

This section applies to the supported Unix same-UID local backend. It is one
option within the [mode contract](boundary.md#supported-modes-and-trusted-channels).
Keyless routes use `secret: {"backend":"none"}` and an empty `secret_ref` in
configuration and admission. No credential is loaded or Authorization header sent.
The admission MAC key still requires a real secret source.

There are three sources: `none`, an owner-only file, and an app-supplied resolver.
Thread-mode apps pass `SecretSource::Resolver` through `ProcessConfig`.
The callback resolves a named key into zeroizing bytes. Foundation calls it only
after process protection is active. Provider keys are resolved lazily after permit
consumption. Resolver failures become `CredentialUnavailable`; their text is discarded.
Resolver configs cannot be serialized. `ProcessConfig::validate_child_process()`
refuses them with `ResolverRequiresThreadMode`; no callback crosses into a child.
In thread mode, the credential boundary protects against accidents, not same-process code.

The app's resolver must return promptly with a value or error and support concurrent
calls. Startup resolves the admission key synchronously, including when reopening
for recovery. Direct dispatch awaits provider resolution in `spawn_blocking` after
permit consumption and before request-budget admission or provider execution.
Provider `timeout_seconds` and `max_in_flight` therefore exclude that resolution;
distinct accepted dispatches can invoke the resolver concurrently even when
`max_in_flight` is one. Queued jobs acquire their account slot before resolving,
but their provider timeout still does not bound the callback. Foundation imposes
no resolver deadline. The app owns any lookup deadline and concurrency control
its resolver needs.

A slow resolver delays startup or dispatch and can delay shutdown. Dropping or
cancelling the awaiting future cannot stop an already running callback. Tokio
runtime drop waits for outstanding blocking callbacks to return; a runtime
shutdown timeout only stops waiting and does not terminate them. After a crash,
same-attempt recovery preserves an accepted unfinished direct dispatch as uncertain
and does not rerun its provider lookup or resend it. Reopening still resolves the
admission key. For example, with a one-second provider timeout, a provider resolver
that waits for app release keeps direct dispatch pending without sending HTTP
beyond that second; after release, provider execution starts with its own timeout.
`regression_slow_resolver_delays_dispatch_beyond_provider_timeout` in
`crates/symbiotic-credential-process/tests/egress.rs` pins this behavior with a
test-controlled release and a callback hang guard.

Run `symbiotic-credential-process /absolute/path/config.json`. The JSON configuration
must be an owner-only regular file. Before reading configuration or secrets, the executable
sets both core resource limits to zero and, on Linux, clears dumpability with
`PR_SET_DUMPABLE` (piped core collectors ignore the resource limit). Either failure
refuses startup. Hosts embedding the library must establish equivalent process protection. Socket and runtime directories must be owner-only; socket peers must match the
process UID. A failed peer-credential lookup refuses only that connection, so a
client disconnect cannot terminate the recovery service. No credential is passed in argv, an environment fallback, protocol reply,
provider prompt, routine log or raw diagnostic. This local IPC boundary trusts the
same-user deployment; it is not an OS sandbox against a compromised same-UID process.

`ProcessConfig` version 4 requires `state_dir`, `socket_path`, `admission_key`,
`max_secret_bytes`, `max_frame_bytes`, `max_connections`, `io_timeout_seconds`, `routes`.
Optional `log` sets EnvFilter directives; see [Logs](#logs).
Each route names a concrete `account`; `account_sharing_key` is null for tenant/account
isolation, or explicitly pools execution across routes or tenants. Shared bindings
must agree on account limits. Each route requires the `RouteConfig` fields documented
in the Rust type, with defaults for omitted request/response byte limits as described
below. Field/token/concurrency/timeout settings remain explicit and finite. The escaped identity
field bounds, response allowance and reply envelope must fit `max_frame_bytes`
together; invalid bounds name `max_frame_bytes`, `max_field_bytes` and
`max_response_bytes`, and an oversized reply returns `LimitExceeded`.
These are configured
limits, not measured capacity; their labels and qualification follow
[boundary.md](boundary.md#bounds-as-labelled-settings). Startup registers every
route with the runtime. Concurrency, rate buckets and cooldowns are grouped by
`(tenant, account)` when `account_sharing_key` is null; matching model routes in
independent tenant accounts remain isolated. The same non-null sharing key
explicitly pools execution limits across routes, models and tenants. Bindings
in one group must agree on concurrency and pacing limits or startup is refused.
Route and registry validation runs before creating state, acquiring the process
lock, loading the admission key or opening the runtime. Frame-bound conflicts
return `InvalidFrameConfiguration`; other configuration conflicts return
`InvalidRequest`; actual state/IO failures return `StateUnavailable`.

Applications can check deployment routes before startup with the public
`symbiotic_credential_process::validate_routes(&config.routes, config.max_frame_bytes)`
API. It uses the same route and registry checks as `CredentialProcess::open`,
including destination restrictions, loopback HTTP opt-in, provider settings,
byte limits, accepted-attempt limits, request budgets and shared account policies.
It opens no files or runtime, loads no secrets and invokes no secret resolvers;
validating with a nonexistent state directory does not create it. Process-wide
settings, OS protection, state availability and credential availability remain
startup checks. Validation returns the first refusal in startup order, using the
same `EgressError`; it does not accumulate errors. Frame bounds require
`max_frame_bytes >= 4096 + 24 * max_field_bytes + 4 * max_response_bytes`, with a
minimum of 4124 bytes, and `max_input_bytes <= max_frame_bytes / 2`.
Deserialize into `RouteConfig` first: unsupported `answer_recovery`, `thinking`
and `reasoning_effort` values are refused there with their supported enum values.

Dropping the last process handle explicitly releases its lock so descriptors
inherited by concurrently spawned children cannot delay a subsequent reopen.
Unknown config fields
are refused. `requests_per_minute` and `input_units_per_minute` must be positive
when present; null leaves pacing unrestricted.

### Default provider byte limits

Omitting route `max_input_bytes` or `max_response_bytes` selects **1,048,576 bytes
(1 MiB)** for that field. Explicit positive values override the defaults; zero
and JSON null are refused. The same defaults apply to omitted
`ProviderLimits.max_request_bytes` / `.max_response_bytes` in version-1 registry
bindings and to directly constructed HTTP adapters. The `limits` object itself
remains required for a registry binding. The named constants
[`DEFAULT_MAX_REQUEST_BYTES` and `DEFAULT_MAX_RESPONSE_BYTES`](../../crates/symbiotic-model/src/registry.rs)
live in `symbiotic-model`, re-exported through `symbiotic_ai_runtime::model`.
Credential-process version-4 route deserialization and the adapters use that
shared definition; applications need no local copy.

For an OpenRouter `qwen/qwen3-embedding-8b` route using `compatible_embedding`
with adapter `open_ai_embedding`, operator `openrouter`, dimensions 1024 and
`embedding_input_tokens: 32000`, the 250 × 1024 loopback batch in
[`embedding_route.rs`](../../crates/symbiotic-credential-process/tests/embedding_route.rs)
measures **2,729,388 response bytes**. It needs raised bounds: the test accepts
`max_response_bytes: 8388608` (8 MiB) and `max_frame_bytes: 37748736` (36 MiB),
with `max_field_bytes: 1024`; the default response limit returns
`EgressError::Provider { status: None }` with the `max_response_bytes_exceeded`
diagnostic. These are sufficient bounds for the
synthetic fixture, not a measured live-provider maximum.

These are **provisional configured hard limits per provider call**, not measured
capacity or guaranteed production provider maxima. Repository evidence is limited
to the [example registry's 8,192-token context](../../examples/model-registry.json),
[Jev's 64,000-token request budget](../../crates/symbiotic-model/src/classify.rs),
[retrieval encoder fixtures with a 100,000-byte allowance](../../crates/symbiotic-model/tests/retrieval_transport.rs),
and [chat transport fixtures with 65,536-byte request/response allowances and a
16,000-token Anthropic output default](../../crates/symbiotic-model/tests/anthropic_transport.rs).
The request default covers sixfold JSON escaping of 100,000 input bytes with
448,576 bytes of headroom for wrappers and model names. The response default is
16 times the fixture allowance, leaving 983,040 bytes of headroom for longer text,
thinking blocks and JSON envelopes. The repository does not specify a universal
byte-per-token bound or production maxima; deployments needing more must supply
explicit byte limits based on their provider/model configuration.

`max_input_bytes` still bounds both the typed payload and the complete encoded
HTTP request, enforced by the shared capped encoder before permit consumption and
at transmission. The response bound still applies before buffering successful
HTTP bodies, including chunked bodies. Exceeding either limit returns an error;
no input or output is truncated. Error bodies are discarded and their HTTP status
reported. `max_frame_bytes` has no new default and must still fit twice the request
allowance and the existing fourfold response allowance plus escaped identities
and the reply envelope. A frame too small for the chosen defaults is refused with
the existing configuration error; it is never automatically enlarged.

Child-process admission keys and authenticated provider routes use:

```json
{"backend":"owner_only_file","path":"/private/egress/provider-key"}
```

Keyless provider routes use:

```json
{"backend":"none"}
```

The `none` source is only for keyless provider routes, never admission keys.
Unknown secret backends are refused during configuration deserialization.
File values are exact UTF-8 bytes, with no automatic trimming. Files must be owned by
the process UID, be regular, have no group/other permission bits, and not be symlinks.
Static file values are never rotated by Foundation. Login renewal uses the
[login account protocol](#passthrough-routes-and-login-accounts).
Secret buffers and adapter key storage zeroize on drop.

Configured providers include `open_ai_chat { operator, thinking, reasoning_effort }`,
`anthropic_chat { operator, thinking }`, `jev_classifier { operator }`,
`gemini_embedding { dimensions }`, `compatible_embedding { adapter, operator,
dimensions, embedding_full_dimensions, embedding_input_tokens }` (adapter `open_ai_embedding` or
`ollama_embedding`), and `cohere_rerank { operator, rerank_input_bytes,
rerank_candidates, rerank_context_tokens, rerank_query_tokens }`. The token budgets
are the deployed model's usable query/document context after special/template tokens
and its query capacity. The shared adapter conservatively bounds UTF-8 bytes against
these token capacities and sends `max_tokens_per_doc` explicitly; see
[the admission contract](ai-runtime.md#configured-registry). They compile into the same validated registry as embedded
calls. Compatible transports use the configured base URL and preserve one HTTP
request per permit; Ollama batching is refused. Chat uses its configured base URL; Gemini is pinned
to `https://generativelanguage.googleapis.com/v1beta` and safe model-name characters.
HTTPS is required except explicitly enabled loopback HTTP. Userinfo, URL queries,
fragments, caller-controlled hosts and redirects are refused. The credential process ignores
ambient HTTP/HTTPS/ALL proxy settings so only the configured destination receives secrets.

OpenAI-compatible `thinking` is optional (`enabled` or `disabled`) and sends
`{"thinking":{"type":"enabled"}}` or `{"thinking":{"type":"disabled"}}` only
when configured. Optional typed `reasoning_effort` (`low`, `medium`, `high`) sends
that string only when configured; combining it with disabled thinking is refused.
Rabbithole's deployed DeepSeek profile uses enabled thinking and low effort. These
settings participate in the existing route configuration revision and use the same
encoder for admission byte checks and HTTP transmission.

Direct dispatch `DispatchResult` chat output includes
`finish_reason: Option<FinishReason>`. OpenAI `stop` and
Anthropic `end_turn`/`stop_sequence` map to `stop`; OpenAI `length` and Anthropic
`max_tokens`/`model_context_window_exceeded` map to `length`; other reported values
map to `other`. An absent provider reason remains absent. Token-limited text remains
available with its finish reason and accounting receipt, including recovery.
Arbitrary provider reason strings are excluded from direct dispatch output.
Anthropic tool, pause and refusal outcomes remain visible errors under the existing
Messages adapter contract; their partial text is never returned as a supported chat answer.

`ProviderPayload::Classify(ClassifyRequest)` uses the existing Jev System One
adapter at `POST {destination}/systemone`. The state is a JSON object containing
application data; questions are typed Noul, Choice or Score values. No arbitrary
HTTP body or provider options pass through. Question and option vector order is
the provider presentation order; returned answers preserve question order. The
route model is also the expected served model. Direct dispatch returns only the
validated `ClassifierAnswer` vector; raw response, provider metadata and local
trace labels are excluded. Applications own classification meaning and thresholds.

With `answer_recovery: "retain"`, job completions retain the sanitized canonical
runtime response, as described by
`JobDelivery::output`: chat retains the provider's finish-reason string, and
classification includes `served_model` and `trace` alongside `answers`. Raw
provider responses and provider trace metadata are removed; runtime bookkeeping
remains in the trace. The direct dispatch normalization and answers-only
guarantees do not apply to job completions.

Classification uses the same signed payload digest, grant revision acceptance,
exclusive authority deadline, permit consumption, account reservation and recovery
path as chat. Admission and transmission share the Jev encoder and question/token
checks. One permit authorizes one HTTP send; reattachment or recovery never resends
it. A crash after sending and before durable completion retains unknown spend and
refuses replay. Existing model jobs also execute classification through their shared
runner. No new database, index, queue, cache or scheduler is introduced.

The `rabbithole_*` integration tests in
`crates/symbiotic-credential-process/tests/egress.rs` cover settings omission and
exact request bodies, finish reasons and recovered output, typed Jev answers and
once-only accounting, pre-consumption validation, and executable kill/reopen
without a second provider request.

`max_input_bytes` bounds both the typed payload and the complete encoded HTTP body,
including model names, repeated Gemini batch wrappers and JSON escaping. The same capped
provider encoder runs before permit consumption/secret loading and at transmission;
the adapters send its resulting bytes without re-serializing them. Oversized admission
returns `LimitExceeded` without consuming the permit.

The shared adapter-result boundary (`secrets::credential_boundary` in
`symbiotic-model`) wraps every credential-bearing HTTP adapter's complete result,
including typed decoding and answer validation, before runtime bookkeeping.
Queued calls also require the credential owner's opaque guard before any result
writes; raw adapters and chat-backed classification use that same boundary.
Injected credential-bearing adapters without a Foundation-owned guard are refused.
On
success it refuses credential echoes in raw JSON and typed output: exact bytes,
JSON-escaped UTF-8, numeric re-spellings (including `arbitrary_precision` numbers),
percent-encoded UTF-8 (upper/lower hex), standard Base64 and URL-safe Base64
(padded/unpadded). It discards raw provider JSON on every successful result,
including keyless calls where credential inspection does not apply. On failure it
preserves typed error classes with static messages, so provider text never reaches
receipts, traces, queue storage or the response cache. All adapter clients are
Foundation-owned, disable redirects and HTTP retries, and ignore ambient proxies;
public client injection is unavailable.
The shared Gemini adapter rejects non-finite embedding components in single and
batch responses with a static provider failure; dispatch retains an unknown charge.
Other transformations are outside that finite guarantee. Persistent response caching,
request debug capture and raw trace metadata forwarding are disabled. Provider-side
cache isolation remains a provider-configuration/deployment responsibility; the broker does not
claim control over a remote provider's private prefix-cache implementation.

In socket mode, binders take an exclusive `flock` on the owner-only (`0600`)
`<socket_path>.bind.lock` file beside the socket (for example, `egress.sock.bind.lock`
for `egress.sock`), including when different state
directories name the same socket. The lock covers the connection probe, stale
socket removal, bind and permission setup, and is released when bind returns.
Only `ECONNREFUSED` permits stale socket removal; a live listener or non-socket
path is refused. Keep the lock file in place across restarts: removing it could
let competing binders lock different inodes. Preserve the state directory to
preserve single-use and accounting history.

## Evidence boundary

Targeted synthetic loopback tests exercise credential injection, all declared encodings,
response/error/log isolation, durable replay, grant-revision publication and acceptance, unknown charges,
cancellation, same-attempt attachment after lost permit/completion IPC replies, restart
recovery, digest mismatch refusal, exclusive result expiry, no cache, new-attempt retry,
pinned destinations, redirects, response/frame
limits, file protection, unsupported secret backend refusal and real executable IPC. These fixtures
are not a live-provider qualification or physical power-loss certification. The tests cover this backend's revision ordering and ledger accounting; Memory must
implement the trusted publication integration and its data authorization checks.
Memory input-authorization and guarded-commit verification belongs to Memory.

The `rabbithole_usage_metadata_round_trips_in_process_and_recovery`,
`rabbithole_classification_usage_metadata_survives_projection`,
`rabbithole_response_identity_without_token_usage_recovers`, and
`rabbithole_failure_classes_round_trip_in_process_and_recovery` tests cover safe
usage and typed failure recovery through the in-process client. The shared HTTP
unit tests cover numeric/date retry hints and send/body-read timeouts.
`regression_http_connect_failure_has_per_call_zero_charge_evidence` covers a
refused connection, error-local evidence and preservation through diagnostics;
`regression_http_tls_setup_failure_is_zero_but_connect_timeout_is_unknown`
covers TLS setup failure and timeout; `regression_http_status_failure_has_unknown_charge`
and `regression_direct_and_egress_transport_errors_keep_their_contracts` cover
HTTP 500 and send/body-read failures retaining unknown charge.
`regression_http_connect_failure_releases_spend_and_allows_bounded_retry`
covers in-process chat/Jev reservation release, recovered accounting, advancing
retry dispatches and refusal at the configured attempt limit. Its fixture expires
the existing durable cooldown between retries to avoid waiting for production jitter. All new network regressions
have explicit deadlines; they do not qualify live providers.

## Passthrough routes and login accounts

Version-4 `ProcessConfig` accepts an optional `passthrough` section.
`PassthroughConfig` names a second Unix socket, a connection limit, exact routes
and an optional health file. A deployment may configure passthrough without typed
routes. `ProcessConfig::validate_routes()` checks both route sets and their shared
account policies without reading credentials or creating state. The existing
free `validate_routes(routes, max_frame_bytes)` function checks typed routes only.

Each `PassthroughRoute` binds an exact method and origin-form path to one HTTPS
URL. Query strings, percent escapes, fragments, backslashes, duplicate request
headers, `Transfer-Encoding` and `Expect` are refused. Requests require HTTP/1.1,
`Host` and a bounded `Content-Length`. GET may omit Content-Length for an empty body. The existing `max_frame_bytes` setting
bounds the complete HTTP header block. Route byte settings bound the body.
The second socket uses the typed socket's owner-only directory check, bind lock
and same-UID peer check. It accepts one request per connection.

The provider receives the caller body unchanged. Only configured `forward_headers`
are copied, with printable ASCII values of 1 to 8 192 bytes. Authentication,
cookies, host, forwarding and hop-by-hop headers are excluded. The process injects
the credential and `Accept-Encoding: identity`. The client ignores ambient proxies,
disables HTTP retries and follows no redirects.

Routes register with `Runtime::account_admission`. This facade uses the same
`ModelAdmission` FIFO semaphore, `ModelRateState` request bucket and queue backend
as typed providers. The backend's existing cooldown row remains authoritative;
no passthrough queue, cooldown database or second scheduler is created. Registration
requires matching slot caps, request rates and `rate_burst_seconds`. The burst
setting defaults to zero, as in the existing runtime. Typed configuration routes
use that default. Shared typed bindings must leave input-unit pacing and provider
request limits unset. Raw requests consume the shared request bucket; their
undecoded bodies have no typed input-unit estimate.
Chat Completions POST reserves each dispatched attempt in the existing spend ledger.
GET listings and Responses do not create reservations.
`ModelRequestGrant::start_checked` holds a consumer's admission guard through the
first transport poll. A deferred send consumes no pacing. This orders login-wide
new-session holds even when routes use distinct runtime account scopes. Failure
counts share the runtime account scope; session holds share the login store.
The bare cooldown backend costs one read before preparation and one under the
send gate, plus a monotonic write when extending a cooldown. Session checks add
bounded memory lookups and no database rows. Holding Responses item events prevents partial
items reaching callers; retaining original complete items earns exact continuation
replay. The health file remains a derived view of that process and backend state.
An eligible attempt reads the existing cooldown twice: before preparation and
under the existing account rate gate immediately before dispatch. A 429 takes
that gate for one monotonic deadline update. The gate stays held through the first
transport poll, then releases while the response is pending. Typed sends use the
same ordering. Waiting repeats the lookup after each deadline. The facade adds
no stored copy.

Slot waits last until admission or caller disconnect. Each send takes the existing
rate gate, rechecks credential/cooldown state and charges pacing. First sends,
resends and continuation sends use that same path. Both `login_required` and
`renewal_failed` refuse admission after every wait. A static-key 401 gets a local
fixed 401 without upstream headers. Login 401 renewal remains limited to one exchange.

The Responses adapter classifies failures as `throttle`, `network`, `server` or
`final`. One retry engine applies `retry_policy` overrides to defaults: unlimited
same-route retries for throttle/network, three total attempts for server, and fail
for final. HTTP Retry-After takes precedence over the shared core retry backoff on
consecutive account failures. Successful completion resets that count. Optional
`fallback_routes` names are used only by `switch`; an empty list means fail.
`PassthroughConfig.retry_policies` contains named overrides selected through the
local `x-symbiotic-retry-policy` header. Unknown names get 400.
`CredentialProcess::open_with_retry_hook` supplies the embedded-only hook.
Neither the header nor hook is sent upstream. The queue never relies on provider
retry behavior in client tools.

The caller may send local `x-symbiotic-request-id` with 1–64 printable ASCII bytes.
Health lists identified waits with their reason and Unix-second start time.
`x-symbiotic-queue-waited-ms` reports total waits before the first downstream byte.
`max_queue_wait_seconds`, absent by default, bounds cooldown waiting only. Local
bounded-wait responses carry only a computed Retry-After and local headers.

`new_session.session_headers`, absent by default, identifies session tuples. Each
route keeps a memory-only set of successful tuples, at most 100 000, oldest-first
and expired after 24 h. Missing every configured header disables session treatment.
With no hold, new sessions flow freely under ordinary account limits. A retried
new-session 429 sets a login-wide hold using Retry-After or shared backoff without
setting account cooldown. New-session waiters release their slots; seen sessions
continue. After the hold, one probe is allowed until a new-session 2xx opens the
gate. Another 429 extends the hold. Health exposes the hold's Unix deadline.

The provider-neutral `symbiotic-model::item_stream` enum carries complete opaque
items, deltas and their identity, provider sequence, usage and classified errors.
The adapters support `stream: "openai_responses"` and `stream: "openai_chat_completions"`.
Chat Completions accepts SSE and non-streaming JSON. Original bytes are relayed
without re-encoding. Unchanged Responses events round-trip exactly.
For Responses, the credential process holds each item's events until its done event, then releases
them in order.
Responses POST performs no response-body credential matching (operator decision 2026-10-26).
Issue #162 requires Chat Completions and GET JSON listings to scrub held credentials
using the model credential encoding set. One incremental scrubber replaces matches
with `[REDACTED]`, including matches split across network chunks.
The corresponding Warden matching-filter removal is tracked by
[warden#493](https://github.com/symbiotic-build/warden/issues/493).
`max_held_event_bytes` defaults to 67 108 864 and bounds one held Responses item's events.
Chat Completions forwards complete SSE records progressively after scrubbing;
the same setting bounds each record, not a complete choice.
Overflow emits a visible error.

On a handled Responses mid-answer failure, the queue discards partial output and continues
with the original input plus every completed provider item exactly as received,
including encrypted reasoning. Complete interleaved items survive partial-item
discard. Release compacts discarded output positions.
Joined attempts use the first response ID,
contiguous sequence numbers and offset output indexes. Later created/in-progress
events are suppressed. One final completed event contains all output items and
summed reported usage. Fail decisions emit the original error event.
Caller disconnect cancels all work. Only JSON and uncompressed event streams are
accepted. Idle timeout is classified as network failure and uses the same policy.
Chat Completions uses the same retry engine before the first downstream byte.
After that byte, a failure stays visible and no policy or hook may retry or switch. Fallback routes must use the same method and dialect.
The SSE `[DONE]` sentinel completes a response; EOF before it is a disconnect.
A final usage chunk supplies prompt, completion, cached and reasoning token counts.
Non-streaming JSON uses its `usage` object. Successful terminal responses settle
reported numeric usage in the canonical ledger. Missing usage, errors, cancellation
and disconnects keep `Unknown`. Provider identity text and answer bytes are excluded.
The ledger adds one existing receipt row per dispatched Chat Completions POST attempt and uses its
native reserve/finish transactions. It adds no table or index. Independent generated
attempt invocations prevent unknown receipts from blocking subsequent passthrough sends.
Settlement precedes the final HTTP chunk; a ledger failure leaves HTTP incomplete.

For model listings, configure the existing `method: "get"`, exact `path` (for example
`/v1/models`), destination URL and account credential. Keep the account's `stream`
value. GET accepts JSON only and uses the same scrubber and account admission.
No new configuration key or spend receipt is needed for listings.
Responses receipts and usage trailers remain tracked by
[#165](https://github.com/symbiotic-build/symbiotic-foundation/issues/165).

For a MiMo host, this example is one entry in `passthrough.routes`. The host
provisions the key file with owner-only access and raw key bytes without a trailing
newline. The limits are example deployment
values; account-sharing routes must use matching account limits.

```json
{
  "name": "mimo-chat",
  "tenant": "warden",
  "account": "mimo",
  "account_sharing_key": null,
  "method": "post",
  "path": "/v1/chat/completions",
  "destination": "https://token-plan-ams.xiaomimimo.com/v1/chat/completions",
  "secret": {"backend": "owner_only_file", "path": "/run/warden-model/mimo-api-key"},
  "stream": "openai_chat_completions",
  "forward_headers": ["content-type", "accept"],
  "return_headers": ["x-request-id"],
  "max_request_bytes": 8388608,
  "max_response_bytes": 67108864,
  "max_in_flight": 2,
  "requests_per_minute": null,
  "idle_timeout_seconds": 120
}
```

The caller selects `xiaomi/mimo-v2.6-flash` in its request body. For streamed usage,
it asks for `stream_options.include_usage: true`. Foundation does not add or rewrite
those request fields. A listing route uses `method: "get"`, `path: "/v1/models"`
and its fixed provider models URL, with the same account and limits.


Configured `return_headers` carry safe printable ASCII values of 1–8 192 bytes.
Recognized provider rate-limit headers are forwarded on every status, including
200, with the stricter 1–256 byte bound. The Responses adapter recognizes only
primary/secondary used-percent, window-minutes, reset-at, limit-name and the four
specified credit/reached-type headers. It accepts hyphenated server family names.
Promotional headers are excluded. Values containing credentials are dropped.
The latest safe provider rate-limit values and observation time appear in health.
Responses without those headers leave the last observation. Authentication,
cookies and hop headers are refused in configuration. Local responses carry no
upstream headers. An unmatched method/path returns 404 `route_not_found`.

`SecretSource::LoginFile { path, family }` is valid only for passthrough.
`chatgpt_codex`, `xai_grok` and `kimi_code` are closed login families with fixed issuers,
client IDs, renewal encodings and credential headers. The version-1 JSON store
refuses unknown fields, wrong family values and insecure files. Tokens use
zeroizing storage and have no diagnostic representation. A lifetime-exclusive
nonblocking lock prevents another owner or import while the process runs.

Renewal occurs within 300 s of expiry under one async mutex per store.
Grok logins also renew in the background at expiry minus 300 s while the process
runs, without requests. Codex and Kimi renew only on requests.
Concurrent callers and background renewal share the completed renewal. Before an
exchange, the process writes and syncs a nonempty renewal fence and syncs its directory. It installs
rotated tokens by syncing a temporary store, renaming it and syncing the directory.
Import follows this same durable-install-before-fence-clear ordering.
Only then does it truncate and sync the fence. A cancelled or uncertain exchange
leaves the fence set and requires fresh import. A transport failure established
before send clears the fence and permits renewal on the next request.
Grok background renewal retries safe-to-retry failures after 60 s and stops when
the login requires import or the fence remains set.
One upstream 401 forces renewal and one resend. A second 401 marks
`renewal_failed` and returns 401. It sets the existing refusal fence so restart
still requires a fresh import; a reopened store reports `login_required`.
The process refuses new admission before writing that terminal refusal fence.
A fence write failure cannot restore the rejected login's in-process usability.
An unusable login returns 503
`{"error":{"code":"credential_unavailable"}}` without forwarding.

The import command reads a native provider login without running a provider tool
or making a network request:

```sh
symbiotic-credential-process login import --family chatgpt_codex --native native-login.json --store login.json
```

For Kimi Code, use `--family kimi_code` and the Kimi CLI's owner-only
`~/.kimi-code/credentials/kimi-code.json` as `--native`. The process reads flat
`access_token`, `refresh_token` and numeric `expires_at` fields. It rounds
fractional Unix seconds down and renews with a form grant at
`https://auth.kimi.com/api/oauth/token`, using public client ID
`17e5f671-d194-4dfb-9706-5516cb48c098`. Kimi's provider destination is
`https://api.kimi.com/coding/v1`; its route requires the Chat Completions dialect
from [#162](https://github.com/symbiotic-build/symbiotic-foundation/issues/162).

Import validates the family-specific fields and expiry, takes the
same lifetime lock and atomically installs an owner-only store. Replacing a store
requires `--replace`. A successful import clears the renewal fence. The native
login file becomes stale after the first refresh-token rotation.

When `health_path` is configured, the process atomically publishes version-1 JSON
at startup, on state changes and at least every 10 s. Runtime cooldown-write
notifications also trigger publication when typed providers set the deadline.
Each replacement is synced and has mode 0640; the provisioned directory supplies its group. The file contains
only `version`, `state`, `updated_at` and per-route `name`, `login`, `expires_at`
and `cooldown_until`, plus `retried_events`, `waiting`, `rate_limits`,
`rate_limits_seen_at` and `new_session_hold_until`. Login states are `ok`,
`expiring` within 300 s for Grok or two days for Codex and Kimi,
`renewal_failed`, `login_required`, or `static`. SIGTERM and SIGINT publish
`state: "stopping"` before closing the socket. Readers reject missing or malformed
files, timestamps older than 30 s, stopping processes and unusable login states.
There is no socket health endpoint.

Only Chat Completions POST creates passthrough spend receipts; Responses POST and
GET listings create none. Passthrough adds no usage trailers. Typed-route
admission, accounting and recovery retain their existing protocol.
Provider fixtures use a test-only mapping to loopback HTTP; production routes
always require HTTPS. Tests cover real Unix sockets and executable shutdown in
[`passthrough.rs`](../../crates/symbiotic-credential-process/tests/passthrough.rs),
with provider fixtures in
[`passthrough_tests.rs`](../../crates/symbiotic-credential-process/src/passthrough_tests.rs).
Login fixtures are in
[`login.rs`](../../crates/symbiotic-credential-process/tests/login.rs)
and the login module's unit tests.

## Logs

The credential-process binary uses `symbiotic_core::logging` (feature `logging`).
It writes one JSON object per line to stderr, which systemd sends to the journal.
Event fields are at the top level, alongside `timestamp`, `level`, `target` and
`spans`. A request event has this shape:

```json
{"timestamp":"2026-10-27T00:00:00Z","level":"INFO","message":"request completed","route":"responses","outcome":"complete","status":200,"attempts":2,"queue_waited_ms":1000,"elapsed_ms":1200,"target":"symbiotic_credential_process::passthrough::forward","spans":[{"name":"model_request","request_id":"synthetic-request"}]}
```

Passthrough events are `request admitted`, `queue wait`, `request sent`,
`upstream status`, `upstream failure` and `request completed`.
`upstream failure` remains a warning. Waits that round to zero milliseconds are
debug events; other waits are info events. `request sent` reports the accumulated
queue wait at info level.
Completion reports the caller's HTTP status, attempt count, total queue wait,
elapsed time and `complete` or an error code. If no HTTP status was sent,
completion omits `status`. A disconnected caller has outcome `cancelled`; its
completion still counts a send already started and the wait in progress.
Malformed heads, unknown routes and oversized bodies also emit one completion
with zero attempts. Disconnects during body receipt emit `cancelled` without a status.
The request span uses the validated `x-symbiotic-request-id` header. The span is
ERROR level, so a `warn` filter still keeps the request ID on warnings.
Absent or invalid IDs use `local-` plus a generated UUID v4 in logs only.
Health waiting entries still require a valid caller-supplied ID.
Credentials, bodies and other header values are excluded from these events.

The optional top-level config field `log` accepts EnvFilter directives, for example
`"log": "warn,symbiotic_credential_process::passthrough::forward=debug"`.
A nonempty `SYMBIOTIC_LOG` overrides it; the default is `info`.
Invalid directives refuse startup. To override a systemd instance on build-i9,
run `systemctl edit warden-model@codex` and add:

```ini
[Service]
Environment="SYMBIOTIC_LOG=info,symbiotic_credential_process::passthrough::forward=debug"
```

Restart the instance to apply the override.
Find one request across all instances by its exact span field, or find IDs
starting with a prefix. `journalctl -g` uses PCRE2 regular expressions over JSON
lines. JSON-encode the ID first, regex-escape that encoding, then shell-quote the
command. Raw IDs and `\Q...\E` substitution miss JSON escapes and can mishandle
embedded `\E` or shell quotes. build-i9 has `python3` and no `jq`.

This example ID, `job"part\tail`, contains both a quote and a backslash.
Python prints two shell-quoted commands. Run the exact command for that ID,
or the prefix command for all IDs beginning with it:

```sh
python3 - <<'PY'
import json, re, shlex
request_id = r'job"part\tail'
encoded = json.dumps(request_id)
for match, value in [("exact", encoded), ("prefix", encoded[:-1])]:
    pattern = re.escape('"request_id":' + value)
    print("# " + match)
    print(shlex.join(["journalctl", "-u", "warden-model@*", "--since",
                      "1 hour ago", "-o", "cat", "-g", pattern]))
PY
```

Show only upstream failures of one instance:

```sh
journalctl -u warden-model@codex --since today -o cat -g '"message":"upstream failure"'
```
