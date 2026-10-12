# Generic job store version 1 (proposal)

Following the lead's queue design §§13–14, version 1 provides atomic batch enqueue
with scoped keys, deduplication and key conflicts; claims with leases and
claim-generation fencing; completions with delivery leases and fenced Accepted
or Discarded confirmation; job/group cancellation; owner-tag erasure with a
sticky purged flag; live population/input/result bounds; bounded maintenance; transaction
participation; per-job lookup/status and paged Failed/Uncertain diagnostics.
Payloads and handler results are opaque bytes (`Vec<u8>` in memory, SQLite
`BLOB`), preserved exactly. Paid answers remain exclusively in the spend ledger;
the store retains only their receipt reference and reconciliation state.

Following §16, the claim is the handoff: one transaction marks the job Running
for its claim generation and returns its payload. The runner passes that payload
directly to the handler, with no entry lookup or snapshot checks. Cancellation
or owner erasure committed before the claim prevents it; after the claim, the
heartbeat signals the running handler. Erasure permits in-flight work to finish
but stores no output or recovery copy.

After a crash, an expired handler lease can be claimed again only within the
job's frozen attempt ceiling. A crash between claim and handler entry consumes
an attempt; with `max_attempts = 1`, recovery ends Refused without running the
handler. Handlers with side effects must deduplicate by the stable scoped job
key, which stays the same across claims. The claim generation
(`JobContext::attempt`) is only a fencing token for queue writes.

Later: admission attempts/notices → PR 4 (Memory egress); checkpoints → Warden adoption PR; cache-origin results → the caching consumer PR; priority/background share → the shared-scheduler consumer PR; group summaries/resumable rebuild → Memory adoption PR (D5b Q3).

The store uses `JobConfig` version 1 on SQLite only, including `:memory:` for
an in-memory runtime. Every default below is
**PROVISIONAL**; R9-D2 settles final values. Apps supply versioned configuration.

| Setting | Provisional default | Reason |
|---|---:|---|
| `max_live_jobs` | 1,024 | Bound pending, running (including Uncertain) and final-but-unconfirmed jobs. |
| `max_pending_bytes` | 16 MiB | Bound retained raw input bytes plus encoded metadata before writes. |
| `max_batch` | 64 | Bound atomic enqueue and confirmation requests. |
| `max_page` | 64 | Bound claim, delivery and diagnostic selections. |
| `max_page_bytes` | 1 MiB | Bound serialized candidate and completion arrays. |
| `max_result_bytes` | 16 MiB | Refuse larger raw handler results or encoded paid answers before retention. A handler result must also fit one `max_page_bytes` delivery page, or its job fails. |
| `maintenance_bytes_per_pass` | 16 MiB | Bound raw payload/result and ledger recovery bytes erased per expiry pass. |
| `claim_lease_seconds` | 30 seconds | Permit timely handler recovery after a lost worker. |
| `delivery_lease_seconds` | 30 seconds | Reduce overlap while retaining at-least-once delivery. |
| `maintenance_batch` | 64 | Bound each expiry pass. |
| `retention_seconds` | 7 days | Allow consumer recovery when no explicit result deadline is provided. |

Enqueue and claim use `(created_at, id)` order. Final delivery uses
`(finished_at, id)`; diagnostics use ascending IDs with exclusive cursors.
Owner erasure and group cancellation visit ascending IDs atomically.
SQL timestamps are INTEGER Unix
milliseconds within chrono's representable range; SQLite quantizes
ordinary clock precision to milliseconds and rejects leap seconds or overflowing
deadlines before commit.

Pending utilization is derived from unfinished canonical rows: raw payload
length plus the encoded metadata tuple `(scope, key, group, owners, kind,
execution, max_attempts)`. No pending-byte counter is stored.

Expiry processes jobs in `(recovery_until, id)` order, up to `maintenance_batch`
and `maintenance_bytes_per_pass`. The byte budget counts raw payload, handler result and paid ledger recovery
copies erased by the pass. A nonempty pass always erases at least one job: a job
larger than the budget is erased alone. Subsequent jobs are deferred if they
would exceed the remaining budget; a pass stops when its budget is reached.
Unfinished jobs never expire solely because their recovery deadline passed.

Following §14, enqueue refuses new jobs with `QueueFull` at `max_live_jobs`;
replaying existing keys consumes no capacity. Claim, completion, recovery expiry
and owner erasure do not free population capacity: consumer confirmation does.
The live count is derived from the existing unfinished and unconfirmed-final
indexes. Lowering the bound refuses new work until enough jobs are confirmed;
existing work remains claimable, deliverable, confirmable and erasable.
Unconfirmed finals retain their partial delivery index; Failed/Uncertain
diagnostics retain their partial index. Owner membership is derived from
canonical owners in an indexed table and removed on confirmation. Erasure and
group cancellation process the bounded live population in one transaction.

Following §14 and the lead's settled tombstone decision, confirmation retains
only scoped identity, key, request digest, final state, disposition and receipt
reference, plus the latest delivery ordinal. The digest continues to reject
conflicting key replays; confirmation accepts only issued ordinals
`1..=delivery_generation`, including earlier deliveries after a lease expires.
All other job columns become SQL `NULL` and owner memberships are deleted in
the same transaction. The shared `JobRecord` read API supplies neutral values
for absent operational fields (empty/zero/false, absent options, Handler
execution and Unix epoch creation time); these defaults are not persisted.

Completion selection reads stored encoded result lengths and delivery metadata
before loading saved answers. The existing length column counts the serialized
byte-array representation for exact page preflight, while the result limit and
maintenance budget count raw bytes. `Complete` keeps a handler result only when
its one-item delivery fits `max_page_bytes`, measured with the widest delivery
ordinal and lease. Otherwise the same write ends the job Failed with
`queue_failure` (Cancelled when cancellation was requested), keeps no output and
returns `ResultRefused`; the runner reports it as `ResultTooLarge`. Because one
write refuses and finishes the job, an expiring claim cannot run the handler
again in between. The raw size this allows depends on
the result's byte values and the job's metadata. A raw byte encodes as two to
four bytes (`0,` to `255,`). So with the 1 MiB default page, a result made of
byte values 100 to 255 is refused above about 256 KiB raw, and one made of byte
values 0 to 9 above about 512 KiB. A newly completed handler result therefore
never blocks later deliveries. Two cases can still block them: handler metadata larger than
one page, and a `max_page_bytes` lowered after a result was stored
([#149](https://github.com/symbiotic-sh/symbiotic-foundation/issues/149)).
Diagnostics and expiry maintenance use content-free projections; expiry deletes
copies directly. Encoded lengths and
all indexes are rebuildable from canonical rows. The unreleased SQLite schema
version is 16; older layouts are refused without migration.
