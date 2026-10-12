//! Generic job records and atomic transitions, independent of the runner.
//!
//! IDs and all operations are scoped to a tenant, restore incarnation and queue.
//! Enqueue/claim order is `(created_at, id)`; final deliveries use `(finished_at,
//! id)`. Diagnostic and admission-notice cursors use ascending IDs.
//! SQLite owns atomicity: an error must roll back every write in an operation.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use symbiotic_core::DiagnosticCode;
use thiserror::Error;

/// Trusted host clock, sampled once after acquiring the operation's write transaction.
pub type JobClock = std::sync::Arc<dyn Fn() -> DateTime<Utc> + Send + Sync>;

/// Authorization namespace supplied by the trusted host, never by an unverified client.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobScope {
    /// Tenant namespace.
    pub tenant: String,
    /// Restore incarnation.
    pub incarnation: String,
    /// Queue within the tenant.
    pub queue: String,
}

/// Scoped durable identity; the job key is the logical invocation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobId {
    /// Authorization namespace.
    pub scope: JobScope,
    /// Opaque row identity.
    pub id: String,
}

/// Versioned store policy, supplied by the owning app.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobConfig {
    /// Supported configuration format (1).
    pub version: u32,
    /// Maximum atomic enqueue/ack batch (64).
    pub max_batch: usize,
    /// Maximum delivery/diagnostic/claim page (64).
    pub max_page: usize,
    /// Maximum serialized delivery page (1 MiB).
    pub max_page_bytes: usize,
    /// Maximum retained result: raw handler bytes or encoded paid answer (16 MiB; PROVISIONAL).
    /// A handler result whose delivery cannot fit one `max_page_bytes` page fails its job.
    pub max_result_bytes: usize,
    /// Hard pending, running and final-but-unconfirmed population bound (1024; PROVISIONAL).
    pub max_live_jobs: usize,
    /// Hard unfinished input-byte bound, including metadata (16 MiB).
    pub max_pending_bytes: usize,
    /// Delivery lease length (30 seconds).
    pub delivery_lease_seconds: u64,
    /// Claim lease length (30 seconds).
    pub claim_lease_seconds: u64,
    /// Final-result retention without an explicit deadline (7 days).
    pub retention_seconds: u64,
    /// Maximum expired results deleted in one maintenance pass (64).
    pub maintenance_batch: usize,
    /// Raw recovery-copy/result bytes erased per pass (16 MiB; PROVISIONAL).
    /// A nonempty pass always erases at least one job, even if it exceeds the budget.
    pub maintenance_bytes_per_pass: usize,
}

impl Default for JobConfig {
    fn default() -> Self {
        Self {
            version: 1,
            max_batch: 64,
            max_page: 64,
            max_page_bytes: 1024 * 1024,
            max_result_bytes: 16 * 1024 * 1024,
            max_live_jobs: 1024,
            max_pending_bytes: 16 * 1024 * 1024,
            delivery_lease_seconds: 30,
            claim_lease_seconds: 30,
            retention_seconds: 7 * 24 * 60 * 60,
            maintenance_batch: 64,
            maintenance_bytes_per_pass: 16 * 1024 * 1024,
        }
    }
}

/// Attempt ceiling frozen on first enqueue; duplicate keys cannot renew it.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobLimits {
    /// Maximum execution attempts, at least one.
    pub max_attempts: u32,
}

/// Execution owner, needed to distinguish reclaimable handler leases from paid uncertainty.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Execution {
    /// Product handler with no paid transport.
    Handler,
    /// Model transport; expired leases require ledger reconciliation in PR 3.
    Model,
}

/// One immutable keyed invocation.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobSpec {
    /// Idempotency key, unique within the scope.
    pub key: String,
    /// Optional cancellation/status label.
    pub group: Option<String>,
    /// Opaque input-owner tags; any match erases all recovery copies.
    pub owners: Vec<String>,
    /// Handler kind used to scope claims.
    pub kind: String,
    /// Execution/recovery owner.
    pub execution: Execution,
    /// Full waiting copy.
    pub payload: Vec<u8>,
    /// Opaque signed authority, interpreted only by the execution owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<Vec<u8>>,
    /// Frozen execution limits.
    pub limits: JobLimits,
    /// Limits final-result availability only.
    pub recovery_until: Option<DateTime<Utc>>,
}

/// Durable lifecycle, including separate confirmation dispositions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum JobState {
    /// Waiting for a claim.
    Pending,
    /// Waiting for a successor signed authority; no claim or charge consumed.
    AwaitingAdmission,
    /// Leased to a worker.
    Running,
    /// Charge may exist; cannot be claimed blindly.
    Uncertain,
    /// Final success.
    Succeeded,
    /// Final failure.
    Failed,
    /// Final cancellation.
    Cancelled,
    /// Final refusal.
    Refused,
    /// Erased input/output.
    Purged,
    /// Consumer saved the result.
    Accepted,
    /// Consumer rejected the result.
    Discarded,
}

impl JobState {
    /// Whether execution/reconciliation still needs the waiting copy.
    pub fn unfinished(self) -> bool {
        matches!(
            self,
            Self::Pending | Self::AwaitingAdmission | Self::Running | Self::Uncertain
        )
    }
    /// Whether a confirmation already won.
    pub fn acked(self) -> bool {
        matches!(self, Self::Accepted | Self::Discarded)
    }
}

/// Origin of a final answer; paid output is owned exclusively by the ledger.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ResultOrigin {
    /// Ledger owns the saved answer and receipt.
    Paid,
    /// Product-handler output; no receipt required.
    Handler,
}

/// Canonical job row. Confirmed rows retain only scoped identity, key, digest,
/// disposition (`state`), final state, receipt and delivery generation.
/// Other fields use neutral values when reading a tombstone: absent options,
/// empty collections/strings, zero counters, false flags, Handler execution and
/// the Unix epoch for `created_at`. These values are not stored metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobRecord {
    /// Scoped row identity.
    pub id: JobId,
    /// Logical invocation key.
    pub key: String,
    /// Digest retained after payload deletion.
    pub digest: String,
    /// Status/cancellation group.
    pub group: Option<String>,
    /// Input provenance.
    pub owners: Vec<String>,
    /// Handler kind.
    pub kind: String,
    /// Execution/recovery owner.
    pub execution: Execution,
    /// Current lifecycle.
    pub state: JobState,
    /// Final state retained after confirmation.
    pub final_state: Option<JobState>,
    /// Waiting copy; never removed solely because an unfinished job is old.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    /// Current opaque signed authority; absent for embedded jobs and tombstones.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission: Option<Vec<u8>>,
    /// Frozen ceiling.
    pub max_attempts: u32,
    /// Monotonic claim fence.
    pub generation: u64,
    /// Current claim deadline.
    pub lease_until: Option<DateTime<Utc>>,
    /// Cancellation intent for running work.
    pub cancel_requested: bool,
    /// Sticky erasure fence checked before every result commit.
    pub purged: bool,
    /// Handler recovery copy; never contains paid output.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<u8>>,
    /// Rebuildable encoded result length for content-free delivery preflight.
    #[serde(skip)]
    pub output_bytes: usize,
    /// Result provenance.
    pub origin: Option<ResultOrigin>,
    /// Optional accounting reference, with no copied usage.
    pub receipt: Option<String>,
    /// Final-result availability deadline.
    pub recovery_until: Option<DateTime<Utc>>,
    /// Whether recovery output has expired.
    pub result_expired: bool,
    /// Enqueue order timestamp.
    pub created_at: DateTime<Utc>,
    /// Final-delivery order timestamp.
    pub finished_at: Option<DateTime<Utc>>,
    /// Latest delivery ordinal; older issued ordinals remain confirmable.
    pub delivery_generation: u64,
    /// Delivery overlap reduction only, not an acknowledgement fence.
    pub delivery_until: Option<DateTime<Utc>>,
    /// Static failure diagnostic, never provider text.
    pub diagnostic: Option<DiagnosticCode>,
}

impl JobRecord {
    /// One-based execution claim ordinal, derived from the fence rather than copied.
    pub fn attempt(&self) -> u64 {
        self.generation
    }
}

/// Keyed enqueue result.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Enqueued {
    /// Newly inserted job.
    Inserted(JobId),
    /// Existing unfinished or unconfirmed final job.
    Joined(JobId),
    /// Confirmed content-free tombstone.
    AlreadyDone(Box<JobRecord>),
}

/// Consumer disposition; first confirmation wins.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Disposition {
    /// Durably saved by the consumer.
    Accepted,
    /// Consumer refused the answer.
    Discarded,
}

/// Scoped issued delivery ordinal, authenticated by the transport in PR 4.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryToken {
    /// Delivered job.
    pub job: JobId,
    /// One-based delivery ordinal.
    pub generation: u64,
}

/// Final completion with a confirmable token.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Delivery {
    /// Snapshot of the job (payload is not disclosed).
    pub completion: JobRecord,
    /// Issued confirmation fence.
    pub token: DeliveryToken,
}

/// Bounded completion page.
#[derive(Clone, Debug)]
pub struct CompletionPage {
    /// Final deliveries within the requested page.
    pub items: Vec<Delivery>,
}

/// One confirmation result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AckResult {
    /// This confirmation won.
    Acked(Disposition),
    /// A prior confirmation won.
    AlreadyAcked(Disposition),
}

/// Bounded operational diagnostic.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobDiagnostic {
    /// Affected identity.
    pub id: JobId,
    /// Failed or Uncertain.
    pub state: JobState,
    /// Static cause, if available.
    pub code: Option<DiagnosticCode>,
}

/// Bounded diagnostic page in ascending ID order.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticPage {
    /// At most the requested count.
    pub items: Vec<JobDiagnostic>,
    /// Last returned ID; use as the exclusive next cursor.
    pub after: Option<String>,
}

/// Cancellation target within the authorized scope.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Selector {
    /// Explicit scoped identities.
    Ids(Vec<JobId>),
    /// Every matching group row, processed atomically.
    Group(String),
}

/// Hard pending utilization, derived from unfinished canonical rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PendingUsage {
    /// Unfinished job count (including Running and Uncertain).
    pub items: usize,
    /// Retained unfinished input bytes, including metadata.
    pub bytes: usize,
}

/// Ledger-owner evidence used only inside the shared acceptance/settlement transaction.
/// This is an internal trusted-host operation, never an instruction from a client.
#[derive(Clone, Debug)]
pub enum JobResolution {
    /// Reservation may have dispatched; preserve input for reconciliation.
    Uncertain { receipt: String },
    /// Trusted ledger/adapter established zero charge or durable pre-dispatch release.
    KnownZeroCharge { receipt: String },
    /// Ledger already holds a committed paid result, including direct-call recovery.
    PaidResult {
        receipt: String,
        recovery_until: Option<DateTime<Utc>>,
    },
    /// Reconciliation established a terminal failure.
    Failed {
        receipt: String,
        diagnostic: DiagnosticCode,
    },
}

/// Atomic store operations. Runner/recovery and ledger ownership stay outside this module.
#[derive(Clone, Debug)]
pub enum JobRequest {
    /// Trusted execution owner marks an unsent job as requiring renewed authority.
    AwaitAdmission(JobId),
    /// Replace authority and resume an unsent job; owner validates the successor.
    Admit {
        /// Job within the authorized scope.
        job: JobId,
        /// Opaque successor bytes validated by the execution owner.
        admission: Vec<u8>,
    },
    /// Bounded, content-free notices; these are never confirmable completions.
    AdmissionNotices {
        /// Exclusive notice ID cursor; None starts at the first notice.
        after: Option<String>,
        /// Maximum number of waiting job notices.
        limit: usize,
    },
    /// All-or-none enqueue; joined keys consume no additional capacity.
    Enqueue(Vec<JobSpec>),
    /// Claim one job within handler kinds, given currently available account slots.
    Claim {
        kinds: Vec<String>,
        slots_available: usize,
    },
    /// Inspect a bounded claim page before obtaining the existing account slot.
    Candidates {
        kinds: Vec<String>,
        limit: usize,
        max_bytes: usize,
    },
    /// Claim the inspected job inside the account owner's acceptance transaction.
    /// The trusted caller holds its account/handler slot; the store is not a limiter.
    ClaimJob(JobId),
    /// Ledger owner claims and records the reservation reference in its transaction.
    ClaimPaid { job: JobId, receipt: String },
    /// Execution-owner refusal before accepting a paid attempt; no receipt is fabricated.
    RefusePending {
        job: JobId,
        diagnostic: DiagnosticCode,
    },
    /// Bounded ledger-recovery page; active leases are never inspected.
    RecoveryCandidates {
        kinds: Vec<String>,
        after: Option<String>,
        limit: usize,
        max_bytes: usize,
    },
    /// Extend a live claim and return cancellation intent without reading content.
    Heartbeat { job: JobId, generation: u64 },
    /// Set a final result or Uncertain under a live generation.
    Complete {
        job: JobId,
        generation: u64,
        state: JobState,
        origin: ResultOrigin,
        output: Option<Vec<u8>>,
        receipt: Option<String>,
        diagnostic: Option<DiagnosticCode>,
    },
    /// Resolve a paid job using verified ledger evidence, including an expired claim.
    Resolve {
        job: JobId,
        generation: u64,
        resolution: JobResolution,
    },
    /// Final results in delivery order; serialized byte bound.
    Completions { limit: usize, max_bytes: usize },
    /// Atomic batch confirm; paid-output discard participates via the transaction seam.
    Ack(Vec<(DeliveryToken, Disposition)>),
    /// Finalize waiting work, signal running work; never retries cancelled work.
    Cancel(Selector),
    /// Sticky owner erasure, including concurrent/late completion writes.
    PurgeOwner(String),
    /// Page static diagnostics.
    Diagnostics {
        group: String,
        after: Option<String>,
        limit: usize,
    },
    /// Expire at most the configured number of final recovery copies.
    Maintain,
    /// Content-free scoped status; never loads input, admission or output.
    Status(JobId),
    /// Authorized internal lookup for runner/reconciliation.
    Get(JobId),
    /// Derived pending count/bytes for backpressure.
    PendingUsage,
}

/// Result matching the requested operation.
#[derive(Clone, Debug)]
pub enum JobResponse {
    /// Keyed batch dispositions.
    Enqueued(Vec<Enqueued>),
    /// Claimed or internally fetched row.
    Job(Option<Box<JobRecord>>),
    /// Bounded candidate snapshots for the runner/account owner.
    Candidates(Vec<JobRecord>),
    /// Final results.
    Deliveries(CompletionPage),
    /// Confirmation dispositions.
    Acks(Vec<AckResult>),
    /// Cancellation/purge/maintenance changed this many rows.
    Changed(usize),
    /// Paged static diagnostics.
    Diagnostics(DiagnosticPage),
    /// Pending utilization.
    Usage(PendingUsage),
    /// Renewed live claim; true means cancellation or owner erasure was requested.
    Heartbeat(bool),
    /// Successful single-row write.
    Done,
    /// `Complete` kept no handler result: its one-item delivery page (`bytes`)
    /// exceeds `max_page_bytes`. The same write ended the job Failed with
    /// `queue_failure`, or Cancelled when cancellation was requested.
    ResultRefused { bytes: usize },
}

/// Visible failures; messages contain static codes or scoped IDs, never content.
#[derive(Clone, Debug, Error, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobError {
    /// Request crossed tenant/incarnation/queue authorization scope.
    #[error("job scope refused")]
    Scope,
    /// No authorized job exists.
    #[error("job not found")]
    NotFound,
    /// Same key with different input.
    #[error("job key conflict")]
    KeyConflict,
    /// Hard live population or pending input-byte bound reached.
    #[error("job queue full")]
    QueueFull,
    /// An eligible completion cannot fit individually in the requested page.
    #[error("completion exceeds page byte bound: {job:?} ({bytes} bytes)")]
    CompletionTooLarge { job: JobId, bytes: usize },
    /// A handler result exceeds `max_result_bytes` (`bytes` is its raw length). The
    /// runner also reports a [`JobResponse::ResultRefused`] page length with it.
    #[error("result exceeds byte bound: {job:?} ({bytes} bytes)")]
    ResultTooLarge { job: JobId, bytes: usize },
    /// An inspected job cannot fit individually in the requested candidate page.
    #[error("claim candidate exceeds page byte bound: {job:?} ({bytes} bytes)")]
    CandidateTooLarge { job: JobId, bytes: usize },
    /// Invalid configuration, state transition or page bound.
    #[error("invalid job request")]
    InvalidRequest,
    /// Signed authority elapsed before final claim acceptance; await renewal.
    #[error("job authority expired")]
    AuthorityExpired,
    /// Expired or superseded claim.
    #[error("stale job claim")]
    StaleClaim,
    /// An unfinished job cannot be acknowledged.
    #[error("cannot confirm an unfinished job")]
    NotFinal,
    /// The caller-selected backend does not provide a job store.
    #[error("job store unavailable")]
    Unavailable,
    /// Execution-owner failure, preserving the closed diagnostic without content.
    #[error("job execution failed: {0}")]
    Execution(DiagnosticCode),
    /// Backend/serialization failure.
    #[error("job storage failure")]
    Storage,
}

/// Count canonical JSON bytes without allocating an encoded copy.
#[doc(hidden)]
pub fn encoded_bytes(value: &impl Serialize) -> Result<usize, JobError> {
    struct Counter(usize);
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| std::io::Error::other("encoded length overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value).map_err(|_| JobError::Storage)?;
    Ok(counter.0)
}

/// Derive retained-input utilization from the canonical row; no byte projection is stored.
#[doc(hidden)]
pub fn job_input_bytes(row: &JobRecord) -> Result<usize, JobError> {
    if !row.state.unfinished() {
        return Ok(0);
    }
    encoded_bytes(&(
        &row.id.scope,
        &row.key,
        &row.group,
        &row.owners,
        &row.kind,
        row.execution,
        row.max_attempts,
    ))
    .and_then(|metadata| {
        metadata
            .checked_add(row.payload.as_ref().map_or(0, Vec::len))
            .and_then(|bytes| bytes.checked_add(row.admission.as_ref().map_or(0, Vec::len)))
            .ok_or(JobError::Storage)
    })
}
