//! Durable model jobs on the canonical spend connection. Paid answers stay in
//! the ledger; deliveries borrow them and never persist a second result copy.
use crate::{ModelBinding, ModelError, Runtime, SpendReservation, SpendState, model, spend};
use chrono::Utc;
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use symbiotic_core::DiagnosticCode;
use symbiotic_queue::{
    jobs::*,
    runner::{JobRunner, RunnerConfig, RunnerError},
};
use symbiotic_trace::UsageTrace;
use tokio::sync::watch;

fn storage(_: impl std::fmt::Debug) -> ModelError {
    ModelError::Queue(DiagnosticCode::SpendLedgerUnavailable)
}

/// Final delivery plus the ledger's sole paid recovery answer. The answer is
/// absent after expiry, confirmation or purge; accounting remains queryable.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ModelDelivery {
    /// Job metadata and confirmation token.
    pub delivery: Delivery,
    /// Same-invocation answer, with no cache reuse across keys.
    pub output: Option<serde_json::Value>,
}

/// Optional trusted admission owner for model jobs crossing an authenticated boundary.
/// Successor and claim checks run inside the same IMMEDIATE transaction as the transition.
/// Returning false at claim waits for renewed authority without reserving spend.
pub trait ModelJobAdmission: Send + Sync {
    /// Validate a signed initial authority before storing a waiting copy.
    fn enqueue(&self, spec: &JobSpec) -> Result<(), JobError>;
    /// Validate a successor against the canonical frozen row and previous authority.
    /// False is an exact replay: leave the current lifecycle unchanged.
    fn admit(&self, row: &JobRecord, admission: &[u8]) -> Result<bool, JobError>;
    /// Record signed-attempt acceptance in the claim/reservation transaction.
    fn accept(
        &self,
        tx: &Transaction<'_>,
        row: &JobRecord,
        reservation: &SpendReservation,
    ) -> Result<(), JobError>;
    /// Bind the resolved credential's request identity before transport, preserving
    /// the existing per-job allowance. Runs in the canonical writer transaction.
    fn before_send(
        &self,
        _tx: &Transaction<'_>,
        _row: &JobRecord,
        _credential_fingerprint: Option<String>,
    ) -> Result<(), JobError> {
        Ok(())
    }
    /// Retire a request binding in the canonical job completion transaction,
    /// including completed attempts whose charge remains unknown.
    fn finish(&self, _tx: &Transaction<'_>, _row: &JobRecord) -> Result<(), JobError> {
        Ok(())
    }
    /// Recheck current authority after obtaining the account slot, before claim/reservation.
    fn claim(
        &self,
        tx: &Transaction<'_>,
        row: &JobRecord,
        now: chrono::DateTime<Utc>,
    ) -> Result<bool, JobError>;
}

/// One trusted scoped job API on the runtime's ledger connection.
#[derive(Clone)]
pub struct ModelJobs {
    runtime: Runtime,
    ledger: Arc<spend::SqliteSpendLedger>,
    scope: JobScope,
    config: JobConfig,
    admission: Option<Arc<dyn ModelJobAdmission>>,
}

impl Runtime {
    /// Open model jobs in an authenticated tenant/incarnation/queue namespace.
    /// A persistent runtime is required, as for direct paid execution.
    pub fn model_jobs(&self, scope: JobScope, config: JobConfig) -> Result<ModelJobs, JobError> {
        let jobs = ModelJobs {
            runtime: self.clone(),
            ledger: self.inner.job_ledger.clone().ok_or(JobError::Unavailable)?,
            scope,
            config,
            admission: None,
        };
        jobs.request_sync(JobRequest::PendingUsage)?;
        Ok(jobs)
    }
}

/// Encode the complete request and frozen credential-free execution binding as
/// the job's canonical waiting copy. This is operational input, not another store.
pub fn model_job_payload<P>(
    binding: &ModelBinding<P>,
    request: &impl serde::Serialize,
) -> Result<Vec<u8>, JobError> {
    let identity = binding
        .identity
        .as_ref()
        .filter(|id| id.is_valid())
        .ok_or(JobError::InvalidRequest)?;
    serde_json::to_vec(&serde_json::json!({"binding": identity, "sharing": binding.account_sharing_key, "request": request})).map_err(|_| JobError::Storage)
}
fn input(
    payload: &[u8],
) -> Result<
    (
        crate::BindingIdentity,
        Option<crate::AccountSharingKey>,
        serde_json::Value,
    ),
    JobError,
> {
    let value: serde_json::Value =
        serde_json::from_slice(payload).map_err(|_| JobError::InvalidRequest)?;
    let identity = serde_json::from_value(
        value
            .get("binding")
            .ok_or(JobError::InvalidRequest)?
            .clone(),
    )
    .map_err(|_| JobError::InvalidRequest)?;
    let sharing = serde_json::from_value(
        value
            .get("sharing")
            .ok_or(JobError::InvalidRequest)?
            .clone(),
    )
    .map_err(|_| JobError::InvalidRequest)?;
    Ok((
        identity,
        sharing,
        value
            .get("request")
            .ok_or(JobError::InvalidRequest)?
            .clone(),
    ))
}
fn paid_resolution(
    tx: &Transaction<'_>,
    receipt: &crate::SpendReceipt,
    invocation: &str,
    now: chrono::DateTime<Utc>,
    max_bytes: usize,
) -> Result<JobResolution, JobError> {
    let bytes: usize = tx.query_row("SELECT coalesce(length(CAST(recovery AS BLOB)),0) FROM spend_receipts WHERE reference=?1", [receipt.reservation.reference.as_str()], |r| r.get(0)).map_err(|_| JobError::Storage)?;
    let (erased, deadline) =
        spend::constrain_job_recovery_in(tx, &receipt.reservation.reference, invocation, now)
            .map_err(|_| JobError::Storage)?;
    if erased || bytes > max_bytes {
        tx.execute(
            "UPDATE spend_receipts SET recovery=NULL,recovery_expires_at=NULL WHERE reference=?1",
            [receipt.reservation.reference.as_str()],
        )
        .map_err(|_| JobError::Storage)?;
        return Ok(JobResolution::Failed {
            receipt: receipt.reservation.reference.as_str().into(),
            diagnostic: if erased {
                DiagnosticCode::InvocationCompleted
            } else {
                DiagnosticCode::QueueResultTooLarge
            },
        });
    }
    Ok(JobResolution::PaidResult {
        receipt: receipt.reservation.reference.as_str().into(),
        recovery_until: Some(deadline.unwrap_or(now)),
    })
}

// One terminal receipt resolution serves both expired claims and pending jobs
// whose direct invocation committed before the job claimed its first attempt.
fn receipt_resolution(
    tx: &Transaction<'_>,
    receipt: &crate::SpendReceipt,
    invocation: &str,
    now: chrono::DateTime<Utc>,
    max_bytes: usize,
) -> Result<JobResolution, JobError> {
    if receipt.output.is_some() {
        paid_resolution(tx, receipt, invocation, now, max_bytes)
    } else if receipt.state == SpendState::Released {
        Ok(JobResolution::KnownZeroCharge {
            receipt: receipt.reservation.reference.as_str().into(),
        })
    } else if receipt.state == SpendState::Settled {
        Ok(JobResolution::Failed {
            receipt: receipt.reservation.reference.as_str().into(),
            diagnostic: DiagnosticCode::InvocationCompleted,
        })
    } else {
        Ok(JobResolution::Uncertain {
            receipt: receipt.reservation.reference.as_str().into(),
        })
    }
}

// This canonical registration survives confirmation and erasure: neither removes
// the kind's single binding. IMMEDIATE transactions serialize registration/enqueue.
fn bind_kind(
    tx: &Transaction<'_>,
    scope: &JobScope,
    kind: &str,
    identity: &crate::BindingIdentity,
    sharing: &Option<crate::AccountSharingKey>,
) -> Result<(), JobError> {
    let scope = serde_json::to_string(scope).map_err(|_| JobError::Storage)?;
    let binding = serde_json::to_string(&(identity, sharing)).map_err(|_| JobError::Storage)?;
    let previous: Option<String> = tx
        .query_row(
            "SELECT binding FROM model_job_bindings WHERE scope=?1 AND kind=?2",
            params![scope, kind],
            |r| r.get(0),
        )
        .optional()
        .map_err(|_| JobError::Storage)?;
    if previous
        .as_ref()
        .is_some_and(|previous| previous != &binding)
    {
        return Err(JobError::KeyConflict);
    }
    tx.execute(
        "INSERT OR IGNORE INTO model_job_bindings(scope,kind,binding) VALUES (?1,?2,?3)",
        params![scope, kind, binding],
    )
    .map_err(|_| JobError::Storage)?;
    Ok(())
}

// Resolve through the frozen registry even after cancellation removes payloads.
// Latest receipt wins: a Pending job may still point at a Released predecessor.
fn job_receipt_reference_in(
    tx: &rusqlite::Transaction<'_>,
    scope: &symbiotic_queue::jobs::JobScope,
    key: &str,
    kind: &str,
) -> Result<Option<crate::SpendReceiptRef>, ModelError> {
    let binding: String = tx
        .query_row(
            "SELECT binding FROM model_job_bindings WHERE scope=?1 AND kind=?2",
            params![serde_json::to_string(scope).map_err(storage)?, kind],
            |row| row.get(0),
        )
        .map_err(storage)?;
    let (identity, sharing): (
        symbiotic_core::BindingIdentity,
        Option<symbiotic_core::AccountSharingKey>,
    ) = serde_json::from_str(&binding).map_err(storage)?;
    let account = crate::account_scope(&identity, sharing.as_ref())?;
    let invocation = symbiotic_model::execution_invocation_identity(
        &identity,
        &spend::job_invocation_key(scope, key)?,
    )?;
    spend::invocation_reference_in(tx, &account, &invocation)
}

impl ModelJobs {
    /// Derive whether a kind has work requiring execution or ledger recovery.
    pub async fn needs_execution(&self, kind: String) -> Result<bool, JobError> {
        let jobs = self.clone();
        tokio::task::spawn_blocking(move || {
            jobs.transaction(|tx, now| {
                symbiotic_queue_sqlite::jobs::jobs_need_execution_in_transaction(
                    tx,
                    &jobs.scope,
                    kind,
                    now,
                )
            })
        })
        .await
        .map_err(|_| JobError::Storage)?
    }
    /// Attach the trusted admission owner; signed jobs never retry automatically.
    pub fn with_admission(mut self, admission: Arc<dyn ModelJobAdmission>) -> Self {
        self.admission = Some(admission);
        self
    }
    /// D1 invocation key for direct execution/recovery of this scoped job.
    /// The scope and caller key identify one invocation across job attempts.
    pub fn invocation_key(&self, key: &str) -> Result<String, JobError> {
        spend::job_invocation_key(&self.scope, key).map_err(|_| JobError::Storage)
    }
    fn transaction<T>(
        &self,
        f: impl FnOnce(&mut Transaction<'_>, chrono::DateTime<Utc>) -> Result<T, JobError>,
    ) -> Result<T, JobError> {
        let mut conn = self.ledger.0.lock().map_err(|_| JobError::Storage)?;
        let mut tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|_| JobError::Storage)?;
        let now = self.ledger.now();
        let result = f(&mut tx, now)?;
        tx.commit().map_err(|_| JobError::Storage)?;
        Ok(result)
    }
    fn op(
        &self,
        tx: &mut Transaction<'_>,
        now: chrono::DateTime<Utc>,
        request: JobRequest,
    ) -> Result<JobResponse, JobError> {
        symbiotic_queue_sqlite::jobs::jobs_in_transaction(
            tx,
            &self.scope,
            &self.config,
            now,
            request,
        )
    }
    /// Execute one bounded store operation. Ack, owner erasure and expiry discard
    /// ledger recovery in the same transaction as job-copy deletion.
    /// Use [`Self::completions`] for paid-answer delivery with its byte preflight.
    pub async fn request(&self, request: JobRequest) -> Result<JobResponse, JobError> {
        if !matches!(
            request,
            JobRequest::Enqueue(_)
                | JobRequest::Admit { .. }
                | JobRequest::AdmissionNotices { .. }
                | JobRequest::Ack(_)
                | JobRequest::Cancel(_)
                | JobRequest::PurgeOwner(_)
                | JobRequest::Diagnostics { .. }
                | JobRequest::Maintain
                | JobRequest::Status(_)
                | JobRequest::Get(_)
                | JobRequest::PendingUsage
        ) {
            return Err(JobError::InvalidRequest);
        }
        let jobs = self.clone();
        tokio::task::spawn_blocking(move || jobs.request_sync(request))
            .await
            .map_err(|_| JobError::Storage)?
    }
    fn request_sync(&self, request: JobRequest) -> Result<JobResponse, JobError> {
        if let JobRequest::Enqueue(specs) = &request {
            if specs.len() > self.config.max_batch {
                return Err(JobError::InvalidRequest);
            }
            for spec in specs {
                if let Some(admission) = &self.admission {
                    admission.enqueue(spec)?;
                } else if spec.admission.is_some() {
                    return Err(JobError::InvalidRequest);
                }
                if spec.execution != Execution::Model {
                    return Err(JobError::InvalidRequest);
                }
                let (identity, sharing, _) = input(&spec.payload)?;
                if !identity.is_valid()
                    || identity.tenant.0 != self.scope.tenant
                    || crate::account_scope(&identity, sharing.as_ref()).is_err()
                {
                    return Err(JobError::InvalidRequest);
                }
            }
        }
        let purging = matches!(request, JobRequest::PurgeOwner(_));
        let confirming = matches!(request, JobRequest::Ack(_));
        if matches!(
            request,
            JobRequest::Completions { .. }
                | JobRequest::Resolve { .. }
                | JobRequest::ClaimPaid { .. }
                | JobRequest::Claim { .. }
                | JobRequest::ClaimJob(_)
        ) {
            return Err(JobError::InvalidRequest);
        }
        self.transaction(|tx, now| {
            if let JobRequest::Admit { job, admission } = &request {
                let JobResponse::Job(Some(row)) = self.op(tx, now, JobRequest::Get(job.clone()))? else { return Err(JobError::NotFound) };
                if !self.admission.as_ref().ok_or(JobError::InvalidRequest)?.admit(&row, admission)? { return Ok(JobResponse::Done); }
            }
            if let JobRequest::Enqueue(specs) = &request {
                for spec in specs {
                    let (identity, sharing, _) = input(&spec.payload)?;
                    bind_kind(tx, &self.scope, &spec.kind, &identity, &sharing)?;
                }
            }
            let affected = symbiotic_queue_sqlite::jobs::paid_copies_for_request(tx, &self.scope, &self.config, now, &request)?;
            let enqueued = match &request {
                JobRequest::Enqueue(specs) => specs.iter().map(|spec| (spec.key.clone(), spec.kind.clone())).collect(),
                _ => Vec::new(),
            };
            let response = self.op(tx, now, request)?;
            for (key, kind) in enqueued {
                if let Some(reference) = job_receipt_reference_in(tx, &self.scope, &key, &kind)
                    .map_err(|_| JobError::Storage)? {
                    spend::constrain_job_recovery_in(tx, &reference, &self.invocation_key(&key)?, now)
                        .map_err(|_| JobError::Storage)?;
                }
            }
            for row in affected {
                let attach_receipt = purging && row.state.unfinished();
                let receipt = if (purging || confirming) && row.execution == Execution::Model {
                    job_receipt_reference_in(tx, &self.scope, &row.key, &row.kind)
                        .map_err(|_| JobError::Storage)?
                        .map(|reference| reference.as_str().to_string())
                        .or(row.receipt)
                } else {
                    row.receipt
                };
                if let Some(receipt) = receipt
                    && let JobResponse::Job(Some(row)) = self.op(tx, now, JobRequest::Get(row.id))?
                    && (row.state.acked() || row.purged || row.result_expired) {
                    if attach_receipt {
                        tx.execute("UPDATE jobs SET receipt=?3 WHERE scope=?1 AND id=?2", params![serde_json::to_string(&row.id.scope).map_err(|_| JobError::Storage)?, row.id.id, receipt]).map_err(|_| JobError::Storage)?;
                    }
                    tx.execute("UPDATE spend_receipts SET recovery=NULL,recovery_expires_at=NULL WHERE reference=?1", [&receipt]).map_err(|_| JobError::Storage)?;
                }
            }
            Ok(response)
        })
    }
    /// Deliver oldest final jobs within both bounds. Ledger answer sizes are
    /// preflighted before loading content or committing any delivery lease.
    pub async fn completions(
        &self,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<ModelDelivery>, JobError> {
        let jobs = self.clone();
        tokio::task::spawn_blocking(move || jobs.completions_sync(limit, max_bytes))
            .await
            .map_err(|_| JobError::Storage)?
    }
    fn completions_sync(
        &self,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<ModelDelivery>, JobError> {
        if limit == 0
            || limit > self.config.max_page
            || max_bytes < 2
            || max_bytes > self.config.max_page_bytes
        {
            return Err(JobError::InvalidRequest);
        }
        self.transaction(|tx, now| {
            let mut items = Vec::new();
            let mut total = 2usize;
            for _ in 0..limit {
                tx.execute_batch("SAVEPOINT model_delivery").map_err(|_| JobError::Storage)?;
                let JobResponse::Deliveries(page) = symbiotic_queue_sqlite::jobs::jobs_in_transaction(tx, &self.scope, &self.config, now, JobRequest::Completions { limit: 1, max_bytes: self.config.max_page_bytes })? else { return Err(JobError::InvalidRequest) };
                let Some(mut delivery) = page.items.into_iter().next() else {
                    tx.execute_batch("RELEASE model_delivery").map_err(|_| JobError::Storage)?;
                    break;
                };
                let receipt = delivery.completion.receipt.clone();
                let reference = receipt.as_deref();
                let len: usize = match reference {
                    Some(reference) if !delivery.completion.result_expired && !delivery.completion.purged => tx.query_row("SELECT coalesce(length(CAST(recovery AS BLOB)),4) FROM spend_receipts WHERE reference=?1 AND recovery_expires_at>?2", params![reference, now.to_rfc3339()], |r| r.get(0)).optional().map_err(|_| JobError::Storage)?.unwrap_or(4),
                    _ => 4,
                };
                // Wire shape is {delivery,output}; recovery is canonical compact JSON.
                let bytes = encoded_bytes(&delivery)?.checked_add(len).and_then(|n| n.checked_add(23)).ok_or(JobError::Storage)?;
                if bytes + 2 > max_bytes { return Err(JobError::CompletionTooLarge { job: delivery.completion.id, bytes: bytes + 2 }); }
                let next = total.checked_add(bytes + usize::from(!items.is_empty())).ok_or(JobError::Storage)?;
                if next > max_bytes {
                    tx.execute_batch("ROLLBACK TO model_delivery; RELEASE model_delivery").map_err(|_| JobError::Storage)?;
                    break;
                }
                let output = match reference {
                    Some(reference) if !delivery.completion.result_expired && !delivery.completion.purged => spend::receipt_in(tx, &crate::SpendReceiptRef::new(reference).map_err(|_| JobError::Storage)?, now).map_err(|_| JobError::Storage)?.and_then(|r| r.recovery),
                    _ => None,
                };
                if output.is_none() && delivery.completion.origin == Some(ResultOrigin::Paid)
                    && matches!(delivery.completion.state, JobState::Succeeded | JobState::Cancelled)
                    && delivery.completion.diagnostic.is_none() {
                    delivery.completion.result_expired = true;
                }
                if delivery.completion.result_expired && let Some(reference) = reference {
                    tx.execute("UPDATE spend_receipts SET recovery=NULL,recovery_expires_at=NULL WHERE reference=?1", [reference]).map_err(|_| JobError::Storage)?;
                }
                tx.execute_batch("RELEASE model_delivery").map_err(|_| JobError::Storage)?;
                total = next;
                items.push(ModelDelivery { delivery, output });
            }
            Ok(items)
        })
    }

    async fn maintain(
        self,
        mut stop: watch::Receiver<bool>,
        interval: Duration,
    ) -> Result<(), RunnerError> {
        let mut timer = tokio::time::interval(interval);
        loop {
            if *stop.borrow() {
                return Ok(());
            }
            tokio::select! {
                _ = stop.changed() => {},
                _ = timer.tick() => {
                    let jobs = self.clone();
                    tokio::task::spawn_blocking(move || jobs.request_sync(JobRequest::Maintain)).await.map_err(|_| RunnerError::WorkerTask)??;
                }
            }
        }
    }

    async fn recover(&self, kind: String) -> Result<(), JobError> {
        let mut after = None;
        loop {
            let jobs = self.clone();
            let kind = kind.clone();
            let cursor = after.clone();
            let (count, last) = tokio::task::spawn_blocking(move || jobs.transaction(|tx, now| {
                let JobResponse::Candidates(rows) = jobs.op(tx, now, JobRequest::RecoveryCandidates { kinds: vec![kind], after: cursor, limit: jobs.config.max_page, max_bytes: jobs.config.max_page_bytes })? else { return Err(JobError::InvalidRequest) };
                let count = rows.len();
                let last = rows.last().map(|r| r.id.id.clone());
                for row in rows {
                    let reference = crate::SpendReceiptRef::new(row.receipt.as_deref().ok_or(JobError::Storage)?).map_err(|_| JobError::Storage)?;
                    let receipt = spend::receipt_in(tx, &reference, now).map_err(|_| JobError::Storage)?.ok_or(JobError::Storage)?;
                    let resolution = receipt_resolution(tx, &receipt, &jobs.invocation_key(&row.key)?, now, jobs.config.max_result_bytes)?;
                    jobs.op(tx, now, JobRequest::Resolve { job: row.id.clone(), generation: row.generation, resolution })?;
                    if row.recovery_until.is_some_and(|until| until <= now) && receipt.output.is_some() {
                        tx.execute("UPDATE spend_receipts SET recovery=NULL,recovery_expires_at=NULL WHERE reference=?1", [receipt.reservation.reference.as_str()]).map_err(|_| JobError::Storage)?;
                    }
                }
                Ok((count, last))
            })).await.map_err(|_| JobError::Storage)??;
            if count == 0 {
                return Ok(());
            }
            after = last;
        }
    }
}

#[derive(Clone)]
struct Claim {
    id: JobId,
    generation: u64,
    receipt: String,
    max_attempts: u32,
}
struct ClaimOwner {
    answer_recovery: crate::AnswerRecovery,
    jobs: ModelJobs,
    candidate: JobId,
    claim: Mutex<Option<Claim>>,
    heartbeat: Duration,
    finished: AtomicBool,
    stop: watch::Receiver<bool>,
}
impl ClaimOwner {
    fn claim_record(&self) -> Result<Claim, ModelError> {
        self.claim
            .lock()
            .map_err(storage)?
            .clone()
            .ok_or_else(|| storage(()))
    }
    fn previous_in(
        &self,
        tx: &mut Transaction<'_>,
        now: chrono::DateTime<Utc>,
        reservation: &SpendReservation,
    ) -> Result<(bool, Option<crate::SpendReceipt>), JobError> {
        let JobResponse::Job(Some(current)) =
            self.jobs
                .op(tx, now, JobRequest::Get(self.candidate.clone()))?
        else {
            return Err(JobError::NotFound);
        };
        if current.state != JobState::Pending {
            return Ok((true, None));
        }
        let previous = spend::invocation_in(tx, &reservation.account, &reservation.invocation, now)
            .map_err(|_| JobError::Storage)?;
        if previous
            .as_ref()
            .is_some_and(|r| r.reservation.binding != reservation.binding)
        {
            return Err(JobError::KeyConflict);
        }
        if let Some(receipt) = &previous
            && (receipt.output.is_some() || receipt.state == SpendState::Settled)
        {
            self.resolve(
                tx,
                &current.id,
                current.generation,
                now,
                receipt_resolution(
                    tx,
                    receipt,
                    &self.jobs.invocation_key(&current.key)?,
                    now,
                    self.jobs.config.max_result_bytes,
                )?,
            )?;
            if current.recovery_until.is_some_and(|until| until <= now) {
                tx.execute("UPDATE spend_receipts SET recovery=NULL,recovery_expires_at=NULL WHERE reference=?1", [receipt.reservation.reference.as_str()]).map_err(|_| JobError::Storage)?;
            }
            return Ok((true, None));
        }
        Ok((false, previous))
    }
    fn resolve(
        &self,
        tx: &mut Transaction<'_>,
        job: &JobId,
        generation: u64,
        now: chrono::DateTime<Utc>,
        resolution: JobResolution,
    ) -> Result<(), JobError> {
        self.jobs.op(
            tx,
            now,
            JobRequest::Resolve {
                job: job.clone(),
                generation,
                resolution,
            },
        )?;
        Ok(())
    }
}
impl model::ModelJob for ClaimOwner {
    fn recover(&self, reservation: &SpendReservation) -> Result<bool, ModelError> {
        let recovered = self
            .jobs
            .transaction(|tx, now| self.previous_in(tx, now, reservation).map(|(done, _)| done))
            .map_err(storage)?;
        if recovered {
            self.finished.store(true, Ordering::SeqCst);
        }
        Ok(recovered)
    }
    fn claim(
        &self,
        reservation: &SpendReservation,
        limit: u32,
    ) -> Result<Option<Vec<u8>>, ModelError> {
        if *self.stop.borrow() {
            self.finished.store(true, Ordering::SeqCst);
            return Ok(None);
        }
        let result = self.jobs.transaction(|tx, now| {
            let (done, previous) = self.previous_in(tx, now, reservation)?;
            if done {
                return Ok(None);
            }
            if let Some(receipt) = previous
                && receipt.state == SpendState::Unknown
            {
                let JobResponse::Job(Some(row)) = self.jobs.op(
                    tx,
                    now,
                    JobRequest::ClaimPaid {
                        job: self.candidate.clone(),
                        receipt: receipt.reservation.reference.as_str().into(),
                    },
                )?
                else {
                    return Ok(None);
                };
                self.resolve(
                    tx,
                    &row.id,
                    row.generation,
                    now,
                    JobResolution::Uncertain {
                        receipt: receipt.reservation.reference.as_str().into(),
                    },
                )?;
                return Ok(None);
            }
            if let Some(admission) = &self.jobs.admission {
                let JobResponse::Job(Some(row)) =
                    self.jobs
                        .op(tx, now, JobRequest::Get(self.candidate.clone()))?
                else {
                    return Err(JobError::NotFound);
                };
                if !admission.claim(tx, &row, now)? {
                    self.jobs.op(tx, now, JobRequest::AwaitAdmission(row.id))?;
                    return Ok(None);
                }
            }
            if self.jobs.admission.is_some() {
                tx.execute_batch("SAVEPOINT model_acceptance")
                    .map_err(|_| JobError::Storage)?;
            }
            let JobResponse::Job(row) = self.jobs.op(
                tx,
                now,
                JobRequest::ClaimPaid {
                    job: self.candidate.clone(),
                    receipt: reservation.reference.as_str().into(),
                },
            )?
            else {
                return Err(JobError::InvalidRequest);
            };
            let Some(row) = row else {
                return Ok(None);
            };
            // Signed jobs require successor authority for each further claim;
            // their frozen ceiling is independent of a route's one-send retry policy.
            let attempt_limit = if self.jobs.admission.is_some() {
                row.max_attempts
            } else {
                row.max_attempts.min(limit)
            };
            let accepted = spend::SqliteSpendLedger::reserve_with_limit_in(
                tx,
                reservation,
                Some(attempt_limit),
                false,
            )
            .map_err(|error| JobError::Execution(error.code()))?;
            if !accepted {
                return Err(JobError::Storage);
            }
            if let Some(admission) = &self.jobs.admission {
                match admission.accept(tx, &row, reservation) {
                    Ok(()) => {}
                    Err(JobError::AuthorityExpired) => {
                        tx.execute_batch("ROLLBACK TO model_acceptance; RELEASE model_acceptance")
                            .map_err(|_| JobError::Storage)?;
                        self.jobs.op(tx, now, JobRequest::AwaitAdmission(row.id))?;
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
                tx.execute_batch("RELEASE model_acceptance")
                    .map_err(|_| JobError::Storage)?;
            }
            Ok(Some(*row))
        });
        let result = match result {
            Ok(result) => result,
            Err(JobError::Execution(
                code @ (DiagnosticCode::SpendBudgetExhausted
                | DiagnosticCode::AttemptBudgetExhausted
                | DiagnosticCode::InvocationCompleted),
            )) => {
                model::ModelJob::refuse(self, code)?;
                return Ok(None);
            }
            Err(JobError::Execution(code)) => return Err(ModelError::Queue(code)),
            Err(error) => return Err(storage(error)),
        };
        let payload = result
            .as_ref()
            .map(|row| {
                let (_, _, request) =
                    input(row.payload.as_deref().ok_or(JobError::InvalidRequest)?)?;
                serde_json::to_vec(&request).map_err(|_| JobError::Storage)
            })
            .transpose()
            .map_err(storage)?;
        *self.claim.lock().map_err(storage)? = result
            .map(|row| {
                Ok(Claim {
                    id: row.id,
                    generation: row.generation,
                    receipt: row.receipt.ok_or_else(|| storage(()))?,
                    max_attempts: row.max_attempts,
                })
            })
            .transpose()?;
        if payload.is_none() {
            self.finished.store(true, Ordering::SeqCst);
        }
        Ok(payload)
    }
    fn admit_request(&self, credential_fingerprint: Option<String>) -> Result<(), ModelError> {
        let Some(admission) = &self.jobs.admission else {
            return Ok(());
        };
        let claim = self.claim_record()?;
        self.jobs
            .transaction(|tx, now| {
                // Recovery can preserve the generation while clearing the lease.
                // Renew and check cancellation atomically with request binding.
                match self.jobs.op(
                    tx,
                    now,
                    JobRequest::Heartbeat {
                        job: claim.id.clone(),
                        generation: claim.generation,
                    },
                )? {
                    JobResponse::Heartbeat(false) => {}
                    JobResponse::Heartbeat(true) => {
                        return Err(JobError::Execution(DiagnosticCode::InvocationCompleted));
                    }
                    _ => return Err(JobError::InvalidRequest),
                }
                let JobResponse::Job(Some(row)) =
                    self.jobs.op(tx, now, JobRequest::Get(claim.id.clone()))?
                else {
                    return Err(JobError::NotFound);
                };
                admission.before_send(tx, &row, credential_fingerprint)
            })
            .map_err(|error| match error {
                JobError::Execution(code) => ModelError::InvalidRequest(code),
                other => storage(other),
            })
    }
    fn heartbeat(&self) -> Result<bool, ModelError> {
        let (job, generation) = {
            let claim = self.claim.lock().map_err(storage)?;
            let claim = claim.as_ref().ok_or_else(|| storage(()))?;
            (claim.id.clone(), claim.generation)
        };
        self.jobs
            .transaction(|tx, now| {
                match self
                    .jobs
                    .op(tx, now, JobRequest::Heartbeat { job, generation })?
                {
                    JobResponse::Heartbeat(cancel) => Ok(cancel),
                    _ => Err(JobError::InvalidRequest),
                }
            })
            .map_err(|error| {
                if error == JobError::StaleClaim {
                    ModelError::Queue(DiagnosticCode::QueueFailure)
                } else {
                    storage(error)
                }
            })
    }
    fn finish(
        &self,
        state: SpendState,
        usage: Option<UsageTrace>,
        mut output: Option<serde_json::Value>,
        mut failure: Option<DiagnosticCode>,
        retry: bool,
    ) -> Result<(), ModelError> {
        let row = self.claim_record()?;
        let oversized = output
            .as_ref()
            .filter(|_| self.answer_recovery != crate::AnswerRecovery::Off)
            .map(serde_json::to_vec)
            .transpose()
            .map_err(storage)?
            .is_some_and(|bytes| bytes.len() > self.jobs.config.max_result_bytes);
        let evidence = output
            .as_ref()
            .map(|_| serde_json::json!({"output_received": true}));
        let answer_not_retained =
            evidence.is_some() && self.answer_recovery == crate::AnswerRecovery::Off;
        if answer_not_retained {
            output = None;
        }
        if oversized && !answer_not_retained {
            output = None;
            failure = Some(DiagnosticCode::QueueResultTooLarge);
        }
        let reference = crate::SpendReceiptRef::new(&row.receipt)?;
        self.jobs
            .transaction(|tx, now| {
                let JobResponse::Job(Some(current)) =
                    self.jobs.op(tx, now, JobRequest::Get(row.id.clone()))?
                else {
                    return Err(JobError::NotFound);
                };
                if current.generation != row.generation {
                    return Err(JobError::StaleClaim);
                }
                spend::SqliteSpendLedger::save_recovery_in(
                    tx,
                    &reference,
                    &output,
                    self.jobs.ledger.retention(),
                    current.recovery_until,
                    Some(&self.jobs.invocation_key(&current.key)?),
                    now,
                )
                .map_err(|_| JobError::Storage)?;
                spend::SqliteSpendLedger::finish_in(tx, &reference, state, usage, evidence)
                    .map_err(|_| JobError::Storage)?;
                if let Some(admission) = &self.jobs.admission {
                    admission.finish(tx, &current)?;
                }
                if output.is_none() && current.cancel_requested && !current.purged {
                    self.jobs.op(
                        tx,
                        now,
                        JobRequest::Complete {
                            job: current.id.clone(),
                            generation: current.generation,
                            state: JobState::Cancelled,
                            origin: ResultOrigin::Paid,
                            output: None,
                            receipt: Some(reference.as_str().into()),
                            diagnostic: failure,
                        },
                    )?;
                    return Ok(());
                }
                let resolution = if answer_not_retained {
                    JobResolution::PaidResult {
                        receipt: reference.as_str().into(),
                        recovery_until: Some(now),
                    }
                } else if output.is_some() {
                    let receipt = spend::receipt_in(tx, &reference, now)
                        .map_err(|_| JobError::Storage)?
                        .ok_or(JobError::Storage)?;
                    paid_resolution(
                        tx,
                        &receipt,
                        &self.jobs.invocation_key(&current.key)?,
                        now,
                        self.jobs.config.max_result_bytes,
                    )?
                } else if state == SpendState::Released && retry {
                    JobResolution::KnownZeroCharge {
                        receipt: reference.as_str().into(),
                    }
                } else if state == SpendState::Unknown && !oversized {
                    JobResolution::Uncertain {
                        receipt: reference.as_str().into(),
                    }
                } else {
                    JobResolution::Failed {
                        receipt: reference.as_str().into(),
                        diagnostic: failure.ok_or(JobError::InvalidRequest)?,
                    }
                };
                self.resolve(tx, &current.id, current.generation, now, resolution)
            })
            .map_err(storage)?;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn release(&self) -> Result<(), ModelError> {
        let row = self.claim_record()?;
        let reference = crate::SpendReceiptRef::new(&row.receipt)?;
        // The shared release owner records durable pre-dispatch evidence.
        self.jobs
            .transaction(|tx, now| {
                spend::SqliteSpendLedger::release_in(tx, &reference)
                    .map_err(|_| JobError::Storage)?;
                self.resolve(
                    tx,
                    &row.id,
                    row.generation,
                    now,
                    JobResolution::KnownZeroCharge {
                        receipt: reference.as_str().into(),
                    },
                )
            })
            .map_err(storage)?;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn refuse(&self, code: DiagnosticCode) -> Result<(), ModelError> {
        self.jobs
            .transaction(|tx, now| {
                self.jobs.op(
                    tx,
                    now,
                    JobRequest::RefusePending {
                        job: self.candidate.clone(),
                        diagnostic: code,
                    },
                )?;
                Ok(())
            })
            .map_err(storage)?;
        self.finished.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn eligible(&self) -> Result<bool, ModelError> {
        let eligible = !*self.stop.borrow()
            && self
                .jobs
                .transaction(|tx, now| {
                    let JobResponse::Job(Some(row)) =
                        self.jobs
                            .op(tx, now, JobRequest::Get(self.candidate.clone()))?
                    else {
                        return Err(JobError::NotFound);
                    };
                    Ok(row.state == JobState::Pending && !row.cancel_requested && !row.purged)
                })
                .map_err(storage)?;
        if !eligible {
            self.finished.store(true, Ordering::SeqCst);
        }
        Ok(eligible)
    }
    fn can_retry(&self, limit: u32) -> Result<bool, ModelError> {
        if self.jobs.admission.is_some() {
            return Ok(false);
        }
        let claim = self.claim_record()?;
        if claim.generation >= u64::from(claim.max_attempts.min(limit)) {
            return Ok(false);
        }
        if self.heartbeat()? {
            return Ok(false);
        }
        self.jobs
            .transaction(|tx, now| {
                let reference =
                    crate::SpendReceiptRef::new(&claim.receipt).map_err(|_| JobError::Storage)?;
                let receipt = spend::receipt_in(tx, &reference, now)
                    .map_err(|_| JobError::Storage)?
                    .ok_or(JobError::Storage)?;
                Ok(receipt
                    .attempt_limit
                    .is_none_or(|limit| receipt.attempts_used < limit))
            })
            .map_err(storage)
    }
    fn attempt(&self) -> Result<u32, ModelError> {
        u32::try_from(self.claim_record()?.generation).map_err(storage)
    }
    fn heartbeat_interval(&self) -> Duration {
        self.heartbeat
    }
}

macro_rules! model_runner {
    ($name:ident, $bind:ident, $call:ident, $provider:ident, $request:ty, $doc:literal) => {
        impl ModelJobs {
            #[doc = $doc]
            pub async fn $name<P>(&self, binding: ModelBinding<P>, config: RunnerConfig, kind: String) -> Result<JobRunner, RunnerError>
            where P: crate::$provider + Clone + 'static {
                let bound = self.runtime.bind(binding.provider.descriptor(), &binding).map_err(|_| JobError::InvalidRequest)?;
                let heartbeat = config.heartbeat_interval_ms.map(Duration::from_millis).unwrap_or(Duration::from_secs(self.config.claim_lease_seconds) / 3);
                if config.version != 1 || config.worker_count == 0 || config.poll_interval_ms == 0 || config.maintenance_interval_ms == 0 || heartbeat.is_zero() || heartbeat > Duration::from_secs(self.config.claim_lease_seconds) / 2 || kind.is_empty() || binding.accepted_spend.is_some() || bound.policy.request_debug_dir.is_some() || bound.sinks.identity.tenant.0 != self.scope.tenant {
                    return Err(JobError::InvalidRequest.into());
                }
                self.transaction(|tx, _| bind_kind(tx, &self.scope, &kind, &bound.sinks.identity, &binding.account_sharing_key))?;
                let jobs = self.clone();
                let maintenance = self.clone();
                let poll = Duration::from_millis(config.poll_interval_ms);
                Ok(JobRunner::start_owned(config.worker_count, move |mut stop| {
                    let jobs = jobs.clone(); let binding = binding.clone(); let kind = kind.clone();
                    async move {
                        loop {
                            if *stop.borrow() { return Ok(()); }
                            jobs.recover(kind.clone()).await?;
                            let query = jobs.clone(); let query_kind = kind.clone();
                            let response = tokio::task::spawn_blocking(move || query.request_sync(JobRequest::Candidates { kinds: vec![query_kind], limit: 1, max_bytes: query.config.max_page_bytes })).await.map_err(|_| RunnerError::WorkerTask)??;
                            let JobResponse::Candidates(rows) = response else { return Err(JobError::InvalidRequest.into()) };
                            if let Some(mut candidate) = rows.into_iter().next() {
                                if candidate.execution != Execution::Model { return Err(JobError::InvalidRequest.into()); }
                                let owner = Arc::new(ClaimOwner { answer_recovery: binding.answer_recovery, jobs: jobs.clone(), candidate: candidate.id.clone(), claim: Mutex::new(None), heartbeat, finished: AtomicBool::new(false), stop: stop.clone() });
                                let request = {
                                    let payload = candidate.payload.take().ok_or(JobError::InvalidRequest)?;
                                    input(&payload).and_then(|(_, _, request)| serde_json::from_value::<$request>(request).map_err(|_| JobError::InvalidRequest))
                                };
                                let result = match request {
                                    Ok(request) => {
                                        let mut binding = binding.clone().with_invocation(jobs.invocation_key(&candidate.key)?);
                                        binding.job_owner = Some(owner.clone());
                                        jobs.runtime.$bind(binding).map_err(|_| JobError::InvalidRequest)?.$call(request).await
                                    }
                                    Err(_) => { model::ModelJob::refuse(owner.as_ref(), DiagnosticCode::InvalidConfiguration).map_err(|_| JobError::Storage)?; continue; }
                                };
                                if let Err(ModelError::InvalidRequest(code)) = &result
                                    && !owner.finished.load(Ordering::SeqCst) {
                                    model::ModelJob::refuse(owner.as_ref(), *code).map_err(|_| JobError::Storage)?;
                                    continue;
                                }
                                if let Err(error) = result
                                    && (!owner.finished.load(Ordering::SeqCst) || !error.diagnostics().is_empty() || matches!(error, ModelError::Queue(_)) && error.code() != DiagnosticCode::InvocationCompleted) {
                                    if error.diagnostics().is_empty() {
                                        return Err(JobError::Execution(error.code()).into());
                                    }
                                    return Err(RunnerError::Workers(std::iter::once(error.code()).chain(error.diagnostics().iter().copied()).map(|code| JobError::Execution(code).into()).collect()));
                                }
                            } else {
                                tokio::select! { _ = stop.changed() => {}, _ = tokio::time::sleep(poll) => {} }
                            }
                        }
                    }
                }, move |rx| maintenance.maintain(rx, Duration::from_millis(config.maintenance_interval_ms))))
            }
        }
    };
}
model_runner!(
    start_chat,
    chat,
    chat,
    ChatProvider,
    crate::ChatRequest,
    "Start chat workers without caller resubmission."
);
model_runner!(
    start_embedding,
    embedding,
    embed,
    EmbeddingProvider,
    crate::EmbeddingRequest,
    "Start embedding workers without caller resubmission."
);
model_runner!(
    start_rerank,
    rerank,
    rerank,
    RerankProvider,
    crate::RerankRequest,
    "Start reranking workers without caller resubmission."
);
model_runner!(
    start_classifier,
    classifier,
    classify,
    ClassifierProvider,
    crate::ClassifyRequest,
    "Start classifier workers without caller resubmission."
);

#[cfg(test)]
mod review_tests {
    use super::*;

    #[tokio::test]
    async fn request_admission_rejects_invalidated_claims_before_binding() {
        struct Admitted(std::sync::atomic::AtomicUsize);
        impl ModelJobAdmission for Admitted {
            fn enqueue(&self, _: &JobSpec) -> Result<(), JobError> {
                Ok(())
            }
            fn admit(&self, _: &JobRecord, _: &[u8]) -> Result<bool, JobError> {
                Ok(true)
            }
            fn claim(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: chrono::DateTime<Utc>,
            ) -> Result<bool, JobError> {
                Ok(true)
            }
            fn accept(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: &SpendReservation,
            ) -> Result<(), JobError> {
                Ok(())
            }
            fn before_send(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: Option<String>,
            ) -> Result<(), JobError> {
                self.0.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }
        let dir = tempfile::tempdir().expect("directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("permissions");
        }
        let runtime = Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .expect("runtime");
        let admission = Arc::new(Admitted(std::sync::atomic::AtomicUsize::new(0)));
        let jobs = runtime
            .model_jobs(
                JobScope {
                    tenant: "tenant".into(),
                    incarnation: "1".into(),
                    queue: "q".into(),
                },
                JobConfig::default(),
            )
            .expect("jobs")
            .with_admission(admission.clone());
        let binding = ModelBinding::new(model::StaticChatProvider::new("response"))
            .with_identity(crate::BindingIdentity::new("tenant", "p", "1", "a"));
        for case in [
            "live",
            "expired",
            "recovered",
            "reconciled",
            "cancelled",
            "purged",
        ] {
            let spec = JobSpec {
                key: case.into(),
                group: None,
                owners: vec![case.into()],
                kind: "chat".into(),
                execution: Execution::Model,
                payload: model_job_payload(&binding, &serde_json::json!({"input": case}))
                    .expect("payload"),
                admission: Some(b"signed".to_vec()),
                limits: JobLimits { max_attempts: 3 },
                recovery_until: None,
            };
            let JobResponse::Enqueued(rows) = jobs
                .request(JobRequest::Enqueue(vec![spec]))
                .await
                .expect("enqueue")
            else {
                panic!("enqueued")
            };
            let Enqueued::Inserted(id) = &rows[0] else {
                panic!("inserted")
            };
            let (_stop, stop) = watch::channel(false);
            let owner = ClaimOwner {
                answer_recovery: crate::AnswerRecovery::default(),
                jobs: jobs.clone(),
                candidate: id.clone(),
                claim: Mutex::new(None),
                heartbeat: Duration::from_millis(10),
                finished: AtomicBool::new(false),
                stop,
            };
            let reservation = SpendReservation {
                reference: crate::SpendReceiptRef::new(case).expect("reference"),
                account: "account".into(),
                invocation: case.into(),
                binding: "binding".into(),
                request_limit: None,
            };
            assert!(
                model::ModelJob::claim(&owner, &reservation, 3)
                    .expect("claim")
                    .is_some()
            );
            if matches!(case, "expired" | "recovered" | "reconciled") {
                jobs.transaction(|tx, now| {
                    // Expire without a wall-clock wait; recovery still uses the real owner.
                    tx.execute(
                        "UPDATE jobs SET lease_until=?1 WHERE id=?2",
                        params![
                            (now - chrono::Duration::seconds(1)).timestamp_millis(),
                            id.id
                        ],
                    )
                    .map_err(|_| JobError::Storage)?;
                    Ok(())
                })
                .expect("expire");
            }
            if matches!(case, "recovered" | "reconciled") {
                jobs.recover("chat".into()).await.expect("recover");
                let JobResponse::Job(Some(row)) = jobs
                    .request(JobRequest::Get(id.clone()))
                    .await
                    .expect("recovered row")
                else {
                    panic!("row")
                };
                assert_eq!(row.state, JobState::Uncertain);
                assert_eq!(
                    row.generation,
                    owner.claim_record().expect("claim").generation
                );
                assert!(row.lease_until.is_none());
            }
            if case == "reconciled" {
                jobs.transaction(|tx, _| {
                    spend::SqliteSpendLedger::finish_in(
                        tx,
                        &reservation.reference,
                        SpendState::Released,
                        None,
                        None,
                    )
                    .map_err(|_| JobError::Storage)
                })
                .expect("reconcile zero charge");
            }
            if case == "cancelled" {
                jobs.request(JobRequest::Cancel(Selector::Ids(vec![id.clone()])))
                    .await
                    .expect("cancel");
            }
            if case == "purged" {
                jobs.request(JobRequest::PurgeOwner(case.into()))
                    .await
                    .expect("purge");
            }
            let bound_before = admission.0.load(Ordering::SeqCst);
            let result = model::ModelJob::admit_request(&owner, None);
            if case == "live" {
                result.expect("live claim admitted");
                assert_eq!(admission.0.load(Ordering::SeqCst), bound_before + 1);
            } else {
                let error = result.expect_err(case);
                assert_eq!(
                    error.code(),
                    if matches!(case, "cancelled" | "purged") {
                        DiagnosticCode::InvocationCompleted
                    } else {
                        DiagnosticCode::SpendLedgerUnavailable
                    },
                    "{case}"
                );
                assert_eq!(admission.0.load(Ordering::SeqCst), bound_before, "{case}");
            }
            let receipt = spend::receipt_in(
                &jobs.ledger.0.lock().expect("ledger"),
                &reservation.reference,
                chrono::Utc::now(),
            )
            .expect("receipt")
            .expect("present");
            assert_eq!(
                receipt.state,
                if case == "reconciled" {
                    SpendState::Released
                } else {
                    SpendState::Unknown
                },
                "{case}"
            );
        }
    }

    #[tokio::test]
    async fn final_acceptance_expiry_rolls_back_and_resumes_the_same_runner() {
        struct Expiring(AtomicBool);
        impl ModelJobAdmission for Expiring {
            fn enqueue(&self, _: &JobSpec) -> Result<(), JobError> {
                Ok(())
            }
            fn admit(&self, _: &JobRecord, _: &[u8]) -> Result<bool, JobError> {
                self.0.store(true, Ordering::SeqCst);
                Ok(true)
            }
            fn claim(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: chrono::DateTime<Utc>,
            ) -> Result<bool, JobError> {
                Ok(true)
            }
            fn accept(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: &SpendReservation,
            ) -> Result<(), JobError> {
                if self.0.load(Ordering::SeqCst) {
                    Ok(())
                } else {
                    Err(JobError::AuthorityExpired)
                }
            }
        }
        let dir = tempfile::tempdir().expect("directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("permissions");
        }
        let runtime = Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .expect("runtime");
        let jobs = runtime
            .model_jobs(
                JobScope {
                    tenant: "tenant".into(),
                    incarnation: "1".into(),
                    queue: "q".into(),
                },
                JobConfig::default(),
            )
            .expect("jobs")
            .with_admission(Arc::new(Expiring(AtomicBool::new(false))));
        let binding = ModelBinding::new(model::StaticChatProvider::new("renewed answer"))
            .with_identity(crate::BindingIdentity::new("tenant", "p", "1", "a"))
            .with_policy(crate::ModelQueueConfig::default());
        let request = crate::ChatRequest {
            messages: vec![],
            max_output_tokens: None,
            temperature: None,
            response_format: None,
            role_binding: None,
            source: None,
            metadata: serde_json::Value::Null,
        };
        let spec = JobSpec {
            key: "expiry".into(),
            kind: "chat".into(),
            group: None,
            owners: vec![],
            execution: Execution::Model,
            payload: model_job_payload(&binding, &request).expect("payload"),
            admission: Some(b"first".to_vec()),
            limits: JobLimits { max_attempts: 1 },
            recovery_until: None,
        };
        let JobResponse::Enqueued(items) = jobs
            .request(JobRequest::Enqueue(vec![spec]))
            .await
            .expect("enqueue")
        else {
            panic!("enqueued")
        };
        let Enqueued::Inserted(id) = &items[0] else {
            panic!("inserted")
        };
        let runner = jobs
            .start_chat(
                binding,
                RunnerConfig {
                    poll_interval_ms: 1,
                    ..Default::default()
                },
                "chat".into(),
            )
            .await
            .expect("runner");
        let row = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let JobResponse::Job(Some(row)) = jobs
                    .request(JobRequest::Get(id.clone()))
                    .await
                    .expect("row")
                else {
                    panic!("row")
                };
                assert!(!runner.is_finished(), "expiry must not stop the worker");
                if row.state == JobState::AwaitingAdmission {
                    break row;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("await admission");
        assert_eq!(row.generation, 0);
        assert!(row.receipt.is_none());
        assert_eq!(row.admission.as_deref(), Some(b"first".as_slice()));
        {
            let conn = jobs.ledger.0.lock().expect("ledger");
            assert_eq!(
                conn.query_row("SELECT count(*) FROM spend_receipts", [], |r| r
                    .get::<_, usize>(0))
                    .expect("reservations"),
                0
            );
        }
        jobs.request(JobRequest::Admit {
            job: id.clone(),
            admission: b"renewed".to_vec(),
        })
        .await
        .expect("renewal");
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let JobResponse::Job(Some(row)) = jobs
                    .request(JobRequest::Get(id.clone()))
                    .await
                    .expect("row")
                else {
                    panic!("row")
                };
                assert!(!runner.is_finished());
                if row.state == JobState::Succeeded {
                    assert_eq!(row.generation, 1);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("same worker resumes");
        runner.shutdown().await.expect("healthy runner");
    }

    #[tokio::test]
    async fn trial_purge_discovers_receipts_without_reading_saved_answers() {
        use rusqlite::hooks::{AuthAction, AuthContext, Authorization};

        let dir = tempfile::tempdir().expect("temp directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("permissions");
        }
        let runtime = Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .expect("runtime");
        let jobs = runtime
            .model_jobs(
                JobScope {
                    tenant: "tenant".into(),
                    incarnation: "1".into(),
                    queue: "q".into(),
                },
                JobConfig::default(),
            )
            .expect("jobs");
        let binding = ModelBinding::new(model::StaticChatProvider::new("paid answer"))
            .with_identity(crate::BindingIdentity::new("tenant", "p", "1", "a"))
            .with_policy(crate::ModelQueueConfig::default());
        let request = crate::ChatRequest {
            messages: vec![],
            max_output_tokens: None,
            temperature: None,
            response_format: None,
            role_binding: None,
            source: None,
            metadata: serde_json::json!({}),
        };
        let spec = JobSpec {
            key: "key".into(),
            group: None,
            owners: vec!["owner".into()],
            kind: "chat".into(),
            execution: Execution::Model,
            payload: model_job_payload(&binding, &request).expect("payload"),
            admission: None,
            limits: JobLimits { max_attempts: 1 },
            recovery_until: None,
        };
        runtime
            .execute_chat(binding, &jobs.invocation_key("key").expect("key"), request)
            .await
            .expect("paid answer");
        let JobResponse::Enqueued(rows) = jobs
            .request_sync(JobRequest::Enqueue(vec![spec]))
            .expect("enqueue")
        else {
            panic!("enqueue response")
        };
        let Enqueued::Inserted(id) = &rows[0] else {
            panic!("inserted")
        };
        jobs.transaction(|tx, now| {
            let reference: String = tx
                .query_row(
                    "SELECT reference FROM spend_receipts WHERE recovery IS NOT NULL",
                    [],
                    |r| r.get(0),
                )
                .expect("saved answer");
            jobs.op(
                tx,
                now,
                JobRequest::Resolve {
                    job: id.clone(),
                    generation: 0,
                    resolution: JobResolution::PaidResult {
                        receipt: reference,
                        recovery_until: None,
                    },
                },
            )?;
            Ok(())
        })
        .expect("completed paid job with receipt");
        jobs.ledger
            .0
            .lock()
            .expect("ledger")
            .authorizer(Some(|ctx: AuthContext<'_>| match ctx.action {
                AuthAction::Read {
                    table_name: "spend_receipts",
                    column_name: "recovery" | "output",
                    ..
                } => Authorization::Deny,
                _ => Authorization::Allow,
            }));
        let result = jobs.request_sync(JobRequest::PurgeOwner("owner".into()));
        let conn = jobs.ledger.0.lock().expect("ledger");
        conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>);
        assert!(matches!(
            result.expect("purge without answer reads"),
            JobResponse::Changed(1)
        ));
        assert_eq!(conn.query_row("SELECT count(*) FROM spend_receipts WHERE recovery IS NOT NULL OR recovery_expires_at IS NOT NULL", [], |r| r.get::<_, usize>(0)).expect("erased recovery"), 0);
    }

    #[tokio::test]
    async fn execution_lookup_ignores_retained_history_and_waiting_admission() {
        use std::sync::atomic::AtomicUsize;

        let dir = tempfile::tempdir().expect("directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("permissions");
        }
        let runtime = Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .expect("runtime");
        let scope = JobScope {
            tenant: "tenant".into(),
            incarnation: "1".into(),
            queue: "q".into(),
        };
        let jobs = runtime
            .model_jobs(scope.clone(), JobConfig::default())
            .expect("jobs");
        let steps = Arc::new(AtomicUsize::new(0));
        let mut measurements = Vec::new();
        let mut previous = 0;
        for retained in [10, 10_000] {
            {
                let mut conn = jobs.ledger.0.lock().expect("ledger");
                let tx = conn.transaction().expect("transaction");
                {
                    let scope = serde_json::to_string(&scope).expect("scope");
                    let mut insert = tx.prepare(
                        "INSERT INTO jobs(scope,id,key,digest,kind,state,delivery_generation) VALUES (?1,?2,?2,'digest','chat','\"Accepted\"',1)",
                    ).expect("prepare tombstones");
                    for id in previous..retained {
                        insert
                            .execute(params![scope, id.to_string()])
                            .expect("tombstone");
                    }
                }
                tx.commit().expect("commit");
                let steps = steps.clone();
                conn.progress_handler(
                    1,
                    Some(move || {
                        steps.fetch_add(1, Ordering::Relaxed);
                        false
                    }),
                );
            }
            assert!(!jobs.needs_execution("chat".into()).await.expect("lookup"));
            jobs.ledger
                .0
                .lock()
                .expect("ledger")
                .progress_handler(0, None::<fn() -> bool>);
            measurements.push(steps.swap(0, Ordering::Relaxed));
            previous = retained;
        }
        assert_eq!(
            measurements[0], measurements[1],
            "retained history must not affect lookup work: {measurements:?}"
        );
        eprintln!("execution lookup VM steps at 10 vs 10,000 tombstones: {measurements:?}");

        // An existence lookup must not decode payloads or other job content.
        for state in [
            JobState::AwaitingAdmission,
            JobState::Pending,
            JobState::Running,
            JobState::Uncertain,
            JobState::Succeeded,
            JobState::Failed,
        ] {
            jobs.ledger
                .0
                .lock()
                .expect("ledger")
                .execute(
                    "UPDATE jobs SET state=?1, payload=42 WHERE id='0'",
                    [serde_json::to_string(&state).expect("state")],
                )
                .expect("state");
            assert_eq!(
                jobs.needs_execution("chat".into()).await.expect("lookup"),
                matches!(
                    state,
                    JobState::Pending | JobState::Running | JobState::Uncertain
                ),
                "{state:?}"
            );
        }
        jobs.ledger
            .0
            .lock()
            .expect("ledger")
            .execute("UPDATE jobs SET state='\"Pending\"' WHERE id='0'", [])
            .expect("pending");
        assert!(
            !jobs
                .needs_execution("embedding".into())
                .await
                .expect("other kind")
        );
        let other = runtime
            .model_jobs(
                JobScope {
                    queue: "other".into(),
                    ..scope
                },
                JobConfig::default(),
            )
            .expect("other scope");
        assert!(
            !other
                .needs_execution("chat".into())
                .await
                .expect("other scope lookup")
        );
    }

    #[tokio::test]
    async fn review_17_claim_retains_only_metadata_for_heartbeats() {
        let dir = tempfile::tempdir().expect("temp directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("permissions");
        }
        let runtime = Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .expect("runtime");
        let jobs = runtime
            .model_jobs(
                JobScope {
                    tenant: "tenant".into(),
                    incarnation: "1".into(),
                    queue: "q".into(),
                },
                JobConfig::default(),
            )
            .expect("jobs");
        let binding = ModelBinding::new(model::StaticChatProvider::new("response"))
            .with_identity(crate::BindingIdentity::new("tenant", "p", "1", "a"));
        let spec = JobSpec {
            key: "key".into(),
            group: None,
            owners: vec![],
            kind: "chat".into(),
            execution: Execution::Model,
            payload: model_job_payload(&binding, &serde_json::json!({"data": "x".repeat(65536)}))
                .expect("payload"),
            admission: None,
            limits: JobLimits { max_attempts: 1 },
            recovery_until: None,
        };
        let JobResponse::Enqueued(rows) = jobs
            .request(JobRequest::Enqueue(vec![spec]))
            .await
            .expect("enqueue")
        else {
            panic!("enqueue response")
        };
        let Enqueued::Inserted(id) = &rows[0] else {
            panic!("inserted")
        };
        let JobResponse::Job(Some(candidate)) = jobs
            .request(JobRequest::Get(id.clone()))
            .await
            .expect("row")
        else {
            panic!("row response")
        };
        let (_stop, stop) = watch::channel(false);
        let owner = ClaimOwner {
            answer_recovery: crate::AnswerRecovery::default(),
            jobs,
            candidate: candidate.id.clone(),
            claim: Mutex::new(None),
            heartbeat: Duration::from_millis(10),
            finished: AtomicBool::new(false),
            stop,
        };
        let reservation = SpendReservation {
            reference: crate::SpendReceiptRef::new("metadata-test").expect("reference"),
            account: "account".into(),
            invocation: "invocation".into(),
            binding: "binding".into(),
            request_limit: None,
        };
        assert!(
            model::ModelJob::claim(&owner, &reservation, 1)
                .expect("claim")
                .is_some()
        );
        assert!(
            std::mem::size_of_val(&owner.claim_record().expect("metadata"))
                < std::mem::size_of::<JobRecord>()
        );
        assert!(!model::ModelJob::heartbeat(&owner).expect("heartbeat"));
    }

    #[tokio::test]
    async fn signed_claims_use_frozen_job_ceiling_across_successor_admissions() {
        struct Admitted;
        impl ModelJobAdmission for Admitted {
            fn accept(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: &SpendReservation,
            ) -> Result<(), JobError> {
                Ok(())
            }
            fn enqueue(&self, _: &JobSpec) -> Result<(), JobError> {
                Ok(())
            }
            fn admit(&self, _: &JobRecord, _: &[u8]) -> Result<bool, JobError> {
                Ok(true)
            }
            fn claim(
                &self,
                _: &Transaction<'_>,
                _: &JobRecord,
                _: chrono::DateTime<Utc>,
            ) -> Result<bool, JobError> {
                Ok(true)
            }
        }
        let dir = tempfile::tempdir().expect("directory");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
                .expect("permissions");
        }
        let runtime = Runtime::open(crate::RuntimeConfig {
            state_dir: Some(dir.path().into()),
            ..Default::default()
        })
        .expect("runtime");
        let jobs = runtime
            .model_jobs(
                JobScope {
                    tenant: "tenant".into(),
                    incarnation: "1".into(),
                    queue: "q".into(),
                },
                JobConfig::default(),
            )
            .expect("jobs")
            .with_admission(Arc::new(Admitted));
        let binding = ModelBinding::new(model::StaticChatProvider::new("response"))
            .with_identity(crate::BindingIdentity::new("tenant", "p", "1", "a"));
        let spec = JobSpec {
            key: "key".into(),
            group: None,
            owners: vec![],
            kind: "chat".into(),
            execution: Execution::Model,
            payload: model_job_payload(&binding, &serde_json::json!({"input": "x"}))
                .expect("payload"),
            admission: Some(b"first".to_vec()),
            limits: JobLimits { max_attempts: 3 },
            recovery_until: None,
        };
        let JobResponse::Enqueued(rows) = jobs
            .request(JobRequest::Enqueue(vec![spec]))
            .await
            .expect("enqueue")
        else {
            panic!("enqueue response")
        };
        let Enqueued::Inserted(id) = &rows[0] else {
            panic!("inserted")
        };
        let (_stop, stop) = watch::channel(false);
        let owner = ClaimOwner {
            answer_recovery: crate::AnswerRecovery::default(),
            jobs: jobs.clone(),
            candidate: id.clone(),
            claim: Mutex::new(None),
            heartbeat: Duration::from_millis(10),
            finished: AtomicBool::new(false),
            stop,
        };
        let mut reservation = SpendReservation {
            reference: crate::SpendReceiptRef::new("first").expect("reference"),
            account: "account".into(),
            invocation: "invocation".into(),
            binding: "binding".into(),
            request_limit: None,
        };
        assert!(
            model::ModelJob::claim(&owner, &reservation, 1)
                .expect("first claim")
                .is_some()
        );
        model::ModelJob::finish(
            &owner,
            SpendState::Released,
            None,
            None,
            Some(DiagnosticCode::AuthenticationRejected),
            true,
        )
        .expect("trusted zero charge");
        let JobResponse::Job(Some(row)) = jobs
            .request(JobRequest::Get(id.clone()))
            .await
            .expect("row")
        else {
            panic!("row")
        };
        assert_eq!(row.state, JobState::AwaitingAdmission);
        jobs.request(JobRequest::Admit {
            job: id.clone(),
            admission: b"successor".to_vec(),
        })
        .await
        .expect("successor");
        reservation.reference = crate::SpendReceiptRef::new("second").expect("reference");
        assert!(
            model::ModelJob::claim(&owner, &reservation, 1)
                .expect("successor claim")
                .is_some()
        );
        let receipt = spend::receipt_in(
            &jobs.ledger.0.lock().expect("ledger"),
            &reservation.reference,
            chrono::Utc::now(),
        )
        .expect("receipt")
        .expect("present");
        assert_eq!(receipt.attempt_limit, Some(3));
        assert_eq!(receipt.attempts_used, 2);
    }
}
